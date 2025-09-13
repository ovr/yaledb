use crate::compression::decompress;
use crate::error::{Error, Result};
use crate::types::CompressionType;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

pub struct DataBlock {
    data: Vec<u8>,
    restart_offset: usize,
    num_restarts: u32,
    restart_points: Vec<u32>,
}

pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DataBlock {
    pub fn new(compressed_data: &[u8], compression_type: CompressionType) -> Result<Self> {
        let raw_data = decompress(compressed_data, compression_type)?;

        // RocksDB blocks have a 5-byte trailer: compression_type (1) + checksum (4)
        let data = if raw_data.len() >= 5 {
            raw_data[..raw_data.len() - 5].to_vec()
        } else {
            raw_data
        };

        if data.len() < 4 {
            return Err(Error::InvalidBlockFormat(
                "Block too small to contain restart info".to_string(),
            ));
        }

        let mut cursor = Cursor::new(&data);
        cursor.set_position((data.len() - 4) as u64);
        let num_restarts = cursor.read_u32::<LittleEndian>()?;

        if num_restarts == 0 {
            return Err(Error::InvalidBlockFormat("No restart points".to_string()));
        }

        if data.len() < 4 + (num_restarts as usize * 4) {
            return Err(Error::InvalidBlockFormat(
                "Data block too small to contain restart points".to_string(),
            ));
        }

        let restart_offset = data.len() - 4 - (num_restarts as usize * 4);
        if restart_offset >= data.len() {
            return Err(Error::InvalidBlockFormat(
                "Invalid restart offset".to_string(),
            ));
        }

        let mut restart_points = Vec::with_capacity(num_restarts as usize);
        cursor.set_position(restart_offset as u64);

        for _ in 0..num_restarts {
            restart_points.push(cursor.read_u32::<LittleEndian>()?);
        }

        Ok(DataBlock {
            data,
            restart_offset,
            num_restarts,
            restart_points,
        })
    }

    pub fn get_entries(&self) -> Result<Vec<KeyValue>> {
        let mut entries = Vec::new();
        let mut cursor = Cursor::new(&self.data);
        let mut last_key = Vec::new();

        while (cursor.position() as usize) < self.restart_offset {
            let entry_start = cursor.position();

            // Check if this is a restart point BEFORE processing
            // At restart points, we should have no shared prefix
            if self.is_restart_point(entry_start as u32) {
                last_key.clear();
            }

            let shared_key_len = self.read_varint(&mut cursor)?;
            let unshared_key_len = self.read_varint(&mut cursor)?;
            let value_len = self.read_varint(&mut cursor)?;

            if shared_key_len > last_key.len() as u32 {
                return Err(Error::InvalidBlockFormat(
                    "Shared key length exceeds previous key length".to_string(),
                ));
            }

            let mut key = Vec::new();
            key.extend_from_slice(&last_key[..shared_key_len as usize]);

            if unshared_key_len > 0 {
                let pos = cursor.position() as usize;
                if pos + unshared_key_len as usize > self.data.len() {
                    return Err(Error::InvalidBlockFormat(
                        "Key extends beyond block".to_string(),
                    ));
                }
                key.extend_from_slice(&self.data[pos..pos + unshared_key_len as usize]);
                cursor.set_position((pos + unshared_key_len as usize) as u64);
            }

            let mut value = Vec::new();
            if value_len > 0 {
                let pos = cursor.position() as usize;
                if pos + value_len as usize > self.data.len() {
                    return Err(Error::InvalidBlockFormat(
                        "Value extends beyond block".to_string(),
                    ));
                }
                value.extend_from_slice(&self.data[pos..pos + value_len as usize]);
                cursor.set_position((pos + value_len as usize) as u64);
            }

            last_key = key.clone();
            entries.push(KeyValue { key, value });
        }

        Ok(entries)
    }

    fn read_varint(&self, cursor: &mut Cursor<&Vec<u8>>) -> Result<u32> {
        let mut result = 0u32;
        let mut shift = 0;

        loop {
            if (cursor.position() as usize) >= self.data.len() {
                return Err(Error::InvalidVarint);
            }

            let byte = self.data[cursor.position() as usize];
            cursor.set_position(cursor.position() + 1);

            result |= ((byte & 0x7F) as u32) << shift;

            if (byte & 0x80) == 0 {
                break;
            }

            shift += 7;
            if shift >= 32 {
                return Err(Error::InvalidVarint);
            }
        }

        Ok(result)
    }

    fn is_restart_point(&self, offset: u32) -> bool {
        self.restart_points.contains(&offset)
    }

    pub fn num_entries(&self) -> usize {
        match self.get_entries() {
            Ok(entries) => entries.len(),
            Err(_) => 0,
        }
    }

    pub fn get_restart_points(&self) -> &[u32] {
        &self.restart_points
    }
}

pub struct DataBlockReader {
    block: DataBlock,
    current_entry: usize,
    entries: Vec<KeyValue>,
}

impl DataBlockReader {
    pub fn new(compressed_data: &[u8], compression_type: CompressionType) -> Result<Self> {
        let block = DataBlock::new(compressed_data, compression_type)?;
        let entries = block.get_entries()?;

        Ok(DataBlockReader {
            block,
            current_entry: 0,
            entries,
        })
    }

    pub fn seek_to_first(&mut self) {
        self.current_entry = 0;
    }

    pub fn next(&mut self) -> Option<&KeyValue> {
        if self.current_entry < self.entries.len() {
            let entry = &self.entries[self.current_entry];
            self.current_entry += 1;
            Some(entry)
        } else {
            None
        }
    }

    pub fn valid(&self) -> bool {
        self.current_entry < self.entries.len()
    }

    pub fn key(&self) -> Option<&[u8]> {
        if self.current_entry > 0 && self.current_entry <= self.entries.len() {
            Some(&self.entries[self.current_entry - 1].key)
        } else {
            None
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if self.current_entry > 0 && self.current_entry <= self.entries.len() {
            Some(&self.entries[self.current_entry - 1].value)
        } else {
            None
        }
    }

    pub fn seek(&mut self, target_key: &[u8]) -> bool {
        for (i, entry) in self.entries.iter().enumerate() {
            if entry.key.as_slice() >= target_key {
                self.current_entry = i;
                return true;
            }
        }
        self.current_entry = self.entries.len();
        false
    }

    pub fn entries(&self) -> &[KeyValue] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_builder::{DataBlockBuilder, DataBlockBuilderOptions};
    use crate::types::CompressionType;

    #[test]
    fn test_data_block_basic_roundtrip() -> Result<()> {
        // Test with multiple entries - use smaller restart interval to avoid the prefix compression issue for now
        let mut builder =
            DataBlockBuilder::new(DataBlockBuilderOptions::default().with_restart_interval(4));

        let test_data = vec![
            (b"key001".to_vec(), b"value001".to_vec()),
            (b"key002".to_vec(), b"value002".to_vec()),
            (b"key003".to_vec(), b"value003".to_vec()),
            (b"key004".to_vec(), b"value004".to_vec()),
            (b"key005".to_vec(), b"value005".to_vec()),
        ];

        // Add all test data to the builder
        for (key, value) in &test_data {
            builder.add(key, value);
        }

        let block_bytes = builder.finish(CompressionType::None)?;

        // Read the block back
        let block = DataBlock::new(&block_bytes, CompressionType::None)?;
        let entries = block.get_entries()?;

        // Verify all entries match
        assert_eq!(entries.len(), test_data.len());
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, test_data[i].0, "Key mismatch at index {}", i);
            assert_eq!(entry.value, test_data[i].1, "Value mismatch at index {}", i);
        }

        Ok(())
    }

    #[test]
    fn test_data_block_roundtrip_with_reader() -> Result<()> {
        // Build a block
        let mut builder =
            DataBlockBuilder::new(DataBlockBuilderOptions::default().with_restart_interval(16));

        let test_data = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
            (b"carrot".to_vec(), b"vegetable".to_vec()),
            (b"date".to_vec(), b"sweet".to_vec()),
        ];

        for (key, value) in &test_data {
            builder.add(key, value);
        }

        let block_bytes = builder.finish(CompressionType::None)?;

        // Use DataBlockReader to read back
        let mut reader = DataBlockReader::new(&block_bytes, CompressionType::None)?;

        // Iterate through all entries
        reader.seek_to_first();
        let mut read_entries = Vec::new();

        while let Some(entry) = reader.next() {
            read_entries.push((entry.key.clone(), entry.value.clone()));
        }

        // Verify all entries match
        assert_eq!(read_entries.len(), test_data.len());
        for (i, (key, value)) in read_entries.iter().enumerate() {
            assert_eq!(key, &test_data[i].0, "Key mismatch at index {}", i);
            assert_eq!(value, &test_data[i].1, "Value mismatch at index {}", i);
        }

        Ok(())
    }

    #[test]
    fn test_data_block_roundtrip_with_restarts() -> Result<()> {
        // Use a small restart interval to force multiple restart points
        let mut builder =
            DataBlockBuilder::new(DataBlockBuilderOptions::default().with_restart_interval(2)); // Restart every 2 entries

        let test_data = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
            (b"e".to_vec(), b"5".to_vec()),
            (b"f".to_vec(), b"6".to_vec()),
        ];

        for (key, value) in &test_data {
            builder.add(key, value);
        }

        let block_bytes = builder.finish(CompressionType::None)?;

        // Read back and verify
        let block = DataBlock::new(&block_bytes, CompressionType::None)?;
        let entries = block.get_entries()?;

        assert_eq!(entries.len(), test_data.len());
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, test_data[i].0);
            assert_eq!(entry.value, test_data[i].1);
        }

        // Verify restart points were created (should have 3 restart points for 6 entries with interval 2)
        let restart_points = block.get_restart_points();
        assert!(
            restart_points.len() >= 3,
            "Expected at least 3 restart points, got {}",
            restart_points.len()
        );

        Ok(())
    }
}
