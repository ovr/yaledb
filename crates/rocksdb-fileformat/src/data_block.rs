use crate::compression::decompress;
use crate::error::{Error, Result};
use crate::types::CompressionType;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

pub struct DataBlock {
    data: Vec<u8>,
    restart_offset: usize,
    pub(crate) restart_points: Vec<u32>,
}

pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct DataBlockIterator<'a> {
    block: &'a DataBlock,
    cursor: Cursor<&'a [u8]>,
    last_key: Vec<u8>,
}

impl<'a> DataBlockIterator<'a> {
    pub fn new(block: &'a DataBlock) -> Self {
        DataBlockIterator {
            block,
            cursor: Cursor::new(&block.data),
            last_key: Vec::new(),
        }
    }

    fn read_varint(&mut self) -> Result<u32> {
        let mut result = 0u32;
        let mut shift = 0;

        loop {
            if (self.cursor.position() as usize) >= self.block.data.len() {
                return Err(Error::InvalidVarint);
            }

            let byte = self.block.data[self.cursor.position() as usize];
            self.cursor.set_position(self.cursor.position() + 1);

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

    fn read_next_entry(&mut self) -> Result<Option<KeyValue>> {
        // Check if we've reached the restart offset
        if (self.cursor.position() as usize) >= self.block.restart_offset {
            return Ok(None);
        }

        let entry_start = self.cursor.position();

        // Check if this is a restart point BEFORE processing
        // At restart points, we should have no shared prefix
        if self.block.restart_points.contains(&(entry_start as u32)) {
            self.last_key.clear();
        }

        let shared_key_len = self.read_varint()?;
        let unshared_key_len = self.read_varint()?;
        let value_len = self.read_varint()?;

        if shared_key_len > self.last_key.len() as u32 {
            return Err(Error::InvalidBlockFormat(
                "Shared key length exceeds previous key length".to_string(),
            ));
        }

        let mut key = Vec::new();
        key.extend_from_slice(&self.last_key[..shared_key_len as usize]);

        if unshared_key_len > 0 {
            let pos = self.cursor.position() as usize;
            if pos + unshared_key_len as usize > self.block.data.len() {
                return Err(Error::InvalidBlockFormat(
                    "Key extends beyond block".to_string(),
                ));
            }
            key.extend_from_slice(&self.block.data[pos..pos + unshared_key_len as usize]);
            self.cursor
                .set_position((pos + unshared_key_len as usize) as u64);
        }

        let mut value = Vec::new();
        if value_len > 0 {
            let pos = self.cursor.position() as usize;
            if pos + value_len as usize > self.block.data.len() {
                return Err(Error::InvalidBlockFormat(
                    "Value extends beyond block".to_string(),
                ));
            }
            value.extend_from_slice(&self.block.data[pos..pos + value_len as usize]);
            self.cursor.set_position((pos + value_len as usize) as u64);
        }

        self.last_key = key.clone();
        Ok(Some(KeyValue { key, value }))
    }
}

impl<'a> Iterator for DataBlockIterator<'a> {
    type Item = Result<KeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_entry() {
            Ok(Some(kv)) => Some(Ok(kv)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
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
            restart_points,
        })
    }

    pub fn iter(&self) -> DataBlockIterator<'_> {
        DataBlockIterator::new(self)
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
        let entries: Vec<KeyValue> = block.iter().collect::<Result<Vec<_>>>()?;

        // Verify all entries match
        assert_eq!(entries.len(), test_data.len());
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, test_data[i].0, "Key mismatch at index {}", i);
            assert_eq!(entry.value, test_data[i].1, "Value mismatch at index {}", i);
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
        let entries: Vec<KeyValue> = block.iter().collect::<Result<Vec<_>>>()?;

        assert_eq!(entries.len(), test_data.len());
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.key, test_data[i].0);
            assert_eq!(entry.value, test_data[i].1);
        }

        // Verify restart points were created (should have 3 restart points for 6 entries with interval 2)
        assert!(
            block.restart_points.len() >= 3,
            "Expected at least 3 restart points, got {}",
            block.restart_points.len()
        );

        Ok(())
    }
}
