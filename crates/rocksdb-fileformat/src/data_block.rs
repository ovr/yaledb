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

            if self.is_restart_point(entry_start as u32) {
                last_key.clear();
            }
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
    use crate::types::CompressionType;

    #[test]
    fn test_data_block_uncompressed() {
        let key1 = b"key001";
        let value1 = b"value1";
        let key2 = b"key002";
        let value2 = b"value2";

        let mut block_data = Vec::new();

        block_data.extend_from_slice(&encode_varint(0));
        block_data.extend_from_slice(&encode_varint(key1.len() as u32));
        block_data.extend_from_slice(&encode_varint(value1.len() as u32));
        block_data.extend_from_slice(key1);
        block_data.extend_from_slice(value1);

        let restart_point_1 = block_data.len() as u32;

        block_data.extend_from_slice(&encode_varint(0));
        block_data.extend_from_slice(&encode_varint(key2.len() as u32));
        block_data.extend_from_slice(&encode_varint(value2.len() as u32));
        block_data.extend_from_slice(key2);
        block_data.extend_from_slice(value2);

        block_data.extend_from_slice(&0u32.to_le_bytes());
        block_data.extend_from_slice(&restart_point_1.to_le_bytes());
        block_data.extend_from_slice(&2u32.to_le_bytes());

        // Add 5-byte trailer: compression_type (0) + checksum (0)
        block_data.push(0); // compression type = None
        block_data.extend_from_slice(&0u32.to_le_bytes()); // checksum

        let data_block = DataBlock::new(&block_data, CompressionType::None).unwrap();
        let entries = data_block.get_entries().unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, key1);
        assert_eq!(entries[0].value, value1);
        assert_eq!(entries[1].key, key2);
        assert_eq!(entries[1].value, value2);
    }

    #[test]
    fn test_data_block_reader() {
        let key1 = b"key001";
        let value1 = b"value1";

        let mut block_data = Vec::new();
        block_data.extend_from_slice(&encode_varint(0));
        block_data.extend_from_slice(&encode_varint(key1.len() as u32));
        block_data.extend_from_slice(&encode_varint(value1.len() as u32));
        block_data.extend_from_slice(key1);
        block_data.extend_from_slice(value1);

        block_data.extend_from_slice(&0u32.to_le_bytes());
        block_data.extend_from_slice(&1u32.to_le_bytes());

        // Add 5-byte trailer: compression_type (0) + checksum (0)
        block_data.push(0); // compression type = None
        block_data.extend_from_slice(&0u32.to_le_bytes()); // checksum

        let mut reader = DataBlockReader::new(&block_data, CompressionType::None).unwrap();

        reader.seek_to_first();
        assert!(reader.valid());

        let entry = reader.next().unwrap();
        assert_eq!(entry.key, key1);
        assert_eq!(entry.value, value1);

        assert!(!reader.valid());
        assert!(reader.next().is_none());
    }

    fn encode_varint(mut value: u32) -> Vec<u8> {
        let mut result = Vec::new();
        while value >= 0x80 {
            result.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        result.push(value as u8);
        result
    }
}
