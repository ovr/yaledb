use crate::block_handle::BlockHandle;
use crate::compression::decompress;
use crate::error::{Error, Result};
use crate::types::CompressionType;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

pub struct IndexEntry {
    pub key: Vec<u8>,
    pub block_handle: BlockHandle,
}

pub struct IndexBlock {
    data: Vec<u8>,
    restart_offset: usize,
    num_restarts: u32,
    restart_points: Vec<u32>,
}

impl IndexBlock {
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
                "Index block too small to contain restart info".to_string(),
            ));
        }

        let mut cursor = Cursor::new(&data);
        cursor.set_position((data.len() - 4) as u64);
        let num_restarts = cursor.read_u32::<LittleEndian>()?;

        // Check if this looks like a valid restart count (reasonable small number)
        if num_restarts == 0 || num_restarts > 1000 {
            // This might not be a standard block format
            // Try to parse as a single-entry index with no restart points
            let data_len = data.len();
            return Ok(IndexBlock {
                data,
                restart_offset: data_len,
                num_restarts: 1,
                restart_points: vec![0],
            });
        }

        if data.len() < 4 + (num_restarts as usize * 4) {
            // Fallback to simple format
            let data_len = data.len();
            return Ok(IndexBlock {
                data,
                restart_offset: data_len,
                num_restarts: 1,
                restart_points: vec![0],
            });
        }

        let restart_offset = data.len() - 4 - (num_restarts as usize * 4);
        if restart_offset >= data.len() {
            return Err(Error::InvalidBlockFormat(
                "Invalid restart offset in index block".to_string(),
            ));
        }

        let mut restart_points = Vec::with_capacity(num_restarts as usize);
        cursor.set_position(restart_offset as u64);

        for _ in 0..num_restarts {
            restart_points.push(cursor.read_u32::<LittleEndian>()?);
        }

        Ok(IndexBlock {
            data,
            restart_offset,
            num_restarts,
            restart_points,
        })
    }

    pub fn get_entries(&self) -> Result<Vec<IndexEntry>> {
        let mut entries = Vec::new();
        let mut cursor = Cursor::new(&self.data);
        let mut last_key = Vec::new();

        // Try to find a valid starting point by looking for an entry with shared_len=0
        let mut start_pos = 0;
        if self.data.len() > 0 && self.data[0] != 0 {
            // First byte is not 0 (shared_len), so look for a restart point
            for &restart_pos in &self.restart_points {
                if restart_pos < self.data.len() as u32 && restart_pos > 0 {
                    if (restart_pos as usize) < self.data.len()
                        && self.data[restart_pos as usize] == 0
                    {
                        start_pos = restart_pos as usize;
                        break;
                    }
                }
            }
        }

        cursor.set_position(start_pos as u64);

        while (cursor.position() as usize) < self.restart_offset {
            let entry_start = cursor.position();

            let shared_key_len = self.read_varint(&mut cursor)?;
            let unshared_key_len = self.read_varint(&mut cursor)?;
            let value_len = self.read_varint(&mut cursor)?;

            if shared_key_len > last_key.len() as u32 {
                return Err(Error::InvalidBlockFormat(
                    "Shared key length exceeds previous key length in index block".to_string(),
                ));
            }

            let mut key = Vec::new();
            key.extend_from_slice(&last_key[..shared_key_len as usize]);

            if unshared_key_len > 0 {
                let pos = cursor.position() as usize;
                if pos + unshared_key_len as usize > self.data.len() {
                    return Err(Error::InvalidBlockFormat(
                        "Index key extends beyond block".to_string(),
                    ));
                }
                key.extend_from_slice(&self.data[pos..pos + unshared_key_len as usize]);
                cursor.set_position((pos + unshared_key_len as usize) as u64);
            }

            if value_len == 0 {
                return Err(Error::InvalidBlockFormat(
                    "Index entry must have value (block handle)".to_string(),
                ));
            }

            let value_start = cursor.position() as usize;
            if value_start + value_len as usize > self.data.len() {
                return Err(Error::InvalidBlockFormat(
                    "Index value extends beyond block".to_string(),
                ));
            }

            let value_data = &self.data[value_start..value_start + value_len as usize];
            let block_handle = self.parse_block_handle(value_data)?;
            cursor.set_position((value_start + value_len as usize) as u64);

            last_key = key.clone();
            entries.push(IndexEntry { key, block_handle });

            if self.is_restart_point(entry_start as u32) {
                last_key.clear();
            }
        }

        Ok(entries)
    }

    fn parse_block_handle(&self, data: &[u8]) -> Result<BlockHandle> {
        let mut cursor = Cursor::new(data);

        let offset = self.read_varint_from_slice(&mut cursor)?;
        let size = self.read_varint_from_slice(&mut cursor)?;

        Ok(BlockHandle {
            offset: offset as u64,
            size: size as u64,
        })
    }

    fn read_varint_from_slice(&self, cursor: &mut Cursor<&[u8]>) -> Result<u32> {
        let mut result = 0u32;
        let mut shift = 0;

        loop {
            let data = cursor.get_ref();
            let pos = cursor.position() as usize;

            if pos >= data.len() {
                return Err(Error::InvalidVarint);
            }

            let byte = data[pos];
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

    pub fn find_block_for_key(&self, target_key: &[u8]) -> Result<Option<BlockHandle>> {
        let entries = self.get_entries()?;

        for entry in entries.iter() {
            if entry.key.as_slice() >= target_key {
                return Ok(Some(entry.block_handle.clone()));
            }
        }

        if let Some(last_entry) = entries.last() {
            Ok(Some(last_entry.block_handle.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn get_all_block_handles(&self) -> Result<Vec<BlockHandle>> {
        let entries = self.get_entries()?;
        Ok(entries
            .into_iter()
            .map(|entry| entry.block_handle)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_builder::IndexBlockBuilder;
    use crate::error::Result;
    use crate::types::{ChecksumType, CompressionType};

    #[test]
    fn test_roundtrip_index_block_single_entry() -> Result<()> {
        let key1 = b"key001";
        let handle1 = BlockHandle {
            offset: 100,
            size: 200,
        };

        let mut builder = IndexBlockBuilder::new(16);
        builder.add_index_entry(key1, &handle1);
        let block_data = builder.finish(CompressionType::None, ChecksumType::CRC32c, None, None)?;

        let index_block = IndexBlock::new(&block_data, CompressionType::None)?;
        let entries = index_block.get_entries()?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, key1);
        assert_eq!(entries[0].block_handle.offset, handle1.offset);
        assert_eq!(entries[0].block_handle.size, handle1.size);
        Ok(())
    }

    #[test]
    fn test_roundtrip_find_block_for_key() -> Result<()> {
        let key1 = b"key001";
        let key2 = b"key002";
        let handle1 = BlockHandle {
            offset: 100,
            size: 200,
        };
        let handle2 = BlockHandle {
            offset: 300,
            size: 150,
        };

        let mut builder = IndexBlockBuilder::new(1); // Use restart_interval of 1 to create restart points
        builder.add_index_entry(key1, &handle1);
        builder.add_index_entry(key2, &handle2);
        let block_data = builder.finish(CompressionType::None, ChecksumType::CRC32c, None, None)?;

        let index_block = IndexBlock::new(&block_data, CompressionType::None)?;

        let result = index_block.find_block_for_key(b"key000")?;
        assert!(result.is_some());
        assert_eq!(result.unwrap().offset, handle1.offset);

        let result = index_block.find_block_for_key(b"key001")?;
        assert!(result.is_some());
        assert_eq!(result.unwrap().offset, handle1.offset);

        let result = index_block.find_block_for_key(b"key002")?;
        assert!(result.is_some());
        assert_eq!(result.unwrap().offset, handle2.offset);
        Ok(())
    }
}
