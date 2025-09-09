use crate::block_handle::BlockHandle;
use crate::error::{Error, Result};
use crate::types::{FOOTER_SIZE, ROCKSDB_MAGIC_NUMBER};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read, Seek, SeekFrom};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub format_version: u32,
}

impl Footer {
    pub fn read_from<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let file_size = reader.seek(SeekFrom::End(0))?;

        if file_size < 16 {
            return Err(Error::FileTooSmall);
        }

        // Read the last 12 bytes to get format version and magic number
        reader.seek(SeekFrom::End(-12))?;
        let mut tail_data = [0u8; 12];
        reader.read_exact(&mut tail_data)?;

        let format_version = u32::from_le_bytes(tail_data[0..4].try_into().unwrap());
        let magic = u64::from_le_bytes(tail_data[4..12].try_into().unwrap());

        if magic != ROCKSDB_MAGIC_NUMBER {
            return Err(Error::InvalidMagicNumber(magic));
        }

        // Read a chunk that should contain the block handles
        // RocksDB block handles are typically small, so 100 bytes should be enough
        let chunk_size = std::cmp::min(100, file_size.saturating_sub(12) as usize);
        reader.seek(SeekFrom::End(-((chunk_size + 12) as i64)))?;
        let mut footer_chunk = vec![0u8; chunk_size];
        reader.read_exact(&mut footer_chunk)?;

        // Find where the block handles start by scanning for non-zero bytes from the end
        let mut handles_end = chunk_size;
        for i in (0..chunk_size).rev() {
            if footer_chunk[i] != 0 {
                handles_end = i + 1;
                break;
            }
        }

        // Parse block handles from the beginning up to the handles_end
        let handle_data = &footer_chunk[..handles_end];
        let mut cursor = Cursor::new(handle_data);

        let metaindex_handle = BlockHandle::decode_from(&mut cursor)?;
        let index_handle = BlockHandle::decode_from(&mut cursor)?;

        Ok(Footer {
            metaindex_handle,
            index_handle,
            format_version,
        })
    }

    pub fn decode_from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() != FOOTER_SIZE {
            return Err(Error::InvalidFooterSize(data.len()));
        }

        let mut cursor = Cursor::new(data);

        let (metaindex_handle, metaindex_consumed) =
            BlockHandle::decode_from_bytes(&data[cursor.position() as usize..])?;
        cursor.set_position(cursor.position() + metaindex_consumed as u64);

        let (index_handle, index_consumed) =
            BlockHandle::decode_from_bytes(&data[cursor.position() as usize..])?;
        cursor.set_position(cursor.position() + index_consumed as u64);

        let padding_start = cursor.position() as usize;
        let format_version_start = FOOTER_SIZE - 12;
        let magic_start = FOOTER_SIZE - 8;

        // Check padding up to format version
        for &byte in &data[padding_start..format_version_start] {
            if byte != 0 {
                return Err(Error::DataCorruption(
                    "Non-zero padding in footer".to_string(),
                ));
            }
        }

        // Read format version
        cursor.set_position(format_version_start as u64);
        let format_version = cursor.read_u32::<LittleEndian>()?;

        cursor.set_position(magic_start as u64);
        let magic = cursor.read_u64::<LittleEndian>()?;

        if magic != ROCKSDB_MAGIC_NUMBER {
            return Err(Error::InvalidMagicNumber(magic));
        }

        Ok(Footer {
            metaindex_handle,
            index_handle,
            format_version,
        })
    }

    pub fn encode_to_bytes(&self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(FOOTER_SIZE);

        self.metaindex_handle.encode_to(&mut data)?;
        self.index_handle.encode_to(&mut data)?;

        let used_bytes = data.len();

        // Format: block handles + padding + format_version (4 bytes) + magic (8 bytes)
        let padding_size = FOOTER_SIZE - used_bytes - 12; // 4 bytes for format version + 8 for magic
        data.extend(vec![0u8; padding_size]);
        data.extend(&self.format_version.to_le_bytes());
        data.extend(&ROCKSDB_MAGIC_NUMBER.to_le_bytes());

        assert_eq!(data.len(), FOOTER_SIZE);
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_roundtrip() {
        let original = Footer {
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 0,
        };

        let encoded = original.encode_to_bytes().unwrap();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode_from_bytes(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_footer_roundtrip_with_format_version() {
        let original = Footer {
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 5,
        };

        let encoded = original.encode_to_bytes().unwrap();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode_from_bytes(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_footer_magic_number_validation() {
        let footer = Footer {
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 0,
        };

        let mut encoded = footer.encode_to_bytes().unwrap();

        encoded[FOOTER_SIZE - 1] = 0xFF;

        let result = Footer::decode_from_bytes(&encoded);
        assert!(matches!(result, Err(Error::InvalidMagicNumber(_))));
    }

    #[test]
    fn test_footer_size_validation() {
        let data = vec![0u8; 10]; // Wrong size
        let result = Footer::decode_from_bytes(&data);
        assert!(matches!(result, Err(Error::InvalidFooterSize(10))));
    }

    #[test]
    fn test_footer_padding_validation() {
        let footer = Footer {
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 0,
        };

        let mut encoded = footer.encode_to_bytes().unwrap();

        let padding_start =
            footer.metaindex_handle.encoded_length() + footer.index_handle.encoded_length();
        if padding_start < FOOTER_SIZE - 12 {
            // Now we need to account for format version (4 bytes) + magic (8 bytes)
            encoded[padding_start] = 0xFF; // Corrupt padding

            let result = Footer::decode_from_bytes(&encoded);
            assert!(matches!(result, Err(Error::DataCorruption(_))));
        }
    }
}
