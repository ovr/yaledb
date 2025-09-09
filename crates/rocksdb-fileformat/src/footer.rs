use crate::block_handle::BlockHandle;
use crate::error::{Error, Result};
use crate::types::{FOOTER_SIZE, ROCKSDB_MAGIC_NUMBER};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read, Seek, SeekFrom};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
}

impl Footer {
    pub fn read_from<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let file_size = reader.seek(SeekFrom::End(0))?;

        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::FileTooSmall);
        }

        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        let mut footer_data = vec![0u8; FOOTER_SIZE];
        reader.read_exact(&mut footer_data)?;

        Self::decode_from_bytes(&footer_data)
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
        let magic_start = FOOTER_SIZE - 8;

        for &byte in &data[padding_start..magic_start] {
            if byte != 0 {
                return Err(Error::DataCorruption(
                    "Non-zero padding in footer".to_string(),
                ));
            }
        }

        cursor.set_position(magic_start as u64);
        let magic = cursor.read_u64::<LittleEndian>()?;

        if magic != ROCKSDB_MAGIC_NUMBER {
            return Err(Error::InvalidMagicNumber(magic));
        }

        Ok(Footer {
            metaindex_handle,
            index_handle,
        })
    }

    pub fn encode_to_bytes(&self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(FOOTER_SIZE);

        self.metaindex_handle.encode_to(&mut data)?;
        self.index_handle.encode_to(&mut data)?;

        let used_bytes = data.len();
        let padding_size = FOOTER_SIZE - used_bytes - 8; // 8 bytes for magic number
        data.extend(vec![0u8; padding_size]);

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
        };

        let mut encoded = footer.encode_to_bytes().unwrap();

        let padding_start =
            footer.metaindex_handle.encoded_length() + footer.index_handle.encoded_length();
        if padding_start < FOOTER_SIZE - 8 {
            encoded[padding_start] = 0xFF; // Corrupt padding

            let result = Footer::decode_from_bytes(&encoded);
            assert!(matches!(result, Err(Error::DataCorruption(_))));
        }
    }
}
