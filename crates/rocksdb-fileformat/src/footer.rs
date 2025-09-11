use byteorder::{ByteOrder, LittleEndian};

use crate::block_handle::BlockHandle;
use crate::error::{Error, Result};
use crate::types::{ChecksumType, FOOTER_SIZE, LEGACY_MAGIC_NUMBER, ROCKSDB_MAGIC_NUMBER};
use std::io::{Cursor, Read, Seek, SeekFrom};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    pub checksum_type: ChecksumType,
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub format_version: u32,
}

struct ReverseCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ReverseCursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: data.len(),
        }
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        if self.pos < 8 {
            return Err(Error::DataCorruption(
                "Unable to read data from cursor, because it's end".to_string(),
            ));
        }

        self.pos -= 8;

        Ok(LittleEndian::read_u64(&self.data[self.pos..self.pos + 8]))
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        if self.pos < 4 {
            return Err(Error::DataCorruption(
                "Unable to read data from cursor, because it's end".to_string(),
            ));
        }

        self.pos -= 4;

        Ok(LittleEndian::read_i32(&self.data[self.pos..self.pos + 4]))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        if self.pos < 4 {
            return Err(Error::DataCorruption(
                "Unable to read data from cursor, because it's end".to_string(),
            ));
        }

        self.pos -= 4;

        Ok(LittleEndian::read_u32(&self.data[self.pos..self.pos + 4]))
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        if self.pos < 1 {
            return Err(Error::DataCorruption(
                "Unable to read data from cursor, because it's end".to_string(),
            ));
        }

        self.pos -= 1;

        Ok(self.data[self.pos])
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if self.pos < buf.len() {
            return Err(Error::DataCorruption(
                "Unable to read data from cursor, because it's end".to_string(),
            ));
        }

        self.pos -= buf.len();
        buf.copy_from_slice(&self.data[self.pos..self.pos + buf.len()]);

        Ok(())
    }
}

impl Footer {
    pub fn read_from<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let file_size = reader.seek(SeekFrom::End(0))?;

        // Minimum file size is 48 bytes (legacy footer)
        if file_size < 48 {
            return Err(Error::FileTooSmall);
        }

        // First, check for the new magic number at position -8
        reader.seek(SeekFrom::End(-8))?;
        let mut magic_bytes = [0u8; 8];
        reader.read_exact(&mut magic_bytes)?;
        let magic = u64::from_le_bytes(magic_bytes);

        if magic == ROCKSDB_MAGIC_NUMBER {
            // New format - read format version to determine footer size
            reader.seek(SeekFrom::End(-12))?;
            let mut version_bytes = [0u8; 4];
            reader.read_exact(&mut version_bytes)?;

            let footer_size = 53;

            // Read the full footer
            if file_size < footer_size as u64 {
                return Err(Error::FileTooSmall);
            }
            reader.seek(SeekFrom::End(-(footer_size as i64)))?;
            let mut footer_data = vec![0u8; footer_size];
            reader.read_exact(&mut footer_data)?;

            let input_offset = file_size - (footer_size as u64);
            Self::decode_from_bytes(&footer_data, input_offset)
        } else {
            // Check for legacy magic number at position -48
            reader.seek(SeekFrom::End(-48))?;
            let mut legacy_magic_bytes = [0u8; 8];
            reader.read_exact(&mut legacy_magic_bytes)?;
            let legacy_magic = u64::from_le_bytes(legacy_magic_bytes);

            if legacy_magic == LEGACY_MAGIC_NUMBER {
                // Legacy format (v0) - 48-byte footer
                reader.seek(SeekFrom::End(-48))?;
                let mut footer_data = vec![0u8; 48];
                reader.read_exact(&mut footer_data)?;
                let input_offset = file_size - 48;
                Self::decode_from_bytes(&footer_data, input_offset)
            } else {
                Err(Error::InvalidMagicNumber(magic))
            }
        }
    }

    pub fn decode_from_bytes(data: &[u8], input_offset: u64) -> Result<Self> {
        // Check for magic number at the end
        if data.len() < 12 {
            return Err(Error::InvalidFooterSize(data.len()));
        }

        // +---------------------------------------------------------------+
        // | checksum (1B) | part2 (40B) | format_version (4B) | magic (8B)|
        // +---------------------------------------------------------------+
        let mut cursor = ReverseCursor::new(&data);
        let magic = cursor.read_u64()?;

        // Handle legacy format (v0) first
        if magic == LEGACY_MAGIC_NUMBER {
            if data.len() != 48 {
                return Err(Error::InvalidFooterSize(data.len()));
            }

            // Legacy format: varint handles directly in the 40 bytes before magic
            let mut cursor = Cursor::new(&data);
            let metaindex_handle = BlockHandle::decode_from(&mut cursor)?;
            let index_handle = BlockHandle::decode_from(&mut cursor)?;

            return Ok(Footer {
                checksum_type: ChecksumType::CRC32c, // Legacy assumes CRC32c
                metaindex_handle,
                index_handle,
                format_version: 0,
            });
        }

        if magic != ROCKSDB_MAGIC_NUMBER {
            return Err(Error::InvalidMagicNumber(magic));
        }

        let format_version = cursor.read_u32()?;
        if format_version >= 6 {
            // second part!
            // 8 + 16 = 24 bytes padded, reserved
            {
                // 16 bytes of unchecked reserved padding
                let mut skip_bytes = [0u8; 16];
                cursor.read_exact(&mut skip_bytes).map_err(|err| {
                    Error::DataCorruption(format!(
                        "Unable to read 16 bytes for reserved padding: {:?}",
                        err
                    ))
                })?;

                // 8 bytes of checked reserved padding (expected to be zero unless using a
                // future feature).
                let reserved = cursor.read_u64().map_err(|err| {
                    Error::DataCorruption(format!("Unable to read reserved 8 bytes: {:?}", err))
                })?;
                if reserved != 0 {
                    return Err(Error::Unsupported(format!(
                        "File uses a future feature not supported in this version: {}",
                        reserved
                    )));
                }
            }

            // TODO: Fix me
            let adjustment = 5;
            let footer_offset = input_offset - adjustment;

            let metaindex_size = cursor.read_i32()? as u64;
            let metaindex_handle = BlockHandle::new(footer_offset - metaindex_size, metaindex_size);

            // Index handle is null for v6+
            let index_handle = BlockHandle::new(0, 0);

            let _base_context_checksum = cursor.read_i32().map_err(|err| {
                Error::DataCorruption(format!("Unable to read base context checksum: {:?}", err))
            })?;

            let _stored_checksum = cursor.read_i32().map_err(|err| {
                Error::DataCorruption(format!("Unable to read stored checksum: {:?}", err))
            })?;

            {
                let mut magic_bytes = [0u8; 4];
                cursor.read_exact(&mut magic_bytes).map_err(|err| {
                    Error::DataCorruption(format!("Unable to read footer magic bytes: {:?}", err))
                })?;

                // Check for extended magiс
                if magic_bytes != [0x3e, 0x00, 0x7a, 0x00] {
                    return Err(Error::DataCorruption(format!(
                        "Invalid extended magic, actual: {:?}",
                        magic_bytes
                    )));
                }
            }

            let checksum_type = ChecksumType::try_from(cursor.read_u8()?)?;

            Ok(Footer {
                checksum_type,
                metaindex_handle,
                index_handle,
                format_version,
            })
        } else {
            let version_start = data.len() - 12;

            // Format v1-v5
            // Some v5 files don't have checksum type byte (legacy-style)
            // Check if first byte looks like a varint (doesn't have high bit set)
            let (checksum_type, phase2_data) = if data[0] <= 0x7F && format_version >= 1 {
                // Might have checksum type
                match ChecksumType::try_from(data[0]) {
                    Ok(ct) => (ct, &data[1..version_start]),
                    Err(_) => (ChecksumType::CRC32c, &data[..version_start]),
                }
            } else {
                // No checksum type byte
                (ChecksumType::CRC32c, &data[..version_start])
            };

            // Parse block handles
            let mut padded_cursor: Cursor<&[u8]> = Cursor::new(phase2_data);
            let metaindex_handle = BlockHandle::decode_from(&mut padded_cursor)?;
            let index_handle = BlockHandle::decode_from(&mut padded_cursor)?;

            Ok(Footer {
                checksum_type,
                metaindex_handle,
                index_handle,
                format_version,
            })
        }
    }

    pub fn encode_to_bytes(&self) -> Result<Vec<u8>> {
        if self.format_version >= 6 {
            // v6+ format - not implemented for encoding yet
            return Err(Error::UnsupportedOperation(
                "Encoding v6+ footer not supported".to_string(),
            ));
        }

        let mut data = Vec::with_capacity(FOOTER_SIZE);

        // Write checksum type first for v1+
        data.push(self.checksum_type as u8);

        // Write block handles
        self.metaindex_handle.encode_to(&mut data)?;
        self.index_handle.encode_to(&mut data)?;

        let used_bytes = data.len();

        // Format: checksum_type(1) + block_handles + padding + format_version(4) + magic(8)
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
    fn test_footer_roundtrip() -> Result<()> {
        let original = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 5,
        };

        let encoded = original.encode_to_bytes()?;
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let footer_offset = 1000; // Example footer offset
        let decoded = Footer::decode_from_bytes(&encoded, footer_offset)?;
        assert_eq!(decoded, original);
        Ok(())
    }

    #[test]
    fn test_footer_roundtrip_with_format_version() -> Result<()> {
        let original = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 5,
        };

        let encoded = original.encode_to_bytes()?;
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let footer_offset = 2000; // Example footer offset
        let decoded = Footer::decode_from_bytes(&encoded, footer_offset)?;
        assert_eq!(original, decoded);
        Ok(())
    }

    #[test]
    fn test_footer_magic_number_validation() -> Result<()> {
        let footer = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 5,
        };

        let mut encoded = footer.encode_to_bytes()?;

        encoded[FOOTER_SIZE - 1] = 0xFF;

        let footer_offset = 1500; // Example footer offset
        let result = Footer::decode_from_bytes(&encoded, footer_offset);
        assert!(matches!(result, Err(Error::InvalidMagicNumber(_))));
        Ok(())
    }

    #[test]
    fn test_footer_size_validation() -> Result<()> {
        let data = vec![0u8; 10]; // Wrong size
        let footer_offset = 0; // Example footer offset
        let result = Footer::decode_from_bytes(&data, footer_offset);
        // Any size < 8 should fail due to magic number check
        assert!(result.is_err());
        Ok(())
    }
}
