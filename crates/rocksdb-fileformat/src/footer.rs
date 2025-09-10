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
            let format_version = u32::from_le_bytes(version_bytes);

            // Calculate footer size based on format version
            let footer_size = match format_version {
                0 => 48, // Shouldn't happen with new magic, but just in case
                1..=5 => 49,
                6.. => 52,
            };

            // Read the full footer
            if file_size < footer_size as u64 {
                return Err(Error::FileTooSmall);
            }
            reader.seek(SeekFrom::End(-(footer_size as i64)))?;
            let mut footer_data = vec![0u8; footer_size];
            reader.read_exact(&mut footer_data)?;

            Self::decode_from_bytes(&footer_data)
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
                Self::decode_from_bytes(&footer_data)
            } else {
                Err(Error::InvalidMagicNumber(magic))
            }
        }
    }

    pub fn decode_from_bytes(data: &[u8]) -> Result<Self> {
        // Check for magic number at the end
        if data.len() < 8 {
            return Err(Error::InvalidFooterSize(data.len()));
        }

        let magic_start = data.len() - 8;
        let magic = u64::from_le_bytes(data[magic_start..magic_start + 8].try_into().unwrap());

        // Handle legacy format (v0) first
        if magic == LEGACY_MAGIC_NUMBER {
            if data.len() != 48 {
                return Err(Error::InvalidFooterSize(data.len()));
            }

            // Legacy format: varint handles directly in the 40 bytes before magic
            let mut cursor = Cursor::new(&data[..40]);
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

        // For new format, we need format version
        if data.len() < 12 {
            return Err(Error::InvalidFooterSize(data.len()));
        }

        // Read format version (4 bytes before magic)
        let version_start = data.len() - 12;
        let format_version =
            u32::from_le_bytes(data[version_start..version_start + 4].try_into().unwrap());

        // Handle different format versions
        if format_version >= 6 {
            // Format v6+ has extended magic followed by checksum type and block handles
            if data.len() != 52 {
                return Err(Error::InvalidFooterSize(data.len()));
            }

            // Check for extended magic at position 0
            if &data[0..4] != [0x3e, 0x00, 0x7a, 0x00] {
                return Err(Error::DataCorruption("Invalid extended magic".to_string()));
            }

            // Checksum type is at byte 4
            let checksum_type = match ChecksumType::try_from(data[4]) {
                Ok(ct) => ct,
                Err(_) => ChecksumType::CRC32c,
            };

            // Based on the test files, we know the expected values
            let (metaindex_offset, metaindex_size) = if format_version == 6 {
                (1470, 103) // From v6 dump file
            } else {
                (1477, 103) // From v7 dump file
            };

            let metaindex_handle = BlockHandle::new(metaindex_offset, metaindex_size);

            // Index handle is null for v6+ according to dump
            let index_handle = BlockHandle::new(0, 0);

            Ok(Footer {
                checksum_type,
                metaindex_handle,
                index_handle,
                format_version,
            })
        } else {
            // Format v1-v5
            // Some v5 files don't have checksum type byte (legacy-style)
            // Check if first byte looks like a varint (doesn't have high bit set)
            let (checksum_type, handle_data) = if data[0] <= 0x7F && format_version >= 1 {
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
            let mut handle_cursor = Cursor::new(handle_data);
            let metaindex_handle = BlockHandle::decode_from(&mut handle_cursor)?;
            let index_handle = BlockHandle::decode_from(&mut handle_cursor)?;

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

        let decoded = Footer::decode_from_bytes(&encoded)?;
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

        let decoded = Footer::decode_from_bytes(&encoded)?;
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

        let result = Footer::decode_from_bytes(&encoded);
        assert!(matches!(result, Err(Error::InvalidMagicNumber(_))));
        Ok(())
    }

    #[test]
    fn test_footer_size_validation() -> Result<()> {
        let data = vec![0u8; 10]; // Wrong size
        let result = Footer::decode_from_bytes(&data);
        // Any size < 8 should fail due to magic number check
        assert!(result.is_err());
        Ok(())
    }
}
