// Copyright 2024 YaleDB Contributors
// SPDX-License-Identifier: Apache-2.0

use byteorder::{ByteOrder, LittleEndian};

use crate::block_handle::BlockHandle;
use crate::error::{Error, Result};
use crate::types::{
    ChecksumType, FOOTER_SIZE, LEGACY_MAGIC_NUMBER, ROCKSDB_MAGIC_NUMBER,
    checksum_modifier_for_context,
};
use std::io::{Cursor, Read, Seek, SeekFrom};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    pub checksum_type: ChecksumType,
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub format_version: u32,
    /// Base context checksum used as entropy source for footer checksum calculation.
    /// Only present in format version 6 and higher. This value is combined with the
    /// footer's file offset to create a unique checksum modifier, preventing block
    /// reuse attacks by ensuring checksums are position-dependent.
    pub base_context_checksum: Option<u32>,
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
                base_context_checksum: None,
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

            let base_context_checksum = cursor.read_i32().map_err(|err| {
                Error::DataCorruption(format!("Unable to read base context checksum: {:?}", err))
            })? as u32;

            let stored_checksum = cursor.read_i32().map_err(|err| {
                Error::DataCorruption(format!("Unable to read stored checksum: {:?}", err))
            })? as u32;

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

            // Perform checksum verification
            let mut footer_copy = data.to_vec();
            // Zero out the checksum field (bytes 5-8 from the start)
            footer_copy[5..9].fill(0);

            let computed_checksum = checksum_type.calculate(&footer_copy);
            let modified_checksum = computed_checksum.wrapping_add(checksum_modifier_for_context(
                base_context_checksum,
                input_offset,
            ));

            if modified_checksum != stored_checksum {
                return Err(Error::DataCorruption(format!(
                    "Footer checksum mismatch at offset {}: expected {:#x}, computed {:#x}",
                    input_offset, stored_checksum, modified_checksum
                )));
            }

            Ok(Footer {
                checksum_type,
                metaindex_handle,
                index_handle,
                format_version,
                base_context_checksum: Some(base_context_checksum),
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
                base_context_checksum: None,
            })
        }
    }

    pub fn encode_to_bytes(&self, offset: u64) -> Result<Vec<u8>> {
        if self.format_version >= 6 {
            // Reverse order, see ReverseCuros
            let mut data = Vec::with_capacity(53);

            // 1. checksum type (1 byte) - first byte, read last by ReverseCursor
            data.push(self.checksum_type as u8);
            // 2. extended magic bytes (4 bytes)
            data.extend(&[0x3e, 0x00, 0x7a, 0x00]);

            // 3. footer checksum (4 bytes as i32), initially zero
            data.extend(&[0u8; 4]);

            // 4. base context checksum (4 bytes as i32)
            let base_context_checksum = self.base_context_checksum.unwrap_or(0);
            data.extend(&(base_context_checksum as i32).to_le_bytes());
            // 5. metaindex size (4 bytes as i32)
            data.extend(&(self.metaindex_handle.size as i32).to_le_bytes());

            // 6. checked reserved (8 bytes, must be zero)
            data.extend(&[0u8; 8]);
            // 7. unchecked reserved padding (16 bytes)
            data.extend(&[0u8; 16]);

            data.extend(&self.format_version.to_le_bytes());
            data.extend(&ROCKSDB_MAGIC_NUMBER.to_le_bytes());

            // Calculate checksum with the provided offset
            let computed_checksum = self.checksum_type.calculate(&data);
            let modified_checksum = computed_checksum
                .wrapping_add(checksum_modifier_for_context(base_context_checksum, offset));

            // Write the checksum to bytes 5-8 (where the checksum field is)
            data[5..9].copy_from_slice(&(modified_checksum as i32).to_le_bytes());

            Ok(data)
        } else {
            // v1-v5 format (49 bytes)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_magic_number_validation() -> Result<()> {
        let footer = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 5,
            base_context_checksum: None,
        };

        let mut encoded = footer.encode_to_bytes(1500)?; // Using footer offset from test

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

    #[test]
    fn test_footer_roundtrip_v5() -> Result<()> {
        let original = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
            format_version: 5,
            base_context_checksum: None,
        };

        let footer_offset = 1000; // Example footer offset
        let encoded = original.encode_to_bytes(footer_offset)?;
        assert_eq!(encoded.len(), FOOTER_SIZE);
        let decoded = Footer::decode_from_bytes(&encoded, footer_offset)?;

        // Compare all fields to ensure proper roundtrip encoding/decoding
        assert_eq!(decoded.checksum_type, original.checksum_type);
        assert_eq!(
            decoded.metaindex_handle.size,
            original.metaindex_handle.size
        );
        assert_eq!(
            decoded.metaindex_handle.offset,
            original.metaindex_handle.offset
        );
        assert_eq!(decoded.index_handle.size, original.index_handle.size);
        assert_eq!(decoded.index_handle.offset, original.index_handle.offset);
        assert_eq!(decoded.format_version, original.format_version);
        assert_eq!(
            decoded.base_context_checksum,
            original.base_context_checksum
        );
        Ok(())
    }

    #[test]
    fn test_footer_v6_roundtrip() -> Result<()> {
        // For v6+, the metaindex offset is calculated from (input_offset - 5) - metaindex_size
        // So we need to use a footer offset that's large enough
        let input_offset = 100000; // Large value to avoid overflow
        let metaindex_size = 500;
        let expected_metaindex_offset = (input_offset - 5) - metaindex_size; // adjustment = 5

        let original = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new(expected_metaindex_offset, metaindex_size),
            index_handle: BlockHandle::new(0, 0), // Null for v6+
            format_version: 6,
            base_context_checksum: Some(0x12345678),
        };

        let encoded = original.encode_to_bytes(input_offset)?;
        assert_eq!(encoded.len(), 53); // v6+ footer size

        let decoded = Footer::decode_from_bytes(&encoded, input_offset)?;

        // Compare all fields except possibly checksum calculation differences due to offset
        assert_eq!(decoded.checksum_type, original.checksum_type);
        assert_eq!(
            decoded.metaindex_handle.size,
            original.metaindex_handle.size
        );
        assert_eq!(decoded.metaindex_handle.offset, expected_metaindex_offset);
        assert_eq!(decoded.index_handle, original.index_handle);
        assert_eq!(decoded.format_version, original.format_version);
        assert_eq!(
            decoded.base_context_checksum,
            original.base_context_checksum
        );
        Ok(())
    }

    #[test]
    fn test_footer_v6_with_different_checksum_types() -> Result<()> {
        let checksum_types = [
            ChecksumType::None,
            ChecksumType::CRC32c,
            ChecksumType::Hash,
            ChecksumType::Hash64,
            ChecksumType::XXH3,
        ];

        for checksum_type in checksum_types {
            let input_offset = 50000; // Large value to avoid overflow
            let metaindex_size = 1024;

            let footer = Footer {
                checksum_type,
                metaindex_handle: BlockHandle::new(
                    (input_offset - 5) - metaindex_size,
                    metaindex_size,
                ),
                index_handle: BlockHandle::new(0, 0),
                format_version: 6,
                base_context_checksum: Some(0xABCDEF12),
            };

            let encoded = footer.encode_to_bytes(input_offset)?;
            assert_eq!(encoded.len(), 53);

            let decoded = Footer::decode_from_bytes(&encoded, input_offset)?;

            assert_eq!(decoded.checksum_type, checksum_type);
            assert_eq!(decoded.format_version, 6);
            assert_eq!(decoded.base_context_checksum, Some(0xABCDEF12));
        }
        Ok(())
    }

    #[test]
    fn test_footer_v7_roundtrip() -> Result<()> {
        let input_offset = 75000; // Large value to avoid overflow
        let metaindex_size = 2048;

        let original = Footer {
            checksum_type: ChecksumType::XXH3,
            metaindex_handle: BlockHandle::new((input_offset - 5) - metaindex_size, metaindex_size),
            index_handle: BlockHandle::new(0, 0), // Null for v6+
            format_version: 7,
            base_context_checksum: Some(0x87654321),
        };

        let encoded = original.encode_to_bytes(input_offset)?;
        assert_eq!(encoded.len(), 53); // v7 also uses 53 bytes

        let decoded = Footer::decode_from_bytes(&encoded, input_offset)?;

        assert_eq!(decoded.checksum_type, original.checksum_type);
        assert_eq!(
            decoded.metaindex_handle.size,
            original.metaindex_handle.size
        );
        assert_eq!(
            decoded.metaindex_handle.offset,
            original.metaindex_handle.offset
        );
        assert_eq!(decoded.index_handle, original.index_handle);
        assert_eq!(decoded.format_version, original.format_version);
        assert_eq!(
            decoded.base_context_checksum,
            original.base_context_checksum
        );
        Ok(())
    }

    #[test]
    fn test_footer_v6_no_base_context_checksum() -> Result<()> {
        // Test with None base context checksum - should default to 0
        let input_offset = 25000; // Large value to avoid overflow
        let metaindex_size = 512;

        let footer = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new((input_offset - 5) - metaindex_size, metaindex_size),
            index_handle: BlockHandle::new(0, 0),
            format_version: 6,
            base_context_checksum: None,
        };

        let encoded = footer.encode_to_bytes(input_offset)?;
        assert_eq!(encoded.len(), 53);

        let decoded = Footer::decode_from_bytes(&encoded, input_offset)?;

        // Since encoding uses 0 when None, and decoding always creates Some(...),
        // we expect Some(0) after roundtrip
        assert_eq!(decoded.base_context_checksum, Some(0));
        assert_eq!(decoded.format_version, 6);
        Ok(())
    }

    #[test]
    fn test_footer_v6_encoding_with_offset() -> Result<()> {
        // Test that encoding with different offsets produces different checksums
        let footer = Footer {
            checksum_type: ChecksumType::CRC32c,
            metaindex_handle: BlockHandle::new(0, 256),
            index_handle: BlockHandle::new(0, 0),
            format_version: 6,
            base_context_checksum: Some(0x11223344),
        };

        let encoded_offset_0 = footer.encode_to_bytes(0)?;
        let encoded_offset_1000 = footer.encode_to_bytes(1000)?;

        // Different offsets should produce different encoded results (due to checksum)
        assert_ne!(encoded_offset_0, encoded_offset_1000);
        assert_eq!(encoded_offset_0.len(), 53);
        assert_eq!(encoded_offset_1000.len(), 53);
        Ok(())
    }
}
