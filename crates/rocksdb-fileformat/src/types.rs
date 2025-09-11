pub const ROCKSDB_MAGIC_NUMBER: u64 = 0x88e241b785f4cff7;
pub const LEGACY_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

pub const FOOTER_SIZE: usize = 49;

pub const MAX_BLOCK_HANDLE_ENCODED_LENGTH: usize = 20;

pub const DEFAULT_BLOCK_SIZE: usize = 4096;
pub const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;

/// https://github.com/facebook/rocksdb/blob/v10.5.1/include/rocksdb/table.h#L55
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    None = 0,
    CRC32c = 1,
    Hash = 2,
    Hash64 = 3,
    // Supported since RocksDB 6.27
    XXH3 = 4,
}

impl TryFrom<u8> for ChecksumType {
    type Error = crate::error::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ChecksumType::None),
            1 => Ok(ChecksumType::CRC32c),
            2 => Ok(ChecksumType::Hash),
            3 => Ok(ChecksumType::Hash64),
            4 => Ok(ChecksumType::XXH3),
            _ => Err(crate::error::Error::UnsupportedChecksumType(value)),
        }
    }
}

impl ChecksumType {
    /// Calculate checksum for the given data using RocksDB-compatible algorithms
    pub fn calculate(self, data: &[u8]) -> u32 {
        match self {
            ChecksumType::None => 0,
            ChecksumType::CRC32c => {
                // Apply RocksDB CRC32c masking: rotate right by 15 bits and add constant
                const MASK_DELTA: u32 = 0xa282ead8;
                let crc = crc32c::crc32c(data);
                ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA)
            }
            ChecksumType::Hash => {
                use xxhash_rust::xxh32::xxh32;
                xxh32(data, 0)
            }
            ChecksumType::Hash64 => {
                use xxhash_rust::xxh64::xxh64;
                (xxh64(data, 0) & 0xFFFFFFFF) as u32
            }
            ChecksumType::XXH3 => {
                if data.is_empty() {
                    // Special case for empty data
                    0
                } else {
                    use xxhash_rust::xxh3::xxh3_64;
                    // Compute XXH3 on all bytes except the last one
                    let without_last = &data[..data.len() - 1];
                    let v = (xxh3_64(without_last) & 0xFFFFFFFF) as u32;
                    // Apply ModifyChecksumForLastByte with the last byte
                    const RANDOM_PRIME: u32 = 0x6b9083d9;
                    let last_byte = data[data.len() - 1] as u32;
                    v ^ (last_byte.wrapping_mul(RANDOM_PRIME))
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None = 0,
    Snappy = 1,
    Zlib = 2,
    BZip2 = 3,
    LZ4 = 4,
    LZ4HC = 5,
    XPRESS = 6,
    ZSTD = 7,
}

impl TryFrom<u8> for CompressionType {
    type Error = crate::error::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Snappy),
            2 => Ok(CompressionType::Zlib),
            3 => Ok(CompressionType::BZip2),
            4 => Ok(CompressionType::LZ4),
            5 => Ok(CompressionType::LZ4HC),
            6 => Ok(CompressionType::XPRESS),
            7 => Ok(CompressionType::ZSTD),
            _ => Err(crate::error::Error::UnsupportedCompressionType(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatVersion {
    V5 = 5,
    V6 = 6,
    V7 = 7,
}

impl TryFrom<u32> for FormatVersion {
    type Error = crate::error::Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            5 => Ok(FormatVersion::V5),
            6 => Ok(FormatVersion::V6),
            7 => Ok(FormatVersion::V7),
            _ => Err(crate::error::Error::UnsupportedFormatVersion(value)),
        }
    }
}

/// Configuration options for SstFileWriter
#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub compression: CompressionType,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub format_version: FormatVersion,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            compression: CompressionType::None,
            block_size: DEFAULT_BLOCK_SIZE,
            block_restart_interval: DEFAULT_BLOCK_RESTART_INTERVAL,
            format_version: FormatVersion::V5,
        }
    }
}

/// Configuration options for reading SST files
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Whether to verify checksums when reading the file.
    /// Enabled by default for data integrity protection across all format versions.
    pub verify_checksums: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            verify_checksums: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_checksum_against_rocksdb() {
        // Test data generated from real RocksDB ComputeBuiltinChecksum
        // This ensures our Rust implementation matches RocksDB's behavior exactly
        const TEST_CASES: &[(&str, ChecksumType, &[u8], u32)] = &[
            ("empty", ChecksumType::None, &[], 0x00000000),
            ("empty", ChecksumType::CRC32c, &[], 0xa282ead8),
            ("empty", ChecksumType::Hash, &[], 0x02cc5d05),
            ("empty", ChecksumType::Hash64, &[], 0x51d8e999),
            ("empty", ChecksumType::XXH3, &[], 0x00000000),
            ("single_byte", ChecksumType::None, &[0x41], 0x00000000),
            ("single_byte", ChecksumType::CRC32c, &[0x41], 0x3e60adb3),
            ("single_byte", ChecksumType::Hash, &[0x41], 0x10659a4d),
            ("single_byte", ChecksumType::Hash64, &[0x41], 0xd095b684),
            ("single_byte", ChecksumType::XXH3, &[0x41], 0x7762eedb),
            (
                "hello_world",
                ChecksumType::None,
                &[
                    0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21,
                ],
                0x00000000,
            ),
            (
                "hello_world",
                ChecksumType::CRC32c,
                &[
                    0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21,
                ],
                0xc3538582,
            ),
            (
                "hello_world",
                ChecksumType::Hash,
                &[
                    0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21,
                ],
                0x4007de50,
            ),
            (
                "hello_world",
                ChecksumType::Hash64,
                &[
                    0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21,
                ],
                0x080fe47f,
            ),
            (
                "hello_world",
                ChecksumType::XXH3,
                &[
                    0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21,
                ],
                0x368463b9,
            ),
            (
                "binary_data",
                ChecksumType::None,
                &[0x00, 0x01, 0x02, 0x03, 0xff, 0xfe, 0xfd, 0xfc],
                0x00000000,
            ),
            (
                "binary_data",
                ChecksumType::CRC32c,
                &[0x00, 0x01, 0x02, 0x03, 0xff, 0xfe, 0xfd, 0xfc],
                0x78d58d88,
            ),
            (
                "binary_data",
                ChecksumType::Hash,
                &[0x00, 0x01, 0x02, 0x03, 0xff, 0xfe, 0xfd, 0xfc],
                0x617c5b1f,
            ),
            (
                "binary_data",
                ChecksumType::Hash64,
                &[0x00, 0x01, 0x02, 0x03, 0xff, 0xfe, 0xfd, 0xfc],
                0xbfef626c,
            ),
            (
                "binary_data",
                ChecksumType::XXH3,
                &[0x00, 0x01, 0x02, 0x03, 0xff, 0xfe, 0xfd, 0xfc],
                0xdd655b30,
            ),
            (
                "repeated_x",
                ChecksumType::None,
                &[
                    0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
                    0x58, 0x58, 0x58,
                ],
                0x00000000,
            ),
            (
                "repeated_x",
                ChecksumType::CRC32c,
                &[
                    0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
                    0x58, 0x58, 0x58,
                ],
                0x5e3ae519,
            ),
            (
                "repeated_x",
                ChecksumType::Hash,
                &[
                    0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
                    0x58, 0x58, 0x58,
                ],
                0xa1cd4bfc,
            ),
            (
                "repeated_x",
                ChecksumType::Hash64,
                &[
                    0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
                    0x58, 0x58, 0x58,
                ],
                0x91542cc1,
            ),
            (
                "repeated_x",
                ChecksumType::XXH3,
                &[
                    0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58,
                    0x58, 0x58, 0x58,
                ],
                0x2bbfd401,
            ),
            (
                "all_zeros",
                ChecksumType::None,
                &[
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ],
                0x00000000,
            ),
            (
                "all_zeros",
                ChecksumType::CRC32c,
                &[
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ],
                0xd8576fb9,
            ),
            (
                "all_zeros",
                ChecksumType::Hash,
                &[
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ],
                0x8e022b3a,
            ),
            (
                "all_zeros",
                ChecksumType::Hash64,
                &[
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ],
                0x16247c32,
            ),
            (
                "all_zeros",
                ChecksumType::XXH3,
                &[
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ],
                0xc3ed6bc7,
            ),
            (
                "all_ones",
                ChecksumType::None,
                &[
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff,
                ],
                0x00000000,
            ),
            (
                "all_ones",
                ChecksumType::CRC32c,
                &[
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff,
                ],
                0x3aa4c936,
            ),
            (
                "all_ones",
                ChecksumType::Hash,
                &[
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff,
                ],
                0xd85160aa,
            ),
            (
                "all_ones",
                ChecksumType::Hash64,
                &[
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff,
                ],
                0x6a57c444,
            ),
            (
                "all_ones",
                ChecksumType::XXH3,
                &[
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff,
                ],
                0xbfb5dfd3,
            ),
            (
                "ascii_sequence",
                ChecksumType::None,
                &[
                    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43,
                    0x44, 0x45, 0x46,
                ],
                0x00000000,
            ),
            (
                "ascii_sequence",
                ChecksumType::CRC32c,
                &[
                    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43,
                    0x44, 0x45, 0x46,
                ],
                0x02925688,
            ),
            (
                "ascii_sequence",
                ChecksumType::Hash,
                &[
                    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43,
                    0x44, 0x45, 0x46,
                ],
                0xf9f50986,
            ),
            (
                "ascii_sequence",
                ChecksumType::Hash64,
                &[
                    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43,
                    0x44, 0x45, 0x46,
                ],
                0xdd7aeaa6,
            ),
            (
                "ascii_sequence",
                ChecksumType::XXH3,
                &[
                    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43,
                    0x44, 0x45, 0x46,
                ],
                0xfe511f1c,
            ),
            (
                "longer_text",
                ChecksumType::None,
                &[
                    0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f,
                    0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20,
                    0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79,
                    0x20, 0x64, 0x6f, 0x67,
                ],
                0x00000000,
            ),
            (
                "longer_text",
                ChecksumType::CRC32c,
                &[
                    0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f,
                    0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20,
                    0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79,
                    0x20, 0x64, 0x6f, 0x67,
                ],
                0xaa8b2f9c,
            ),
            (
                "longer_text",
                ChecksumType::Hash,
                &[
                    0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f,
                    0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20,
                    0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79,
                    0x20, 0x64, 0x6f, 0x67,
                ],
                0xe85ea4de,
            ),
            (
                "longer_text",
                ChecksumType::Hash64,
                &[
                    0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f,
                    0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20,
                    0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79,
                    0x20, 0x64, 0x6f, 0x67,
                ],
                0x1fda71bc,
            ),
            (
                "longer_text",
                ChecksumType::XXH3,
                &[
                    0x54, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69, 0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f,
                    0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70, 0x73, 0x20,
                    0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79,
                    0x20, 0x64, 0x6f, 0x67,
                ],
                0xc02e563c,
            ),
        ];

        for (name, checksum_type, data, expected) in TEST_CASES {
            let result = checksum_type.calculate(data);
            assert_eq!(
                result, *expected,
                "Failed for test case '{}' with checksum type {:?}. Expected 0x{:08x}, got 0x{:08x}",
                name, checksum_type, expected, result
            );
        }
    }
}
