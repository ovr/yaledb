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
pub struct Options {
    pub compression: CompressionType,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub format_version: FormatVersion,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            compression: CompressionType::None,
            block_size: DEFAULT_BLOCK_SIZE,
            block_restart_interval: DEFAULT_BLOCK_RESTART_INTERVAL,
            format_version: FormatVersion::V5,
        }
    }
}
