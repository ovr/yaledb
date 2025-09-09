pub const ROCKSDB_MAGIC_NUMBER: u64 = 0x88e241b785f4cff7;

pub const FOOTER_SIZE: usize = 48;

pub const MAX_BLOCK_HANDLE_ENCODED_LENGTH: usize = 20;

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
    V2 = 2,
    V3 = 3,
    V4 = 4,
}

impl TryFrom<u32> for FormatVersion {
    type Error = crate::error::Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(FormatVersion::V2),
            3 => Ok(FormatVersion::V3),
            4 => Ok(FormatVersion::V4),
            _ => Err(crate::error::Error::UnsupportedFormatVersion(value)),
        }
    }
}
