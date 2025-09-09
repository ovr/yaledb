use crate::error::{Error, Result};
use crate::types::CompressionType;

/// Decompress data according to the specified compression type
pub fn decompress(data: &[u8], compression_type: CompressionType) -> Result<Vec<u8>> {
    match compression_type {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => decompress_snappy(data),
        CompressionType::Zlib => decompress_zlib(data),
        CompressionType::LZ4 => decompress_lz4(data),
        CompressionType::ZSTD => decompress_zstd(data),
        _ => Err(Error::UnsupportedCompressionType(compression_type as u8)),
    }
}

fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    snap::raw::Decoder::new()
        .decompress_vec(data)
        .map_err(|e| Error::Decompression(format!("Snappy decompression failed: {}", e)))
}

fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| Error::Decompression(format!("Zlib decompression failed: {}", e)))?;

    Ok(decompressed)
}

fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    // LZ4 in RocksDB includes a 4-byte uncompressed size header
    if data.len() < 4 {
        return Err(Error::Decompression("LZ4 data too short".to_string()));
    }

    let uncompressed_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let compressed_data = &data[4..];

    lz4::block::decompress(compressed_data, Some(uncompressed_size as i32))
        .map_err(|e| Error::Decompression(format!("LZ4 decompression failed: {}", e)))
}

fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(data)
        .map_err(|e| Error::Decompression(format!("ZSTD decompression failed: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let data = b"hello world";
        let result = decompress(data, CompressionType::None).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_snappy_compression() {
        let original = b"hello world hello world hello world";
        let compressed = snap::raw::Encoder::new().compress_vec(original).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Snappy).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_zlib_compression() {
        use flate2::Compression;
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let original = b"hello world hello world hello world";
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let decompressed = decompress(&compressed, CompressionType::Zlib).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_lz4_compression() {
        let original = b"hello world hello world hello world";
        let compressed_block = lz4::block::compress(original, None, false).unwrap();

        // Create LZ4 data with uncompressed size header (as RocksDB does)
        let mut lz4_data = Vec::new();
        lz4_data.extend_from_slice(&(original.len() as u32).to_le_bytes());
        lz4_data.extend_from_slice(&compressed_block);

        let decompressed = decompress(&lz4_data, CompressionType::LZ4).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_zstd_compression() {
        let original = b"hello world hello world hello world";
        let compressed = zstd::stream::encode_all(&original[..], 0).unwrap();

        let decompressed = decompress(&compressed, CompressionType::ZSTD).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_unsupported_compression() {
        let data = b"hello world";
        let result = decompress(data, CompressionType::BZip2);
        assert!(matches!(result, Err(Error::UnsupportedCompressionType(_))));
    }
}
