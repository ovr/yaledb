# RocksDB SST Test Fixtures

This directory contains tools and fixtures for generating real RocksDB SST (Sorted String Table) files with comprehensive coverage of format versions, checksum algorithms, and compression types for testing the `rocksdb-fileformat` crate.

## Overview

The fixtures are generated using RocksDB's official `SstFileWriter` API to ensure they are authentic SST files that match the exact format specifications. The generator creates a matrix of files covering all combinations of format versions, checksum types, and compression algorithms.

## Generated Fixture Matrix

By default, the generator creates **60 files** covering:

### Format Versions
- **Version 5**: Enhanced Bloom filters with faster and more accurate implementation
- **Version 6**: Improved data integrity with enhanced checksum verification for misplaced data detection  
- **Version 7**: Latest format with all optimizations and performance improvements

### Checksum Types (5 types)
- **kNoChecksum**: No checksum validation (for performance testing)
- **kCRC32c**: CRC32C with hardware acceleration when available
- **kxxHash**: xxHash 32-bit algorithm for fast checksumming
- **kxxHash64**: xxHash 64-bit algorithm (truncated to 32-bit for storage)
- **kXXH3**: Latest xxHash3 algorithm with best performance/quality balance

### Compression Types (4 types)
- **None**: No compression for baseline testing
- **Snappy**: Google's Snappy compression (fast compression/decompression)
- **LZ4**: Extremely fast lossless compression algorithm
- **ZSTD**: Facebook's Zstandard compression (best compression ratio)

## File Structure

```
sst_files/
├── v5/
│   ├── v5_nocsum_none.sst      # Format v5, No checksum, No compression
│   ├── v5_nocsum_snappy.sst    # Format v5, No checksum, Snappy
│   ├── v5_nocsum_lz4.sst       # Format v5, No checksum, LZ4
│   ├── v5_nocsum_zstd.sst      # Format v5, No checksum, ZSTD
│   ├── v5_crc32c_none.sst      # Format v5, CRC32C, No compression
│   ├── v5_crc32c_snappy.sst    # Format v5, CRC32C, Snappy
│   ├── ... (20 files total)
├── v6/
│   └── ... (20 files, same pattern)
└── v7/
    └── ... (20 files, same pattern)
```

## Test Data Structure

Each SST file contains identical logical data but with different format parameters:
- **50 key-value pairs**
- **Keys**: `key000` to `key049` (lexicographically sorted)
- **Values**: `value_v{version}_{checksum}_{compression}_{index}`
  - Example: `value_v5_crc32c_snappy_000`
- **Bloom filter**: 10-bit filter for efficient key lookups

## Usage

### Basic Generation (60 files)
```bash
# Build and generate the standard test set
make run
```

### Full Matrix Generation (90 files)
```bash
# Generate all combinations including less common compressions (zlib, lz4hc)
make matrix
```

### Selective Generation
```bash
# Build the generator
make

# Generate only specific combinations
./generate_fixtures --version 5 --checksum crc32c --compression snappy
./generate_fixtures --checksum xxh3  # All versions and compressions with XXH3
./generate_fixtures --compression lz4 # All versions and checksums with LZ4
```

### Available Options
```bash
./generate_fixtures --help
```

## Testing Integration

The fixtures are used in the `rocksdb-fileformat` crate tests:

```rust
// Example usage in tests
let path = fixture_path("v5/v5_crc32c_snappy.sst");
let mut reader = SstReader::open(&path)?;
let footer = reader.read_footer()?;
assert_eq!(footer.format_version, 5);
```

## Verification

All generated files can be verified using RocksDB's official tools:

```bash
# Verify file integrity
rocksdb_sst_dump --file=sst_files/v5/v5_crc32c_none.sst --command=verify

# Inspect file contents
rocksdb_sst_dump --file=sst_files/v5/v5_crc32c_none.sst --command=scan
```

## Dependencies

- **RocksDB**: Version 10.5.1 or compatible
- **C++17** compatible compiler
- **pkg-config** for dependency detection

### Installation
```bash
# macOS
brew install rocksdb

# Ubuntu/Debian
sudo apt-get install librocksdb-dev

# CentOS/RHEL
sudo yum install rocksdb-devel
```

## File Metadata

See `metadata.json` for complete details about each generated file, including:
- Exact file paths and parameters
- Generation timestamp
- RocksDB version used
- File descriptions

This comprehensive fixture set enables thorough testing of:
- Format version compatibility
- Checksum algorithm validation
- Compression/decompression correctness
- Cross-version data integrity
- Performance characteristics across different configurations