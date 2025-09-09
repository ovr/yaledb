# RocksDB SST Test Fixtures

This directory contains tools and fixtures for generating real RocksDB SST (Sorted String Table) files with different format versions for testing the `rocksdb-fileformat` crate.

## Overview

The fixtures are generated using RocksDB's official `SstFileWriter` API to ensure they are authentic SST files that match the exact format specifications. Each fixture file represents a different RocksDB format version with known test data.

## Generated Fixtures

| File | Format Version | Description | Features |
|------|----------------|-------------|-----------|
| `format_v5.sst` | 5 | Enhanced Bloom filters | Faster and more accurate Bloom filter implementation |
| `format_v6.sst` | 6 | Improved data integrity | Enhanced checksum verification for misplaced data detection |
| `format_v7.sst` | 7 | Latest format | Most recent format with all optimizations |

## Test Data Structure

Each SST file contains:
- **50 key-value pairs**
- **Keys**: `key000` to `key049` (lexicographically sorted)
- **Values**: `value_v{version}_{index}` (e.g., `value_v5_000`)
- **Different compression**: Each version uses different compression algorithms
  - Version 5: Snappy compression
  - Version 6: LZ4 compression
  - Version 7: ZSTD compression
