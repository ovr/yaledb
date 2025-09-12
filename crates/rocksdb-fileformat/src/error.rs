// Copyright 2024 YaleDB Contributors
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid magic number: expected 0x88e241b785f4cff7, got {0:#x}")]
    InvalidMagicNumber(u64),

    #[error("Invalid footer size: expected {}, got {}", crate::types::FOOTER_SIZE, .0)]
    InvalidFooterSize(usize),

    #[error("Invalid block handle: {0}")]
    InvalidBlockHandle(String),

    #[error("Unsupported compression type: {0}")]
    UnsupportedCompressionType(u8),

    #[error("Unsupported: {0}")]
    Unsupported(String),

    #[error("Unsupported checksum type: {0}")]
    UnsupportedChecksumType(u8),

    #[error("Unsupported format version: {0}")]
    UnsupportedFormatVersion(u32),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Invalid varint encoding")]
    InvalidVarint,

    #[error("Data corruption detected: {0}")]
    DataCorruption(String),

    #[error("Block not found")]
    BlockNotFound,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Invalid block format: {0}")]
    InvalidBlockFormat(String),

    #[error(
        "File too small: expected at least {} bytes",
        crate::types::FOOTER_SIZE
    )]
    FileTooSmall,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
}

pub type Result<T> = std::result::Result<T, Error>;
