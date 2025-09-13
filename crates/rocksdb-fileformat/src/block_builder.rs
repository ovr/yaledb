use crate::block_handle::BlockHandle;
use crate::compression::compress;
use crate::error::Result;
use crate::types::CompressionType;
use byteorder::{LittleEndian, WriteBytesExt};

/// Configuration options for DataBlockBuilder
#[derive(Debug, Clone)]
pub struct DataBlockBuilderOptions {
    /// Number of entries between restart points for prefix compression
    pub restart_interval: usize,
    /// Target block size in bytes (for future use)
    pub block_size_target: Option<usize>,
    /// Whether to enable checksum verification (for future use)
    pub enable_checksums: bool,
}

impl Default for DataBlockBuilderOptions {
    fn default() -> Self {
        Self {
            restart_interval: 16,
            block_size_target: None,
            enable_checksums: false,
        }
    }
}

impl DataBlockBuilderOptions {
    /// Set the restart interval
    pub fn with_restart_interval(mut self, restart_interval: usize) -> Self {
        self.restart_interval = restart_interval;
        self
    }

    /// Set the target block size
    pub fn with_block_size_target(mut self, size: usize) -> Self {
        self.block_size_target = Some(size);
        self
    }

    /// Enable checksum verification
    pub fn with_checksums(mut self, enable: bool) -> Self {
        self.enable_checksums = enable;
        self
    }
}

/// Builder for data blocks with prefix compression and restart points
pub struct DataBlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    options: DataBlockBuilderOptions,
    last_key: Vec<u8>,
    finished: bool,
}

impl DataBlockBuilder {
    /// Create a new DataBlockBuilder with the specified options
    pub fn new(options: DataBlockBuilderOptions) -> Self {
        let mut builder = DataBlockBuilder {
            buffer: Vec::new(),
            restarts: Vec::new(),
            counter: 0,
            options,
            last_key: Vec::new(),
            finished: false,
        };

        // Add first restart point
        builder.restarts.push(0);
        builder
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished);
        assert!(self.counter <= self.options.restart_interval);
        assert!(self.buffer.len() < u32::MAX as usize);

        let mut shared = 0;
        if self.counter < self.options.restart_interval {
            // Find shared prefix with last key
            let min_len = std::cmp::min(self.last_key.len(), key.len());
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            // Restart
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        // Encode entry: shared_length(varint) non_shared_length(varint) value_length(varint) key_delta value
        self.encode_varint(shared as u32);
        self.encode_varint(non_shared as u32);
        self.encode_varint(value.len() as u32);

        // Add key delta
        self.buffer.extend_from_slice(&key[shared..]);

        // Add value
        self.buffer.extend_from_slice(value);

        // Update state
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    pub fn finish(&mut self, compression_type: CompressionType) -> Result<Vec<u8>> {
        if self.finished {
            panic!("DataBlockBuilder already finished");
        }
        self.finished = true;

        // Add restart array
        for restart in &self.restarts {
            self.buffer.write_u32::<LittleEndian>(*restart).unwrap();
        }

        // Add restart count
        self.buffer
            .write_u32::<LittleEndian>(self.restarts.len() as u32)
            .unwrap();

        // First, create the raw block data with the 5-byte trailer
        let mut raw_block = self.buffer.clone();
        raw_block.push(compression_type as u8);
        raw_block.write_u32::<LittleEndian>(0).unwrap(); // dummy checksum

        // For uncompressed blocks, return as-is
        // For compressed blocks, compress only the data without the trailer,
        // then add the trailer after compression
        if compression_type == CompressionType::None {
            Ok(raw_block)
        } else {
            // Compress the data (without the trailer)
            let compressed_data = compress(&self.buffer, compression_type)?;

            // Add the trailer after compression
            let mut result = compressed_data;
            result.push(compression_type as u8);
            result.write_u32::<LittleEndian>(0).unwrap(); // dummy checksum

            Ok(result)
        }
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.finished = false;
    }

    pub fn empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn size_estimate(&self) -> usize {
        self.buffer.len() + 4 * self.restarts.len() + 4 + 5 // restarts + count + trailer
    }

    fn encode_varint(&mut self, mut value: u32) {
        while value >= 0x80 {
            self.buffer.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        self.buffer.push(value as u8);
    }
}

/// Builder for index blocks that track data block locations
pub struct IndexBlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
    finished: bool,
}

impl IndexBlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        let mut builder = IndexBlockBuilder {
            buffer: Vec::new(),
            restarts: Vec::new(),
            counter: 0,
            restart_interval,
            last_key: Vec::new(),
            finished: false,
        };

        // Add first restart point
        builder.restarts.push(0);
        builder
    }

    pub fn add_index_entry(&mut self, key: &[u8], block_handle: &BlockHandle) {
        assert!(!self.finished);
        assert!(self.counter <= self.restart_interval);
        assert!(self.buffer.len() < u32::MAX as usize);

        let mut shared = 0;
        if self.counter < self.restart_interval {
            // Find shared prefix with last key
            let min_len = std::cmp::min(self.last_key.len(), key.len());
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            // Restart
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        // Encode block handle as value
        let mut handle_data = Vec::new();
        self.encode_varint_to(&mut handle_data, block_handle.offset as u32);
        self.encode_varint_to(&mut handle_data, block_handle.size as u32);

        // Encode entry: shared_length(varint) non_shared_length(varint) value_length(varint) key_delta block_handle
        self.encode_varint(shared as u32);
        self.encode_varint(non_shared as u32);
        self.encode_varint(handle_data.len() as u32);

        // Add key delta
        self.buffer.extend_from_slice(&key[shared..]);

        // Add block handle
        self.buffer.extend_from_slice(&handle_data);

        // Update state
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    pub fn finish(&mut self, compression_type: CompressionType) -> Result<Vec<u8>> {
        if self.finished {
            panic!("IndexBlockBuilder already finished");
        }
        self.finished = true;

        // Add restart array
        for restart in &self.restarts {
            self.buffer.write_u32::<LittleEndian>(*restart).unwrap();
        }

        // Add restart count
        self.buffer
            .write_u32::<LittleEndian>(self.restarts.len() as u32)
            .unwrap();

        // Add block trailer: compression type (1 byte) + checksum (4 bytes)
        let mut block_data = self.buffer.clone();
        block_data.push(compression_type as u8);

        // Add dummy checksum (0 for now)
        block_data.write_u32::<LittleEndian>(0).unwrap();

        // Compress if needed
        let compressed_data = compress(&block_data, compression_type)?;

        Ok(compressed_data)
    }

    pub fn empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn encode_varint(&mut self, mut value: u32) {
        while value >= 0x80 {
            self.buffer.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        self.buffer.push(value as u8);
    }

    fn encode_varint_to(&self, buffer: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            buffer.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        buffer.push(value as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CompressionType;

    #[test]
    fn test_data_block_builder_simple() -> Result<()> {
        let mut builder = DataBlockBuilder::new(DataBlockBuilderOptions::default().with_restart_interval(16));

        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");

        let block_data = builder.finish(CompressionType::None)?;
        assert!(!block_data.is_empty());
        Ok(())
    }

    #[test]
    fn test_data_block_builder_with_compression() -> Result<()> {
        let mut builder = DataBlockBuilder::new(DataBlockBuilderOptions::default().with_restart_interval(16));

        // Add multiple entries to test compression
        for i in 0..10 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let compressed_block = builder.finish(CompressionType::Snappy)?;
        assert!(!compressed_block.is_empty());
        Ok(())
    }

    #[test]
    fn test_index_block_builder() -> Result<()> {
        let mut builder = IndexBlockBuilder::new(16);

        let handle1 = BlockHandle {
            offset: 0,
            size: 100,
        };
        let handle2 = BlockHandle {
            offset: 100,
            size: 150,
        };

        builder.add_index_entry(b"key1", &handle1);
        builder.add_index_entry(b"key2", &handle2);

        let block_data = builder.finish(CompressionType::None)?;
        assert!(!block_data.is_empty());
        Ok(())
    }

    #[test]
    fn test_data_block_builder_empty() -> Result<()> {
        let builder = DataBlockBuilder::new(DataBlockBuilderOptions::default().with_restart_interval(16));
        assert!(builder.empty());
        Ok(())
    }

    #[test]
    fn test_data_block_builder_reset() -> Result<()> {
        let mut builder = DataBlockBuilder::new(DataBlockBuilderOptions::default().with_restart_interval(16));
        builder.add(b"key1", b"value1");
        assert!(!builder.empty());

        builder.reset();
        assert!(builder.empty());
        Ok(())
    }
}
