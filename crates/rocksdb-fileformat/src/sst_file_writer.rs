use crate::block_builder::{DataBlockBuilder, IndexBlockBuilder};
use crate::block_handle::BlockHandle;
use crate::error::{Error, Result};
use crate::footer::Footer;
use crate::types::{CompressionType, Options};
use byteorder::{LittleEndian, WriteBytesExt};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Entry type for SST files  
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryType {
    Put,
    Delete,
    Merge,
}

/// SST file writer that matches RocksDB's SstFileWriter API
pub struct SstFileWriter {
    options: Options,
    writer: Option<BufWriter<File>>,
    data_block_builder: DataBlockBuilder,
    index_block_builder: IndexBlockBuilder,
    offset: u64,
    num_entries: u64,
    last_key: Vec<u8>,
    finished: bool,
    pending_index_entry: Option<(Vec<u8>, BlockHandle)>,
}

impl SstFileWriter {
    /// Create a new SstFileWriter with the given options
    pub fn create(opts: &Options) -> Self {
        SstFileWriter {
            options: opts.clone(),
            writer: None,
            data_block_builder: DataBlockBuilder::new(opts.block_restart_interval),
            index_block_builder: IndexBlockBuilder::new(opts.block_restart_interval),
            offset: 0,
            num_entries: 0,
            last_key: Vec::new(),
            finished: false,
            pending_index_entry: None,
        }
    }

    /// Open a file for writing
    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.writer.is_some() {
            return Err(Error::InvalidArgument("File already open".to_string()));
        }

        let file = File::create(path)?;
        self.writer = Some(BufWriter::new(file));
        self.offset = 0;
        self.num_entries = 0;
        self.last_key.clear();
        self.finished = false;

        Ok(())
    }

    /// Add a key-value pair to the SST file
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.add_entry(key.as_ref(), value.as_ref(), EntryType::Put)
    }

    /// Add a merge entry to the SST file
    pub fn merge<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.add_entry(key.as_ref(), value.as_ref(), EntryType::Merge)
    }

    /// Add a delete entry to the SST file
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.add_entry(key.as_ref(), &[], EntryType::Delete)
    }

    /// Finish writing the SST file
    pub fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Err(Error::InvalidArgument("Already finished".to_string()));
        }

        if self.writer.is_none() {
            return Err(Error::InvalidArgument("No file open".to_string()));
        }

        // Flush any remaining data block
        if !self.data_block_builder.empty() {
            self.flush_data_block()?;
        }

        // Prepare all data to write
        let index_block_data = self.index_block_builder.finish(CompressionType::None)?;
        let index_handle = BlockHandle {
            offset: self.offset,
            size: index_block_data.len() as u64,
        };

        let metaindex_data = self.create_empty_metaindex_block()?;
        let metaindex_offset = self.offset + index_block_data.len() as u64;
        let metaindex_handle = BlockHandle {
            offset: metaindex_offset,
            size: metaindex_data.len() as u64,
        };

        let footer = Footer {
            metaindex_handle,
            index_handle,
            format_version: self.options.format_version as u32,
        };
        let footer_data = footer.encode_to_bytes()?;

        // Now write everything
        let writer = self.writer.as_mut().unwrap();
        writer.write_all(&index_block_data)?;
        self.offset += index_block_data.len() as u64;

        writer.write_all(&metaindex_data)?;
        self.offset += metaindex_data.len() as u64;

        writer.write_all(&footer_data)?;

        writer.flush()?;
        self.finished = true;

        Ok(())
    }

    /// Get the current file size
    pub fn file_size(&self) -> u64 {
        self.offset
    }

    fn add_entry(&mut self, key: &[u8], value: &[u8], entry_type: EntryType) -> Result<()> {
        if self.finished {
            return Err(Error::InvalidArgument("Writer is finished".to_string()));
        }

        if self.writer.is_none() {
            return Err(Error::InvalidArgument("No file open".to_string()));
        }

        // Check key ordering
        if !self.last_key.is_empty() && key <= self.last_key.as_slice() {
            return Err(Error::InvalidArgument(
                "Keys must be added in strictly increasing order".to_string(),
            ));
        }

        // Check if we need to flush the current data block
        if self.data_block_builder.size_estimate() >= self.options.block_size
            && !self.data_block_builder.empty()
        {
            self.flush_data_block()?;
        }

        // Encode the value with entry type
        let encoded_value = self.encode_entry_value(value, entry_type);

        // Add to current data block
        self.data_block_builder.add(key, &encoded_value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.num_entries += 1;

        Ok(())
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block_builder.empty() {
            return Ok(());
        }

        let writer = self.writer.as_mut().unwrap();

        // Finish the current data block
        let block_data = self.data_block_builder.finish(self.options.compression)?;

        // Create block handle
        let block_handle = BlockHandle {
            offset: self.offset,
            size: block_data.len() as u64,
        };

        // Write data block
        writer.write_all(&block_data)?;
        self.offset += block_data.len() as u64;

        // Add to pending index entry (we'll use the last key of this block)
        if let Some((prev_key, prev_handle)) = self.pending_index_entry.take() {
            self.index_block_builder
                .add_index_entry(&prev_key, &prev_handle);
        }

        // Store this block's info for the next index entry
        self.pending_index_entry = Some((self.last_key.clone(), block_handle));

        // Reset data block builder
        self.data_block_builder.reset();

        Ok(())
    }

    fn encode_entry_value(&self, value: &[u8], entry_type: EntryType) -> Vec<u8> {
        // For simplicity, we'll encode the entry type as a prefix byte
        // In a real implementation, you might want to follow RocksDB's internal key format more closely
        let mut encoded = Vec::with_capacity(value.len() + 1);
        encoded.push(entry_type as u8);
        encoded.extend_from_slice(value);
        encoded
    }

    fn create_empty_metaindex_block(&self) -> Result<Vec<u8>> {
        // Create an empty metaindex block
        let mut block_data = Vec::new();

        // Empty block with just restart info
        block_data.write_u32::<LittleEndian>(0)?; // restart point at 0
        block_data.write_u32::<LittleEndian>(1)?; // one restart point

        // Add block trailer: compression type (1 byte) + checksum (4 bytes)
        block_data.push(CompressionType::None as u8);
        block_data.write_u32::<LittleEndian>(0)?; // dummy checksum

        Ok(block_data)
    }
}

impl Drop for SstFileWriter {
    fn drop(&mut self) {
        if !self.finished && self.writer.is_some() {
            // Try to finish gracefully, but don't panic on error
            let _ = self.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::sst_reader::SstReader;
    use crate::types::{CompressionType, FormatVersion};
    use tempfile::tempdir;

    #[test]
    fn test_create_writer() -> Result<()> {
        let opts = Options::default();
        let writer = SstFileWriter::create(&opts);
        assert_eq!(writer.file_size(), 0);
        Ok(())
    }

    #[test]
    fn test_write_and_read_simple() -> Result<()> {
        let dir =
            tempdir().map_err(|e| Error::InvalidArgument(format!("Temp dir failed: {}", e)))?;
        let path = dir.path().join("test.sst");

        let opts = Options {
            compression: CompressionType::None,
            block_size: 4096,
            block_restart_interval: 16,
            format_version: FormatVersion::V5,
        };

        // Write data
        {
            let mut writer = SstFileWriter::create(&opts);
            writer.open(&path)?;
            writer.put(b"key1", b"value1")?;
            writer.put(b"key2", b"value2")?;
            writer.put(b"key3", b"value3")?;
            writer.finish()?;
        }

        // Read data back
        let mut reader = SstReader::open(&path)?;
        let footer = reader.read_footer()?;
        assert!(footer.index_handle.size > 0);
        Ok(())
    }

    #[test]
    fn test_key_ordering_enforced() -> Result<()> {
        let dir =
            tempdir().map_err(|e| Error::InvalidArgument(format!("Temp dir failed: {}", e)))?;
        let path = dir.path().join("test.sst");

        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);
        writer.open(&path)?;

        writer.put(b"key2", b"value2")?;

        // This should fail because key1 < key2
        let result = writer.put(b"key1", b"value1");
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_different_operations() -> Result<()> {
        let dir =
            tempdir().map_err(|e| Error::InvalidArgument(format!("Temp dir failed: {}", e)))?;
        let path = dir.path().join("test.sst");

        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);
        writer.open(&path)?;

        writer.put(b"key1", b"value1")?;
        writer.delete(b"key2")?;
        writer.merge(b"key3", b"merge_value")?;
        writer.finish()?;

        assert!(writer.file_size() > 0);
        Ok(())
    }

    #[test]
    fn test_compression() -> Result<()> {
        let dir =
            tempdir().map_err(|e| Error::InvalidArgument(format!("Temp dir failed: {}", e)))?;
        let path = dir.path().join("test.sst");

        let opts = Options {
            compression: CompressionType::Snappy,
            block_size: 1024, // Small block size to ensure compression
            block_restart_interval: 16,
            format_version: FormatVersion::V5,
        };

        let mut writer = SstFileWriter::create(&opts);
        writer.open(&path)?;

        // Add many similar keys to get good compression
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}_some_long_repeated_data", i);
            writer.put(key.as_bytes(), value.as_bytes())?;
        }

        writer.finish()?;
        assert!(writer.file_size() > 0);
        Ok(())
    }

    #[test]
    fn test_empty_file() -> Result<()> {
        let dir =
            tempdir().map_err(|e| Error::InvalidArgument(format!("Temp dir failed: {}", e)))?;
        let path = dir.path().join("empty.sst");

        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);
        writer.open(&path)?;
        writer.finish()?;

        // Should be able to create an empty SST file
        assert!(writer.file_size() > 0); // Will have at least footer
        Ok(())
    }

    #[test]
    fn test_file_not_open() -> Result<()> {
        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);

        // Should fail when no file is open
        let result = writer.put(b"key1", b"value1");
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_already_finished() -> Result<()> {
        let dir =
            tempdir().map_err(|e| Error::InvalidArgument(format!("Temp dir failed: {}", e)))?;
        let path = dir.path().join("test.sst");

        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);
        writer.open(&path)?;
        writer.finish()?;

        // Should fail after finish
        let result = writer.put(b"key1", b"value1");
        assert!(result.is_err());

        // Should fail to finish again
        let result = writer.finish();
        assert!(result.is_err());
        Ok(())
    }
}
