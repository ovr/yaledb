use crate::data_block::DataBlockReader;
use crate::error::Result;
use crate::index_block::IndexBlock;
use crate::sst_reader::SstReader;
use crate::types::CompressionType;

pub trait SstIterator {
    fn seek_to_first(&mut self) -> Result<()>;
    fn seek_to_last(&mut self) -> Result<()>;
    fn seek(&mut self, key: &[u8]) -> Result<()>;
    fn next(&mut self) -> Result<bool>;
    fn prev(&mut self) -> Result<bool>;
    fn valid(&self) -> bool;
    fn key(&self) -> Option<&[u8]>;
    fn value(&self) -> Option<&[u8]>;
}

pub struct SstTableIterator {
    sst_reader: SstReader,
    index_block: IndexBlock,
    current_data_block: Option<DataBlockReader>,
    current_block_index: usize,
    all_block_handles: Vec<crate::block_handle::BlockHandle>,
    compression_type: CompressionType,
    valid: bool,
}

impl SstTableIterator {
    pub fn new(mut sst_reader: SstReader, compression_type: CompressionType) -> Result<Self> {
        let footer = sst_reader.get_footer();
        let index_data = sst_reader.read_block(footer.index_handle.clone())?;
        let index_block = IndexBlock::new(&index_data, CompressionType::None)?;
        let all_block_handles = index_block.get_all_block_handles()?;

        Ok(SstTableIterator {
            sst_reader,
            index_block,
            current_data_block: None,
            current_block_index: 0,
            all_block_handles,
            compression_type,
            valid: false,
        })
    }

    fn load_data_block(&mut self, block_index: usize) -> Result<()> {
        if block_index >= self.all_block_handles.len() {
            self.current_data_block = None;
            self.valid = false;
            return Ok(());
        }

        let block_handle = self.all_block_handles[block_index].clone();
        let data_block_reader = self
            .sst_reader
            .read_data_block_reader(block_handle, self.compression_type)?;

        self.current_data_block = Some(data_block_reader);
        self.current_block_index = block_index;
        Ok(())
    }

    pub fn entries_count(&self) -> usize {
        match &self.current_data_block {
            Some(reader) => reader.entries().len(),
            None => 0,
        }
    }

    pub fn block_count(&self) -> usize {
        self.all_block_handles.len()
    }
}

impl SstIterator for SstTableIterator {
    fn seek_to_first(&mut self) -> Result<()> {
        if self.all_block_handles.is_empty() {
            self.valid = false;
            return Ok(());
        }

        self.load_data_block(0)?;

        if let Some(ref mut data_block) = self.current_data_block {
            data_block.seek_to_first();
            self.valid = data_block.valid();
        } else {
            self.valid = false;
        }

        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<()> {
        if self.all_block_handles.is_empty() {
            self.valid = false;
            return Ok(());
        }

        let last_block_index = self.all_block_handles.len() - 1;
        self.load_data_block(last_block_index)?;

        if let Some(ref mut data_block) = self.current_data_block {
            while data_block.next().is_some() {}
            self.valid = false;

            let entries_len = data_block.entries().len();
            if entries_len > 0 {
                data_block.seek_to_first();
                for _ in 1..entries_len {
                    data_block.next();
                }
                self.valid = data_block.valid();
            }
        } else {
            self.valid = false;
        }

        Ok(())
    }

    fn seek(&mut self, target_key: &[u8]) -> Result<()> {
        let block_handle = self.index_block.find_block_for_key(target_key)?;

        if let Some(handle) = block_handle {
            if let Some(block_index) = self
                .all_block_handles
                .iter()
                .position(|h| h.offset == handle.offset && h.size == handle.size)
            {
                self.load_data_block(block_index)?;

                if let Some(ref mut data_block) = self.current_data_block {
                    self.valid = data_block.seek(target_key);
                } else {
                    self.valid = false;
                }
            } else {
                self.valid = false;
            }
        } else {
            self.valid = false;
        }

        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }

        if let Some(ref mut data_block) = self.current_data_block {
            if data_block.next().is_some() && data_block.valid() {
                return Ok(true);
            }

            let next_block_index = self.current_block_index + 1;
            if next_block_index < self.all_block_handles.len() {
                self.load_data_block(next_block_index)?;

                if let Some(ref mut new_data_block) = self.current_data_block {
                    new_data_block.seek_to_first();
                    self.valid = new_data_block.valid();
                    return Ok(self.valid);
                }
            }
        }

        self.valid = false;
        Ok(false)
    }

    fn prev(&mut self) -> Result<bool> {
        if !self.valid {
            return Ok(false);
        }

        if self.current_block_index > 0 {
            let prev_block_index = self.current_block_index - 1;
            self.load_data_block(prev_block_index)?;

            if let Some(ref mut data_block) = self.current_data_block {
                data_block.seek_to_first();
                while data_block.next().is_some() {}

                let entries_len = data_block.entries().len();
                if entries_len > 0 {
                    data_block.seek_to_first();
                    for _ in 1..entries_len {
                        data_block.next();
                    }
                    self.valid = data_block.valid();
                    return Ok(self.valid);
                }
            }
        }

        self.valid = false;
        Ok(false)
    }

    fn valid(&self) -> bool {
        self.valid
    }

    fn key(&self) -> Option<&[u8]> {
        if self.valid {
            self.current_data_block.as_ref()?.key()
        } else {
            None
        }
    }

    fn value(&self) -> Option<&[u8]> {
        if self.valid {
            self.current_data_block.as_ref()?.value()
        } else {
            None
        }
    }
}

pub struct SstEntryIterator {
    iterator: SstTableIterator,
}

impl SstEntryIterator {
    pub fn new(sst_reader: SstReader, compression_type: CompressionType) -> Result<Self> {
        let iterator = SstTableIterator::new(sst_reader, compression_type)?;
        Ok(SstEntryIterator { iterator })
    }

    pub fn collect_all(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut entries = Vec::new();
        self.iterator.seek_to_first()?;

        while self.iterator.valid() {
            if let (Some(key), Some(value)) = (self.iterator.key(), self.iterator.value()) {
                entries.push((key.to_vec(), value.to_vec()));
            }
            if !self.iterator.next()? {
                break;
            }
        }

        Ok(entries)
    }

    pub fn find(&mut self, target_key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.iterator.seek(target_key)?;

        if self.iterator.valid() {
            if let Some(key) = self.iterator.key() {
                if key == target_key {
                    return Ok(self.iterator.value().map(|v| v.to_vec()));
                }
            }
        }

        Ok(None)
    }

    pub fn entries_count(&self) -> usize {
        self.iterator.entries_count()
    }

    pub fn block_count(&self) -> usize {
        self.iterator.block_count()
    }
}

impl Iterator for SstEntryIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterator.valid() {
            let result = match (self.iterator.key(), self.iterator.value()) {
                (Some(key), Some(value)) => {
                    let entry = Ok((key.to_vec(), value.to_vec()));
                    match self.iterator.next() {
                        Ok(_) => Some(entry),
                        Err(e) => Some(Err(e)),
                    }
                }
                _ => None,
            };

            result
        } else {
            None
        }
    }
}
