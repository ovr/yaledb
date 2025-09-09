use crate::error::Result;
use crate::footer::Footer;
use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::Path;

pub struct SstReader {
    reader: BufReader<File>,
    file_size: u64,
}

impl SstReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let file_size = reader.seek(std::io::SeekFrom::End(0))?;
        reader.seek(std::io::SeekFrom::Start(0))?;

        Ok(SstReader { reader, file_size })
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn read_footer(&mut self) -> Result<Footer> {
        Footer::read_from(&mut self.reader)
    }

    pub fn validate_magic_number(&mut self) -> Result<()> {
        let _footer = self.read_footer()?;
        // Footer validation includes magic number check, so if we got here successfully,
        // the magic number is valid
        Ok(())
    }

    pub fn get_format_version(&mut self) -> Result<u32> {
        let footer = self.read_footer()?;
        Ok(footer.format_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_path(filename: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("fixtures");
        path.push("sst_files");
        path.push(filename);
        path
    }

    #[test]
    fn test_open_nonexistent_file() {
        let result = SstReader::open("nonexistent.sst");
        assert!(result.is_err());
    }

    #[test]
    fn test_format_v5() {
        let path = fixture_path("format_v5.sst");
        let mut reader = SstReader::open(&path).expect("Should open format_v5.sst");

        let format_version = reader
            .get_format_version()
            .expect("Should get format version");
        assert_eq!(format_version, 5);

        let result = reader.validate_magic_number();
        assert!(
            result.is_ok(),
            "Magic number validation should pass for format_v5.sst"
        );

        let footer = reader
            .read_footer()
            .expect("Should read footer successfully");

        // Verify footer has valid block handles
        assert!(
            footer.metaindex_handle.offset > 0,
            "Metaindex offset should be > 0"
        );
        assert!(
            footer.metaindex_handle.size > 0,
            "Metaindex size should be > 0"
        );
        assert!(footer.index_handle.offset > 0, "Index offset should be > 0");
        assert!(footer.index_handle.size > 0, "Index size should be > 0");
    }

    #[test]
    fn test_format_v6() {
        let path = fixture_path("format_v6.sst");
        let mut reader = SstReader::open(&path).expect("Should open format_v6.sst");

        let format_version = reader
            .get_format_version()
            .expect("Should get format version");
        assert_eq!(format_version, 6);

        let result = reader.validate_magic_number();
        assert!(
            result.is_ok(),
            "Magic number validation should pass for format_v6.sst"
        );

        let footer = reader
            .read_footer()
            .expect("Should read footer successfully");

        // Verify footer has valid block handles
        assert!(
            footer.metaindex_handle.offset > 0,
            "Metaindex offset should be > 0"
        );
        assert!(
            footer.metaindex_handle.size > 0,
            "Metaindex size should be > 0"
        );
        assert!(footer.index_handle.offset > 0, "Index offset should be > 0");
        assert!(footer.index_handle.size > 0, "Index size should be > 0");
    }

    #[test]
    fn test_format_v7() {
        let path = fixture_path("format_v7.sst");
        let mut reader = SstReader::open(&path).expect("Should open format_v7.sst");

        let format_version = reader
            .get_format_version()
            .expect("Should get format version");
        assert_eq!(format_version, 7);

        let result = reader.validate_magic_number();
        assert!(
            result.is_ok(),
            "Magic number validation should pass for format_v7.sst"
        );

        let footer = reader
            .read_footer()
            .expect("Should read footer successfully");

        // Verify footer has valid block handles
        assert!(
            footer.metaindex_handle.offset > 0,
            "Metaindex offset should be > 0"
        );
        assert!(
            footer.metaindex_handle.size > 0,
            "Metaindex size should be > 0"
        );
        assert!(footer.index_handle.offset > 0, "Index offset should be > 0");
        assert!(footer.index_handle.size > 0, "Index size should be > 0");
    }
}
