pub mod block_handle;
pub mod error;
pub mod footer;
pub mod types;

pub use block_handle::BlockHandle;
pub use error::{Error, Result};
pub use footer::Footer;
pub use types::{CompressionType, FormatVersion};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_functionality() {
        let handle = BlockHandle::new(100, 200);
        assert_eq!(handle.offset, 100);
        assert_eq!(handle.size, 200);

        let footer = Footer {
            metaindex_handle: BlockHandle::new(1000, 500),
            index_handle: BlockHandle::new(1500, 200),
        };

        let encoded = footer.encode_to_bytes().unwrap();
        let decoded = Footer::decode_from_bytes(&encoded).unwrap();
        assert_eq!(footer, decoded);
    }
}
