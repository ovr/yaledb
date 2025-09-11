use crate::error::{Error, Result};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    pub fn null() -> Self {
        Self { offset: 0, size: 0 }
    }

    pub fn is_null(&self) -> bool {
        self.offset == 0 && self.size == 0
    }

    pub fn decode_from<R: Read>(reader: &mut R) -> Result<Self> {
        let offset = read_varint64(reader)?;
        let size = read_varint64(reader)?;
        Ok(Self { offset, size })
    }

    pub fn encode_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_varint64(writer, self.offset)?;
        write_varint64(writer, self.size)?;
        Ok(())
    }

    pub fn encoded_length(&self) -> usize {
        varint64_length(self.offset) + varint64_length(self.size)
    }

    pub fn decode_from_bytes(data: &[u8]) -> Result<(Self, usize)> {
        let mut cursor = Cursor::new(data);
        let handle = Self::decode_from(&mut cursor)?;
        let consumed = cursor.position() as usize;
        Ok((handle, consumed))
    }

    pub fn encode_to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.encoded_length());
        self.encode_to(&mut buf)?;
        Ok(buf)
    }
}

fn read_varint64<R: Read>(reader: &mut R) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    for _ in 0..10 {
        let byte = reader.read_u8()?;

        if (byte & 0x80) == 0 {
            result |= (byte as u64) << shift;
            return Ok(result);
        }

        result |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }

    Err(Error::InvalidVarint)
}

fn write_varint64<W: Write>(writer: &mut W, mut value: u64) -> Result<()> {
    while value >= 0x80 {
        writer.write_u8((value as u8) | 0x80)?;
        value >>= 7;
    }
    writer.write_u8(value as u8)?;
    Ok(())
}

fn varint64_length(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint64_encoding() -> Result<()> {
        let test_cases = vec![0, 127, 128, 16383, 16384, 2097151, 2097152];

        for value in test_cases {
            let mut buf = Vec::new();
            write_varint64(&mut buf, value)?;
            let mut cursor = Cursor::new(&buf);
            let decoded = read_varint64(&mut cursor)?;
            assert_eq!(value, decoded);
        }
        Ok(())
    }

    #[test]
    fn test_block_handle_encoding() -> Result<()> {
        let handle = BlockHandle::new(12345, 67890);
        let encoded = handle.encode_to_bytes()?;
        let (decoded, consumed) = BlockHandle::decode_from_bytes(&encoded)?;

        assert_eq!(handle, decoded);
        assert_eq!(consumed, encoded.len());
        Ok(())
    }

    #[test]
    fn test_block_handle_encoded_length() -> Result<()> {
        let handle = BlockHandle::new(12345, 67890);
        let encoded = handle.encode_to_bytes()?;
        assert_eq!(handle.encoded_length(), encoded.len());
        Ok(())
    }
}
