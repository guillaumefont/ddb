use std::io::{Read, Result, Write};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::varint::{
    read::{async_read_varint, read_varint},
    write::{async_write_varint, write_varint},
};

pub fn write_bytes<W: Write>(value: &[u8], writer: &mut W) -> Result<()> {
    write_varint(value.len(), writer)?;
    writer.write_all(value)?;
    Ok(())
}

pub fn write_string<W: Write>(value: &str, writer: &mut W) -> Result<()> {
    write_bytes(value.as_bytes(), writer)?;
    Ok(())
}

pub fn read_bytes<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let len: usize = read_varint(reader)?;
    let mut buffer = vec![0; len as usize];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

pub fn read_string<R: Read>(reader: &mut R) -> Result<String> {
    let bytes = read_bytes(reader)?;
    Ok(String::from_utf8(bytes).unwrap())
}

pub async fn async_write_bytes<W: AsyncWriteExt + Unpin>(
    value: &[u8],
    writer: &mut W,
) -> Result<()> {
    async_write_varint(value.len(), writer).await?;
    writer.write_all(value).await?;
    Ok(())
}

pub async fn async_write_string<W: AsyncWriteExt + Unpin>(
    value: &str,
    writer: &mut W,
) -> Result<()> {
    async_write_bytes(value.as_bytes(), writer).await?;
    Ok(())
}

pub async fn async_read_bytes<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let len: usize = async_read_varint(reader).await?;
    let mut buffer = vec![0; len as usize];
    reader.read_exact(&mut buffer).await?;
    Ok(buffer)
}

pub async fn async_read_string<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<String> {
    let bytes = async_read_bytes(reader).await?;
    Ok(String::from_utf8(bytes).unwrap())
}
