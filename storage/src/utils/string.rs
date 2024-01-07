use std::io::Result;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::varint::{read::async_read_varint, write::async_write_varint};

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
