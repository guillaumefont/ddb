use std::io::Read;
use std::io::Result;
use std::io::SeekFrom;
use std::io::Write;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

use crate::utils::varint::read::async_read_varint;
use crate::utils::varint::read::read_varint;
use crate::utils::varint::write::write_varint;

#[derive(Debug)]
pub struct SstBlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl SstBlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let offset = read_varint(reader)?;
        let size = read_varint(reader)?;
        Ok(Self { offset, size })
    }

    pub async fn async_read_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let offset = async_read_varint(reader).await?;
        let size = async_read_varint(reader).await?;
        Ok(Self { offset, size })
    }

    pub fn to_value(&self) -> Vec<u8> {
        let mut res = Vec::new();
        write_varint(self.offset, &mut res).unwrap();
        write_varint(self.size, &mut res).unwrap();
        res
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_varint(self.offset, writer)?;
        write_varint(self.size, writer)?;
        Ok(())
    }
}

pub async fn block_from_handle<R: AsyncReadExt + AsyncSeekExt + Unpin>(
    reader: &mut R,
    handle: &SstBlockHandle,
) -> Result<Vec<u8>> {
    let mut block = vec![0; handle.size as usize];
    reader.seek(SeekFrom::Start(handle.offset)).await?;
    reader.read_exact(&mut block).await?;
    Ok(block)
}
