// Write varint to stream

use num_traits::{FromBytes, ToBytes, Unsigned};
use std::io::{Result, Write};
use std::mem::size_of;

pub fn fixedint_write<W: Write, N: Unsigned + ToBytes>(value: N, writer: &mut W) -> Result<()> {
    let buff = value.to_le_bytes();
    writer.write_all(buff.as_ref())?;
    Ok(())
}

pub fn fixedint_read<R: std::io::Read, N: Unsigned + FromBytes<Bytes = [u8]>>(
    reader: &mut R,
) -> Result<N>
where
    <N as FromBytes>::Bytes: Sized,
{
    let mut buffer = vec![0; size_of::<N>()];
    reader.read_exact(&mut buffer)?;

    Ok(N::from_le_bytes(&buffer))
}
