// Write varint to stream

use std::io::{Result, Write, Read};

pub fn write_fixed_u8<W: Write>(value: u8, writer: &mut W) -> Result<()> {
    let buff = value.to_le_bytes();
    writer.write_all(buff.as_ref())?;
    Ok(())
}

pub fn write_fixed_u16<W: Write>(value: u16, writer: &mut W) -> Result<()> {
    let buff = value.to_le_bytes();
    writer.write_all(buff.as_ref())?;
    Ok(())
}

pub fn write_fixed_u32<W: Write>(value: u32, writer: &mut W) -> Result<()> {
    let buff = value.to_le_bytes();
    writer.write_all(buff.as_ref())?;
    Ok(())
}

pub fn write_fixed_u64<W: Write>(value: u64, writer: &mut W) -> Result<()> {
    let buff = value.to_le_bytes();
    writer.write_all(buff.as_ref())?;
    Ok(())
}


pub fn read_fixed_u8<R: Read>(
    reader: &mut R,
) -> Result<u8> {
    let mut buffer = [0];
    reader.read_exact(&mut buffer)?;
    Ok(u8::from_le_bytes(buffer))
}

pub fn read_fixed_u16<R: Read>(
    reader: &mut R,
) -> Result<u16> {
    let mut buffer = [0, 0];
    reader.read_exact(&mut buffer)?;
    Ok(u16::from_le_bytes(buffer))
}

pub fn read_fixed_u32<R: Read>(
    reader: &mut R,
) -> Result<u32> {
    let mut buffer = [0, 0, 0, 0];
    reader.read_exact(&mut buffer)?;
    Ok(u32::from_le_bytes(buffer))
}


pub fn read_fixed_u64<R: Read>(
    reader: &mut R,
) -> Result<u64> {
    let mut buffer = [0, 0, 0, 0, 0, 0, 0, 0];
    reader.read_exact(&mut buffer)?;
    Ok(u64::from_le_bytes(buffer))
}