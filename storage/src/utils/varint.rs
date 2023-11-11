// Write varint to stream

use num_traits::{ToPrimitive, Unsigned, FromPrimitive, AsPrimitive};
use std::io::{Result, Write};
use std::ops::{BitAnd, BitOrAssign, Shl, ShrAssign};

pub fn write_varint<W: Write, N: AsPrimitive<u8> +  ToPrimitive +  Copy + Unsigned + ShrAssign<u8> + BitAnd<N, Output = N>>(
    mut value: N,
    mut writer: W,
) -> Result<()> {
    loop {
        let mut byte: u8 = value.to_u8().unwrap() & 0b01111111;
        value >>= 7;
        if !value.is_zero() {
            byte |= 0b10000000;
        }
        writer.write_all(&[byte])?;
        if value.is_zero() {
            break;
        }
    }
    Ok(())
}

pub fn read_varint<
    R: std::io::Read,
    N: FromPrimitive + Copy + Unsigned + ShrAssign<u8> + BitAnd + Shl<i32, Output = N> + BitOrAssign,

>(
    mut reader: R,
) -> Result<N> {
    let mut value = N::zero();
    let mut shift = 0;
    loop {
        let mut byte = [0u8];
        reader.read_exact(&mut byte)?;
        let byte = byte[0];
        value |=  N::from_u8(byte & 0b01111111).unwrap() << shift;
        shift += 7;
        if byte & 0b10000000 == 0 {
            break;
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn write_varint_test() {
        let mut buffer = Cursor::new(Vec::new());
        write_varint(150u32, &mut buffer).unwrap();
        write_varint(150u64, &mut buffer).unwrap();
        write_varint(150u128, &mut buffer).unwrap();
        write_varint(150usize, &mut buffer).unwrap();
        write_varint(150u8, &mut buffer).unwrap();
        write_varint(150u16, &mut buffer).unwrap();
        assert_eq!(buffer.into_inner(), vec![
            150, 01,
            150, 01,
            150, 01,
            150, 01,
            150, 01,
            150, 01
        ]);
    }

    #[test]
    fn read_varint_test() {
        let buffer = vec![150, 1];
        let reader = Cursor::new(buffer);
        let res: u64 = read_varint(reader).unwrap();
        assert_eq!(res, 150);
    }
}