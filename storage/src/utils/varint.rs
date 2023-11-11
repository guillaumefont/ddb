
use num_traits::{AsPrimitive, FromPrimitive, ToPrimitive, Unsigned};
use std::io::{Result, Write};
use std::ops::{BitAnd, BitOrAssign, Shl, ShrAssign};

pub fn varint_length<N: Unsigned + ShrAssign<u8>>(value: N) -> usize {
    let mut length = 0;
    let mut value = value;
    loop {
        length += 1;
        value >>= 7;
        if value.is_zero() {
            break;
        }
    }
    length
}

pub fn varint_write<
    W: Write,
    N: AsPrimitive<u8> + ToPrimitive + Copy + Unsigned + ShrAssign<u8> + BitAnd<N, Output = N>,
>(
    mut value: N,
    writer: &mut W,
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

pub fn varint_read<
    R: std::io::Read,
    N: FromPrimitive + Copy + Unsigned + ShrAssign<u8> + BitAnd + Shl<i32, Output = N> + BitOrAssign,
>(
    reader: &mut R,
) -> Result<N> {
    let mut value = N::zero();
    let mut shift = 0;
    loop {
        let mut byte = [0u8];
        reader.read_exact(&mut byte)?;
        let byte = byte[0];
        value |= N::from_u8(byte & 0b01111111).unwrap() << shift;
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
    fn varint_write_test() {
        let mut buffer = Cursor::new(Vec::new());
        varint_write(150u32, &mut buffer).unwrap();
        varint_write(150u64, &mut buffer).unwrap();
        varint_write(150u128, &mut buffer).unwrap();
        varint_write(150usize, &mut buffer).unwrap();
        varint_write(150u8, &mut buffer).unwrap();
        varint_write(150u16, &mut buffer).unwrap();
        assert_eq!(
            buffer.into_inner(),
            vec![150, 01, 150, 01, 150, 01, 150, 01, 150, 01, 150, 01]
        );
    }

    #[test]
    fn varint_read_test() {
        let buffer = vec![150, 1];
        let mut reader = Cursor::new(buffer);
        let res: u64 = varint_read(&mut reader).unwrap();
        assert_eq!(res, 150);
    }

    #[test]
    fn varint_length_test() {
        assert_eq!(varint_length(150u32), 2);
        assert_eq!(varint_length(150u64), 2);
        assert_eq!(varint_length(150u128), 2);
        assert_eq!(varint_length(150usize), 2);
        assert_eq!(varint_length(150u8), 2);
        assert_eq!(varint_length(150u16), 2);
    }
}
