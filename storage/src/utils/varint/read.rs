use std::{
    io::Read,
    io::Result,
    ops::{BitOrAssign, Shl},
};

use num_traits::{FromPrimitive, Unsigned};
use tokio::io::{AsyncRead, AsyncReadExt};

pub fn read_varint<
    N: Unsigned + FromPrimitive + Shl<i32, Output = N> + BitOrAssign + Copy,
    R: Read,
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

pub async fn async_read_varint<
    N: Unsigned + FromPrimitive + Shl<i32, Output = N> + BitOrAssign + Copy,
    R: AsyncReadExt + Unpin,
>(
    reader: &mut R,
) -> Result<N> {
    let mut value = N::zero();
    let mut shift = 0;
    loop {
        let mut byte = [0u8];
        reader.read_exact(&mut byte).await?;
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
    fn varint_read_test() {
        let buffer = vec![150, 1];
        let mut reader = Cursor::new(buffer);
        let res: u64 = read_varint(&mut reader).unwrap();
        assert_eq!(res, 150);
    }
}
