use std::io::Result;
use std::{io::Write, ops::ShrAssign};

use num_traits::{AsPrimitive, Unsigned};
use tokio::io::AsyncWriteExt;

pub async fn async_write_varint<
    N: Unsigned + ShrAssign<i32> + AsPrimitive<u8> + Copy,
    W: AsyncWriteExt + Unpin,
>(
    mut value: N,
    writer: &mut W,
) -> Result<()> {
    loop {
        let mut byte: u8 = value.as_() & 0b01111111;
        value >>= 7;
        if !value.is_zero() {
            byte |= 0b10000000;
        }
        writer.write_u8(byte).await?;
        if value.is_zero() {
            break;
        }
    }
    Ok(())
}

pub fn write_varint<N: Unsigned + AsPrimitive<u8> + ShrAssign<i32> + Copy, W: Write>(
    mut value: N,
    writer: &mut W,
) -> Result<()> {
    loop {
        let mut byte: u8 = value.as_() & 0b01111111;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn varint_write_test() {
        let mut buffer = Cursor::new(Vec::new());
        write_varint(150u32, &mut buffer).unwrap();
        write_varint(150u64, &mut buffer).unwrap();
        write_varint(150usize, &mut buffer).unwrap();
        assert_eq!(buffer.into_inner(), vec![150, 01, 150, 01, 150, 01]);
    }
}
