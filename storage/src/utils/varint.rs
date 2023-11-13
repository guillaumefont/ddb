
use std::io::{Result, Write};

pub fn len_varint_u32(value: u32) -> usize {
    let mut length = 0;
    let mut value = value;
    loop {
        length += 1;
        value >>= 7;
        if value == 0 {
            break;
        }
    }
    length
}

pub fn len_varint_u64(value: u64) -> usize {
    let mut length = 0;
    let mut value = value;
    loop {
        length += 1;
        value >>= 7;
        if value == 0 {
            break;
        }
    }
    length
}

pub fn len_varint_usize(value: usize) -> usize {
    let mut length = 0;
    let mut value = value;
    loop {
        length += 1;
        value >>= 7;
        if value == 0 {
            break;
        }
    }
    length
}

pub fn write_varint_u32<
    W: Write,
>(
    mut value: u32,
    writer: &mut W,
) -> Result<()> {
    loop {
        let mut byte: u8 = (value as u8) & 0b01111111;
        value >>= 7;
        if value != 0 {
            byte |= 0b10000000;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            break;
        }
    }
    Ok(())
}

pub fn write_varint_u64<
    W: Write,
>(
    mut value: u64,
    writer: &mut W,
) -> Result<()> {
    loop {
        let mut byte: u8 = (value as u8) & 0b01111111;
        value >>= 7;
        if value != 0 {
            byte |= 0b10000000;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            break;
        }
    }
    Ok(())
}

pub fn write_varint_usize<
    W: Write,
>(
    mut value: usize,
    writer: &mut W,
) -> Result<()> {
    loop {
        let mut byte: u8 = (value as u8) & 0b01111111;
        value >>= 7;
        if value != 0 {
            byte |= 0b10000000;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            break;
        }
    }
    Ok(())
}

pub fn read_varint_u32<
    R: std::io::Read,
>(
    reader: &mut R,
) -> Result<u32> {
    let mut value = 0u32;
    let mut shift = 0;
    loop {
        let mut byte = [0u8];
        reader.read_exact(&mut byte)?;
        let byte = byte[0];
        value |= ((byte & 0b01111111) as u32) << shift;
        shift += 7;
        if byte & 0b10000000 == 0 {
            break;
        }
    }
    Ok(value)
}

pub fn read_varint_u64<
    R: std::io::Read,
>(
    reader: &mut R,
) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let mut byte = [0u8];
        reader.read_exact(&mut byte)?;
        let byte = byte[0];
        value |= ((byte & 0b01111111) as u64) << shift;
        shift += 7;
        if byte & 0b10000000 == 0 {
            break;
        }
    }
    Ok(value)
}

pub fn read_varint_usize<
    R: std::io::Read,
>(
    reader: &mut R,
) -> Result<usize> {
    let mut value = 0usize;
    let mut shift = 0;
    loop {
        let mut byte = [0u8];
        reader.read_exact(&mut byte)?;
        let byte = byte[0];
        value |= ((byte & 0b01111111) as usize) << shift;
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
        write_varint_u32(150u32, &mut buffer).unwrap();
        write_varint_u64(150u64, &mut buffer).unwrap();
        write_varint_usize(150usize, &mut buffer).unwrap();
        assert_eq!(
            buffer.into_inner(),
            vec![150, 01, 150, 01, 150, 01]
        );
    }

    #[test]
    fn varint_read_test() {
        let buffer = vec![150, 1];
        let mut reader = Cursor::new(buffer);
        let res: u64 = read_varint_u64(&mut reader).unwrap();
        assert_eq!(res, 150);
    }

    #[test]
    fn varint_length_test() {
        assert_eq!(len_varint_u32(150u32), 2);
        assert_eq!(len_varint_u64(150u64), 2);
        assert_eq!(len_varint_usize(150usize), 2);
    }
}
