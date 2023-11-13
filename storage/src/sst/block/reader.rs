use std::{io::{Cursor, Seek, Result, SeekFrom, Read}, mem::size_of};

use crate::utils::{fixedint::read_fixed_u32, varint::read_varint_usize};


pub struct SstBlockReader {
    block: Vec<u8>,
    restarts: Vec<u32>,
}

impl SstBlockReader {
    pub fn new(block: Vec<u8>) -> Result<Self> {
        let block_len = block.len();
        let mut cursor = Cursor::new(block);
        cursor.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;
        let restart_count = read_fixed_u32(&mut cursor)?;
        let mut restarts = Vec::with_capacity(restart_count as usize);
        restarts.push(0);
        let footer_offset = block_len - size_of::<u32>() * (restart_count as usize + 1);
        cursor.seek(SeekFrom::Start(footer_offset as u64))?;
        for _ in 0..restart_count {
            restarts.push(read_fixed_u32(&mut cursor)?);
        }
        let block = cursor.into_inner();
        Ok(Self {
            block,
            restarts,
        })
    }

    pub fn iter<'a>(&'a self) -> SstBlockIterator<'a> {
        SstBlockIterator::new(self)
    }
}

pub struct SstBlockIterator<'a> {
    cursor: Cursor<&'a [u8]>,
    prev_key: Vec<u8>,
    block_len: usize,
}

impl<'a> SstBlockIterator<'a> {
    pub fn new(reader: &'a SstBlockReader) -> Self {
        let block_len = reader.block.len() - size_of::<u32>() * reader.restarts.len();
        let slice = &reader.block[0..block_len];
        let cursor = Cursor::new(slice);
        let prev_key = Vec::new();
        Self {
            cursor,
            prev_key,
            block_len
        }
    }

}

impl<'a> Iterator for SstBlockIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.position() as usize == self.block_len {
            return None
        }

        let shared = read_varint_usize(&mut self.cursor).unwrap();
        let non_shared = read_varint_usize(&mut self.cursor).unwrap();
        let value_len = read_varint_usize(&mut self.cursor).unwrap();

        println!("shared: {}, non_shared: {}, value_len: {}", shared, non_shared, value_len);

        let mut key = self.prev_key.to_vec();
        key.resize(shared + non_shared, 0);
        self.cursor.read_exact( &mut key[shared..shared + non_shared]).unwrap();

        let mut value = vec![0; value_len];
        self.cursor.read_exact(&mut value).unwrap();

        self.prev_key = key.clone();

        Some((key, value))
    }
}