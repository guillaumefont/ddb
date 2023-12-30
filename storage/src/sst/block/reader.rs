use std::{
    io::{Cursor, Read, Result, Seek, SeekFrom},
    mem::size_of,
};
use tracing::{instrument, span, Level};

use crate::utils::{fixedint::read_u32, varint::read::read_varint};

#[derive(Debug)]
pub struct SstBlockReader {
    block: Vec<u8>,
    restarts: Vec<u32>,
}

impl SstBlockReader {
    pub fn new(block: Vec<u8>) -> Result<Self> {
        let block_len = block.len();
        let mut cursor = Cursor::new(block);
        cursor.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;
        let restart_count = read_u32(&mut cursor)?;
        let mut restarts = Vec::with_capacity(restart_count as usize);
        restarts.push(0);
        let footer_offset = block_len - size_of::<u32>() * (restart_count as usize + 1);
        cursor.seek(SeekFrom::Start(footer_offset as u64))?;
        for _ in 0..restart_count {
            restarts.push(read_u32(&mut cursor)?);
        }
        let block = cursor.into_inner();
        Ok(Self { block, restarts })
    }

    #[instrument]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.iter_from(key)
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.to_vec())
    }

    pub fn iter_from<'a>(&'a self, from: &'a [u8]) -> SstBlockIterator<'a> {
        SstBlockIterator::new_from(self, from)
    }

    pub fn iter<'a>(&'a self) -> SstBlockIterator<'a> {
        SstBlockIterator::new(self)
    }
}

pub struct SstBlockIterator<'a> {
    slice: &'a [u8],
    cursor: Cursor<&'a [u8]>,
    key: Vec<u8>,
    block_len: usize,
}

impl<'a> SstBlockIterator<'a> {
    pub fn new_from(reader: &'a SstBlockReader, from: &'a [u8]) -> Self {
        let block_len = reader.block.len() - size_of::<u32>() * reader.restarts.len();
        let slice = &reader.block[0..block_len];
        let mut cursor = Cursor::new(slice);
        let mut key = Vec::new();

        let restart_count = reader.restarts.len();

        let mut left = 0;
        let mut right = restart_count;

        while left + 1 < right {
            let mid = (left + right) / 2;
            cursor
                .seek(SeekFrom::Start(reader.restarts[mid] as u64))
                .unwrap();

            let shared: usize = read_varint(&mut cursor).unwrap();
            let non_shared: usize = read_varint(&mut cursor).unwrap();
            let _: usize = read_varint(&mut cursor).unwrap();
            debug_assert!(shared == 0);
            let mut restart_key = vec![9; non_shared];
            cursor.read_exact(&mut restart_key[0..non_shared]).unwrap();
            if restart_key.as_slice() <= from {
                left = mid;
            } else {
                right = mid;
            }
        }

        if left == restart_count {
            cursor.seek(SeekFrom::End(0)).unwrap();
        } else {
            let mut pos = cursor
                .seek(SeekFrom::Start(reader.restarts[left] as u64))
                .unwrap();

            loop {
                if pos as usize == block_len {
                    cursor.seek(SeekFrom::End(0)).unwrap();
                    break;
                }
                let shared: usize = read_varint(&mut cursor).unwrap();
                let non_shared: usize = read_varint(&mut cursor).unwrap();
                let value_len: usize = read_varint(&mut cursor).unwrap();

                let mut next_key = vec![0; shared + non_shared];
                // copy [shared] bytes from key to prev_key
                next_key[0..shared].copy_from_slice(&key[0..shared]);
                cursor
                    .read_exact(&mut next_key[shared..(shared + non_shared)])
                    .unwrap();

                if next_key.as_slice() >= from {
                    // Backtrack
                    cursor.seek(SeekFrom::Start(pos)).unwrap();
                    break;
                } else {
                    pos = cursor.seek(SeekFrom::Current(value_len as i64)).unwrap();
                    key = next_key;
                }
            }
        };

        Self {
            slice,
            cursor,
            key,
            block_len,
        }
    }

    pub fn new(reader: &'a SstBlockReader) -> Self {
        let block_len = reader.block.len() - size_of::<u32>() * reader.restarts.len();
        let slice = &reader.block[0..block_len];
        let cursor = Cursor::new(slice);
        Self {
            slice,
            cursor,
            key: Vec::new(),
            block_len,
        }
    }
}

impl<'a> Iterator for SstBlockIterator<'a> {
    type Item = (Vec<u8>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.position() as usize == self.block_len {
            return None;
        }

        let shared: usize = read_varint(&mut self.cursor).unwrap();
        let non_shared: usize = read_varint(&mut self.cursor).unwrap();
        let value_len: usize = read_varint(&mut self.cursor).unwrap();

        // let mut key = self.key.to_vec();
        self.key.resize(shared + non_shared, 0);
        self.cursor
            .read_exact(&mut self.key[shared..shared + non_shared])
            .unwrap();

        let value_pos = self.cursor.position() as usize;
        self.cursor
            .seek(SeekFrom::Current(value_len as i64))
            .unwrap();
        let value = &self.slice[value_pos..value_pos + value_len];

        Some((self.key.clone(), value))
    }
}
