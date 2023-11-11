use std::{io::Result, mem::size_of};

use crate::{
    db::options::DbOptions,
    utils::{
        fixedint::fixedint_write,
        varint::{varint_length, varint_write},
    },
};

pub struct SstBlockWriter {
    db_options: DbOptions,
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    estimate: usize,
    counter: usize,
    last_key: Vec<u8>,
}

impl SstBlockWriter {
    pub fn new(db_options: DbOptions) -> Self {
        let restarts = vec![0];
        let estimate = size_of::<u32>() + size_of::<u32>();
        Self {
            db_options,
            buffer: Vec::new(),
            restarts,
            estimate,
            counter: 0,
            last_key: Vec::new(),
        }
    }

    pub fn estimate_after_append(&self, key: &[u8], value: &[u8]) -> usize {
        let mut estimate = self.estimate;
        estimate += key.len() + value.len();

        if self.counter >= self.db_options.block_restart_interval {
            estimate += size_of::<u32>();
        }

        estimate += size_of::<u32>();
        estimate += varint_length(key.len());
        estimate += varint_length(value.len());

        estimate
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        debug_assert!(self.counter <= self.db_options.block_restart_interval);

        let shared = if self.counter >= self.db_options.block_restart_interval {
            self.restarts.push(self.buffer.len() as u32);
            self.estimate += size_of::<u32>();
            self.counter = 0;
            0
        } else {
            slice_shared_offset(&self.last_key, key)
        };

        self.last_key = key.to_vec();
        let non_shared = key.len() - shared;
        let curr_size = self.buffer.len();

        varint_write(shared, &mut self.buffer)?;
        varint_write(non_shared, &mut self.buffer)?;
        varint_write(key.len(), &mut self.buffer)?;

        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(&value);

        self.counter += 1;

        self.estimate = self.buffer.len() - curr_size;

        Ok(())
    }

    pub fn finalize(mut self) -> Result<Vec<u8>> {
        for restart in &self.restarts {
            fixedint_write(*restart, &mut self.buffer)?;
        }
        fixedint_write((&self.restarts).len(), &mut self.buffer)?;

        Ok(self.buffer)
    }
}

fn slice_shared_offset(left: &[u8], right: &[u8]) -> usize {
    let mut offset = 0;
    for (l, r) in left.iter().zip(right.iter()) {
        if l != r {
            break;
        }
        offset += 1;
    }
    offset
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn slice_shared_offset_test() {
        assert_eq!(slice_shared_offset(b"hello", b"world"), 0);
        assert_eq!(slice_shared_offset(b"hello", b"hell"), 4);
        assert_eq!(slice_shared_offset(b"hello", b"hello"), 5);
        assert_eq!(slice_shared_offset(b"hello", b"hello world"), 5);
    }
}
