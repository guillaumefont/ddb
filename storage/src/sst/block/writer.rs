use std::{io::Result, mem::size_of};

use crate::{
    db::options::DbOptions,
    utils::{
        fixedint::write_fixed_u32, varint::{len_varint_u32, len_varint_usize, write_varint_usize},
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
        estimate += len_varint_usize(key.len());
        estimate += len_varint_usize(value.len());

        estimate
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        debug_assert!(self.counter <= self.db_options.block_restart_interval);
        debug_assert!(key > &self.last_key);

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

        write_varint_usize(shared, &mut self.buffer)?;
        write_varint_usize(non_shared, &mut self.buffer)?;
        write_varint_usize(key.len(), &mut self.buffer)?;

        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(&value);

        self.counter += 1;

        self.estimate = self.buffer.len() - curr_size;

        Ok(())
    }

    pub fn finalize(mut self) -> Result<Vec<u8>> {
        for restart in &self.restarts {
            write_fixed_u32(*restart, &mut self.buffer)?;
        }
        write_fixed_u32((&self.restarts).len() as u32, &mut self.buffer)?;

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

    use crate::sst::block::reader;

    use super::*;

    #[test]
    fn slice_shared_offset_test() {
        assert_eq!(slice_shared_offset(b"hello", b"world"), 0);
        assert_eq!(slice_shared_offset(b"hello", b"hell"), 4);
        assert_eq!(slice_shared_offset(b"hello", b"hello"), 5);
        assert_eq!(slice_shared_offset(b"hello", b"hello world"), 5);
    }

    #[test]
    fn read_write() {
        let mut writer = SstBlockWriter::new(DbOptions::default());
        writer.append(b"hello0", b"world0").unwrap();
        writer.append(b"hello1", b"world1").unwrap();
        writer.append(b"hello2", b"world2").unwrap();

        let block = writer.finalize().unwrap();

        let reader = reader::SstBlockReader::new(block).unwrap();
        let mut iter = reader.iter();

        let (key0, value0) = iter.next().unwrap();
        assert_eq!(key0, b"hello0");
        assert_eq!(value0, b"world0");

        let (key1, value1) = iter.next().unwrap();
        assert_eq!(key1, b"hello1");
        assert_eq!(value1, b"world1");

        let (key2, value2) = iter.next().unwrap();
        assert_eq!(key2, b"hello2");
        assert_eq!(value2, b"world2");

        assert_eq!(iter.next(), None);
        
    }
}
