use std::io::Result;
use std::mem::size_of;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::db::options::DbOptions;
use crate::utils::crc32::Crc32;

#[repr(u8)]
enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

const HEADER_SIZE: usize = size_of::<u32>() + size_of::<u16>() + size_of::<u8>();

pub struct Wal {
    seq_num: u32,
    file: File,
    options: DbOptions,
    remaining_block_size: usize,
}

impl Wal {
    pub async fn create(seq_num: u32, path: PathBuf, options: DbOptions) -> Result<Self> {
        let path = path.join(format!("WAL-{}", seq_num));
        println!("wal path {}", path.to_str().unwrap());
        let file = File::create(path).await?;
        let remaining_block_size = options.wal_block_size;
        Ok(Self {
            seq_num,
            file,
            remaining_block_size,
            options,
        })
    }

    pub async fn load(seq_num: u32, path: PathBuf, options: DbOptions) -> Result<Self> {
        let path = path.join(format!("WAL-{}", seq_num));
        let file = File::open(path).await?;
        let remaining_block_size = options.wal_block_size;
        Ok(Self {
            seq_num,
            file,
            remaining_block_size,
            options,
        })
    }

    fn _record_type(
        data_size: usize,
        written_size: usize,
        available_payload_size: usize,
    ) -> RecordType {
        let is_first = written_size == 0;
        let can_finish = available_payload_size >= (data_size - written_size);
        match (is_first, can_finish) {
            (true, true) => RecordType::Full,
            (true, false) => RecordType::First,
            (false, false) => RecordType::Middle,
            (false, true) => RecordType::Last,
        }
    }

    pub async fn append(&mut self, data: &[u8]) -> Result<()> {
        let data_size = data.len();
        let mut written_size = 0;
        // let mut remaining_data_size = data_size;

        while written_size < data_size {
            let available_payload_size = self.remaining_block_size - HEADER_SIZE;
            let written_payload_size = if available_payload_size > (data_size - written_size) {
                data_size - written_size
            } else {
                available_payload_size
            };
            let writable_slice = &data[written_size..(written_size + written_payload_size)];
            let record_type = Self::_record_type(data_size, written_size, available_payload_size);

            let crc = Crc32::hash(writable_slice);
            self.file.write_u32(crc).await?;
            self.file.write_u16(written_payload_size as u16).await?;
            self.file.write_u8(record_type as u8).await?;
            self.file.write_all(writable_slice).await?;

            written_size += written_payload_size;
            self.remaining_block_size -= written_payload_size + HEADER_SIZE;

            if self.remaining_block_size < HEADER_SIZE {
                if self.remaining_block_size > 0 {
                    let padding = vec![0u8; self.remaining_block_size];
                    self.file.write_all(&padding).await?;
                }
                self.remaining_block_size = self.options.wal_block_size;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs::create_dir_all;

    #[tokio::test]
    pub async fn wal_test() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let mut wal = Wal::create(0, path, DbOptions::default()).await.unwrap();
        wal.append(b"Hello world").await.unwrap();
    }

    #[tokio::test]
    pub async fn wal_long() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let mut wal = Wal::create(0, path, DbOptions::default()).await.unwrap();
        wal.append(vec![b'a'; 1000].as_slice()).await.unwrap();
        wal.append(vec![b'b'; 97270].as_slice()).await.unwrap();
        wal.append(vec![b'c'; 8000].as_slice()).await.unwrap();
    }
}
