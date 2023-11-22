use std::io::{Result, SeekFrom};
use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::Stream;

use crate::sst::block::handle::{block_from_handle, SstBlockHandle};
use crate::sst::block::reader::SstBlockReader;
use crate::sst::filter::SstFilter;

use async_stream::try_stream;

pub struct SstTable {
    path: PathBuf,
    filter: SstFilter,
    index: Vec<(Vec<u8>, SstBlockHandle)>,
}

impl SstTable {
    pub fn new(
        path: impl Into<PathBuf>,
        filter: SstFilter,
        index: Vec<(Vec<u8>, SstBlockHandle)>,
    ) -> Self {
        Self {
            path: path.into(),
            filter,
            index,
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        println!("get path: {:?}", self.path);
        if !self.filter.may_contain(key) {
            return Ok(None);
        }
        match self.index.partition_point(|(k, _)| k.as_slice() <= key) {
            0 => Ok(None),
            i => {
                let (_, handle) = &self.index[i - 1];
                let mut block = vec![0; handle.size as usize];
                let mut file = File::open(&self.path).await?;
                file.seek(SeekFrom::Start(handle.offset)).await?;
                file.read_exact(&mut block).await?;
                let reader = SstBlockReader::new(block)?;
                Ok(reader.get(key))
            }
        }
    }

    pub async fn iter(&self) -> impl Stream<Item = Result<(Vec<u8>, Vec<u8>)>> + '_ {
        let mut file = File::open(&self.path).await.unwrap();
        try_stream! {
            for (_, handle) in &self.index {
                let block = block_from_handle(&mut file, handle).await?;
                let reader = SstBlockReader::new(block)?;
                for (key, value) in reader.iter() {
                    yield (key, value.to_vec())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{db::options::DbOptions, sst::table::writer::SstTableWriter};

    use super::*;
    use futures_util::pin_mut;
    use std::io::Result;
    use tempfile::NamedTempFile;
    use tokio_stream::StreamExt;

    async fn filled_table(file_path: impl Into<PathBuf>, count: usize) -> Result<SstTable> {
        let count_digit = (count - 1).to_string().len();
        println!("count_digit: {}", count_digit);
        let mut writer = SstTableWriter::new(file_path, 100, DbOptions::default()).await?;
        for i in 0..count {
            let key = format!("foo{:0>count_digit$}", i);
            writer.add(key.as_bytes(), key.as_bytes()).await?;
        }
        Ok(writer.finish().await?)
    }

    #[tokio::test]
    async fn read_write() {
        let file_path = NamedTempFile::new().unwrap();
        let table = filled_table(file_path.path(), 1000).await.unwrap();

        let res = table.get(b"foo567").await.unwrap();

        assert!(res.is_some());
        assert_eq!(res.unwrap(), b"foo567");

        let res2 = table.get(b"bar").await.unwrap();
        assert!(res2.is_none());
    }

    #[tokio::test]
    async fn iter_write() {
        let file_path = NamedTempFile::new().unwrap();
        let table_size = 1000000;
        let table = filled_table(file_path.path(), table_size).await.unwrap();

        let iter = table.iter().await;
        let mut i = 0;
        pin_mut!(iter);
        while let Some(Ok((key, value))) = iter.next().await {
            let test = format!("foo{:0>6}", i);
            assert_eq!(key, test.as_bytes());
            assert_eq!(value, test.as_bytes());
            i += 1;
        }
        assert_eq!(i, table_size);
    }
}
