use std::io::{Result, SeekFrom};
use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::Stream;
use tracing::{debug, event, info, instrument, Level};

use crate::sst::block::handle::{block_from_handle, SstBlockHandle};
use crate::sst::block::reader::SstBlockReader;
use crate::sst::filter::SstFilter;

use async_stream::try_stream;

#[derive(Debug)]
pub struct SstTable {
    path: PathBuf,
    filter: SstFilter,
    index: Vec<(Vec<u8>, SstBlockHandle)>,
}

impl SstTable {
    pub fn new<P: Into<PathBuf>>(
        path: P,
        filter: SstFilter,
        index: Vec<(Vec<u8>, SstBlockHandle)>,
    ) -> Self {
        Self {
            path: path.into(),
            filter,
            index,
        }
    }

    #[instrument]
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        debug!(key = String::from_utf8(key.to_vec()).unwrap());
        if !self.filter.may_contain(key) {
            return Ok(None);
        }
        for (k, _) in &self.index {
            event!(
                Level::DEBUG,
                "index: {}",
                String::from_utf8(k.to_vec()).unwrap()
            );
        }
        let partition_point = self.index.partition_point(|(k, _)| k.as_slice() <= key);
        event!(Level::DEBUG, "partition_point: {}", partition_point);
        match partition_point {
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

    #[instrument]
    pub async fn iter_from<'a>(
        &'a self,
        from: &'a [u8],
    ) -> impl Stream<Item = Result<(Vec<u8>, Vec<u8>)>> + '_ {
        let mut file = File::open(&self.path).await.unwrap();
        let partitioned = self.index.partition_point(|(k, _)| k.as_slice() < from) - 1;
        debug!("partitioned: {}", partitioned);
        try_stream! {
            for (_, handle) in &self.index[partitioned..] {
                let block = block_from_handle(&mut file, handle).await?;
                let reader = SstBlockReader::new(block)?;
                for (key, value) in reader.iter_from(from) {
                    yield (key, value.to_vec())
                }
            }
        }
    }

    #[instrument]
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

    use crate::utils::tracing::init_tracer;
    use crate::{db::options::DbOptions, sst::table::writer::SstTableWriter};

    use super::*;
    use futures_util::pin_mut;
    use std::io::Result;
    use tempfile::NamedTempFile;
    use tokio_stream::StreamExt;
    use tracing::{info_span, instrument};

    use tracing::Instrument;

    #[instrument]
    async fn filled_table<P: Into<PathBuf> + std::fmt::Debug>(
        file_path: P,
        count: usize,
        options: DbOptions,
    ) -> Result<SstTable> {
        let count_digit = (count - 1).to_string().len();
        let mut writer = SstTableWriter::new(file_path, count, options).await?;
        for i in 0..count {
            let key = format!("foo{:0>count_digit$}", i);
            writer.add(key.as_bytes(), key.as_bytes()).await?;
        }

        let res = writer.finish().await?;

        Ok(res)
    }

    #[tokio::test]
    async fn read_write() {
        init_tracer();

        let span = info_span!("read_write");
        async move {
            let file_path = NamedTempFile::new().unwrap();
            let table = filled_table(file_path.path(), 1000, DbOptions::default())
                .await
                .unwrap();

            let res = table.get(b"foo382").await.unwrap();

            assert!(res.is_some());
            assert_eq!(res.unwrap(), b"foo382");

            let res2 = table.get(b"foo383").await.unwrap();

            assert!(res2.is_some());
            assert_eq!(res2.unwrap(), b"foo383");

            let res3 = table.get(b"foo384").await.unwrap();
            assert!(res3.is_some());
            assert_eq!(res3.unwrap(), b"foo384");

            let res4 = table.get(b"abc").await.unwrap();
            assert!(res4.is_none());

            let res2 = table.get(b"bar").await.unwrap();
            assert!(res2.is_none());
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn iter_write() {
        init_tracer();

        let span = info_span!("iter_write");
        async move {
            let file_path = NamedTempFile::new().unwrap();
            let table_size = 10000;
            let table = filled_table(file_path.path(), table_size, DbOptions::default())
                .await
                .unwrap();

            let iter = table.iter().await;
            let mut i = 0;
            pin_mut!(iter);
            while let Some(Ok((key, value))) = iter.next().await {
                let test = format!("foo{:0>4}", i);
                assert_eq!(key, test.as_bytes());
                assert_eq!(value, test.as_bytes());
                i += 1;
            }
            assert_eq!(i, table_size);
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn iter_from_write() {
        init_tracer();

        let span = info_span!("iter_from_write");
        async move {
            let file_path = NamedTempFile::new().unwrap();
            let table_size = 1000;
            let options = DbOptions {
                sst_block_size: 4096,
                ..Default::default()
            };

            let table = filled_table(file_path.path(), table_size, options)
                .await
                .unwrap();

            let iter = table.iter_from(b"foo567").await;
            let mut i = 567;
            pin_mut!(iter);
            while let Some(Ok((key, value))) = iter.next().await {
                let test = format!("foo{:0>3}", i);
                assert_eq!(String::from_utf8(key).unwrap(), test);
                assert_eq!(String::from_utf8(value).unwrap(), test);
                i += 1;
            }
            assert_eq!(i, table_size);
        }
        .instrument(span)
        .await;
    }
}
