use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
};

use crate::sst::block::{handle::SstBlockHandle, reader::SstBlockReader};

use super::table::SstTable;
use std::{
    collections::BTreeMap,
    io::{Cursor, Result, SeekFrom},
    mem::size_of,
    path::PathBuf,
};

pub async fn sst_table_writer_new(path: impl Into<PathBuf>) -> Result<SstTable> {
    let file = File::open(path.into()).await?;
    let mut file_reader = BufReader::new(file);

    let footer_len = 20 + size_of::<i64>();
    file_reader
        .seek(SeekFrom::End(-(footer_len as i64)))
        .await?;
    let meta_handle = SstBlockHandle::async_read_from(&mut file_reader).await?;

    let mut meta_block = vec![0; meta_handle.size as usize];
    file_reader
        .seek(SeekFrom::Start(meta_handle.offset))
        .await?;
    file_reader.read_exact(&mut meta_block).await?;

    let meta_reader = SstBlockReader::new(meta_block)?;

    let mut meta_index: BTreeMap<String, Cursor<&[u8]>> = meta_reader
        .iter()
        .map(|(key, value)| (String::from_utf8(key).unwrap(), Cursor::new(value)))
        .collect();

    let filter_handle = SstBlockHandle::read_from(meta_index.get_mut("filter").unwrap());

    let index_handle = SstBlockHandle::read_from(meta_index.get_mut("index").unwrap());

    Err(())
}
