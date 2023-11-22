use tokio::{
    fs::File,
    io::{AsyncSeekExt, BufReader},
};

use crate::sst::{
    block::{
        handle::{block_from_handle, SstBlockHandle},
        reader::SstBlockReader,
    },
    filter::SstFilter,
};

use super::table::SstTable;
use std::{
    collections::BTreeMap,
    io::{Cursor, Result, SeekFrom},
    mem::size_of,
    path::Path,
};

pub async fn sst_table_writer_new(path: impl AsRef<Path>) -> Result<SstTable> {
    let file = File::open(path.as_ref()).await?;
    let mut file_reader = BufReader::new(file);

    let footer_len = 20 + size_of::<i64>();
    file_reader
        .seek(SeekFrom::End(-(footer_len as i64)))
        .await?;
    let meta_handle = SstBlockHandle::async_read_from(&mut file_reader).await?;

    let meta_block = block_from_handle(&mut file_reader, &meta_handle).await?;
    let meta_reader = SstBlockReader::new(meta_block)?;
    let mut meta_index: BTreeMap<String, Cursor<&[u8]>> = meta_reader
        .iter()
        .map(|(key, value)| (String::from_utf8(key.to_vec()).unwrap(), Cursor::new(value)))
        .collect();

    let filter_handle = SstBlockHandle::read_from(meta_index.get_mut("filter").unwrap())?;
    let filter_block = block_from_handle(&mut file_reader, &filter_handle).await?;
    let fiter = SstFilter::from_data(&filter_block, 7);

    let index_handle = SstBlockHandle::read_from(meta_index.get_mut("index").unwrap())?;
    let index_block = block_from_handle(&mut file_reader, &index_handle).await?;
    let index: Vec<(Vec<u8>, SstBlockHandle)> = SstBlockReader::new(index_block)?
        .iter()
        .map(|(k, v)| {
            (
                k.to_vec(),
                SstBlockHandle::read_from(&mut Cursor::new(v)).unwrap(),
            )
        })
        .collect();

    let table = SstTable::new(path.as_ref(), fiter, index);

    Ok(table)
}
