use std::io::Result;
use std::mem::replace;
use std::path::PathBuf;
use tokio::fs::{create_dir_all, File};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::sst::block::handle::SstBlockHandle;
use crate::utils::fixedint::write_u64;
use crate::{
    db::options::DbOptions,
    sst::{block::writer::SstBlockWriter, filter::SstFilter},
};

use super::stats::SstStats;
use super::table::SstTable;

pub struct SstTableWriter {
    file_path: PathBuf,
    file_writer: BufWriter<File>,
    written_size: usize,
    db_options: DbOptions,
    block_writer: SstBlockWriter,
    filter: SstFilter,
    index: Vec<(Vec<u8>, SstBlockHandle)>,
    stats: SstStats,
}

impl SstTableWriter {
    pub async fn new(
        file_path: impl Into<PathBuf>,
        item_count: usize,
        db_options: DbOptions,
    ) -> Result<Self> {
        let file_path = file_path.into();
        let file = File::create(&file_path).await?;
        let file = BufWriter::new(file);
        Ok(Self {
            file_path,
            file_writer: file,
            written_size: 0,
            db_options: db_options.clone(),
            block_writer: SstBlockWriter::new(db_options.sst_block_restart_interval),
            filter: SstFilter::new(item_count, 0.01),
            index: Vec::new(),
            stats: SstStats::default(),
        })
    }

    pub async fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.stats.add_entry(key, value);
        if !self.block_writer.is_empty()
            && self.block_writer.estimate_after_append(key, value) > self.db_options.sst_block_size
        {
            self._process_block().await?;
        }
        self.block_writer.append(key, value)?;
        self.filter.add(key);

        Ok(())
    }

    fn _add_handle(&mut self, block_size: usize) -> SstBlockHandle {
        let offset = self.written_size;
        self.written_size += block_size;
        SstBlockHandle {
            offset: offset as u64,
            size: block_size as u64,
        }
    }

    async fn _process_block(&mut self) -> Result<()> {
        let prev_block = replace(
            &mut self.block_writer,
            SstBlockWriter::new(self.db_options.sst_block_restart_interval),
        );

        let (first_key, block) = prev_block.finalize()?;
        let block_handle = self._add_handle(block.len());
        self.index.push((first_key, block_handle));

        self.file_writer.write_all(&block).await?;
        self.written_size += block.len();

        Ok(())
    }

    fn _index_to_block(&self) -> Result<Vec<u8>> {
        let mut block = SstBlockWriter::new(self.db_options.sst_index_restart_interval);
        for (key, handle) in self.index.iter() {
            let handle_value = handle.to_value();
            block.append(key, handle_value.as_slice())?;
        }
        let (_, block) = block.finalize()?;
        Ok(block)
    }

    pub async fn finish(mut self) -> Result<SstTable> {
        self._process_block().await?;

        // Finish filter block
        let filter_block = self.filter.bitvec.data.as_slice();
        self.stats.set_filter_size(filter_block.len());
        self.file_writer.write_all(filter_block).await?;
        let filter_handle = self._add_handle(filter_block.len());

        // Finish index block
        let index_block = self._index_to_block()?;
        self.stats.set_index_size(index_block.len());
        self.file_writer.write_all(&index_block).await?;
        let index_handle = self._add_handle(index_block.len());

        // Finish meta block
        let mut meta_index = SstBlockWriter::new(usize::MAX);
        meta_index.append(b"filter", &filter_handle.to_value())?;
        meta_index.append(b"index", &index_handle.to_value())?;
        let (_, meta_block) = meta_index.finalize()?;
        self.file_writer.write_all(&meta_block).await?;
        let meta_handle = self._add_handle(meta_block.len());

        // Write footer
        let mut footer = Vec::new();
        meta_handle.write(&mut footer)?;
        footer.resize(20, 0);
        write_u64(0x78e50942a7d0c7be, &mut footer)?;
        self.file_writer.write_all(&footer).await?;

        self.file_writer.flush().await?;

        let table = SstTable::new(self.file_path, self.filter, self.index);

        Ok(table)
    }
}
