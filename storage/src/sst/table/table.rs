use std::io::{Result, SeekFrom};
use std::path::PathBuf;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::sst::block::handle::SstBlockHandle;
use crate::sst::block::reader::SstBlockReader;
use crate::sst::filter::SstFilter;

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
}

#[cfg(test)]
mod tests {
    fn test_partition() {}
}
