use std::io::Result;
use std::path::PathBuf;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};
use tracing::instrument;

use super::entry::ManifestLogEntry;

#[derive(Debug)]
pub struct ManifestWriter {
    seq_num: u64,
    writer: BufWriter<File>,
}

impl ManifestWriter {
    pub async fn new<P: Into<PathBuf>>(seq_num: u64, path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.into())
            .await?;
        let writer = BufWriter::new(file);
        Ok(Self { seq_num, writer })
    }

    #[instrument]
    pub async fn append(&mut self, entries: Vec<ManifestLogEntry>) -> Result<()> {
        for entry in entries {
            entry.write(&mut self.writer).await?;
        }
        self.writer.flush().await?;
        Ok(())
    }
}
