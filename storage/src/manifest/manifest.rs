use std::io::Result;
use std::path::PathBuf;
use tokio::fs::{read_to_string, write};
use tokio::sync::mpsc::Receiver;

use tracing::{info, instrument};

use super::entry::ManifestLogEntry;
use super::writer::ManifestWriter;

pub enum ManifestRequest {
    Append { entries: Vec<ManifestLogEntry> },
    Close,
}

#[derive(Debug)]
pub struct Manifest {
    seq_num: u64,
    path: PathBuf,
    current: ManifestWriter,
    receiver: Receiver<ManifestRequest>,
}

impl Manifest {
    #[instrument]
    pub async fn create(path: PathBuf, receiver: Receiver<ManifestRequest>) -> Result<Self> {
        let seq_num = 0;

        let current_name = "MANIFEST-0";

        let current_path = path.join("CURRENT");
        write(current_path, current_name).await?;
        info!("Created manifest with seq_num: 0");

        let current = ManifestWriter::new(seq_num, path.join(current_name)).await?;

        Ok(Self {
            seq_num,
            path,
            current,
            receiver,
        })
    }

    #[instrument]
    pub async fn load(path: PathBuf, receiver: Receiver<ManifestRequest>) -> Result<Self> {
        let current_path = path.join("CURRENT");
        let current_name = read_to_string(current_path).await?;

        let seq_num = current_name[9..].parse::<u64>().unwrap();
        info!("Loaded manifest with seq_num: {}", seq_num);

        let current = ManifestWriter::new(seq_num, path.join(current_name)).await?;

        Ok(Self {
            seq_num,
            path,
            current,
            receiver,
        })
    }

    #[instrument]
    pub async fn append(&mut self, entries: Vec<ManifestLogEntry>) -> Result<()> {
        self.current.append(entries).await?;
        Ok(())
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                ManifestRequest::Append { entries } => {
                    self.append(entries).await.unwrap();
                }
                ManifestRequest::Close => {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::entry::WalTag;

    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;
    use tempfile::tempdir_in;

    #[tokio::test]
    async fn test_manifest_create() {
        let dir = tempdir_in("tmp").unwrap();
        let path = PathBuf::from(dir.path());
        let (sender, receiver) = tokio::sync::mpsc::channel(1024);
        let mut manifest = Manifest::create(path.clone(), receiver).await.unwrap();
        sender
            .send(ManifestRequest::Append {
                entries: vec![
                    ManifestLogEntry::DbId {
                        db_id: "test2".to_string(),
                    },
                    ManifestLogEntry::WalAddition {
                        log_number: 0,
                        tags: vec![WalTag::SyncedSize { size: 0 }],
                    },
                ],
            })
            .await
            .unwrap();
        sender.send(ManifestRequest::Close).await.unwrap();
        manifest.run().await;
        assert_eq!(manifest.seq_num, 0);
    }
}
