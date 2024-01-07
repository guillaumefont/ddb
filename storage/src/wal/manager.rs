use std::io::Result;
use std::path::PathBuf;

use crate::{
    manifest::{self, manifest::ManifestRequest},
    wal::entry::WalEntry,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct WalRequest {
    batch: Vec<WalEntry>,
}

pub struct WalManager {
    path: PathBuf,
    seq_num: u32,
    manifest_sender: Sender<ManifestRequest>,
}

impl WalManager {
    pub async fn create(path: PathBuf, manifest_sender: Sender<ManifestRequest>) -> Result<Self> {
        manifest_sender
            .send(ManifestRequest::Append {
                entries: vec![manifest::entry::ManifestLogEntry::WalAddition {
                    log_number: 0,
                    tags: vec![],
                }],
            })
            .await
            .unwrap();
        Ok(Self {
            seq_num: 0,
            path,
            manifest_sender,
        })
    }

    pub async fn load(path: PathBuf, manifest_sender: Sender<ManifestRequest>) -> Result<Self> {
        Ok(Self {
            seq_num: 0,
            path,
            manifest_sender,
        })
    }
}
