use std::io::{Cursor, Read, Result};
use std::path::PathBuf;

use crate::db::options::DbOptions;
use crate::utils::fixedint::write_u64;
use crate::{
    manifest::{self, manifest::ManifestRequest},
    utils::fixedint::write_u32,
    wal::entry::WalEntry,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::log::Wal;

pub struct WalRequest {
    seq_num: u64,
    entries: Vec<WalEntry>,
}

impl WalRequest {
    pub fn new(seq_num: u64, entries: Vec<WalEntry>) -> Self {
        Self { seq_num, entries }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut writer = Cursor::new(Vec::new());
        write_u64(self.seq_num, &mut writer).unwrap();
        write_u32(self.entries.len() as u32, &mut writer).unwrap();
        for entry in &self.entries {
            entry.write(&mut writer).unwrap();
        }
        writer.into_inner()
    }
}

pub struct WalManager {
    path: PathBuf,
    seq_num: u32,
    manifest_sender: Sender<ManifestRequest>,
    wal_receiver: Receiver<WalRequest>,
    current_wal: Wal,
    options: DbOptions,
}

impl WalManager {
    pub async fn create(
        path: PathBuf,
        manifest_sender: Sender<ManifestRequest>,
        wal_receiver: Receiver<WalRequest>,
        options: DbOptions,
    ) -> Result<Self> {
        manifest_sender
            .send(ManifestRequest::Append {
                entries: vec![manifest::entry::ManifestLogEntry::WalAddition {
                    log_number: 0,
                    tags: vec![],
                }],
            })
            .await
            .unwrap();

        let current_wal = Wal::create(0, path.clone(), options.clone()).await?;
        Ok(Self {
            seq_num: 0,
            path,
            manifest_sender,
            wal_receiver,
            current_wal,
            options,
        })
    }

    pub async fn load(
        path: PathBuf,
        manifest_sender: Sender<ManifestRequest>,
        wal_receiver: Receiver<WalRequest>,
        options: DbOptions,
    ) -> Result<Self> {
        let current_wal = Wal::load(0, path.clone(), options.clone()).await?;
        Ok(Self {
            seq_num: 0,
            path,
            manifest_sender,
            wal_receiver,
            current_wal,
            options,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Some(msg) = self.wal_receiver.recv().await {
            let vec = msg.to_vec();
            self.current_wal.append(&vec).await?;
        }
        Ok(())
    }
}
