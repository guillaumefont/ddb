use tokio::fs::{create_dir_all, read_to_string, write};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;
use uuid::Uuid;

use crate::db::options::DbOptions;
use crate::manifest::entry::ManifestLogEntry;
use crate::manifest::manifest::{Manifest, ManifestRequest};
use crate::wal::entry::WalEntry;
use crate::wal::manager::{WalManager, WalRequest};
use std::fmt::Debug;
use std::io::Result;
use std::path::PathBuf;
use tracing::instrument;

pub enum DbCmd {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Into<WalEntry> for DbCmd {
    fn into(self) -> WalEntry {
        match self {
            DbCmd::Set { key, value } => WalEntry::Set { key, value },
            DbCmd::Delete { key } => WalEntry::Delete { key },
        }
    }
}

pub struct Db {
    id: Uuid,
    path: PathBuf,
    options: DbOptions,
    manifest: Manifest,
    wal: WalManager,
    seq_num: u64,
    wal_sender: Sender<WalRequest>,
    db_receiver: Receiver<DbCmd>,
}

impl Db {
    #[instrument]
    pub async fn open<P: Into<PathBuf> + Debug>(
        path: P,
        options: DbOptions,
    ) -> Result<(Self, Sender<DbCmd>)> {
        let path = path.into();
        create_dir_all(&path).await?;
        // let manifest = Manifest::open(path.clone()).await?;
        info!("Opening database");

        let (id, created) = Self::open_identity(&path).await?;

        let (manifest, manifest_sender) = Self::open_manifest(&path, created, id).await?;

        let (wal, wal_sender) =
            Self::open_wal(&path, options.clone(), created, manifest_sender).await?;

        let (db_sender, db_receiver) = channel(1024);

        let db = Self {
            id,
            path,
            options,
            manifest,
            wal,
            seq_num: 0,
            wal_sender,
            db_receiver,
        };
        Ok((db, db_sender))
    }

    #[instrument]
    async fn open_identity(path: &PathBuf) -> Result<(Uuid, bool)> {
        let identity_path = path.join("IDENTITY");
        if !identity_path.exists() {
            info!("Creating new database");
            let id = Uuid::new_v4();
            write(identity_path, id.to_string().as_bytes()).await?;
            info!("Id written to file: {}", id);
            Ok((id, true))
        } else {
            info!("Opening existing database");
            let id = read_to_string(identity_path).await?;
            let id = Uuid::parse_str(&id).unwrap();
            info!("Loaded id from file: {}", id);
            Ok((id, false))
        }
    }

    pub async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    pub async fn set<'a>(&mut self, key: &'a [u8], value: &'a [u8]) -> Result<()> {
        self.batch(vec![DbCmd::Set {
            key: key.into(),
            value: value.into(),
        }])
        .await
    }

    pub async fn delete<'a>(&mut self, key: &'a [u8]) -> Result<()> {
        self.batch(vec![DbCmd::Delete { key: key.into() }]).await
    }

    fn incr_seq_num(&mut self) -> u64 {
        let seq_num = self.seq_num;
        self.seq_num += 1;
        seq_num
    }

    pub async fn batch<'a>(&mut self, batch: Vec<DbCmd>) -> Result<()> {
        let seq_num = self.incr_seq_num();
        let batch = batch.into_iter().map(|cmd| cmd.into()).collect();
        let req = WalRequest::new(seq_num, batch);
        self.wal_sender.send(req).await.unwrap();
        Ok(())
    }

    #[instrument]
    async fn open_manifest(
        path: &PathBuf,
        new: bool,
        id: Uuid,
    ) -> Result<(Manifest, Sender<ManifestRequest>)> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1024);
        let manifest = if new {
            info!("Creating new manifest");
            // Sending id
            let mut manifest = Manifest::create(path.clone(), receiver).await?;
            manifest
                .append(vec![ManifestLogEntry::DbId {
                    db_id: id.to_string(),
                }])
                .await?;
            manifest
        } else {
            info!("Opening existing manifest");
            Manifest::load(path.clone(), receiver).await?
        };
        Ok((manifest, sender))
    }

    pub async fn run(&mut self) {
        let _ = self.wal.run().await;
    }

    #[instrument]
    async fn open_wal(
        path: &PathBuf,
        options: DbOptions,
        new: bool,
        manifest_sender: Sender<ManifestRequest>,
    ) -> Result<(WalManager, Sender<WalRequest>)> {
        let (wal_sender, wal_receiver) = tokio::sync::mpsc::channel(1024);
        let wal = if new {
            info!("Creating new wal");
            WalManager::create(path.clone(), manifest_sender, wal_receiver, options).await?
        } else {
            info!("Opening existing wal");
            WalManager::load(path.clone(), manifest_sender, wal_receiver, options).await?
        };
        Ok((wal, wal_sender))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::db::db::Db;
    use crate::db::db::DbCmd;
    use crate::db::options::DbOptions;
    use crate::utils::tracing::init_tracer;
    use tempfile::tempdir;
    use tokio::{
        fs::{create_dir_all, read_to_string},
        join,
    };
    use tracing::{info_span, Instrument};

    #[tokio::test]
    async fn open_table() {
        init_tracer();
        let span = info_span!("open_table");

        async move {
            // let tmpdir = tempdir().unwrap();
            // let path = tmpdir.path();
            let path = Path::new("tmp");
            create_dir_all(path).await.unwrap();
            let (mut db, db_sender) = Db::open(path, DbOptions::default()).await.unwrap();

            let _ = join!(db.run(), async {
                db_sender.send(DbCmd::Set {
                    key: b"foo".to_vec(),
                    value: b"bar".to_vec(),
                })
            });
            let identity_path = path.join("IDENTITY");
            assert!(identity_path.exists());
            assert_eq!(
                read_to_string(identity_path).await.unwrap(),
                db.id.to_string()
            );

            let current_path = path.join("CURRENT");
            assert!(current_path.exists());
            assert_eq!(read_to_string(current_path).await.unwrap(), "MANIFEST-0");

            let manifest_path = path.join("MANIFEST-0");
            assert!(manifest_path.exists());
        }
        .instrument(span)
        .await;
    }
}
