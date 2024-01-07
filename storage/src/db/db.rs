use tokio::fs::{create_dir_all, read_to_string, write};
use tokio::sync::mpsc::Sender;
use tracing::info;
use uuid::Uuid;

use crate::db::options::DbOptions;
use crate::manifest::entry::ManifestLogEntry;
use crate::manifest::manifest::{Manifest, ManifestRequest};
use crate::wal::manager::WalManager;
use std::fmt::Debug;
use std::io::Result;
use std::path::PathBuf;
use tracing::instrument;

pub struct Db {
    id: Uuid,
    path: PathBuf,
    options: DbOptions,
    manifest: Manifest,
    wal: WalManager,
}

impl Db {
    #[instrument]
    pub async fn open<P: Into<PathBuf> + Debug>(path: P, options: DbOptions) -> Result<Self> {
        let path = path.into();
        create_dir_all(&path).await?;
        // let manifest = Manifest::open(path.clone()).await?;
        info!("Opening database");

        let (id, created) = Self::open_identity(&path).await?;

        let (manifest, manifest_sender) = Self::open_manifest(&path, created, id).await?;

        let wal = Self::open_wal(&path, created, manifest_sender).await?;

        let db = Self {
            id,
            path,
            options,
            manifest,
            wal,
        };
        Ok(db)
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

    pub async fn get(&self, key: &[u8]) -> Result<()> {
        Ok(())
    }

    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
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

    #[instrument]
    async fn open_wal(
        path: &PathBuf,
        new: bool,
        manifest_sender: Sender<ManifestRequest>,
    ) -> Result<WalManager> {
        let wal = if new {
            info!("Creating new wal");
            WalManager::create(path.clone(), manifest_sender).await?
        } else {
            info!("Opening existing wal");
            WalManager::load(path.clone(), manifest_sender).await?
        };
        Ok(wal)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::db::db::Db;
    use crate::db::options::DbOptions;
    use crate::utils::tracing::init_tracer;
    // use tempfile::tempdir;
    use tokio::fs::read_to_string;
    use tracing::{info_span, Instrument};

    #[tokio::test]
    async fn open_table() {
        init_tracer();
        let span = info_span!("open_table");

        async move {
            let path = Path::new("tmp/db");
            let db = Db::open(path, DbOptions::default()).await.unwrap();

            db.set(b"foo", b"bar").await.unwrap();
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
