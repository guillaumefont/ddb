use std::{io::Result, path::PathBuf};

use async_stream::try_stream;
use tokio::{
    fs::{read_to_string, File},
    io::BufReader,
};
use tokio_stream::StreamExt;

use super::entry::ManifestLogEntry;

pub async fn iter_from(path: PathBuf) -> impl StreamExt<Item = Result<ManifestLogEntry>> {
    try_stream! {
        let current_path = path.join("CURRENT");
        let current = read_to_string(current_path).await?;

        let log_path = path.join(current);
        let mut reader = BufReader::new(File::open(log_path).await?);
        loop {
            let entry = ManifestLogEntry::read(&mut reader).await?;
            yield entry;
        }
    }
}
