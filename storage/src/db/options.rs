#[derive(Debug, Clone)]
pub struct DbOptions {
    pub sst_block_restart_interval: usize,
    pub sst_index_restart_interval: usize,
    pub sst_block_size: usize,
    pub wal_block_size: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            sst_block_restart_interval: 16,
            sst_index_restart_interval: 16,
            sst_block_size: 4 * 1024,
            wal_block_size: 32 * 1024,
        }
    }
}
