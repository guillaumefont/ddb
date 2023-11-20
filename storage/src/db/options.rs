#[derive(Debug, Clone)]
pub struct DbOptions {
    pub block_restart_interval: usize,
    pub index_restart_interval: usize,
    pub max_block_size: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            block_restart_interval: 16,
            index_restart_interval: 16,
            max_block_size: 4 * 1024,
        }
    }
}
