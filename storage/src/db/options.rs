pub struct DbOptions {
    pub block_restart_interval: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            block_restart_interval: 16,
        }
    }
}
