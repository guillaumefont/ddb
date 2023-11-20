#[derive(Default)]
pub struct SstStats {
    data_size: usize,
    index_size: usize,
    filter_size: usize,
    raw_key_size: usize,
    raw_value_size: usize,
    data_block_count: usize,
    entries_count: usize,
}

impl SstStats {
    pub fn add_entry(&mut self, key: &[u8], value: &[u8]) {
        self.entries_count += 1;
        self.raw_key_size += key.len();
        self.raw_value_size += value.len();
    }

    pub fn add_data_block(&mut self, block_size: usize) {
        self.data_block_count += 1;
        self.data_size += block_size;
    }

    pub fn set_index_size(&mut self, index_size: usize) {
        self.index_size = index_size;
    }

    pub fn set_filter_size(&mut self, filter_size: usize) {
        self.filter_size = filter_size;
    }
}
