pub enum WalEntry {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}
