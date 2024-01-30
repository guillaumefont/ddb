use std::io::Result;
use std::io::Write;

use crate::utils::fixedint::write_u8;
use crate::utils::string::write_bytes;

#[repr(u8)]
pub enum WalEntryType {
    Set = 1,
    Delete = 2,
}

pub enum WalEntry {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WalEntry {
    fn get_type(&self) -> WalEntryType {
        match self {
            WalEntry::Set { .. } => WalEntryType::Set,
            WalEntry::Delete { .. } => WalEntryType::Delete,
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_u8(self.get_type() as u8, writer)?;
        match self {
            WalEntry::Set { key, value } => {
                write_bytes(key, writer)?;
                write_bytes(value, writer)?;
            }
            WalEntry::Delete { key } => {
                write_bytes(key, writer)?;
            }
        }
        Ok(())
    }
}
