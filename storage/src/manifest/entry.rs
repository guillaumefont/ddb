use crate::utils::{
    string::{async_read_bytes, async_read_string, async_write_bytes, async_write_string},
    varint::{read::async_read_varint, write::async_write_varint},
};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use std::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(FromPrimitive, ToPrimitive)]
pub enum ManifestLogEntryType {
    LogNumber = 1,
    PrevFileNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    MaxColumnFamily = 5,
    DeletedFile = 6,
    NewFile = 7,
    InAtomicGroup = 8,
    DbId = 9,
    WalAddition = 10,
    WalDeletion = 11,
}

#[derive(Debug)]
pub enum ManifestLogEntry {
    LogNumber {
        log_number: u64,
    },
    PrevFileNumber {
        prev_file_number: u64,
    },
    NextFileNumber {
        next_file_number: u64,
    },
    LastSequence {
        last_sequence: u64,
    },
    MaxColumnFamily {
        max_column_family: u32,
    },
    DeletedFile {
        level: u32,
        file_number: u64,
    },
    NewFile {
        level: u32,
        file_number: u64,
        file_size: u64,
        smallest: Vec<u8>,
        largest: Vec<u8>,
        smallest_seqno: u64,
        largest_seqno: u64,
        tags: Vec<NewFileTag>,
    },
    InAtomicGroup {
        version_edit_count: u32,
    },
    DbId {
        db_id: String,
    },
    WalAddition {
        log_number: u64,
        tags: Vec<WalTag>,
    },
    WalDeletion {
        log_number: u64,
    },
}

impl ManifestLogEntry {
    fn get_type(&self) -> u32 {
        match self {
            ManifestLogEntry::LogNumber { .. } => ManifestLogEntryType::LogNumber as u32,
            ManifestLogEntry::PrevFileNumber { .. } => ManifestLogEntryType::PrevFileNumber as u32,
            ManifestLogEntry::NextFileNumber { .. } => ManifestLogEntryType::NextFileNumber as u32,
            ManifestLogEntry::LastSequence { .. } => ManifestLogEntryType::LastSequence as u32,
            ManifestLogEntry::MaxColumnFamily { .. } => {
                ManifestLogEntryType::MaxColumnFamily as u32
            }
            ManifestLogEntry::DeletedFile { .. } => ManifestLogEntryType::DeletedFile as u32,
            ManifestLogEntry::NewFile { .. } => ManifestLogEntryType::NewFile as u32,
            ManifestLogEntry::InAtomicGroup { .. } => ManifestLogEntryType::InAtomicGroup as u32,
            ManifestLogEntry::DbId { .. } => ManifestLogEntryType::DbId as u32,
            ManifestLogEntry::WalAddition { .. } => ManifestLogEntryType::WalAddition as u32,
            ManifestLogEntry::WalDeletion { .. } => ManifestLogEntryType::WalDeletion as u32,
        }
    }
    pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        async_write_varint(self.get_type(), writer).await?;
        match self {
            ManifestLogEntry::LogNumber { log_number } => {
                async_write_varint(*log_number, writer).await?;
            }
            ManifestLogEntry::PrevFileNumber { prev_file_number } => {
                async_write_varint(*prev_file_number, writer).await?;
            }
            ManifestLogEntry::NextFileNumber { next_file_number } => {
                async_write_varint(*next_file_number, writer).await?;
            }
            ManifestLogEntry::LastSequence { last_sequence } => {
                async_write_varint(*last_sequence, writer).await?;
            }
            ManifestLogEntry::MaxColumnFamily { max_column_family } => {
                async_write_varint(*max_column_family, writer).await?;
            }
            ManifestLogEntry::DeletedFile { level, file_number } => {
                async_write_varint(*level, writer).await?;
                async_write_varint(*file_number, writer).await?;
            }
            ManifestLogEntry::NewFile {
                level,
                file_number,
                file_size,
                smallest,
                largest,
                smallest_seqno,
                largest_seqno,
                tags,
            } => {
                async_write_varint(*level, writer).await?;
                async_write_varint(*file_number, writer).await?;
                async_write_varint(*file_size, writer).await?;
                async_write_bytes(smallest, writer).await?;
                async_write_bytes(largest, writer).await?;
                async_write_varint(*smallest_seqno, writer).await?;
                async_write_varint(*largest_seqno, writer).await?;
                for tag in tags {
                    tag.write(writer).await?;
                }
                NewFileTag::Terminate.write(writer).await?;
            }
            ManifestLogEntry::InAtomicGroup { version_edit_count } => {
                async_write_varint(*version_edit_count, writer).await?;
            }
            ManifestLogEntry::DbId { db_id } => {
                async_write_varint(db_id.len(), writer).await?;
                writer.write_all(db_id.as_bytes()).await?;
            }
            ManifestLogEntry::WalAddition { log_number, tags } => {
                async_write_varint(*log_number, writer).await?;
                for tag in tags {
                    tag.write(writer).await?;
                }
                WalTag::Terminate.write(writer).await?;
            }
            ManifestLogEntry::WalDeletion { log_number } => {
                async_write_varint(*log_number, writer).await?;
            }
        }
        Ok(())
    }

    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let entry_type_value: u32 = async_read_varint(reader).await?;
        let entry_type = FromPrimitive::from_u32(entry_type_value);
        match entry_type {
            Some(ManifestLogEntryType::LogNumber) => {
                let log_number = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::LogNumber { log_number })
            }
            Some(ManifestLogEntryType::PrevFileNumber) => {
                let prev_file_number = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::PrevFileNumber { prev_file_number })
            }
            Some(ManifestLogEntryType::NextFileNumber) => {
                let next_file_number = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::NextFileNumber { next_file_number })
            }
            Some(ManifestLogEntryType::LastSequence) => {
                let last_sequence = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::LastSequence { last_sequence })
            }
            Some(ManifestLogEntryType::MaxColumnFamily) => {
                let max_column_family = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::MaxColumnFamily { max_column_family })
            }
            Some(ManifestLogEntryType::DeletedFile) => {
                let level = async_read_varint(reader).await?;
                let file_number = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::DeletedFile { level, file_number })
            }
            Some(ManifestLogEntryType::NewFile) => {
                let level = async_read_varint(reader).await?;
                let file_number = async_read_varint(reader).await?;
                let file_size = async_read_varint(reader).await?;
                let smallest = async_read_bytes(reader).await?;
                let largest = async_read_bytes(reader).await?;
                let smallest_seqno = async_read_varint(reader).await?;
                let largest_seqno = async_read_varint(reader).await?;
                let mut tags = Vec::new();
                loop {
                    let tag = NewFileTag::read(reader).await?;
                    match tag {
                        NewFileTag::Terminate => {
                            break;
                        }
                        _ => {
                            tags.push(tag);
                        }
                    }
                }
                Ok(ManifestLogEntry::NewFile {
                    level,
                    file_number,
                    file_size,
                    smallest,
                    largest,
                    smallest_seqno,
                    largest_seqno,
                    tags,
                })
            }
            Some(ManifestLogEntryType::InAtomicGroup) => {
                let version_edit_count = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::InAtomicGroup { version_edit_count })
            }
            Some(ManifestLogEntryType::DbId) => {
                let len: usize = async_read_varint(reader).await?;
                let mut buffer = vec![0; len as usize];
                reader.read_exact(&mut buffer).await?;
                let db_id = String::from_utf8(buffer).unwrap();
                Ok(ManifestLogEntry::DbId { db_id })
            }
            Some(ManifestLogEntryType::WalAddition) => {
                let log_number = async_read_varint(reader).await?;
                let mut tags = Vec::new();
                loop {
                    let tag = WalTag::read(reader).await?;
                    match tag {
                        WalTag::Terminate => {
                            break;
                        }
                        _ => {
                            tags.push(tag);
                        }
                    }
                }
                Ok(ManifestLogEntry::WalAddition { log_number, tags })
            }
            Some(ManifestLogEntryType::WalDeletion) => {
                let log_number = async_read_varint(reader).await?;
                Ok(ManifestLogEntry::WalDeletion { log_number })
            }
            None => {
                panic!("Unknown entry type: {}", entry_type_value);
            }
        }
    }
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum WalTagType {
    Terminate,
    SyncedSize,
}

#[derive(Debug)]
pub enum WalTag {
    Terminate,
    SyncedSize { size: u64 },
}

impl WalTag {
    pub fn get_type(&self) -> u32 {
        match self {
            WalTag::Terminate { .. } => WalTagType::Terminate as u32,
            WalTag::SyncedSize { .. } => WalTagType::SyncedSize as u32,
        }
    }

    pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        async_write_varint(self.get_type(), writer).await?;
        match self {
            WalTag::Terminate => {}
            WalTag::SyncedSize { size } => {
                async_write_varint(*size, writer).await?;
            }
        }
        Ok(())
    }

    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let tag_type_value: u32 = async_read_varint(reader).await?;
        let tag_type = FromPrimitive::from_u32(tag_type_value);
        match tag_type {
            Some(WalTagType::Terminate) => Ok(WalTag::Terminate),
            Some(WalTagType::SyncedSize) => {
                let size = async_read_varint(reader).await?;
                Ok(WalTag::SyncedSize { size })
            }
            None => {
                panic!("Unknown tag type: {}", tag_type_value);
            }
        }
    }
}

#[derive(FromPrimitive, ToPrimitive)]
pub enum NewFileTagType {
    Terminate,
    NeedCompaction,
    FileCreationTime,
    FileCheckSum,
    FileCheckSumFuncName,
}

#[derive(Debug)]
pub enum NewFileTag {
    Terminate,
    NeedCompaction,
    FileCreationTime { time: u64 },
    FileCheckSum { chec_sum: u32 },
    FileCheckSumFuncName { func_name: String },
}

impl NewFileTag {
    pub fn get_type(&self) -> u32 {
        match self {
            NewFileTag::Terminate => NewFileTagType::Terminate as u32,
            NewFileTag::NeedCompaction => NewFileTagType::NeedCompaction as u32,
            NewFileTag::FileCreationTime { .. } => NewFileTagType::FileCreationTime as u32,
            NewFileTag::FileCheckSum { .. } => NewFileTagType::FileCheckSum as u32,
            NewFileTag::FileCheckSumFuncName { .. } => NewFileTagType::FileCheckSumFuncName as u32,
        }
    }

    pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        async_write_varint(self.get_type(), writer).await?;
        match self {
            NewFileTag::Terminate => {}
            NewFileTag::NeedCompaction => {}
            NewFileTag::FileCreationTime { time } => {
                async_write_varint(*time, writer).await?;
            }
            NewFileTag::FileCheckSum { chec_sum } => {
                async_write_varint(*chec_sum, writer).await?;
            }
            NewFileTag::FileCheckSumFuncName { func_name } => {
                async_write_string(func_name, writer).await?;
            }
        }
        Ok(())
    }

    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let tag_type_value: u32 = async_read_varint(reader).await?;
        let tag_type = FromPrimitive::from_u32(tag_type_value);
        match tag_type {
            Some(NewFileTagType::Terminate) => Ok(NewFileTag::Terminate),
            Some(NewFileTagType::NeedCompaction) => Ok(NewFileTag::NeedCompaction),
            Some(NewFileTagType::FileCreationTime) => {
                let time = async_read_varint(reader).await?;
                Ok(NewFileTag::FileCreationTime { time })
            }
            Some(NewFileTagType::FileCheckSum) => {
                let chec_sum = async_read_varint(reader).await?;
                Ok(NewFileTag::FileCheckSum { chec_sum })
            }
            Some(NewFileTagType::FileCheckSumFuncName) => {
                let func_name = async_read_string(reader).await?;
                Ok(NewFileTag::FileCheckSumFuncName { func_name })
            }
            None => {
                panic!("Unknown tag type: {}", tag_type_value);
            }
        }
    }
}
