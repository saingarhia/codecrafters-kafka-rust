pub mod apikey;
pub mod body;
pub mod errors;
pub mod fetch;
pub mod header;
pub mod incoming;
pub mod metadata;
pub mod parser;
pub mod partitions;
pub mod records;
pub mod writer;

// supports version 0 through 4
pub const MIN_SUPPORTED_API_VERSION: u16 = 0;
pub const MAX_SUPPORTED_API_VERSION: u16 = 4;
pub const MIN_SUPPORTED_DESCRIBE_PARTITION_VER: u16 = 0;
pub const MAX_SUPPORTED_DESCRIBE_PARTITION_VER: u16 = 0;

pub const MIN_SUPPORTED_FETCH_VER: u16 = 0;
pub const MAX_SUPPORTED_FETCH_VER: u16 = 16;

#[repr(u16)]
#[derive(Debug)]
pub enum ErrorCodes {
    UnsupportedTopicOrPartition = 3,
    UnsupportedAPIVersion = 35,
}

impl From<ErrorCodes> for u16 {
    fn from(e: ErrorCodes) -> Self {
        e as u16
    }
}
