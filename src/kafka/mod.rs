pub mod apikey;
pub mod errors;
pub mod header;
pub mod body;
pub mod incoming;

// supports version 0 through 4
pub const MIN_SUPPORTED_API_VERSION: u16 = 0;
pub const MAX_SUPPORTED_API_VERSION: u16 = 4;
pub const MIN_SUPPORTED_DESCRIBE_PARTITION_VER: u16 = 0;
pub const MAX_SUPPORTED_DESCRIBE_PARTITION_VER: u16 = 0;

#[repr(u16)]
#[derive(Debug)]
pub enum ErrorCodes {
    UnsupportedAPIVersion = 35,
}

impl From<ErrorCodes> for u16 {
    fn from(e: ErrorCodes) -> Self {
        e as u16
    }
}

