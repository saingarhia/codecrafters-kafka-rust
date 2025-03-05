// errors for this module
use anyhow;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaErrors {
    #[error("Not Yet Implemented: {0}")]
    Unimplemented(String),
    #[error("Invalid API Key: {0}")]
    InvalidApiKey(String),
    #[error("Invalid Writer Argument: {0}")]
    InvalidWriterArg(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/*
impl std::fmt::Display for KafkaErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaErrors::Unimplemented(s) => write!(f, "{}", s),
            KafkaErrors::InvalidApiKey(s) => write!(f, "{}", s),
            KafkaErrors::InvalidWriterArg(s) => write!(f, "{}", s),
            KafkaErrors::IoError => write!(f, "{}", "IO error"),
        }
    }
}
*/

pub type Result<T> = anyhow::Result<T>;
