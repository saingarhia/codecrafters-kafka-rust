// errors for this module
use anyhow;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaErrors {
    Unimplemented(String),
    InvalidApiKey(String)
}

impl std::fmt::Display for KafkaErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaErrors::Unimplemented(s) => write!(f, "{}", s),
            KafkaErrors::InvalidApiKey(s) => write!(f, "{}", s),
        }
    }
}

pub type Result<T> = anyhow::Result<T>;
