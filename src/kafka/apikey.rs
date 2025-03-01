use std::fmt;
use std::io::prelude::*;
use std::io::{BufReader, Read};
use crate::kafka::errors::{self, KafkaErrors};

const FetchAPIKey: u16 = 1;
const ApiVersionsAPIKey: u16 = 18;
const DescribeTopicPartitionsAPIKey: u16 = 75;

#[repr(u16)]
#[derive(Debug, Copy, Clone)]
pub enum ApiKey {
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl ApiKey {
    pub fn parse(b: &mut BufReader<&[u8]>) -> errors::Result<Self> {
        let mut b0 = [0u8; 2];
        b.read_exact(&mut b0)?;
        Ok(Self::try_from(i16::from_be_bytes(b0))?)
    }
}

impl From<ApiKey> for u16 {
    fn from(ak: ApiKey) -> Self {
        ak as u16
    }
}


impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fetch => write!(f, "fetch"),
            Self::ApiVersions => write!(f, "api-versions"),
            Self::DescribeTopicPartitions => write!(f, "describe-topic-partitions"),
        }
    }
}

impl TryFrom<i16> for ApiKey {
    type Error = crate::kafka::errors::KafkaErrors;
    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Fetch),
            18 => Ok(Self::ApiVersions),
            75 => Ok(Self::DescribeTopicPartitions),
            i @ (0..=75) => Err(KafkaErrors::Unimplemented(format!("apikey {i}"))),
            i => Err(KafkaErrors::InvalidApiKey(format!("invalid apikey {i}"))),
        }
    }
}
