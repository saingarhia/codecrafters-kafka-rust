use std::fmt;
use std::io::prelude::*;
use std::io::{BufReader, Read};
use crate::kafka::errors::{self, KafkaErrors};

const FETCH_APIKEY: u16 = 1;
const API_VERSIONS_APIKEY: u16 = 18;
const DESCRIBE_PARTITIONS_APIKEY: u16 = 75;

#[repr(u16)]
#[derive(Debug, Copy, Clone)]
pub enum ApiKey {
    Fetch = FETCH_APIKEY,
    ApiVersions = API_VERSIONS_APIKEY,
    DescribeTopicPartitions = DESCRIBE_PARTITIONS_APIKEY,
}

impl ApiKey {
    pub fn parse(b: &mut BufReader<&[u8]>) -> errors::Result<Self> {
        let mut b0 = [0u8; 2];
        b.read_exact(&mut b0)?;
        Ok(Self::try_from(u16::from_be_bytes(b0))?)
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

impl TryFrom<u16> for ApiKey {
    type Error = crate::kafka::errors::KafkaErrors;
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            FETCH_APIKEY => Ok(Self::Fetch),
            API_VERSIONS_APIKEY => Ok(Self::ApiVersions),
            DESCRIBE_PARTITIONS_APIKEY => Ok(Self::DescribeTopicPartitions),
            i @ (0..=75) => Err(KafkaErrors::Unimplemented(format!("apikey {i}"))),
            i => Err(KafkaErrors::InvalidApiKey(format!("invalid apikey {i}"))),
        }
    }
}

pub struct SupportedApiKeys {
    pub min: u16,
    pub max: u16,
    pub key: u16, 
}

pub const SUPPORTED_APIKEYS: &[SupportedApiKeys; 2] = &[
    // API Versions request
    SupportedApiKeys {
        min: super::MIN_SUPPORTED_API_VERSION,
        max: super::MAX_SUPPORTED_API_VERSION,
        key: API_VERSIONS_APIKEY, 
    },
    // Describe partitions
    SupportedApiKeys {
        min: super::MIN_SUPPORTED_DESCRIBE_PARTITION_VER,
        max: super::MAX_SUPPORTED_DESCRIBE_PARTITION_VER,
        key: DESCRIBE_PARTITIONS_APIKEY, 
    },
];
