use crate::err::{KafkaError, Res};
use std::fmt;
use std::io::prelude::*;

pub enum ApiKey {
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl ApiKey {
    pub fn parse(b: &mut &[u8]) -> Res<Self> {
        let mut b0 = [0u8; 2];
        b.read_exact(&mut b0)?;
        Self::try_from(i16::from_be_bytes(b0))
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
    type Error = crate::err::KafkaError;
    fn try_from(value: i16) -> Res<Self> {
        match value {
            1 => Ok(Self::Fetch),
            18 => Ok(Self::ApiVersions),
            75 => Ok(Self::DescribeTopicPartitions),
            i @ (0..=75) => Err(KafkaError::Unimplemented(format!("apikey {i}"))),
            i => Err(format!("invalid apikey {i}").into()),
        }
    }
}
