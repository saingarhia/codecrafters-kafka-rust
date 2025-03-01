// implements Kafka body
use std::fmt;
use std::io::{BufReader, Read};
use crate::kafka::{apikey, header, errors, partitions};

#[derive(Debug, Clone)]
pub enum RequestBody {
    ApiVersions(u32, u8),   // throttle_ms and tagged buffer etc
    DescribePartitions(partitions::Partitions),
    Fetch(String),
}

impl RequestBody {
    pub fn new(req: &mut BufReader<&[u8]>, t: &header::RequestHeader) -> errors::Result<Self> {
        let s = match t.get_api_key() {
            apikey::ApiKey::Fetch => RequestBody::Fetch("not implemented".into()),
            apikey::ApiKey::ApiVersions => RequestBody::ApiVersions(0, 0),
            apikey::ApiKey::DescribeTopicPartitions => {
                let p = RequestBody::DescribePartitions(partitions::Partitions::new(req)?);
                p
            }
        };
        Ok(s)
    }
    
}

impl fmt::Display for RequestBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", "Not implemented yet!")
    }
}
