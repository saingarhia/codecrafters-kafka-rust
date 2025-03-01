// implements Kafka body
use std::fmt;
use std::io::{BufReader, Read};
use crate::kafka::errors;

#[derive(Debug, Clone)]
pub enum RequestBody {
    ApiVersions(String, String)
}

impl RequestBody {
    pub fn new(_req: &mut BufReader<&[u8]>) -> errors::Result<Self> {
        Ok(RequestBody::ApiVersions("".into(), "".into()))
    }
    
}

impl fmt::Display for RequestBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", "Not implemented yet!")
    }
}
