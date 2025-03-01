use std::io::{self, BufReader, BufWriter, Read, Write};
use std::fmt;
use crate::kafka::{header, body, errors, apikey};
use super::{MIN_SUPPORTED_API_VERSION, MAX_SUPPORTED_API_VERSION, ErrorCodes};

// incoming request parser/handler
//
#[derive(Debug, Clone)]
pub struct Request {
    header: header::RequestHeader,
    body: body::RequestBody,
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}",
            format!(
                concat!("Incoming Request:\n",
                    "    Header: {}\n",
                    "    Body: {}\n"),
                self.header, self.body))
    }
}

impl Request {
    pub fn new(req: &mut BufReader<&[u8]>) -> errors::Result<Self> {
        // lets read message size
        // lets read the header
        let header = header::RequestHeader::new(req)?;
        let body = body::RequestBody::new(req)?;
        Ok(Self{header, body})
    }

    pub fn process(&self, response: &mut BufWriter<&mut [u8]>) -> usize {
        // fill in the correlation id
        response.write(&self.header.get_correlation_id().to_be_bytes());

        println!("Building response for Request: {}", self);
        let api_ver = self.header.get_api_ver();
        match self.header.get_api_key() {
           apikey::ApiKey::Fetch => {},
           apikey::ApiKey::ApiVersions => {
                if api_ver < super::MIN_SUPPORTED_API_VERSION ||
                    api_ver > MAX_SUPPORTED_API_VERSION {
                        let ec = u16::from(ErrorCodes::UnsupportedAPIVersion);
                        let _ = response.write(&ec.to_be_bytes());
                } else {
                        let _ = response.write(&0_i16.to_be_bytes());
                }
                // TODO - clean it up.. need +1 keys
                let _ = response.write(&[2_u8]);
                let _ = response.write(&self.header.get_api_key_num().to_be_bytes());
                let _ = response.write(&MIN_SUPPORTED_API_VERSION.to_be_bytes());
                let _ = response.write(&MAX_SUPPORTED_API_VERSION.to_be_bytes());
                // tag buffer len
                let _ = response.write(&[0_u8]);
                // throttle time in ms
                let _ = response.write(&0_u32.to_be_bytes());
                // tag buffer len
                let _ = response.write(&[0_u8]);
            },
           _ => {},
        }
        response.buffer().len()
    }
}
