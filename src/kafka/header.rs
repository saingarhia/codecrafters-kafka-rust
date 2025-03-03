// implements incoming header
use crate::kafka::{apikey, errors, parser};
use std::fmt;
use std::io::{self, Read};

#[derive(Debug, Clone)]
pub struct RequestHeader {
    api_key: apikey::ApiKey,
    api_ver: u16,
    correlation_id: i32,
    client_id: Option<String>, // nullable string
                               //tag_buffer: compact array
}

#[allow(dead_code)]
impl RequestHeader {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let api_key = apikey::ApiKey::parse(req)?; // first 2 bytes
                                                   // lets read the version
        let mut ver = [0_u8; 2];
        req.read_exact(&mut ver)?;
        let api_ver = u16::from_be_bytes(ver);

        // read Correlationi ID
        let mut cid = [0_u8; 4];
        req.read_exact(&mut cid)?;
        let correlation_id = i32::from_be_bytes(cid);

        let mut client_id: Option<String> = None;
        let mut client_id_length_data = [0_u8; 2];
        req.read_exact(&mut client_id_length_data)?;
        let client_id_length = u16::from_be_bytes(client_id_length_data);
        if client_id_length > 0 {
            let mut client_id_data = vec![0_u8; client_id_length as usize];
            req.read_exact(&mut client_id_data)?;
            if let Ok(ss) = String::from_utf8(client_id_data) {
                client_id.get_or_insert(ss);
            }
        }
        parser::tag_buffer(req)?;

        Ok(Self {
            api_key,
            api_ver,
            correlation_id,
            client_id,
        })
    }

    pub fn get_correlation_id(&self) -> i32 {
        self.correlation_id
    }

    pub fn get_api_key(&self) -> apikey::ApiKey {
        self.api_key
    }

    pub fn get_api_key_num(&self) -> u16 {
        u16::from(self.api_key)
    }

    pub fn get_api_ver(&self) -> u16 {
        self.api_ver
    }

    pub fn get_client_id(&self) -> Option<String> {
        todo!()
    }
}

impl fmt::Display for RequestHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Api Key: {}, Api Version: {}, Correlation ID: {}, Client ID: {}",
            self.api_key,
            self.api_ver,
            self.correlation_id,
            self.client_id.clone().unwrap_or("NA".into())
        )
    }
}
