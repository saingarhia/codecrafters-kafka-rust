// implements incoming header
use std::io::{self, BufReader, Read};
use std::fmt;
use crate::kafka::{apikey, errors};

#[derive(Debug, Clone)]
pub struct RequestHeader {
    api_key: apikey::ApiKey,
    api_ver: u16,
    correlation_id: i32,
    client_id: Option<String>,// nullable string 
    //tag_buffer: compact array
}

impl RequestHeader {
    pub fn new(req: &mut BufReader<&[u8]>) -> errors::Result<Self> {
        let api_key = apikey::ApiKey::parse(req)?; // first 2 bytes
        // lets read the version
        let mut ver = [0_u8; 2];
        req.read_exact(&mut ver)?;
        let api_ver = u16::from_be_bytes(ver);

        // read Correlationi ID
        let mut cid = [0_u8; 4];
        req.read_exact(&mut cid)?;
        let correlation_id = i32::from_be_bytes(cid);

        let client_id_length = [0_u8; 2];
        let mut client_id: Option<String> = None;
        req.read_exact(&mut cid)?;
        let client_id_length = i16::from_be_bytes(client_id_length);
        if client_id_length > 0 {
            let mut s = String::new();
            req.read_to_string(&mut s)?;
            client_id.get_or_insert(s);
            // consume the extra tag buffer
            //let mut tag_buffer = [0_u8; 1];
            //req.read_exact(&mut tag_buffer)?;
        }

        Ok(Self {api_key, api_ver, correlation_id, client_id})
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
        write!(f, "Api Key: {}, Api Version: {}, Correlation ID: {}, Client ID: {}",
                self.api_key, self.api_ver, self.correlation_id,
                self.client_id.clone().unwrap_or("NA".into()))
    }
}
