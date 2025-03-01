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
        println!("header: {}, now building body!!", header);
        let body = body::RequestBody::new(req, &header)?;
        Ok(Self{header, body})
    }

    pub fn process(&self, response: &mut BufWriter<&mut [u8]>) -> usize {
        // fill in the correlation id
        let _ = response.write(&self.header.get_correlation_id().to_be_bytes());

        println!("Building response for Request: {}", self);
        let api_ver = self.header.get_api_ver();
        match &self.body {
           body::RequestBody::Fetch(_s) => {},
           body::RequestBody::ApiVersions(_throttle, _tbuf)=> {
                if api_ver < super::MIN_SUPPORTED_API_VERSION ||
                    api_ver > MAX_SUPPORTED_API_VERSION {
                        let ec = u16::from(ErrorCodes::UnsupportedAPIVersion);
                        let _ = response.write(&ec.to_be_bytes());
                } else {
                        let _ = response.write(&0_i16.to_be_bytes());
                }
                // TODO - clean it up.. need +1 keys
                let _ = response.write(&[apikey::SUPPORTED_APIKEYS.len() as u8 + 1]);
                apikey::SUPPORTED_APIKEYS.iter().for_each(|sk| {
                    let _ = response.write(&sk.key.to_be_bytes());
                    let _ = response.write(&sk.min.to_be_bytes());
                    let _ = response.write(&sk.max.to_be_bytes());
                    // tag buffer len
                    let _ = response.write(&[0_u8]);
                });
                // throttle time in ms
                let _ = response.write(&0_u32.to_be_bytes());
                // tag buffer len
                let _ = response.write(&[0_u8]);
            },
           body::RequestBody::DescribePartitions(p) => {
                // tag buffer is first (immediately after correlation id) as per the test
                let _ = response.write(&[0_u8]);
                // throttleu time in ms 
                let _ = response.write(&0_u32.to_be_bytes());
                // topics array -> including length
                let _ = response.write(&[p.topics.len() as u8 + 1]);
                let ec = u16::from(ErrorCodes::UnsupportedTopicOrPartition);
                let default_topic = [0_u8; 16]; //"00000000-0000-0000-0000-000000000000"];
                p.topics.iter().for_each(|topic| {
                    let _ = response.write(&ec.to_be_bytes());
                    // length
                    let _ = response.write(&[topic.len() as u8]);
                    // topic
                    let _ = response.write(topic);
                    // topic ID " has to all zeros "
                    //let _ = response.write(&default_topic.as_bytes());
                    let _ = response.write(&default_topic);
                    // internal topic
                    let _ = response.write(&[0_u8; 1]);
                }); 
                // partitions array -> empty shoud be 0?
                let _ = response.write(&[1_u8; 1]);
                // authorized operations
                let _ = response.write(&1234_u32.to_be_bytes());

                // tag buffer
                let _ = response.write(&[0_u8; 1]);
                // next cursor = NULL
                let _ = response.write(&[0xFF_u8; 1]);
                // tag buffer
                let _ = response.write(&[0_u8; 1]);
            },
        }
        response.buffer().len()
    }
}
