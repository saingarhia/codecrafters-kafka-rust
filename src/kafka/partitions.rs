use std::io::{BufReader, Read};
use crate::kafka::{parser, errors};

#[derive(Debug, Clone)]
pub struct Cursor {
    pub topic_name: Vec<u8>,
    pub partition_index: i32,
}

#[derive(Debug, Clone)]
pub struct Partitions {
    pub topics: Vec<Vec<u8>>,
    pub response_partition_limit: i32,
    pub cursor: Cursor, 
}

impl Partitions {
    pub fn new(req: &mut BufReader<&[u8]>) -> errors::Result<Self> {
            // read the length and then process the array
        let topics = parser::array(req)?;
        println!("Found topics!!");
        let response_partition_limit = parser::read_int(req)?;
        let topic_name = parser::compact_string(req)?;
        let partition_index = parser::read_int(req)?;
        parser::tag_buffer(req)?;
        Ok(Self {
            topics,
            response_partition_limit,
            cursor: Cursor {
                topic_name,
                partition_index,
            }
        })
    }
}
