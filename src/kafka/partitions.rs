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
        println!("Found {} topics!!", topics.len());
        for t in &topics {
            println!("topic name: {}", String::from_utf8_lossy(t));
        }
        let response_partition_limit = parser::read_int(req)?;
        println!("response partition limit: {}", response_partition_limit);
        //let topic_name = parser::compact_string(req)?;
        //println!("topic name: {}", String::from_utf8_lossy(&topic_name));
        //let partition_index = parser::read_int(req)?;
        //println!("partition index: {}", partition_index);
        let b = parser::read_byte(req)?;
        println!("Newly read byte: {}", b);
        let b = parser::read_byte(req)?;
        println!("Newly read byte: {}", b);
        parser::tag_buffer(req)?;
        println!("Now building final parition structure!!");
        Ok(Self {
            topics,
            response_partition_limit,
            cursor: Cursor {
                topic_name: "".into(),
                partition_index: 0,
            }
        })
    }
}
