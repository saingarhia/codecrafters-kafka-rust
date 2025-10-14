use crate::kafka::{errors, parser, writer};
use std::io::{self, Read, Write};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Cursor {
    pub topic_name: Vec<u8>,
    pub partition_index: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionsRequest {
    pub topics: Vec<String>,
    pub response_partition_limit: i32,
}

impl PartitionsRequest {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        // For v0, array length is INT32
        let topics_len = parser::read_int(req)?;
        let mut topics = Vec::with_capacity(topics_len as usize);
        for _ in 0..topics_len {
            // For v0, string length is INT16
            let name_len = parser::read_short(req)?;
            if name_len > 0 {
                let mut name_buf = vec![0u8; name_len as usize];
                req.read_exact(&mut name_buf)?;
                topics.push(String::from_utf8(name_buf)?);
            } else {
                // Handle null or empty string case if necessary
                topics.push(String::new());
            }
        }
        println!("Found {} topics!!: {:?}", topics.len(), topics);

        let response_partition_limit = parser::read_int(req)?;
        println!("response partition limit: {}", response_partition_limit);

        Ok(Self {
            topics,
            response_partition_limit,
        })
    }
}

// https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions
//
//
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Partition {
    pub error_code: u16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl Partition {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.error_code)?;
        writer::write_bytes(resp, &self.partition_index)?;
        writer::write_bytes(resp, &self.leader_id)?;
        writer::write_bytes(resp, &self.leader_epoch)?;
        writer::write_bytes(resp, &(self.replica_nodes.len() as i32))?;
        self.replica_nodes
            .iter()
            .try_for_each(|rn| writer::write_bytes(resp, rn))?;
        writer::write_bytes(resp, &(self.isr_nodes.len() as i32))?; // isr_nodes
        self.isr_nodes.iter().try_for_each(|inn| writer::write_bytes(resp, inn))?;
        writer::write_bytes(resp, &(self.offline_replicas.len() as i32))?;
        self.offline_replicas
            .iter()
            .try_for_each(|or| writer::write_bytes(resp, or))?;
        Ok(())
    }
}

// for partitions response
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Topic {
    pub error_code: u16,
    pub name: String,
    pub partitions: Vec<Partition>,
}

impl Topic {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.error_code)?;
        // v0 uses STRING for name
        if !self.name.is_empty() {
            writer::write_bytes(resp, &(self.name.len() as i16))?;
            resp.write_all(self.name.as_bytes())?;
        } else {
            writer::write_bytes(resp, &(-1_i16))?;
        }
        writer::write_bytes(resp, &(self.partitions.len() as i32))?;
        self.partitions.iter().try_for_each(|p| p.serialize(resp))?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct NextCursor {
    pub topic_name: Vec<u8>,
    pub partition_index: u32,
    pub tag_buffer: u8,
}

impl NextCursor {
    #[allow(dead_code)]    
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_compact_string(resp, &self.topic_name)?;
        writer::write_bytes(resp, &self.partition_index)?;
        //writer::write_bytes(resp, &self.tag_buffer)?; // tag buffer
        Ok(())
    }
}

// for partitions response
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionsResponse {
    pub throttle_ms: u32,
    pub topics: Vec<Topic>,
}

#[allow(dead_code)]
impl PartitionsResponse {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.throttle_ms)?;
        writer::write_bytes(resp, &(self.topics.len() as i32))?;
        self.topics
            .iter()
            .try_for_each(|topic| topic.serialize(resp))?;
        Ok(())
    }
}
