use crate::kafka::{errors, parser, writer};
use std::io::{self, Read, Write};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Cursor {
    pub topic_name: Vec<u8>,
    pub partition_index: i32,
    pub tag_buffer: u8,
}

impl Cursor {
    #[allow(dead_code)]
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_compact_string(resp, &self.topic_name)?;
        writer::write_bytes(resp, &self.partition_index)?;
        //writer::write_bytes(resp, &self.tag_buffer)?; // tag buffer
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TopicRequest {
    pub name: String,
    pub tag_buffer: u8,
}

impl TopicRequest {
    #[allow(dead_code)]
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        // Assuming COMPACT_STRING for name based on flexible version schemas
        writer::write_compact_string(resp, self.name.as_bytes())?;
        // Assuming tagged fields follow
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionsRequest {
    pub topics: Vec<TopicRequest>,
    pub response_partition_limit: i32,
    pub cursor: Option<Cursor>,
}

impl PartitionsRequest {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        let topics_len = parser::read_byte(req)?;
        let mut topics = Vec::with_capacity(topics_len as usize);
        if topics_len > 0 {
            for _ in 0..(topics_len - 1) {
                // COMPACT_STRING for name
                let name_buf = parser::read_compact_string(req)?;
                let name = String::from_utf8(name_buf)?;
                let tag_buffer = parser::read_byte(req)? as u8;
                topics.push(TopicRequest { name, tag_buffer });
            }
        }
        let response_partition_limit = parser::read_int(req)?;
        // Parse cursor
        let len = parser::read_byte(req)?;
        let cursor = if len == 0xff_u8 as i8 {
            let _tag_buffer = parser::read_byte(req)? as u8;
            None
        } else {
            let partition_index = parser::read_int(req)?;
            let tag_buffer = parser::read_byte(req)? as u8;
            Some(Cursor {
                topic_name: vec![], // TODO - must read name from the stream
                partition_index,
                tag_buffer,
            })
        };

        Ok(Self {
            topics,
            response_partition_limit,
            cursor,
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
    pub eligible_leader_repilcas: Vec<i32>,
    pub last_known_elr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
    pub tagged_field: u8,
}

impl Partition {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.error_code)?;
        writer::write_bytes(resp, &self.partition_index)?;
        writer::write_bytes(resp, &self.leader_id)?;
        writer::write_bytes(resp, &self.leader_epoch)?;
        writer::write_bytes(resp, &(self.replica_nodes.len() as u8))?;
        // replica nodes
        self.replica_nodes
            .iter()
            .try_for_each(|rn| writer::write_bytes(resp, rn))?;

        // ISR nodes
        writer::write_bytes(resp, &(self.isr_nodes.len() as u8))?; // isr_nodes
        self.isr_nodes
            .iter()
            .try_for_each(|rn| writer::write_bytes(resp, rn))?;

        // Eligible Leader Replicas
        writer::write_bytes(resp, &(self.eligible_leader_repilcas.len() as u8))?; // isr_nodes
        self.eligible_leader_repilcas
            .iter()
            .try_for_each(|rn| writer::write_bytes(resp, rn))?;

        // Last Known ELR
        writer::write_bytes(resp, &(self.last_known_elr.len() as u8))?; // isr_nodes
        self.last_known_elr
            .iter()
            .try_for_each(|rn| writer::write_bytes(resp, rn))?;

        // Offline replicas
        writer::write_bytes(resp, &(self.offline_replicas.len() as u8))?;
        self.offline_replicas
            .iter()
            .try_for_each(|or| writer::write_bytes(resp, or))?;
        writer::write_bytes(resp, &self.tagged_field)?;

        Ok(())
    }
}

// for partitions response
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Topic {
    pub error_code: u16,
    pub name: String,
    pub topic_id: u128,
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
    pub topic_authorized_operations: i32,
    pub tagged_field: u8,
}

impl Topic {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.error_code)?;
        // v0 uses COMPACT NULLABLE STRING for name
        // TODO check if -1 must be sent if topic does not exist
        writer::write_bytes(resp, &(1 + self.name.len() as u8))?;
        resp.write_all(self.name.as_bytes())?;

        writer::write_bytes(resp, &self.topic_id)?;
        writer::write_bytes(resp, &self.is_internal)?;
        writer::write_bytes(resp, &(1 + self.partitions.len() as u8))?;
        self.partitions.iter().try_for_each(|p| p.serialize(resp))?;
        writer::write_bytes(resp, &self.topic_authorized_operations)?;
        writer::write_bytes(resp, &self.tagged_field)?;
        Ok(())
    }
}

// for partitions response
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionsResponse {
    pub throttle_ms: u32,
    pub topics: Vec<Topic>,
    pub next_cursor: Option<Cursor>,
    pub tagged_field: u8,
}

#[allow(dead_code)]
impl PartitionsResponse {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.throttle_ms)?;
        writer::write_bytes(resp, &(1 + self.topics.len() as u8))?;
        self.topics
            .iter()
            .try_for_each(|topic| topic.serialize(resp))?;

        if let Some(cursor) = &self.next_cursor {
            cursor.serialize(resp)?;
        } else {
            writer::write_bytes(resp, &(-1_i8))?;
        }

        writer::write_bytes(resp, &self.tagged_field)?;
        Ok(())
    }
}
