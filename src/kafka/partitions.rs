use crate::kafka::{errors, parser, writer};
use std::io::{Read, Write};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Cursor {
    pub topic_name: Vec<u8>,
    pub partition_index: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionsRequest {
    pub topics: Vec<Vec<u8>>,
    pub response_partition_limit: i32,
    pub cursor: Cursor,
}

impl PartitionsRequest {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
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
        //1let b = parser::read_byte(req)?;
        //println!("Newly read byte: {}", b);
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
            },
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
    pub partition_index: u32,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub replica_nodes: Vec<u32>,
    pub isr_nodes: Vec<u32>,
    pub eligible_leader_replicas: Vec<u32>,
    pub last_known_elr: Vec<u32>,
    pub offline_replicas: Vec<u32>,
    pub tag_buffer: u8,
}

impl Partition {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.error_code)?;
        writer::write_bytes(resp, &self.partition_index)?;
        writer::write_bytes(resp, &self.leader_id)?;
        writer::write_bytes(resp, &self.leader_epoch)?;
        writer::write_uvarint(resp, (self.replica_nodes.len() + 1) as i32)?;
        self.replica_nodes
            .iter()
            .try_for_each(|rn| writer::write_bytes(resp, rn))?;
        writer::write_uvarint(resp, (self.isr_nodes.len() + 1) as i32)?;
        self.isr_nodes
            .iter()
            .try_for_each(|inn| writer::write_bytes(resp, inn))?;
        writer::write_uvarint(resp, (self.eligible_leader_replicas.len() + 1) as i32)?;
        self.eligible_leader_replicas
            .iter()
            .try_for_each(|elr| writer::write_bytes(resp, elr))?;
        writer::write_uvarint(resp, (self.last_known_elr.len() + 1) as i32)?;
        self.last_known_elr
            .iter()
            .try_for_each(|elr| writer::write_bytes(resp, elr))?;
        writer::write_varint(resp, self.offline_replicas.len() + 1)?;
        self.offline_replicas
            .iter()
            .try_for_each(|or| writer::write_bytes(resp, or))?;
        writer::write_bytes(resp, &self.tag_buffer)?;
        Ok(())
    }
}

// for partitions response
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Topic {
    pub error_code: u16,
    pub name: Option<Vec<u8>>,
    pub topic_id: u128,
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
    pub topic_authorized_operations: u32,
    pub tag_buffer: u8,
}

impl Topic {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.error_code)?;
        writer::write_nullable_compact_string(resp, self.name.as_deref())?;

        // TODO: not sure why decoder needs this additional 4 byte block??
        //writer::write_bytes(resp, &0_u32)?;

        writer::write_bytes(resp, &self.topic_id)?;
        writer::write_bool(resp, self.is_internal)?;
        writer::write_uvarint(resp, (self.partitions.len() + 1) as i32)?;
        self.partitions.iter().try_for_each(|p| p.serialize(resp))?;
        writer::write_bytes(resp, &self.topic_authorized_operations)?;
        // tag buffer
        writer::write_varint(resp, self.tag_buffer as usize)?;
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
    pub next_cursor: Option<NextCursor>,
    pub tag_buffer: u8,
}

#[allow(dead_code)]
impl PartitionsResponse {
    pub fn serialize<W: Write>(&self, resp: &mut W) -> errors::Result<()> {
        writer::write_bytes(resp, &self.throttle_ms)?;
        writer::write_bytes(resp, &(self.topics.len() as i32))?;
        self.topics
            .iter()
            .try_for_each(|topic| topic.serialize(resp))?;
        match self.next_cursor.as_ref() {
            Some(c) => c.serialize(resp)?,
            None => writer::write_null(resp)?,
        }
        writer::write_bytes(resp, &self.tag_buffer)
    }
}
