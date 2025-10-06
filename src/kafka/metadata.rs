use crate::kafka::errors;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

use crate::kafka::parser;

use super::records;

pub type LogBatchRecords = Vec<records::RecordsBatch>;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub uuid: u128,
    pub record_id1: usize,
    pub record_id2: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub partition_id: i32,
    pub record_id1: usize,
    pub record_id2: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Metadata {
    pub topic_map: HashMap<Vec<u8>, TopicMetadata>,
    pub partition_map: HashMap<u128, Vec<PartitionMetadata>>,
    pub records: LogBatchRecords,
}

impl Metadata {
    pub fn new(filename: &str) -> errors::Result<Self> {
        let f = File::open(filename)?;
        let mut reader = BufReader::new(f);
        Self::decode(&mut reader)
    }

    fn decode<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut topic_map = HashMap::new();
        let mut partition_map = HashMap::new();
        let mut records: Vec<records::RecordsBatch> = vec![];

        loop {
            match records::RecordsBatch::deserialize(buffer) {
                Ok(record) => records.push(record),
                Err(e) => {
                    println!("Not able to read more records - error: {e:?}");
                    break;
                }
            }
        }

        println!("Metadata decode - read records: {}\n", records.len());
        records.iter().enumerate().for_each(|(i, record)| {
            println!("======================= record {i} =======================\n");
            println!("{}", record);
            println!("==========================================================\n");
        });

        Ok(Metadata {
            topic_map,
            partition_map,
            records,
        })
    }
}
