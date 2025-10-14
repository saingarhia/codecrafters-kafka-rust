use crate::kafka::{self, errors};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

use crate::kafka::parser;

use super::{records, ErrorCodes};

pub type LogBatchRecords = Vec<records::RecordsBatch>;

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub uuid: [u8; 16],
    pub uuid_u128: u128,
    pub record_id1: usize,
    pub record_id2: usize,
    pub topic_name: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct PartitionMetadata {
    pub partition_id: i32,
    pub record_id1: usize,
    pub record_id2: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct Metadata {
    pub topic_map: HashMap<u128, TopicMetadata>,
    pub partition_map: HashMap<u128, Vec<PartitionMetadata>>,
    pub records: LogBatchRecords,
}

impl Metadata {
    pub fn new(filename: &str) -> errors::Result<Self> {
        if let Ok(f) = File::open(filename) {
            let mut reader = BufReader::new(f);
            Self::decode(&mut reader)
        } else {
            println!("File not found!! - generating dummy Metadata!");
            Ok(Self {
                ..Default::default()
            })
        }
    }

    fn decode<R: Read>(buffer: &mut R) -> errors::Result<Self> {
        let mut topic_map: HashMap<u128, TopicMetadata> = HashMap::new();
        let mut partition_map: HashMap<u128, Vec<PartitionMetadata>> = HashMap::new();
        let mut records: Vec<records::RecordsBatch> = vec![];

        loop {
            match records::RecordsBatch::deserialize(buffer) {
                Ok(record) => {
                    if record.batch_length == 0 {
                        // Likely an empty read at the end of the file
                        break;
                    }
                    records.push(record)
                }
                Err(e) => {
                    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            // Clean end of file
                            break;
                        }
                    }
                    // For any other error, we propagate it.
                    return Err(e);
                }
            }
        }

        records.iter().enumerate().for_each(|(i, batch)| {
            batch
                .records
                .iter()
                .enumerate()
                .for_each(|(r2, rec)| match &rec.value {
                    kafka::records::KafkaRecordValue::KafkaRecordTopicRecordType(v) => {
                        let uuid = u128::from_be_bytes(v.topic_uuid);
                        let meta = TopicMetadata {
                            uuid: v.topic_uuid.clone(),
                            uuid_u128: uuid,
                            record_id1: i,
                            record_id2: r2,
                            topic_name: String::from_utf8(v.topic_name.clone()).unwrap(),
                        };
                        topic_map
                            .entry(uuid)
                            .and_modify(|v| *v = meta.clone())
                            .or_insert(meta);
                    }
                    kafka::records::KafkaRecordValue::KafkaRecordPartitionType(v) => {
                        let meta = vec![PartitionMetadata {
                            partition_id: v.partition_id,
                            record_id1: i,
                            record_id2: r2,
                        }];
                        let uuid = u128::from_be_bytes(v.topic_uuid);
                        partition_map
                            .entry(uuid)
                            .and_modify(|v| *v = meta.clone())
                            .or_insert(meta);
                    }
                    _ => (),
                });
            println!("======================= batch {i} =======================\n");
            println!("{}", batch);
            println!("Topic Map: {:?}", topic_map);
            println!("Partition Map: {:?}", partition_map);
            println!("==========================================================\n");
        });

        Ok(Metadata {
            topic_map,
            partition_map,
            records,
        })
    }

    #[allow(dead_code)]
    pub fn get_topic(&self, topic: u128) -> Option<&TopicMetadata> {
        self.topic_map.get(&topic)
    }

    #[allow(dead_code)]
    pub fn get_partition() -> PartitionMetadata {
        todo!()
    }
}
