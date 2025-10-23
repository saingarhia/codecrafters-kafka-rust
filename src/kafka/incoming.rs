#[allow(dead_code)]
use super::{ErrorCodes, MAX_SUPPORTED_API_VERSION, MIN_SUPPORTED_API_VERSION};
use crate::kafka::{apikey, body, errors, fetch, header, metadata, partitions, produce, writer};
use std::fmt;
use std::fs::metadata;
use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};

// incoming request parser/handler
//
#[derive(Debug, Clone)]
pub struct Request {
    header: header::RequestHeader,
    body: body::RequestBody,
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            format_args!(
                concat!("Incoming Request:\n", "    Header: {}\n", "    Body: {}\n"),
                self.header, self.body
            )
        )
    }
}

impl Request {
    pub fn new<R: Read>(req: &mut R) -> errors::Result<Self> {
        // lets read message size
        // lets read the header
        let header = header::RequestHeader::new(req)?;
        println!("header: {}, now building body!!", header);
        let body = body::RequestBody::new(req, &header)?;
        Ok(Self { header, body })
    }

    pub fn process<W: Write>(
        &self,
        response: &mut W,
        metadata: &Arc<Mutex<metadata::Metadata>>,
    ) -> errors::Result<()> {
        // fill in the correlation id
        let _ = response.write(&self.header.get_correlation_id().to_be_bytes());

        //println!("Building response for Request: {}", self);
        let api_ver = self.header.get_api_ver();
        match &self.body {
            body::RequestBody::Fetch(fetcher) => {
                // tag buffer is the first after corelation ID
                writer::write_bytes(response, &0_u8)?;
                let fetch_resp = fetch::FetchResponse::new(fetcher, metadata);
                if let Err(e) = fetch_resp.serialize(response) {
                    println!("there's error serializing data: {e:?}");
                }
                println!("Fetch response serialized!!!!!");
            }
            body::RequestBody::ApiVersions(_throttle, _tbuf) => {
                if api_ver < super::MIN_SUPPORTED_API_VERSION || api_ver > MAX_SUPPORTED_API_VERSION
                {
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
            }
            body::RequestBody::DescribePartitions(p) => {
                println!("======================= its DescribePartitions ====================");
                // tag buffer is first (immediately after correlation id) as per the test
                writer::write_bytes(response, &0_u8)?;

                let metadata = metadata.lock().unwrap();
                let mut partitions_included = 0;
                let _topics_length = p.topics.len();
                let pr = partitions::PartitionsResponse {
                    throttle_ms: 0,
                    topics: p
                        .topics
                        .iter()
                        .map(|topic_name| {
                            let topic = metadata.get_topic_by_name(&topic_name.name);
                            let uuid = topic.map(|tt| tt.uuid_u128).unwrap_or(0);
                            let partition = metadata.partition_map.get(&uuid);
                            partitions::Topic {
                                error_code: if topic.is_some() { 0 } else { 3 },
                                name: topic_name.name.clone(),
                                topic_id: uuid,
                                is_internal: false,
                                partitions: partition.map_or(vec![], |pp| {
                                    let mut ps = vec![];
                                    let pps_to_include = pp.len().min(
                                        p.response_partition_limit as usize - partitions_included,
                                    );
                                    partitions_included += pps_to_include;
                                    for i in 0..pps_to_include {
                                        //p.response_partition_limit
                                        ps.push(partitions::Partition {
                                            error_code: 0,
                                            partition_index: pp[i].partition_id,
                                            leader_id: 0,
                                            leader_epoch: 0,
                                            replica_nodes: vec![],
                                            isr_nodes: vec![],
                                            eligible_leader_repilcas: vec![],
                                            last_known_elr: vec![],
                                            offline_replicas: vec![],
                                            tagged_field: 0,
                                        });
                                    }
                                    ps
                                }),
                                topic_authorized_operations: 0x1234,
                                tagged_field: 0,
                            }
                        })
                        .collect(),
                    next_cursor: None,
                    tagged_field: 0,
                };
                pr.serialize(response)?;
            }
            body::RequestBody::Produce(prod) => {
                println!("======================= its Produce ====================");
                // tag buffer is first (immediately after correlation id) as per the test
                writer::write_bytes(response, &0_u8)?;
                let prod_resp = produce::ProduceResponse::new(prod, metadata);
                if let Err(e) = prod_resp.serialize(response) {
                    println!("there's error serializing produce response: {e:?}");
                }
            }
        }
        Ok(())
    }
}
