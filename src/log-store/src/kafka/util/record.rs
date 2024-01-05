// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};

use rskafka::record::Record as KafkaRecord;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    ChecksumMismatchedSnafu, DecodeJsonSnafu, EmptyEntriesSnafu, EncodeJsonSnafu, GetClientSnafu,
    IllegalSequenceSnafu, MissingValueSnafu, ProduceRecordSnafu, Result,
};
use crate::kafka::client_manager::ClientManagerRef;
use crate::kafka::util::offset::Offset;
use crate::kafka::{EntryId, EntryImpl, NamespaceImpl};

/// The current version of Record.
pub(crate) const VERSION: u32 = 0;

/// The estimated size in bytes of a serialized RecordMeta.
/// A record is guaranteed to have sizeof(meta) + sizeof(data) <= max_batch_size - ESTIMATED_META_SIZE.
const ESTIMATED_META_SIZE: usize = 256;

/// The type of a record.
///
/// If the entry is able to fit into a Kafka record, it's converted into a Full record.
/// If the entry is too large to fit into a Kafka record, it's converted into a collection of record.
/// Those records must contain exactly one First record and one Last record, and potentially several
/// Middle record. There may be no Middle record.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum RecordType {
    /// The record is self-contained, i.e. an entry's data is fully stored into this record.
    Full,
    /// The record contains the first part of an entry's data.
    First,
    /// The record contains one of the middle parts of an entry's data.
    /// The sequence of the record is identified by the inner field.
    Middle(usize),
    /// The record contains the last part of an entry's data.
    Last,
}

impl From<RecordType> for i32 {
    fn from(tp: RecordType) -> Self {
        match tp {
            RecordType::Full => -1,
            RecordType::First => 0,
            RecordType::Middle(seq) => seq as i32,
            RecordType::Last => i32::MAX,
        }
    }
}

/// The metadata of a record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordMeta {
    /// The version of the record. Used for backward compatibility.
    version: u32,
    /// The type of the record.
    pub tp: RecordType,
    /// The id of the entry the record associated with.
    pub entry_id: EntryId,
    /// The namespace of the entry the record associated with.
    pub ns: NamespaceImpl,
    /// The computed checksum of the payload.
    checksum: u32,
}

/// The minimal storage unit in the Kafka log store.
/// Our own Record will be converted into a Kafka record during producing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Record {
    /// The metadata of the record.
    meta: RecordMeta,
    /// The payload of the record.
    data: Vec<u8>,
}

impl TryFrom<Record> for KafkaRecord {
    type Error = crate::error::Error;

    fn try_from(record: Record) -> Result<Self> {
        let key = serde_json::to_vec(&record.meta).context(EncodeJsonSnafu)?;
        Ok(KafkaRecord {
            key: Some(key),
            value: Some(record.data),
            timestamp: rskafka::chrono::Utc::now(),
            headers: Default::default(),
        })
    }
}

impl TryFrom<KafkaRecord> for Record {
    type Error = crate::error::Error;

    fn try_from(kafka_record: KafkaRecord) -> Result<Self> {
        let key = kafka_record.key.unwrap();
        let meta = serde_json::from_slice(&key)
            .context(DecodeJsonSnafu)
            .unwrap();
        let data = kafka_record.value.context(MissingValueSnafu)?;
        Ok(Self { meta, data })
    }
}

impl TryFrom<Vec<Record>> for EntryImpl {
    type Error = crate::error::Error;

    fn try_from(records: Vec<Record>) -> Result<Self> {
        check_records(&records)?;

        let entry_id = records[0].meta.entry_id;
        let ns = records[0].meta.ns.clone();
        let data = records.into_iter().flat_map(|record| record.data).collect();
        Ok(EntryImpl {
            data,
            id: entry_id,
            ns,
        })
    }
}

/// Produces a record to a kafka topic.
pub(crate) struct RecordProducer {
    /// The max size (in bytes) of a Kafka record.
    max_record_size: usize,
    /// The namespace of the entries.
    ns: NamespaceImpl,
    /// Entries are buffered before being built into a record.
    entries: Vec<EntryImpl>,
}

impl RecordProducer {
    /// Creates a new producer for producing entries with the given namespace.
    pub(crate) fn new(ns: NamespaceImpl, max_record_size: usize) -> Self {
        Self {
            max_record_size,
            ns,
            entries: Vec::new(),
        }
    }

    /// Populates the entry buffer with the given entries.
    pub(crate) fn with_entries(self, entries: Vec<EntryImpl>) -> Self {
        Self { entries, ..self }
    }

    /// Pushes an entry into the entry buffer.
    pub(crate) fn push(&mut self, entry: EntryImpl) {
        self.entries.push(entry);
    }

    /// Produces the buffered entries to Kafka sever. Those entries may span several Kafka records.
    pub(crate) async fn produce(self, client_manager: &ClientManagerRef) -> Result<Offset> {
        ensure!(!self.entries.is_empty(), EmptyEntriesSnafu);

        // Gets the producer in which a record buffer is maintained.
        let producer = client_manager
            .get_or_insert(&self.ns.topic)
            .await
            .map_err(|e| {
                GetClientSnafu {
                    topic: &self.ns.topic,
                    error: e.to_string(),
                }
                .build()
            })?
            .producer;

        // Stores the offset of the last successfully produced record.
        let mut last_offset = None;
        for entry in self.entries {
            for record in build_records(entry, self.max_record_size - ESTIMATED_META_SIZE) {
                let kafka_record = KafkaRecord::try_from(record)?;
                // Records of a certain region cannot be produced in parallel since their order must be static.
                let offset = producer
                    .produce(kafka_record.clone())
                    .await
                    .map(Offset)
                    .context(ProduceRecordSnafu {
                        topic: &self.ns.topic,
                        size: kafka_record.approximate_size(),
                        limit: self.max_record_size,
                    })?;
                last_offset = Some(offset);
            }
        }
        // Safety: there must be at least one record produced when the entries are guaranteed not empty.
        Ok(last_offset.unwrap())
    }
}

fn check_records(records: &[Record]) -> Result<()> {
    let len = records.len();
    ensure!(
        len > 0,
        IllegalSequenceSnafu {
            error: "Empty sequence"
        }
    );

    let mut sequence = Vec::with_capacity(len);
    let mut entry_ids = HashSet::with_capacity(len);
    let mut namespaces = HashSet::with_capacity(len);
    let mut checksum_matched = HashSet::with_capacity(len);
    for record in records {
        sequence.push(i32::from(record.meta.tp));
        entry_ids.insert(record.meta.entry_id);
        namespaces.insert(&record.meta.ns);
        checksum_matched.insert(record.meta.checksum == crc32fast::hash(&record.data));
    }

    ensure!(
        entry_ids.len() == 1,
        IllegalSequenceSnafu {
            error: "Non-unique entry ids"
        }
    );

    ensure!(
        namespaces.len() == 1,
        IllegalSequenceSnafu {
            error: "Non-unique namespaces"
        }
    );

    ensure!(
        checksum_matched.len() == 1 && checksum_matched.contains(&true),
        ChecksumMismatchedSnafu
    );

    if len == 1 {
        // A sequence with a single record must contain only a Full record.
        if sequence[0] != i32::from(RecordType::Full) {
            return IllegalSequenceSnafu {
                error: "Missing Full record",
            }
            .fail();
        }
    } else {
        // A sequence with multiple records must start with a First record and end with a Last record.
        // In addition, the Middle records must in order.
        let prefix = (0..(len - 1) as i32).collect::<Vec<_>>();
        ensure!(
            sequence[0..len - 1] == prefix
                && sequence.last().unwrap() == &i32::from(RecordType::Last),
            IllegalSequenceSnafu {
                error: "Out of order",
            }
        );
    }

    Ok(())
}

fn record_type(seq: usize, num_records: usize) -> RecordType {
    if seq == 0 {
        RecordType::First
    } else if seq == num_records - 1 {
        RecordType::Last
    } else {
        RecordType::Middle(seq)
    }
}

fn build_records(entry: EntryImpl, max_record_size: usize) -> Vec<Record> {
    if entry.data.len() <= max_record_size {
        let record = Record {
            meta: RecordMeta {
                version: VERSION,
                tp: RecordType::Full,
                entry_id: entry.id,
                ns: entry.ns,
                checksum: crc32fast::hash(&entry.data),
            },
            data: entry.data,
        };
        return vec![record];
    }

    let chunks = entry.data.chunks(max_record_size);
    let num_chunks = chunks.len();
    chunks
        .enumerate()
        .map(|(i, chunk)| Record {
            meta: RecordMeta {
                version: VERSION,
                tp: record_type(i, num_chunks),
                entry_id: entry.id,
                ns: entry.ns.clone(),
                checksum: crc32fast::hash(chunk),
            },
            data: chunk.to_vec(),
        })
        .collect()
}

pub fn maybe_emit_entry(
    record: Record,
    ns: &NamespaceImpl,
    entry_records: &mut HashMap<EntryId, Vec<Record>>,
) -> Result<Option<EntryImpl>> {
    let mut entry = None;
    match record.meta.tp {
        RecordType::Full => {
            entry = Some(EntryImpl::try_from(vec![record])?);
        }
        RecordType::Last => {
            // There must have a sequence prefix before a Last record is read.
            let mut records =
                entry_records
                    .remove(&record.meta.entry_id)
                    .context(IllegalSequenceSnafu {
                        error: "Missing sequence prefix",
                    })?;
            records.push(record);
            entry = Some(EntryImpl::try_from(records)?);
        }
        _ => {
            entry_records
                .entry(record.meta.entry_id)
                .or_default()
                .push(record);
        }
    }

    // Filters entries by namespace.
    if let Some(entry) = entry
        && &entry.ns == ns
    {
        return Ok(Some(entry));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_base::readable_size::ReadableSize;
    use common_config::wal::KafkaConfig;
    use rand::Rng;

    use super::*;
    use crate::kafka::client_manager::ClientManager;

    // Implements some utility methods for testing.
    impl Default for Record {
        fn default() -> Self {
            Self {
                meta: RecordMeta {
                    version: VERSION,
                    tp: RecordType::Full,
                    ns: NamespaceImpl {
                        region_id: 0,
                        topic: "greptimedb_wal_topic".to_string(),
                    },
                    entry_id: 0,
                    checksum: 0,
                },
                data: Vec::new(),
            }
        }
    }

    impl Record {
        /// Overrides tp.
        fn with_tp(&self, tp: RecordType) -> Self {
            Self {
                meta: RecordMeta {
                    tp,
                    ..self.meta.clone()
                },
                ..self.clone()
            }
        }

        /// Overrides data with the given data.
        fn with_data(&self, data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                ..self.clone()
            }
        }

        /// Overrides entry id.
        fn with_entry_id(&self, entry_id: EntryId) -> Self {
            Self {
                meta: RecordMeta {
                    entry_id,
                    ..self.meta.clone()
                },
                ..self.clone()
            }
        }

        /// Overrides namespace.
        fn with_ns(&self, ns: NamespaceImpl) -> Self {
            Self {
                meta: RecordMeta { ns, ..self.meta },
                ..self.clone()
            }
        }

        /// Overrides checksum.
        fn with_checksum(&self, checksum: u32) -> Self {
            Self {
                meta: RecordMeta {
                    checksum,
                    ..self.meta.clone()
                },
                ..self.clone()
            }
        }
    }

    fn new_test_entry<D: AsRef<[u8]>>(data: D, entry_id: EntryId, ns: NamespaceImpl) -> EntryImpl {
        EntryImpl {
            data: data.as_ref().to_vec(),
            id: entry_id,
            ns,
        }
    }

    /// Tests that the `check_records` could handle various record sequences.
    #[test]
    fn test_check_records() {
        // On empty records.
        let got = check_records(&[]).unwrap_err();
        let expected = IllegalSequenceSnafu {
            error: "Empty sequence",
        }
        .build();
        assert_eq!(got.to_string(), expected.to_string());

        // On non-unique entry ids.
        let template = Record::default();
        let got =
            check_records(&[template.with_entry_id(0), template.with_entry_id(1)]).unwrap_err();
        let expected = IllegalSequenceSnafu {
            error: "Non-unique entry ids",
        }
        .build();
        assert_eq!(got.to_string(), expected.to_string());

        // On non-unique namespaces.
        let template = Record::default();
        let got = check_records(&[
            template.with_ns(NamespaceImpl {
                region_id: 1,
                topic: "greptimedb_wal_topic".to_string(),
            }),
            template.with_ns(NamespaceImpl {
                region_id: 2,
                topic: "greptimedb_wal_topic".to_string(),
            }),
        ])
        .unwrap_err();
        let expected = IllegalSequenceSnafu {
            error: "Non-unique namespaces",
        }
        .build();
        assert_eq!(got.to_string(), expected.to_string());

        // On mismatched checksums.
        let template = Record::default()
            .with_data(b"123")
            .with_checksum(crc32fast::hash(b"123"));
        let got = check_records(&[template.with_data(b"234")]).unwrap_err();
        assert_eq!(got.to_string(), ChecksumMismatchedSnafu.build().to_string());

        // On illegal record sequences.
        let missing_full_err = IllegalSequenceSnafu {
            error: "Missing Full record",
        }
        .build()
        .to_string();
        let out_of_order_err = IllegalSequenceSnafu {
            error: "Out of order",
        }
        .build()
        .to_string();

        let template = Record::default();
        let got = check_records(&[template.with_tp(RecordType::First)]).unwrap_err();
        assert_eq!(got.to_string(), missing_full_err);

        let got = check_records(&[template.with_tp(RecordType::Last)]).unwrap_err();
        assert_eq!(got.to_string(), missing_full_err);

        let got = check_records(&[template.with_tp(RecordType::Middle(1))]).unwrap_err();
        assert_eq!(got.to_string(), missing_full_err);

        let got = check_records(&[
            template.with_tp(RecordType::Full),
            template.with_tp(RecordType::First),
        ])
        .unwrap_err();
        assert_eq!(got.to_string(), out_of_order_err);

        let got = check_records(&[
            template.with_tp(RecordType::First),
            template.with_tp(RecordType::Middle(0)),
        ])
        .unwrap_err();
        assert_eq!(got.to_string(), out_of_order_err);

        let got = check_records(&[
            template.with_tp(RecordType::First),
            template.with_tp(RecordType::Middle(1)),
        ])
        .unwrap_err();
        assert_eq!(got.to_string(), out_of_order_err);

        let got = check_records(&[
            template.with_tp(RecordType::First),
            template.with_tp(RecordType::Middle(0)),
            template.with_tp(RecordType::Last),
        ])
        .unwrap_err();
        assert_eq!(got.to_string(), out_of_order_err);

        let got = check_records(&[
            template.with_tp(RecordType::First),
            template.with_tp(RecordType::Middle(2)),
            template.with_tp(RecordType::Last),
        ])
        .unwrap_err();
        assert_eq!(got.to_string(), out_of_order_err);

        let got = check_records(&[
            template.with_tp(RecordType::First),
            template.with_tp(RecordType::Middle(2)),
            template.with_tp(RecordType::Middle(1)),
            template.with_tp(RecordType::Last),
        ])
        .unwrap_err();
        assert_eq!(got.to_string(), out_of_order_err);

        let got = check_records(&[
            template.with_tp(RecordType::First),
            template.with_tp(RecordType::Middle(1)),
            template.with_tp(RecordType::Last),
        ]);
        assert!(got.is_ok());

        let got = check_records(&[
            template.with_tp(RecordType::First),
            template.with_tp(RecordType::Middle(1)),
            template.with_tp(RecordType::Middle(2)),
            template.with_tp(RecordType::Middle(3)),
            template.with_tp(RecordType::Last),
        ]);
        assert!(got.is_ok());
    }

    /// Tests that the `build_records` works as expected.
    #[test]
    fn test_build_records() {
        let max_record_size = 128;

        // On a small entry.
        let ns = NamespaceImpl {
            region_id: 1,
            topic: "greptimedb_wal_topic".to_string(),
        };
        let entry = new_test_entry([b'1'; 100], 0, ns.clone());
        let records = build_records(entry.clone(), max_record_size);
        assert!(records.len() == 1);
        assert_eq!(entry.data, records[0].data);

        // On a large entry.
        let entry = new_test_entry([b'1'; 150], 0, ns.clone());
        let records = build_records(entry.clone(), max_record_size);
        assert!(records.len() == 2);
        assert_eq!(&records[0].data, &[b'1'; 128]);
        assert_eq!(&records[1].data, &[b'1'; 22]);

        // On a way-too large entry.
        let entry = new_test_entry([b'1'; 5000], 0, ns.clone());
        let records = build_records(entry.clone(), max_record_size);
        let matched = entry
            .data
            .chunks(max_record_size)
            .enumerate()
            .all(|(i, chunk)| records[i].data == chunk);
        assert!(matched);
    }

    /// Tests that Record and KafkaRecord are able to be converted back and forth.
    #[test]
    fn test_record_conversion() {
        let record = Record {
            meta: RecordMeta {
                version: VERSION,
                tp: RecordType::Full,
                entry_id: 1,
                ns: NamespaceImpl {
                    region_id: 1,
                    topic: "greptimedb_wal_topic".to_string(),
                },
                checksum: crc32fast::hash(b"12345".as_slice()),
            },
            data: b"12345".to_vec(),
        };
        let kafka_record: KafkaRecord = record.clone().try_into().unwrap();
        let got = Record::try_from(kafka_record).unwrap();
        assert_eq!(record, got);
    }

    /// Tests that the reconstruction of an entry behaves as expected.
    #[test]
    fn test_reconstruct_entry() {
        let template = Record::default();
        let records = vec![
            template
                .with_data(b"111")
                .with_checksum(crc32fast::hash(b"111"))
                .with_tp(RecordType::First),
            template
                .with_data(b"222")
                .with_checksum(crc32fast::hash(b"222"))
                .with_tp(RecordType::Middle(1)),
            template
                .with_data(b"333")
                .with_checksum(crc32fast::hash(b"333"))
                .with_tp(RecordType::Last),
        ];
        let entry = EntryImpl::try_from(records.clone()).unwrap();
        assert_eq!(records[0].meta.entry_id, entry.id);
        assert_eq!(records[0].meta.ns, entry.ns);
        assert_eq!(
            entry.data,
            records
                .into_iter()
                .flat_map(|record| record.data)
                .collect::<Vec<_>>()
        );
    }

    /// Tests that `maybe_emit_entry` works as expected.
    /// This test does not check for illegal record sequences since they're already tested in the `test_check_records` test.
    #[test]
    fn test_maybe_emit_entry() {
        let ns = NamespaceImpl {
            region_id: 1,
            topic: "greptimedb_wal_topic".to_string(),
        };
        let template = Record::default().with_ns(ns.clone());
        let mut entry_records = HashMap::from([
            (
                2,
                vec![template.with_entry_id(2).with_tp(RecordType::First)],
            ),
            (
                3,
                vec![
                    template.with_entry_id(3).with_tp(RecordType::First),
                    template.with_entry_id(3).with_tp(RecordType::Middle(1)),
                ],
            ),
        ]);

        // A Full record arrives.
        let got = maybe_emit_entry(
            template.with_entry_id(0).with_tp(RecordType::Full),
            &ns,
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_some());

        // A First record arrives.
        let got = maybe_emit_entry(
            template.with_entry_id(1).with_tp(RecordType::First),
            &ns,
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_none());

        // A Middle record arrives.
        let got = maybe_emit_entry(
            template.with_entry_id(2).with_tp(RecordType::Middle(1)),
            &ns,
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_none());

        // A Last record arrives.
        let got = maybe_emit_entry(
            template.with_entry_id(3).with_tp(RecordType::Last),
            &ns,
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_some());

        // Check state.
        assert_eq!(entry_records.len(), 2);
        assert!(entry_records.contains_key(&1));
        assert!(entry_records.contains_key(&2));
    }

    #[tokio::test]
    async fn test_produce_large_entry() {
        let max_record_size = 1024 * 1024; // 1MB.
        let topic = format!("greptimedb_wal_topic_{}", rand::thread_rng().gen::<usize>());
        let ns = NamespaceImpl {
            region_id: 1,
            topic,
        };
        let entry = new_test_entry([b'1'; 2000000], 0, ns.clone());
        let producer = RecordProducer::new(ns.clone(), max_record_size).with_entries(vec![entry]);

        // TODO(niebayes): get broker endpoints from env vars.
        let config = KafkaConfig {
            broker_endpoints: vec!["localhost:9092".to_string()],
            max_batch_size: ReadableSize::mb(1),
            ..Default::default()
        };
        let manager = Arc::new(ClientManager::try_new(&config).await.unwrap());
        producer.produce(&manager).await.unwrap();
    }
}
