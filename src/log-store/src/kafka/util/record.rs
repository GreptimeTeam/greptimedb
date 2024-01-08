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

use std::collections::HashMap;

use rskafka::record::Record as KafkaRecord;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    DecodeJsonSnafu, EmptyEntriesSnafu, EncodeJsonSnafu, GetClientSnafu, IllegalSequenceSnafu,
    MissingKeySnafu, MissingValueSnafu, ProduceRecordSnafu, Result,
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
/// - If the entry is able to fit into a Kafka record, it's converted into a Full record.
///
/// - If the entry is too large to fit into a Kafka record, it's converted into a collection of records.
/// Those records must contain exactly one First record and one Last record, and potentially several
/// Middle records. There may be no Middle record.
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
}

/// The minimal storage unit in the Kafka log store.
///
/// An entry will be first converted into several Records before producing.
/// If an entry is able to fit into a KafkaRecord, it converts to a single Record.
/// If otherwise an entry cannot fit into a KafkaRecord, it will be split into a collection of Records.
///
/// A KafkaRecord is the minimal storage unit used by Kafka client and Kafka server.
/// The Kafka client produces KafkaRecords and consumes KafkaRecords, and Kafka server stores
/// a collection of KafkaRecords.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Record {
    /// The metadata of the record.
    pub(crate) meta: RecordMeta,
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
            timestamp: chrono::Utc::now(),
            headers: Default::default(),
        })
    }
}

impl TryFrom<KafkaRecord> for Record {
    type Error = crate::error::Error;

    fn try_from(kafka_record: KafkaRecord) -> Result<Self> {
        let key = kafka_record.key.context(MissingKeySnafu)?;
        let meta = serde_json::from_slice(&key).context(DecodeJsonSnafu)?;
        let data = kafka_record.value.context(MissingValueSnafu)?;
        Ok(Self { meta, data })
    }
}

impl From<Vec<Record>> for EntryImpl {
    fn from(records: Vec<Record>) -> Self {
        let entry_id = records[0].meta.entry_id;
        let ns = records[0].meta.ns.clone();
        let data = records.into_iter().flat_map(|record| record.data).collect();
        EntryImpl {
            data,
            id: entry_id,
            ns,
        }
    }
}

/// Produces a record to a kafka topic.
pub(crate) struct RecordProducer {
    /// The namespace of the entries.
    ns: NamespaceImpl,
    /// Entries are buffered before being built into a record.
    entries: Vec<EntryImpl>,
}

impl RecordProducer {
    /// Creates a new producer for producing entries with the given namespace.
    pub(crate) fn new(ns: NamespaceImpl) -> Self {
        Self {
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
    /// Returns the offset of the last successfully produced record.
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
        let max_record_size =
            client_manager.config.max_batch_size.as_bytes() as usize - ESTIMATED_META_SIZE;
        for entry in self.entries {
            for record in build_records(entry, max_record_size) {
                let kafka_record = KafkaRecord::try_from(record)?;
                // Records of a certain region cannot be produced in parallel since their order must be static.
                let offset = producer
                    .produce(kafka_record.clone())
                    .await
                    .map(Offset)
                    .with_context(|_| ProduceRecordSnafu {
                        topic: &self.ns.topic,
                        size: kafka_record.approximate_size(),
                        limit: max_record_size,
                    })?;
                last_offset = Some(offset);
            }
        }
        // Safety: there must be at least one record produced when the entries are guaranteed not empty.
        Ok(last_offset.unwrap())
    }
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
            },
            data: chunk.to_vec(),
        })
        .collect()
}

pub fn maybe_emit_entry(
    record: Record,
    entry_records: &mut HashMap<EntryId, Vec<Record>>,
) -> Result<Option<EntryImpl>> {
    let mut entry = None;
    match record.meta.tp {
        RecordType::Full => {
            entry = Some(EntryImpl::from(vec![record]));
        }
        RecordType::First => {
            ensure!(
                !entry_records.contains_key(&record.meta.entry_id),
                IllegalSequenceSnafu {
                    error: "First record must be the first"
                }
            );
            entry_records.insert(record.meta.entry_id, vec![record]);
        }
        RecordType::Middle(seq) => {
            let prefix =
                entry_records
                    .get_mut(&record.meta.entry_id)
                    .context(IllegalSequenceSnafu {
                        error: "Middle record must not be the first",
                    })?;
            // Safety: the records are guaranteed not empty if the key exists.
            let last_record = prefix.last().unwrap();
            let legal = match last_record.meta.tp {
                // Legal if this record follows a First record.
                RecordType::First => seq == 1,
                // Legal if this record follows a Middle record just prior to this record.
                RecordType::Middle(last_seq) => last_seq + 1 == seq,
                // Illegal sequence.
                _ => false,
            };
            ensure!(
                legal,
                IllegalSequenceSnafu {
                    error: "Illegal prefix for a Middle record"
                }
            );

            prefix.push(record);
        }
        RecordType::Last => {
            // There must have a sequence prefix before a Last record is read.
            let mut records =
                entry_records
                    .remove(&record.meta.entry_id)
                    .context(IllegalSequenceSnafu {
                        error: "Missing prefix for a Last record",
                    })?;
            records.push(record);
            entry = Some(EntryImpl::from(records));
        }
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_base::readable_size::ReadableSize;
    use common_config::wal::KafkaConfig;
    use uuid::Uuid;

    use super::*;
    use crate::kafka::client_manager::ClientManager;
    use crate::kafka::util::test_util::run_test_with_kafka_wal;

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
    }

    fn new_test_entry<D: AsRef<[u8]>>(data: D, entry_id: EntryId, ns: NamespaceImpl) -> EntryImpl {
        EntryImpl {
            data: data.as_ref().to_vec(),
            id: entry_id,
            ns,
        }
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
            },
            data: b"12345".to_vec(),
        };
        let kafka_record: KafkaRecord = record.clone().try_into().unwrap();
        let got = Record::try_from(kafka_record).unwrap();
        assert_eq!(record, got);
    }

    /// Tests that the reconstruction of an entry works as expected.
    #[test]
    fn test_reconstruct_entry() {
        let template = Record::default();
        let records = vec![
            template.with_data(b"111").with_tp(RecordType::First),
            template.with_data(b"222").with_tp(RecordType::Middle(1)),
            template.with_data(b"333").with_tp(RecordType::Last),
        ];
        let entry = EntryImpl::from(records.clone());
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
        let template = Record::default().with_ns(ns);
        let mut entry_records = HashMap::from([
            (
                1,
                vec![template.with_entry_id(1).with_tp(RecordType::First)],
            ),
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
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_some());

        // A First record arrives with no prefix.
        let got = maybe_emit_entry(
            template.with_entry_id(0).with_tp(RecordType::First),
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_none());

        // A First record arrives with some prefix.
        let got = maybe_emit_entry(
            template.with_entry_id(1).with_tp(RecordType::First),
            &mut entry_records,
        );
        assert!(got.is_err());

        // A Middle record arrives with legal prefix (First).
        let got = maybe_emit_entry(
            template.with_entry_id(2).with_tp(RecordType::Middle(1)),
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_none());

        // A Middle record arrives with legal prefix (Middle).
        let got = maybe_emit_entry(
            template.with_entry_id(2).with_tp(RecordType::Middle(2)),
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_none());

        // A Middle record arrives with illegal prefix.
        let got = maybe_emit_entry(
            template.with_entry_id(2).with_tp(RecordType::Middle(1)),
            &mut entry_records,
        );
        assert!(got.is_err());

        // A Middle record arrives with no prefix.
        let got = maybe_emit_entry(
            template.with_entry_id(22).with_tp(RecordType::Middle(1)),
            &mut entry_records,
        );
        assert!(got.is_err());

        // A Last record arrives with no prefix.
        let got = maybe_emit_entry(
            template.with_entry_id(33).with_tp(RecordType::Last),
            &mut entry_records,
        );
        assert!(got.is_err());

        // A Last record arrives with legal prefix.
        let got = maybe_emit_entry(
            template.with_entry_id(3).with_tp(RecordType::Last),
            &mut entry_records,
        )
        .unwrap();
        assert!(got.is_some());

        // Check state.
        assert_eq!(entry_records.len(), 3);
        assert_eq!(entry_records[&0].len(), 1);
        assert_eq!(entry_records[&1].len(), 1);
        assert_eq!(entry_records[&2].len(), 3);
    }

    #[tokio::test]
    async fn test_produce_large_entry() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                let topic = format!("greptimedb_wal_topic_{}", Uuid::new_v4());
                let ns = NamespaceImpl {
                    region_id: 1,
                    topic,
                };
                let entry = new_test_entry([b'1'; 2000000], 0, ns.clone());
                let producer = RecordProducer::new(ns.clone()).with_entries(vec![entry]);
                let config = KafkaConfig {
                    broker_endpoints,
                    max_batch_size: ReadableSize::mb(1),
                    ..Default::default()
                };
                let manager = Arc::new(ClientManager::try_new(&config).await.unwrap());
                producer.produce(&manager).await.unwrap();
            })
        })
        .await
    }
}
