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
use crate::metrics;

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

// TODO(niebayes): improve the performance of decoding kafka record.
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
    // TODO(niebayes): maybe requires more fine-grained metrics to measure stages of writing to kafka.
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

                metrics::METRIC_KAFKA_PRODUCE_RECORD_COUNTS.inc();
                metrics::METRIC_KAFKA_PRODUCE_RECORD_BYTES_TOTAL
                    .inc_by(kafka_record.approximate_size() as u64);

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

/// TODO(weny): Consider ignoring existing corrupted data instead of throwing an error.
pub fn maybe_emit_entry(
    record: Record,
    entry_records: &mut HashMap<u64, Vec<Record>>,
) -> Result<Option<EntryImpl>> {
    let mut entry = None;
    match record.meta.tp {
        RecordType::Full => {
            entry = Some(EntryImpl::from(vec![record]));
        }
        RecordType::First => {
            ensure!(
                !entry_records.contains_key(&record.meta.ns.region_id),
                IllegalSequenceSnafu {
                    error: format!(
                        "First record must be the first, region_id: {}",
                        record.meta.ns.region_id
                    )
                }
            );
            entry_records.insert(record.meta.ns.region_id, vec![record]);
        }
        RecordType::Middle(seq) => {
            if let Some(previous) = entry_records.get_mut(&record.meta.ns.region_id) {
                // Safety: the records are guaranteed not empty if the key exists.
                let last_record = previous.last().unwrap();
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

                previous.push(record);
            }
        }
        RecordType::Last => {
            // There must have a sequence prefix before a Last record is read.
            if let Some(mut records) = entry_records.remove(&record.meta.ns.region_id) {
                records.push(record);
                entry = Some(EntryImpl::from(records));
            }
        }
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_base::readable_size::ReadableSize;
    use common_wal::config::kafka::DatanodeKafkaConfig;
    use common_wal::test_util::run_test_with_kafka_wal;
    use uuid::Uuid;

    use super::*;
    use crate::kafka::client_manager::ClientManager;

    fn new_test_record(tp: RecordType, entry_id: EntryId, region_id: u64, data: Vec<u8>) -> Record {
        Record {
            meta: RecordMeta {
                version: VERSION,
                tp,
                ns: NamespaceImpl {
                    region_id,
                    topic: "greptimedb_wal_topic".to_string(),
                },
                entry_id,
            },
            data,
        }
    }

    fn new_test_entry_impl<D: AsRef<[u8]>>(
        data: D,
        entry_id: EntryId,
        ns: NamespaceImpl,
    ) -> EntryImpl {
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
        let entry = new_test_entry_impl([b'1'; 100], 0, ns.clone());
        let records = build_records(entry.clone(), max_record_size);
        assert!(records.len() == 1);
        assert_eq!(entry.data, records[0].data);

        // On a large entry.
        let entry = new_test_entry_impl([b'1'; 150], 0, ns.clone());
        let records = build_records(entry.clone(), max_record_size);
        assert!(records.len() == 2);
        assert_eq!(&records[0].data, &[b'1'; 128]);
        assert_eq!(&records[1].data, &[b'1'; 22]);

        // On a way-too large entry.
        let entry = new_test_entry_impl([b'1'; 5000], 0, ns.clone());
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
        let records = vec![
            new_test_record(RecordType::First, 0, 0, vec![1, 1, 1]),
            new_test_record(RecordType::Middle(1), 0, 0, vec![2, 2, 2]),
            new_test_record(RecordType::Last, 0, 0, vec![3, 3, 3]),
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

    #[tokio::test]
    async fn test_produce_large_entry() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                let topic = format!("greptimedb_wal_topic_{}", Uuid::new_v4());
                let ns = NamespaceImpl {
                    region_id: 1,
                    topic,
                };
                let entry = new_test_entry_impl([b'1'; 2000000], 0, ns.clone());
                let producer = RecordProducer::new(ns.clone()).with_entries(vec![entry]);
                let config = DatanodeKafkaConfig {
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

    #[test]
    fn test_maybe_emit_entry_unexpected_first_part() {
        let mut records = HashMap::new();
        let incoming_record = new_test_record(RecordType::First, 0, 0, vec![]);
        assert!(maybe_emit_entry(incoming_record, &mut records)
            .unwrap()
            .is_none());

        let incoming_record = new_test_record(RecordType::First, 0, 0, vec![]);
        assert!(maybe_emit_entry(incoming_record, &mut records)
            .unwrap_err()
            .to_string()
            .contains("First record must be the first"));
    }

    #[test]
    fn test_maybe_emit_entry_ignore_incomplete_part() {
        let mut records = HashMap::new();
        let incoming_record = new_test_record(RecordType::Last, 0, 0, vec![]);
        // Incomplete part will be ignored
        assert!(maybe_emit_entry(incoming_record, &mut records)
            .unwrap()
            .is_none());
        assert!(records.is_empty());

        let mut records = HashMap::new();
        let incoming_record = new_test_record(RecordType::Middle(1), 0, 0, vec![]);
        // Incomplete part will be ignored
        assert!(maybe_emit_entry(incoming_record, &mut records)
            .unwrap()
            .is_none());
        assert!(records.is_empty());
    }

    #[test]
    fn test_maybe_emit_entry_incorrect_order() {
        let mut records = HashMap::new();
        let incoming_record = new_test_record(RecordType::Last, 0, 0, vec![]);
        // Incomplete part will be ignored
        assert!(maybe_emit_entry(incoming_record, &mut records)
            .unwrap()
            .is_none());
        assert!(records.is_empty());

        let mut records = HashMap::new();
        let incoming_record = new_test_record(RecordType::Middle(1), 0, 0, vec![]);
        // Incomplete part will be ignored
        assert!(maybe_emit_entry(incoming_record, &mut records)
            .unwrap()
            .is_none());
        assert!(records.is_empty());
    }

    #[test]
    fn test_maybe_emit_entry() {
        let mut records = HashMap::new();
        let incoming_record = new_test_record(RecordType::Full, 1, 1, vec![1, 1, 1]);
        let ns = incoming_record.meta.ns.clone();
        let entry = maybe_emit_entry(incoming_record, &mut records)
            .unwrap()
            .unwrap();

        assert_eq!(
            entry,
            EntryImpl {
                data: vec![1, 1, 1],
                id: 1,
                ns,
            }
        );

        let mut records = HashMap::new();
        let first_entry = new_test_record(RecordType::First, 1, 1, vec![1, 1, 1]);
        let mid_entry = new_test_record(RecordType::Middle(1), 1, 1, vec![2, 2, 2]);
        let last_entry = new_test_record(RecordType::Last, 1, 1, vec![3, 3, 3]);
        let ns = first_entry.meta.ns.clone();
        assert!(maybe_emit_entry(first_entry, &mut records)
            .unwrap()
            .is_none());
        assert!(maybe_emit_entry(mid_entry, &mut records).unwrap().is_none());
        let entry = maybe_emit_entry(last_entry, &mut records).unwrap().unwrap();

        assert_eq!(
            entry,
            EntryImpl {
                data: vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
                id: 1,
                ns,
            }
        );
    }
}
