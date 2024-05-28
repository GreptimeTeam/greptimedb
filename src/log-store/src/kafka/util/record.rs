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
use std::sync::Arc;

use rskafka::record::Record as KafkaRecord;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::logstore::entry::{Entry, MultiplePartEntry, MultiplePartHeader, NaiveEntry};
use store_api::logstore::provider::{KafkaProvider, Provider};
use store_api::storage::RegionId;

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
pub(crate) const ESTIMATED_META_SIZE: usize = 256;

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
    /// The provide of the entries.
    provider: Arc<KafkaProvider>,
    /// Entries are buffered before being built into a record.
    entries: Vec<Entry>,
}

impl RecordProducer {
    /// Creates a new producer for producing entries with the given namespace.
    pub(crate) fn new(provider: Arc<KafkaProvider>) -> Self {
        Self {
            provider,
            entries: Vec::new(),
        }
    }

    /// Pushes an entry into the entry buffer.
    pub(crate) fn push(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    /// Produces the buffered entries to Kafka sever. Those entries may span several Kafka records.
    /// Returns the offset of the last successfully produced record.
    // TODO(niebayes): maybe requires more fine-grained metrics to measure stages of writing to kafka.
    pub(crate) async fn produce(self, client_manager: &ClientManagerRef) -> Result<Offset> {
        ensure!(!self.entries.is_empty(), EmptyEntriesSnafu);

        // Gets the producer in which a record buffer is maintained.
        let producer = client_manager
            .get_or_insert(&self.provider.topic)
            .await
            .map_err(|e| {
                GetClientSnafu {
                    topic: &self.provider.topic,
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
            for record in convert_to_records(entry) {
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
                        topic: &self.provider.topic,
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

fn convert_to_records(entry: Entry) -> Vec<Record> {
    match entry {
        Entry::Naive(entry) => vec![Record {
            meta: RecordMeta {
                version: VERSION,
                tp: RecordType::Full,
                // TODO(weny): refactor the record meta.
                entry_id: 0,
                ns: NamespaceImpl {
                    region_id: entry.region_id.as_u64(),
                    // TODO(weny): refactor the record meta.
                    topic: String::new(),
                },
            },
            data: entry.data,
        }],
        Entry::MultiplePart(entry) => {
            let mut entries = Vec::with_capacity(entry.parts.len());

            for (idx, part) in entry.parts.into_iter().enumerate() {
                let tp = match entry.headers[idx] {
                    MultiplePartHeader::First => RecordType::First,
                    MultiplePartHeader::Middle(i) => RecordType::Middle(i),
                    MultiplePartHeader::Last => RecordType::Last,
                };
                entries.push(Record {
                    meta: RecordMeta {
                        version: VERSION,
                        tp,
                        // TODO(weny): refactor the record meta.
                        entry_id: 0,
                        ns: NamespaceImpl {
                            region_id: entry.region_id.as_u64(),
                            topic: String::new(),
                        },
                    },
                    data: part,
                })
            }
            entries
        }
    }
}

fn convert_to_naive_entry(provider: Arc<KafkaProvider>, record: Record) -> Entry {
    let region_id = RegionId::from_u64(record.meta.ns.region_id);

    Entry::Naive(NaiveEntry {
        provider: Provider::Kafka(provider),
        region_id,
        // TODO(weny): should be the offset in the topic
        entry_id: record.meta.entry_id,
        data: record.data,
    })
}

fn convert_to_multiple_entry(
    provider: Arc<KafkaProvider>,
    region_id: RegionId,
    records: Vec<Record>,
) -> Entry {
    let mut headers = Vec::with_capacity(records.len());
    let mut parts = Vec::with_capacity(records.len());

    for record in records {
        let header = match record.meta.tp {
            RecordType::Full => unreachable!(),
            RecordType::First => MultiplePartHeader::First,
            RecordType::Middle(i) => MultiplePartHeader::Middle(i),
            RecordType::Last => MultiplePartHeader::Last,
        };
        headers.push(header);
        parts.push(record.data);
    }

    Entry::MultiplePart(MultiplePartEntry {
        provider: Provider::Kafka(provider),
        region_id,
        // TODO(weny): should be the offset in the topic
        entry_id: 0,
        headers,
        parts,
    })
}

/// Constructs entries from `buffered_records`
pub fn remaining_entries(
    provider: &Arc<KafkaProvider>,
    buffered_records: &mut HashMap<RegionId, Vec<Record>>,
) -> Option<Vec<Entry>> {
    if buffered_records.is_empty() {
        None
    } else {
        let mut entries = Vec::with_capacity(buffered_records.len());
        for (region_id, records) in buffered_records.drain() {
            entries.push(convert_to_multiple_entry(
                provider.clone(),
                region_id,
                records,
            ));
        }
        Some(entries)
    }
}

pub fn maybe_emit_entry(
    provider: &Arc<KafkaProvider>,
    record: Record,
    buffered_records: &mut HashMap<RegionId, Vec<Record>>,
) -> Result<Option<Entry>> {
    let mut entry = None;
    match record.meta.tp {
        RecordType::Full => entry = Some(convert_to_naive_entry(provider.clone(), record)),
        RecordType::First => {
            let region_id = record.meta.ns.region_id.into();
            if let Some(records) = buffered_records.insert(region_id, vec![record]) {
                // Incomplete entry
                entry = Some(convert_to_multiple_entry(
                    provider.clone(),
                    region_id,
                    records,
                ))
            }
        }
        RecordType::Middle(seq) => {
            let region_id = record.meta.ns.region_id.into();
            let records = buffered_records.entry(region_id).or_default();

            // Only invalidate complete entries.
            if !records.is_empty() {
                // Safety: the records are guaranteed not empty if the key exists.
                let last_record = records.last().unwrap();
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
                        error: format!(
                            "Illegal sequence of a middle record, last record: {:?}, incoming record: {:?}",
                            last_record.meta.tp,
                            record.meta.tp
                        )
                    }
                );
            }

            records.push(record);
        }
        RecordType::Last => {
            let region_id = record.meta.ns.region_id.into();
            if let Some(mut records) = buffered_records.remove(&region_id) {
                records.push(record);
                entry = Some(convert_to_multiple_entry(
                    provider.clone(),
                    region_id,
                    records,
                ))
            } else {
                // Incomplete entry
                entry = Some(convert_to_multiple_entry(
                    provider.clone(),
                    region_id,
                    vec![record],
                ))
            }
        }
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use super::*;
    use crate::error;

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

    #[test]
    fn test_maybe_emit_entry_emit_naive_entry() {
        let provider = Arc::new(KafkaProvider::new("my_topic".to_string()));
        let region_id = RegionId::new(1, 1);
        let mut buffer = HashMap::new();
        let record = new_test_record(RecordType::Full, 1, region_id.as_u64(), vec![1; 100]);
        let entry = maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .unwrap();
        assert_eq!(
            entry,
            Entry::Naive(NaiveEntry {
                provider: Provider::Kafka(provider),
                region_id,
                entry_id: 1,
                data: vec![1; 100]
            })
        );
    }

    #[test]
    fn test_maybe_emit_entry_emit_incomplete_entry() {
        let provider = Arc::new(KafkaProvider::new("my_topic".to_string()));
        let region_id = RegionId::new(1, 1);
        // `First` overwrite `First`
        let mut buffer = HashMap::new();
        let record = new_test_record(RecordType::First, 1, region_id.as_u64(), vec![1; 100]);
        assert!(maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .is_none());
        let record = new_test_record(RecordType::First, 2, region_id.as_u64(), vec![2; 100]);
        let incomplete_entry = maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .unwrap();

        assert_eq!(
            incomplete_entry,
            Entry::MultiplePart(MultiplePartEntry {
                provider: Provider::Kafka(provider.clone()),
                region_id,
                // TODO(weny): always be 0.
                entry_id: 0,
                headers: vec![MultiplePartHeader::First],
                parts: vec![vec![1; 100]],
            })
        );

        // `Last` overwrite `None`
        let mut buffer = HashMap::new();
        let record = new_test_record(RecordType::Last, 1, region_id.as_u64(), vec![1; 100]);
        let incomplete_entry = maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .unwrap();

        assert_eq!(
            incomplete_entry,
            Entry::MultiplePart(MultiplePartEntry {
                provider: Provider::Kafka(provider.clone()),
                region_id,
                // TODO(weny): always be 0.
                entry_id: 0,
                headers: vec![MultiplePartHeader::Last],
                parts: vec![vec![1; 100]],
            })
        );

        // `First` overwrite `Middle(0)`
        let mut buffer = HashMap::new();
        let record = new_test_record(RecordType::Middle(0), 1, region_id.as_u64(), vec![1; 100]);
        assert!(maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .is_none());
        let record = new_test_record(RecordType::First, 2, region_id.as_u64(), vec![2; 100]);
        let incomplete_entry = maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .unwrap();

        assert_eq!(
            incomplete_entry,
            Entry::MultiplePart(MultiplePartEntry {
                provider: Provider::Kafka(provider),
                region_id,
                // TODO(weny): always be 0.
                entry_id: 0,
                headers: vec![MultiplePartHeader::Middle(0)],
                parts: vec![vec![1; 100]],
            })
        );
    }

    #[test]
    fn test_maybe_emit_entry_illegal_seq() {
        let provider = Arc::new(KafkaProvider::new("my_topic".to_string()));
        let region_id = RegionId::new(1, 1);
        let mut buffer = HashMap::new();
        let record = new_test_record(RecordType::First, 1, region_id.as_u64(), vec![1; 100]);
        assert!(maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .is_none());
        let record = new_test_record(RecordType::Middle(2), 1, region_id.as_u64(), vec![2; 100]);
        let err = maybe_emit_entry(&provider, record, &mut buffer).unwrap_err();
        assert_matches!(err, error::Error::IllegalSequence { .. });

        let mut buffer = HashMap::new();
        let record = new_test_record(RecordType::First, 1, region_id.as_u64(), vec![1; 100]);
        assert!(maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .is_none());
        let record = new_test_record(RecordType::Middle(1), 1, region_id.as_u64(), vec![2; 100]);
        assert!(maybe_emit_entry(&provider, record, &mut buffer)
            .unwrap()
            .is_none());
        let record = new_test_record(RecordType::Middle(3), 1, region_id.as_u64(), vec![2; 100]);
        let err = maybe_emit_entry(&provider, record, &mut buffer).unwrap_err();
        assert_matches!(err, error::Error::IllegalSequence { .. });
    }
}
