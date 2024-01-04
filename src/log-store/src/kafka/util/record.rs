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
    ChecksumMismatchedSnafu, DecodeJsonSnafu, EmptyEntriesSnafu, EncodeJsonSnafu, GetClientSnafu,
    IllegalSequenceSnafu, MissingValueSnafu, ProduceRecordSnafu, Result,
};
use crate::kafka::client_manager::ClientManagerRef;
use crate::kafka::util::offset::Offset;
use crate::kafka::{EntryId, EntryImpl, NamespaceImpl};

/// The current version of Record.
pub(crate) const VERSION: u32 = 0;

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

/// The minimal storage unit in the Kafka log store.
/// Our own Record will be converted into a Kafka record during producing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Record {
    /// The version of the record. Used for backward compatibility.
    version: u32,
    /// The type of the record.
    pub tp: RecordType,
    /// The payload of the record.
    data: Vec<u8>,
    /// The id of the entry the record associated with.
    pub entry_id: EntryId,
    /// The namespace of the entry the record associated with.
    pub ns: NamespaceImpl,
    /// The computed checksum of the payload.
    checksum: u32,
}

impl TryInto<KafkaRecord> for Record {
    type Error = crate::error::Error;

    fn try_into(self) -> Result<KafkaRecord> {
        let value = serde_json::to_vec(&self).context(EncodeJsonSnafu)?;
        Ok(KafkaRecord {
            key: None,
            value: Some(value),
            timestamp: rskafka::chrono::Utc::now(),
            headers: Default::default(),
        })
    }
}

impl TryFrom<KafkaRecord> for Record {
    type Error = crate::error::Error;

    fn try_from(kafka_record: KafkaRecord) -> Result<Self> {
        let value = kafka_record.value.context(MissingValueSnafu)?;
        serde_json::from_slice(&value).context(DecodeJsonSnafu)
    }
}

impl TryFrom<Vec<Record>> for EntryImpl {
    type Error = crate::error::Error;

    fn try_from(records: Vec<Record>) -> Result<Self> {
        check_records(&records)?;

        let entry_id = records[0].entry_id;
        let ns = records[0].ns.clone();
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
            for record in build_records(entry, self.max_record_size) {
                let kafka_record: KafkaRecord = record.try_into()?;
                // Records of a certain region cannot be produced in parallel since their order must be static.
                let offset = producer.produce(kafka_record).await.map(Offset).context(
                    ProduceRecordSnafu {
                        topic: &self.ns.topic,
                    },
                )?;
                last_offset = Some(offset);
            }
        }
        // Safety: there must be at least one record produced when the entries are guaranteed not empty.
        Ok(last_offset.unwrap())
    }

    // pub(crate) async fn record(&self, record: record, producer: )
}

fn check_records(records: &[Record]) -> Result<()> {
    let len = records.len();
    ensure!(
        len > 0,
        IllegalSequenceSnafu {
            error: "Empty sequence"
        }
    );

    let (sequence, matched): (Vec<_>, Vec<_>) = records
        .iter()
        .map(|record| {
            (
                i32::from(record.tp),
                record.checksum == crc32fast::hash(&record.data),
            )
        })
        .unzip();

    ensure!(matched.into_iter().all(|ok| ok), ChecksumMismatchedSnafu);

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
    let payload = entry.data.len();
    let checksum = crc32fast::hash(&entry.data);

    if payload <= max_record_size {
        let record = Record {
            version: VERSION,
            tp: RecordType::Full,
            data: entry.data,
            entry_id: entry.id,
            ns: entry.ns,
            checksum,
        };
        return vec![record];
    }

    let num_records = (payload + max_record_size - 1) / max_record_size;
    let mut records = Vec::with_capacity(num_records);
    for i in 0..num_records {
        let end = max_record_size.min(payload - i);
        let record = Record {
            version: VERSION,
            tp: record_type(i, num_records),
            data: entry.data[i..end].to_vec(),
            entry_id: entry.id,
            ns: entry.ns.clone(),
            checksum,
        };
        records.push(record);
    }
    records
}

pub fn maybe_emit_entry(
    record: Record,
    ns: &NamespaceImpl,
    entry_records: &mut HashMap<EntryId, Vec<Record>>,
) -> Result<Option<EntryImpl>> {
    let mut entry = None;
    match record.tp {
        RecordType::Full => {
            entry = Some(EntryImpl::try_from(vec![record])?);
        }
        RecordType::Last => {
            // There must have a sequence prefix before a Last record is read.
            let mut records =
                entry_records
                    .remove(&record.entry_id)
                    .context(IllegalSequenceSnafu {
                        error: "Missing sequence prefix",
                    })?;
            records.push(record);
            entry = Some(EntryImpl::try_from(records)?);
        }
        _ => {
            entry_records
                .entry(record.entry_id)
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
#[allow(unused)]
mod tests {
    use rand::Rng;

    use super::*;

    // Implements some utility methods for testing.
    impl Default for Record {
        fn default() -> Self {
            Self {
                version: VERSION,
                tp: RecordType::Full,
                ns: NamespaceImpl {
                    region_id: 0,
                    topic: "greptimedb_wal_topic".to_string(),
                },
                data: Vec::new(),
                entry_id: 0,
                checksum: 0,
            }
        }
    }

    impl Record {
        /// Overrides tp.
        pub(crate) fn with_tp(&self, tp: RecordType) -> Self {
            Self { tp, ..self.clone() }
        }

        /// Overrides data with the given data.
        pub(crate) fn with_data(&self, data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                ..self.clone()
            }
        }

        /// Overrides data with random data.
        pub(crate) fn with_random_data(&self) -> Self {
            let data = rand::thread_rng().gen_range(0..u32::MAX).to_string();
            Self {
                data: data.as_bytes().to_vec(),
                ..self.clone()
            }
        }

        /// Overrides entry id.
        pub(crate) fn with_entry_id(&self, entry_id: EntryId) -> Self {
            Self {
                entry_id,
                ..self.clone()
            }
        }

        /// Overrides checksum.
        pub(crate) fn with_checksum(&self, checksum: u32) -> Self {
            Self {
                checksum,
                ..self.clone()
            }
        }
    }

    #[allow(unused)]
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
    fn test_build_records() {}

    /// Tests that Record and KafkaRecord are able to be converted back and forth.
    #[test]
    fn test_record_conversion() {
        let record = Record {
            version: VERSION,
            tp: RecordType::Full,
            data: b"12345".to_vec(),
            entry_id: 1,
            ns: NamespaceImpl {
                region_id: 1,
                topic: "greptimedb_wal_topic".to_string(),
            },
            checksum: crc32fast::hash(b"12345".as_slice()),
        };
        let kafka_record: KafkaRecord = record.clone().try_into().unwrap();
        let got = Record::try_from(kafka_record).unwrap();
        assert_eq!(record, got);
    }

    /// Tests that the reconstruction of an entry behaves as expected.
    #[test]
    fn test_reconstruct_entry() {}

    /// Tests that `maybe_emit_entry` works as expected.
    #[test]
    fn test_maybe_emit_entry() {}
}
