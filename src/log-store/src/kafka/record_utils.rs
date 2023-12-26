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

use rskafka::record::Record;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    DecodeMetaSnafu, EmptyEntriesSnafu, EncodeMetaSnafu, GetClientSnafu, MissingKeySnafu,
    MissingValueSnafu, ProduceRecordSnafu, Result,
};
use crate::kafka::client_manager::ClientManagerRef;
use crate::kafka::offset::Offset;
use crate::kafka::{EntryId, EntryImpl, NamespaceImpl};

/// Record metadata which will be serialized/deserialized to/from the `key` of a Record.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct RecordMeta {
    /// Meta version. Used for backward compatibility.
    version: u32,
    /// The namespace of the entries wrapped in the record.
    ns: NamespaceImpl,
    /// Ids of the entries built into the record.
    entry_ids: Vec<EntryId>,
    /// entry_offsets[i] is the end offset (exclusive) of the data of the i-th entry in the record value.
    entry_offsets: Vec<usize>,
}

impl RecordMeta {
    fn new(ns: NamespaceImpl, entries: &[EntryImpl]) -> Self {
        Self {
            version: 0,
            ns,
            entry_ids: entries.iter().map(|entry| entry.id).collect(),
            entry_offsets: entries
                .iter()
                .map(|entry| entry.data.len())
                .scan(0, |presum, x| {
                    *presum += x;
                    Some(*presum)
                })
                .collect(),
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

    /// Produces the buffered entries to kafka sever as a kafka record.
    /// Returns the kafka offset of the produced record.
    // TODO(niebayes): since the total size of a region's entries may be way-too large,
    // the producer may need to support splitting entries into multiple records.
    pub(crate) async fn produce(self, client_manager: &ClientManagerRef) -> Result<Offset> {
        ensure!(!self.entries.is_empty(), EmptyEntriesSnafu);

        // Produces the record through a client. The client determines when to send the record to kafka server.
        let client = client_manager
            .get_or_insert(&self.ns.topic)
            .await
            .map_err(|e| {
                GetClientSnafu {
                    topic: &self.ns.topic,
                    error: e.to_string(),
                }
                .build()
            })?;
        client
            .producer
            .produce(encode_to_record(self.ns.clone(), self.entries)?)
            .await
            .map(Offset)
            .context(ProduceRecordSnafu {
                topic: &self.ns.topic,
            })
    }
}

fn encode_to_record(ns: NamespaceImpl, entries: Vec<EntryImpl>) -> Result<Record> {
    let meta = RecordMeta::new(ns, &entries);
    let data = entries.into_iter().flat_map(|entry| entry.data).collect();
    Ok(Record {
        key: Some(serde_json::to_vec(&meta).context(EncodeMetaSnafu)?),
        value: Some(data),
        timestamp: rskafka::chrono::Utc::now(),
        headers: Default::default(),
    })
}

pub(crate) fn decode_from_record(record: Record) -> Result<Vec<EntryImpl>> {
    let key = record.key.context(MissingKeySnafu)?;
    let value = record.value.context(MissingValueSnafu)?;
    let meta: RecordMeta = serde_json::from_slice(&key).context(DecodeMetaSnafu)?;

    let mut entries = Vec::with_capacity(meta.entry_ids.len());
    let mut start_offset = 0;
    for (i, end_offset) in meta.entry_offsets.iter().enumerate() {
        entries.push(EntryImpl {
            // TODO(niebayes): try to avoid the clone.
            data: value[start_offset..*end_offset].to_vec(),
            id: meta.entry_ids[i],
            ns: meta.ns.clone(),
        });
        start_offset = *end_offset;
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_test_entry<D: AsRef<[u8]>>(data: D, entry_id: EntryId, ns: NamespaceImpl) -> EntryImpl {
        EntryImpl {
            data: data.as_ref().to_vec(),
            id: entry_id,
            ns,
        }
    }

    #[test]
    fn test_serde_record_meta() {
        let ns = NamespaceImpl {
            region_id: 1,
            topic: "test_topic".to_string(),
        };
        let entries = vec![
            new_test_entry(b"111", 1, ns.clone()),
            new_test_entry(b"2222", 2, ns.clone()),
            new_test_entry(b"33333", 3, ns.clone()),
        ];
        let meta = RecordMeta::new(ns, &entries);
        let encoded = serde_json::to_vec(&meta).unwrap();
        let decoded: RecordMeta = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn test_encdec_record() {
        let ns = NamespaceImpl {
            region_id: 1,
            topic: "test_topic".to_string(),
        };
        let entries = vec![
            new_test_entry(b"111", 1, ns.clone()),
            new_test_entry(b"2222", 2, ns.clone()),
            new_test_entry(b"33333", 3, ns.clone()),
        ];
        let record = encode_to_record(ns, entries.clone()).unwrap();
        let decoded_entries = decode_from_record(record).unwrap();
        assert_eq!(entries, decoded_entries);
    }
}
