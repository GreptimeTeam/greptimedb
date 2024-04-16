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
use snafu::ResultExt;
use store_api::logstore::EntryId;

use crate::error::{EncodeJsonSnafu, Result};
use crate::kafka::EntryImpl;

pub type Sequence = u8;

/// The version of record. Used to provide backward compatibility.
const RECORD_VERSION: u32 = 0;

#[derive(Debug, Serialize, Deserialize)]
pub struct EntryInner {
    pub data: Vec<u8>,
    pub id: EntryId,
    pub seq: Sequence,
    pub region_id: u64,
    pub timestamp: i64,
}

pub fn maybe_split_entry(
    entry: EntryImpl,
    max_entry_size: usize,
    timestamp: i64,
) -> Vec<EntryInner> {
    if entry.data.len() <= max_entry_size {
        return vec![EntryInner {
            data: entry.data,
            id: entry.id,
            seq: 0,
            region_id: entry.ns.region_id,
            timestamp,
        }];
    }
    entry
        .data
        .chunks(max_entry_size)
        .enumerate()
        .map(|(i, chunk)| EntryInner {
            data: chunk.to_vec(),
            id: entry.id,
            seq: i as u8,
            region_id: entry.ns.region_id,
            timestamp,
        })
        .collect()
}

#[derive(Serialize, Deserialize)]
pub struct EntryMeta {
    pub id: EntryId,
    pub seq: Sequence,
    pub region_id: u64,
    pub length: usize,
}

#[derive(Default, Serialize, Deserialize)]
pub struct RecordMeta {
    pub version: u32,
    pub entry_metas: Vec<EntryMeta>,
}

pub struct RecordBuilder {
    timestamp: i64,
    meta: RecordMeta,
    data: Vec<Vec<u8>>,
}

impl RecordBuilder {
    pub fn build_record(entries: Vec<EntryInner>) -> Result<Record> {
        let mut builder = Self::new(entries.len(), RECORD_VERSION, entries[0].timestamp);
        entries.into_iter().for_each(|entry| builder.push(entry));
        builder.try_build()
    }

    pub fn new(capacity: usize, version: u32, timestamp: i64) -> Self {
        Self {
            timestamp,
            meta: RecordMeta {
                version,
                entry_metas: Vec::with_capacity(capacity),
            },
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, entry: EntryInner) {
        self.meta.entry_metas.push(EntryMeta {
            id: entry.id,
            seq: entry.seq,
            region_id: entry.region_id,
            length: entry.data.len(),
        });
        self.data.push(entry.data);
    }

    pub fn try_build(self) -> Result<Record> {
        let encoded_meta = serde_json::to_vec(&self.meta).context(EncodeJsonSnafu)?;
        Ok(Record {
            key: Some(encoded_meta),
            value: Some(self.data.into_iter().flatten().collect()),
            headers: Default::default(),
            timestamp: chrono::DateTime::from_timestamp_millis(self.timestamp).unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use rand_distr::Distribution;

    use crate::kafka::util::maybe_split_entry;
    use crate::kafka::{EntryImpl, NamespaceImpl};

    #[test]
    fn test_split_entry() {
        assert!([
            (0, 128, 1),
            (100, 128, 1),
            (128, 128, 1),
            (1000, 128, 8),
            (1024, 128, 8),
            (1030, 128, 9),
        ]
        .into_iter()
        .all(|(entry_size, max_entry_size, expected_num_entries)| {
            let ns = NamespaceImpl {
                region_id: 1,
                topic: "test_topic".to_string(),
            };
            let data = rand::distributions::Uniform::new(0, 2)
                .sample_iter(&mut rand::thread_rng())
                .take(entry_size)
                .collect::<Vec<_>>();
            let entry = EntryImpl {
                data: data.clone(),
                id: 1,
                ns: ns.clone(),
            };
            let entries = maybe_split_entry(entry, max_entry_size, 0);
            if entries.len() != expected_num_entries {
                return false;
            }
            let assembled_data = entries
                .iter()
                .flat_map(|entry| entry.data.clone())
                .collect::<Vec<_>>();
            data == assembled_data
        }))
    }
}
