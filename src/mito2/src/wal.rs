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

//! Write ahead log of the engine.

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use api::v1::WalEntry;
use async_stream::try_stream;
use common_config::wal::WalOptions;
use common_error::ext::BoxedError;
use futures::stream::BoxStream;
use futures::StreamExt;
use prost::Message;
use snafu::ResultExt;
use store_api::logstore::entry::Entry;
use store_api::logstore::{AppendBatchResponse, LogStore};
use store_api::storage::RegionId;

use crate::error::{
    DecodeWalSnafu, DeleteWalSnafu, EncodeWalSnafu, ReadWalSnafu, Result, WriteWalSnafu,
};

/// WAL entry id.
pub type EntryId = store_api::logstore::entry::Id;
/// A stream that yields tuple of WAL entry id and corresponding entry.
pub type WalEntryStream<'a> = BoxStream<'a, Result<(EntryId, WalEntry)>>;

/// Write ahead log.
///
/// All regions in the engine shares the same WAL instance.
#[derive(Debug, Clone)]
pub struct Wal<S> {
    /// The underlying log store.
    store: Arc<S>,
}

impl<S> Wal<S> {
    /// Creates a new [Wal] from the log store.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

impl<S: LogStore> Wal<S> {
    /// Returns a writer to write to the WAL.
    pub fn writer(&self) -> WalWriter<S> {
        WalWriter {
            store: self.store.clone(),
            entries: Vec::new(),
            entry_encode_buf: Vec::new(),
            namespaces: HashMap::new(),
        }
    }

    /// Scan entries of specific region starting from `start_id` (inclusive).
    pub fn scan<'a>(
        &'a self,
        region_id: RegionId,
        start_id: EntryId,
        wal_options: &'a WalOptions,
    ) -> Result<WalEntryStream> {
        let stream = try_stream!({
            let namespace = self.store.namespace(region_id.into(), wal_options);
            let mut stream = self
                .store
                .read(&namespace, start_id)
                .await
                .map_err(BoxedError::new)
                .context(ReadWalSnafu { region_id })?;

            while let Some(entries) = stream.next().await {
                let entries = entries
                    .map_err(BoxedError::new)
                    .context(ReadWalSnafu { region_id })?;

                for entry in entries {
                    yield decode_entry(region_id, entry)?;
                }
            }
        });

        Ok(Box::pin(stream))
    }

    /// Mark entries whose ids `<= last_id` as deleted.
    pub async fn obsolete(
        &self,
        region_id: RegionId,
        last_id: EntryId,
        wal_options: &WalOptions,
    ) -> Result<()> {
        let namespace = self.store.namespace(region_id.into(), wal_options);
        self.store
            .obsolete(namespace, last_id)
            .await
            .map_err(BoxedError::new)
            .context(DeleteWalSnafu { region_id })
    }
}

/// Decode Wal entry from log store.
fn decode_entry<E: Entry>(region_id: RegionId, entry: E) -> Result<(EntryId, WalEntry)> {
    let entry_id = entry.id();
    let data = entry.data();

    let wal_entry = WalEntry::decode(data).context(DecodeWalSnafu { region_id })?;

    Ok((entry_id, wal_entry))
}

/// WAL batch writer.
pub struct WalWriter<S: LogStore> {
    /// Log store of the WAL.
    store: Arc<S>,
    /// Entries to write.
    entries: Vec<S::Entry>,
    /// Buffer to encode WAL entry.
    entry_encode_buf: Vec<u8>,
    /// Namespaces of regions being written into.
    namespaces: HashMap<RegionId, S::Namespace>,
}

impl<S: LogStore> WalWriter<S> {
    /// Add an wal entry for specific region to the writer's buffer.
    pub fn add_entry(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        wal_entry: &WalEntry,
        wal_options: &WalOptions,
    ) -> Result<()> {
        // Gets or inserts with a newly built namespace.
        let namespace = self
            .namespaces
            .entry(region_id)
            .or_insert_with(|| self.store.namespace(region_id.into(), wal_options))
            .clone();

        // Encode wal entry to log store entry.
        self.entry_encode_buf.clear();
        wal_entry
            .encode(&mut self.entry_encode_buf)
            .context(EncodeWalSnafu { region_id })?;
        let entry = self
            .store
            .entry(&self.entry_encode_buf, entry_id, namespace);

        self.entries.push(entry);

        Ok(())
    }

    /// Write all buffered entries to the WAL.
    pub async fn write_to_wal(&mut self) -> Result<AppendBatchResponse> {
        // TODO(yingwen): metrics.

        let entries = mem::take(&mut self.entries);
        self.store
            .append_batch(entries)
            .await
            .map_err(BoxedError::new)
            .context(WriteWalSnafu)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::{
        value, ColumnDataType, ColumnSchema, Mutation, OpType, Row, Rows, SemanticType, Value,
    };
    use common_test_util::temp_dir::{create_temp_dir, TempDir};
    use futures::TryStreamExt;
    use log_store::raft_engine::log_store::RaftEngineLogStore;
    use log_store::test_util::log_store_util;
    use store_api::storage::SequenceNumber;

    use super::*;

    struct WalEnv {
        _wal_dir: TempDir,
        log_store: Option<Arc<RaftEngineLogStore>>,
    }

    impl WalEnv {
        async fn new() -> WalEnv {
            let wal_dir = create_temp_dir("");
            let log_store =
                log_store_util::create_tmp_local_file_log_store(wal_dir.path().to_str().unwrap())
                    .await;
            WalEnv {
                _wal_dir: wal_dir,
                log_store: Some(Arc::new(log_store)),
            }
        }

        fn new_wal(&self) -> Wal<RaftEngineLogStore> {
            let log_store = self.log_store.clone().unwrap();
            Wal::new(log_store)
        }
    }

    /// Create a new mutation from rows.
    ///
    /// The row format is (string, i64).
    fn new_mutation(op_type: OpType, sequence: SequenceNumber, rows: &[(&str, i64)]) -> Mutation {
        let rows = rows
            .iter()
            .map(|(str_col, int_col)| {
                let values = vec![
                    Value {
                        value_data: Some(value::ValueData::StringValue(str_col.to_string())),
                    },
                    Value {
                        value_data: Some(value::ValueData::TimestampMillisecondValue(*int_col)),
                    },
                ];
                Row { values }
            })
            .collect();
        let schema = vec![
            ColumnSchema {
                column_name: "tag".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "ts".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
        ];

        Mutation {
            op_type: op_type as i32,
            sequence,
            rows: Some(Rows { schema, rows }),
        }
    }

    #[tokio::test]
    async fn test_write_wal() {
        let env = WalEnv::new().await;
        let wal = env.new_wal();
        let wal_options = WalOptions::default();

        let entry = WalEntry {
            mutations: vec![
                new_mutation(OpType::Put, 1, &[("k1", 1), ("k2", 2)]),
                new_mutation(OpType::Put, 2, &[("k3", 3), ("k4", 4)]),
            ],
        };
        let mut writer = wal.writer();
        // Region 1 entry 1.
        writer
            .add_entry(RegionId::new(1, 1), 1, &entry, &wal_options)
            .unwrap();
        // Region 2 entry 1.
        writer
            .add_entry(RegionId::new(1, 2), 1, &entry, &wal_options)
            .unwrap();
        // Region 1 entry 2.
        writer
            .add_entry(RegionId::new(1, 1), 2, &entry, &wal_options)
            .unwrap();

        // Test writing multiple region to wal.
        writer.write_to_wal().await.unwrap();
    }

    fn sample_entries() -> Vec<WalEntry> {
        vec![
            WalEntry {
                mutations: vec![
                    new_mutation(OpType::Put, 1, &[("k1", 1), ("k2", 2)]),
                    new_mutation(OpType::Put, 2, &[("k3", 3), ("k4", 4)]),
                ],
            },
            WalEntry {
                mutations: vec![new_mutation(OpType::Put, 3, &[("k1", 1), ("k2", 2)])],
            },
            WalEntry {
                mutations: vec![
                    new_mutation(OpType::Put, 4, &[("k1", 1), ("k2", 2)]),
                    new_mutation(OpType::Put, 5, &[("k3", 3), ("k4", 4)]),
                ],
            },
            WalEntry {
                mutations: vec![new_mutation(OpType::Put, 6, &[("k1", 1), ("k2", 2)])],
            },
        ]
    }

    fn check_entries(
        expect: &[WalEntry],
        expect_start_id: EntryId,
        actual: &[(EntryId, WalEntry)],
    ) {
        for (idx, (expect_entry, (actual_id, actual_entry))) in
            expect.iter().zip(actual.iter()).enumerate()
        {
            let expect_id_entry = (expect_start_id + idx as u64, expect_entry);
            assert_eq!(expect_id_entry, (*actual_id, actual_entry));
        }
        assert_eq!(expect.len(), actual.len());
    }

    #[tokio::test]
    async fn test_scan_wal() {
        let env = WalEnv::new().await;
        let wal = env.new_wal();
        let wal_options = WalOptions::default();

        let entries = sample_entries();
        let (id1, id2) = (RegionId::new(1, 1), RegionId::new(1, 2));
        let mut writer = wal.writer();
        writer.add_entry(id1, 1, &entries[0], &wal_options).unwrap();
        // Insert one entry into region2. Scan should not return this entry.
        writer.add_entry(id2, 1, &entries[0], &wal_options).unwrap();
        writer.add_entry(id1, 2, &entries[1], &wal_options).unwrap();
        writer.add_entry(id1, 3, &entries[2], &wal_options).unwrap();
        writer.add_entry(id1, 4, &entries[3], &wal_options).unwrap();

        writer.write_to_wal().await.unwrap();

        // Scan all contents region1
        let stream = wal.scan(id1, 1, &wal_options).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        check_entries(&entries, 1, &actual);

        // Scan parts of contents
        let stream = wal.scan(id1, 2, &wal_options).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        check_entries(&entries[1..], 2, &actual);

        // Scan out of range
        let stream = wal.scan(id1, 5, &wal_options).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        assert!(actual.is_empty());
    }

    #[tokio::test]
    async fn test_obsolete_wal() {
        let env = WalEnv::new().await;
        let wal = env.new_wal();
        let wal_options = WalOptions::default();

        let entries = sample_entries();
        let mut writer = wal.writer();
        let region_id = RegionId::new(1, 1);
        writer
            .add_entry(region_id, 1, &entries[0], &wal_options)
            .unwrap();
        writer
            .add_entry(region_id, 2, &entries[1], &wal_options)
            .unwrap();
        writer
            .add_entry(region_id, 3, &entries[2], &wal_options)
            .unwrap();

        writer.write_to_wal().await.unwrap();

        // Delete 1, 2.
        wal.obsolete(region_id, 2, &wal_options).await.unwrap();

        // Put 4.
        let mut writer = wal.writer();
        writer
            .add_entry(region_id, 4, &entries[3], &wal_options)
            .unwrap();
        writer.write_to_wal().await.unwrap();

        // Scan all
        let stream = wal.scan(region_id, 1, &wal_options).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        check_entries(&entries[2..], 3, &actual);
    }
}
