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

pub(crate) mod entry_distributor;
pub(crate) mod entry_reader;
pub(crate) mod raw_entry_reader;

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use api::v1::WalEntry;
use common_error::ext::BoxedError;
use common_telemetry::debug;
use entry_reader::NoopEntryReader;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use prost::Message;
use snafu::ResultExt;
use store_api::logstore::entry::Entry;
use store_api::logstore::provider::Provider;
use store_api::logstore::{AppendBatchResponse, LogStore, WalIndex};
use store_api::storage::RegionId;

use crate::error::{BuildEntrySnafu, DeleteWalSnafu, Result, WriteWalSnafu};
use crate::wal::entry_reader::{LogStoreEntryReader, WalEntryReader};
use crate::wal::raw_entry_reader::{LogStoreRawEntryReader, RegionRawEntryReader};

/// WAL entry id.
pub type EntryId = store_api::logstore::entry::Id;
/// A stream that yields tuple of WAL entry id and corresponding entry.
pub type WalEntryStream<'a> = BoxStream<'a, Result<(EntryId, WalEntry)>>;

/// Write ahead log.
///
/// All regions in the engine shares the same WAL instance.
#[derive(Debug)]
pub struct Wal<S> {
    /// The underlying log store.
    store: Arc<S>,
}

impl<S> Wal<S> {
    /// Creates a new [Wal] from the log store.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<S> {
        &self.store
    }
}

impl<S> Clone for Wal<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
        }
    }
}

impl<S: LogStore> Wal<S> {
    /// Returns a writer to write to the WAL.
    pub fn writer(&self) -> WalWriter<S> {
        WalWriter {
            store: self.store.clone(),
            entries: Vec::new(),
            providers: HashMap::new(),
        }
    }

    /// Returns a [OnRegionOpened] function.
    pub(crate) fn on_region_opened(
        &self,
    ) -> impl FnOnce(RegionId, EntryId, &Provider) -> BoxFuture<Result<()>> {
        let store = self.store.clone();
        move |region_id, last_entry_id, provider| -> BoxFuture<'_, Result<()>> {
            if let Provider::Noop = provider {
                debug!("Skip obsolete for region: {}", region_id);
                return Box::pin(async move { Ok(()) });
            }
            Box::pin(async move {
                store
                    .obsolete(provider, region_id, last_entry_id)
                    .await
                    .map_err(BoxedError::new)
                    .context(DeleteWalSnafu { region_id })
            })
        }
    }

    /// Returns a [WalEntryReader]
    pub(crate) fn wal_entry_reader(
        &self,
        provider: &Provider,
        region_id: RegionId,
        location_id: Option<u64>,
    ) -> Box<dyn WalEntryReader> {
        match provider {
            Provider::RaftEngine(_) => Box::new(LogStoreEntryReader::new(
                LogStoreRawEntryReader::new(self.store.clone()),
            )),
            Provider::Kafka(_) => {
                let reader = if let Some(location_id) = location_id {
                    LogStoreRawEntryReader::new(self.store.clone())
                        .with_wal_index(WalIndex::new(region_id, location_id))
                } else {
                    LogStoreRawEntryReader::new(self.store.clone())
                };

                Box::new(LogStoreEntryReader::new(RegionRawEntryReader::new(
                    reader, region_id,
                )))
            }
            Provider::Noop => Box::new(NoopEntryReader),
        }
    }

    /// Scan entries of specific region starting from `start_id` (inclusive).
    /// Currently only used in tests.
    pub fn scan<'a>(
        &'a self,
        region_id: RegionId,
        start_id: EntryId,
        provider: &'a Provider,
    ) -> Result<WalEntryStream<'a>> {
        let mut reader = self.wal_entry_reader(provider, region_id, None);
        reader.read(provider, start_id)
    }

    /// Mark entries whose ids `<= last_id` as deleted.
    pub async fn obsolete(
        &self,
        region_id: RegionId,
        last_id: EntryId,
        provider: &Provider,
    ) -> Result<()> {
        if let Provider::Noop = provider {
            return Ok(());
        }
        self.store
            .obsolete(provider, region_id, last_id)
            .await
            .map_err(BoxedError::new)
            .context(DeleteWalSnafu { region_id })
    }
}

/// WAL batch writer.
pub struct WalWriter<S: LogStore> {
    /// Log store of the WAL.
    store: Arc<S>,
    /// Entries to write.
    entries: Vec<Entry>,
    /// Providers of regions being written into.
    providers: HashMap<RegionId, Provider>,
}

impl<S: LogStore> WalWriter<S> {
    /// Add a wal entry for specific region to the writer's buffer.
    pub fn add_entry(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        wal_entry: &WalEntry,
        provider: &Provider,
    ) -> Result<()> {
        // Gets or inserts with a newly built provider.
        let provider = self
            .providers
            .entry(region_id)
            .or_insert_with(|| provider.clone());

        let data = wal_entry.encode_to_vec();
        let entry = self
            .store
            .entry(data, entry_id, region_id, provider)
            .map_err(BoxedError::new)
            .context(BuildEntrySnafu { region_id })?;

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
    use api::v1::helper::{tag_column_schema, time_index_column_schema};
    use api::v1::{
        ArrowIpc, BulkWalEntry, ColumnDataType, Mutation, OpType, Row, Rows, Value, bulk_wal_entry,
        value,
    };
    use common_recordbatch::DfRecordBatch;
    use common_test_util::flight::encode_to_flight_data;
    use common_test_util::temp_dir::{TempDir, create_temp_dir};
    use datatypes::arrow;
    use datatypes::arrow::array::{ArrayRef, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::Field;
    use datatypes::arrow_array::StringArray;
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
            tag_column_schema("tag", ColumnDataType::String),
            time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
        ];

        Mutation {
            op_type: op_type as i32,
            sequence,
            rows: Some(Rows { schema, rows }),
            write_hint: None,
        }
    }

    #[tokio::test]
    async fn test_write_wal() {
        let env = WalEnv::new().await;
        let wal = env.new_wal();

        let entry = WalEntry {
            mutations: vec![
                new_mutation(OpType::Put, 1, &[("k1", 1), ("k2", 2)]),
                new_mutation(OpType::Put, 2, &[("k3", 3), ("k4", 4)]),
            ],
            bulk_entries: vec![],
        };
        let mut writer = wal.writer();
        // Region 1 entry 1.
        let region_id = RegionId::new(1, 1);
        writer
            .add_entry(
                region_id,
                1,
                &entry,
                &Provider::raft_engine_provider(region_id.as_u64()),
            )
            .unwrap();
        // Region 2 entry 1.
        let region_id = RegionId::new(1, 2);
        writer
            .add_entry(
                region_id,
                1,
                &entry,
                &Provider::raft_engine_provider(region_id.as_u64()),
            )
            .unwrap();
        // Region 1 entry 2.
        let region_id = RegionId::new(1, 2);
        writer
            .add_entry(
                region_id,
                2,
                &entry,
                &Provider::raft_engine_provider(region_id.as_u64()),
            )
            .unwrap();

        // Test writing multiple region to wal.
        writer.write_to_wal().await.unwrap();
    }

    fn build_record_batch(rows: &[(&str, i64)]) -> DfRecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("tag", arrow::datatypes::DataType::Utf8, false),
            Field::new(
                "ts",
                arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ),
                false,
            ),
        ]));

        let tag = Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| r.0.to_string()),
        )) as ArrayRef;
        let ts = Arc::new(TimestampMillisecondArray::from_iter_values(
            rows.iter().map(|r| r.1),
        )) as ArrayRef;
        DfRecordBatch::try_new(schema, vec![tag, ts]).unwrap()
    }

    fn build_bulk_wal_entry(sequence_number: SequenceNumber, rows: &[(&str, i64)]) -> BulkWalEntry {
        let rb = build_record_batch(rows);
        let (schema, rb) = encode_to_flight_data(rb);
        let max_ts = rows.iter().map(|r| r.1).max().unwrap();
        let min_ts = rows.iter().map(|r| r.1).min().unwrap();
        BulkWalEntry {
            sequence: sequence_number,
            max_ts,
            min_ts,
            timestamp_index: 1,
            body: Some(bulk_wal_entry::Body::ArrowIpc(ArrowIpc {
                schema: schema.data_header,
                data_header: rb.data_header,
                payload: rb.data_body,
            })),
        }
    }

    fn sample_entries() -> Vec<WalEntry> {
        vec![
            WalEntry {
                mutations: vec![
                    new_mutation(OpType::Put, 1, &[("k1", 1), ("k2", 2)]),
                    new_mutation(OpType::Put, 2, &[("k3", 3), ("k4", 4)]),
                ],
                bulk_entries: vec![],
            },
            WalEntry {
                mutations: vec![new_mutation(OpType::Put, 3, &[("k1", 1), ("k2", 2)])],
                bulk_entries: vec![],
            },
            WalEntry {
                mutations: vec![
                    new_mutation(OpType::Put, 4, &[("k1", 1), ("k2", 2)]),
                    new_mutation(OpType::Put, 5, &[("k3", 3), ("k4", 4)]),
                ],
                bulk_entries: vec![],
            },
            WalEntry {
                mutations: vec![new_mutation(OpType::Put, 6, &[("k1", 1), ("k2", 2)])],
                bulk_entries: vec![build_bulk_wal_entry(7, &[("k1", 8), ("k2", 9)])],
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

        let entries = sample_entries();
        let (id1, id2) = (RegionId::new(1, 1), RegionId::new(1, 2));
        let ns1 = Provider::raft_engine_provider(id1.as_u64());
        let ns2 = Provider::raft_engine_provider(id2.as_u64());
        let mut writer = wal.writer();
        writer.add_entry(id1, 1, &entries[0], &ns1).unwrap();
        // Insert one entry into region2. Scan should not return this entry.
        writer.add_entry(id2, 1, &entries[0], &ns2).unwrap();
        writer.add_entry(id1, 2, &entries[1], &ns1).unwrap();
        writer.add_entry(id1, 3, &entries[2], &ns1).unwrap();
        writer.add_entry(id1, 4, &entries[3], &ns1).unwrap();

        writer.write_to_wal().await.unwrap();

        // Scan all contents region1
        let stream = wal.scan(id1, 1, &ns1).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        check_entries(&entries, 1, &actual);

        // Scan parts of contents
        let stream = wal.scan(id1, 2, &ns1).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        check_entries(&entries[1..], 2, &actual);

        // Scan out of range
        let stream = wal.scan(id1, 5, &ns1).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        assert!(actual.is_empty());
    }

    #[tokio::test]
    async fn test_obsolete_wal() {
        let env = WalEnv::new().await;
        let wal = env.new_wal();

        let entries = sample_entries();
        let mut writer = wal.writer();
        let region_id = RegionId::new(1, 1);
        let ns = Provider::raft_engine_provider(region_id.as_u64());
        writer.add_entry(region_id, 1, &entries[0], &ns).unwrap();
        writer.add_entry(region_id, 2, &entries[1], &ns).unwrap();
        writer.add_entry(region_id, 3, &entries[2], &ns).unwrap();

        writer.write_to_wal().await.unwrap();

        // Delete 1, 2.
        wal.obsolete(region_id, 2, &ns).await.unwrap();

        // Put 4.
        let mut writer = wal.writer();
        writer.add_entry(region_id, 4, &entries[3], &ns).unwrap();
        writer.write_to_wal().await.unwrap();

        // Scan all
        let stream = wal.scan(region_id, 1, &ns).unwrap();
        let actual: Vec<_> = stream.try_collect().await.unwrap();
        check_entries(&entries[2..], 3, &actual);
    }
}
