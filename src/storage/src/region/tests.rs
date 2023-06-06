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

//! Region tests.

use std::collections::{HashMap, HashSet};

use arrow::compute::SortOptions;
use common_base::readable_size::ReadableSize;
use common_datasource::compression::CompressionType;
use common_recordbatch::OrderOption;
use common_telemetry::logging;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::prelude::{LogicalTypeId, ScalarVector, WrapperType};
use datatypes::timestamp::TimestampMillisecond;
use datatypes::vectors::{
    BooleanVector, Int64Vector, StringVector, TimestampMillisecondVector, VectorRef,
};
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::NoopLogStore;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::manifest::{Manifest, MAX_VERSION};
use store_api::storage::{
    Chunk, ChunkReader, FlushContext, FlushReason, ReadContext, Region, RegionMeta, ScanRequest,
    SequenceNumber, Snapshot, WriteContext, WriteRequest,
};

use super::*;
use crate::chunk::ChunkReaderImpl;
use crate::compaction::noop::NoopCompactionScheduler;
use crate::engine;
use crate::engine::RegionMap;
use crate::file_purger::noop::NoopFilePurgeHandler;
use crate::flush::{FlushScheduler, PickerConfig, SizeBasedStrategy};
use crate::manifest::action::{RegionChange, RegionMetaActionList};
use crate::manifest::manifest_compress_type;
use crate::manifest::region::RegionManifest;
use crate::manifest::test_utils::*;
use crate::memtable::DefaultMemtableBuilder;
use crate::metadata::RegionMetadata;
use crate::region::{RegionImpl, StoreConfig};
use crate::scheduler::{LocalScheduler, SchedulerConfig};
use crate::sst::{FileId, FsAccessLayer};
use crate::test_util::descriptor_util::RegionDescBuilder;
use crate::test_util::{self, config_util, schema_util, write_batch_util};

mod alter;
mod basic;
mod close;
mod compact;
mod flush;
mod projection;

/// Create metadata of a region with schema: (timestamp, v0).
pub fn new_metadata(region_name: &str) -> RegionMetadata {
    let desc = RegionDescBuilder::new(region_name)
        .push_field_column(("v0", LogicalTypeId::Int64, true))
        .build();
    desc.try_into().unwrap()
}

/// Test region with schema (timestamp, v0).
pub struct TesterBase<S: LogStore> {
    pub region: RegionImpl<S>,
    pub write_ctx: WriteContext,
    pub read_ctx: ReadContext,
}

impl<S: LogStore> TesterBase<S> {
    pub fn with_region(region: RegionImpl<S>) -> TesterBase<S> {
        TesterBase {
            region,
            write_ctx: WriteContext::default(),
            read_ctx: ReadContext::default(),
        }
    }

    pub async fn checkpoint_manifest(&self) {
        let manifest = &self.region.inner.manifest;
        manifest.set_flushed_manifest_version(manifest.last_version() - 1);
        manifest.do_checkpoint().await.unwrap().unwrap();
    }

    pub async fn close(&self) {
        self.region.close(&CloseContext::default()).await.unwrap();
        self.region.inner.wal.close().await.unwrap();
    }

    /// Put without version specified.
    ///
    /// Format of data: (timestamp, v0), timestamp is key, v0 is value.
    pub async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        self.try_put(data).await.unwrap()
    }

    /// Put without version specified, returns [`Result<WriteResponse>`]
    ///
    /// Format of data: (timestamp, v0), timestamp is key, v0 is value.
    pub async fn try_put(&self, data: &[(i64, Option<i64>)]) -> Result<WriteResponse> {
        let data: Vec<(TimestampMillisecond, Option<i64>)> =
            data.iter().map(|(l, r)| ((*l).into(), *r)).collect();
        // Build a batch without version.
        let mut batch = new_write_batch_for_test(false);
        let put_data = new_put_data(&data);
        batch.put(put_data).unwrap();

        self.region.write(&self.write_ctx, batch).await
    }

    /// Put without version specified directly to inner writer.
    pub async fn put_inner(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        let data: Vec<(TimestampMillisecond, Option<i64>)> =
            data.iter().map(|(l, r)| ((*l).into(), *r)).collect();
        let mut batch = new_write_batch_for_test(false);
        let put_data = new_put_data(&data);
        batch.put(put_data).unwrap();

        self.region
            .write_inner(&self.write_ctx, batch)
            .await
            .unwrap()
    }

    pub async fn replay_inner(&self, recovered_metadata: RecoveredMetadataMap) {
        self.region.replay_inner(recovered_metadata).await.unwrap()
    }

    /// Scan all data.
    pub async fn full_scan(&self) -> Vec<(i64, Option<i64>)> {
        logging::info!("Full scan with ctx {:?}", self.read_ctx);
        let snapshot = self.region.snapshot(&self.read_ctx).unwrap();

        let resp = snapshot
            .scan(&self.read_ctx, ScanRequest::default())
            .await
            .unwrap();
        let mut reader = resp.reader;

        let metadata = self.region.in_memory_metadata();
        assert_eq!(metadata.schema(), reader.user_schema());

        let mut dst = Vec::new();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            let chunk = reader.project_chunk(chunk);
            append_chunk_to(&chunk, &mut dst);
        }

        dst
    }

    pub fn committed_sequence(&self) -> SequenceNumber {
        self.region.committed_sequence()
    }

    /// Delete by keys (timestamp).
    pub async fn delete(&self, keys: &[i64]) -> WriteResponse {
        let keys: Vec<TimestampMillisecond> = keys.iter().map(|v| (*v).into()).collect();
        // Build a batch without version.
        let mut batch = new_write_batch_for_test(false);
        let keys = new_delete_data(&keys);
        batch.delete(keys).unwrap();

        self.region.write(&self.write_ctx, batch).await.unwrap()
    }

    /// Returns a reader to scan all data.
    pub async fn full_scan_reader(&self) -> ChunkReaderImpl {
        let snapshot = self.region.snapshot(&self.read_ctx).unwrap();

        let resp = snapshot
            .scan(&self.read_ctx, ScanRequest::default())
            .await
            .unwrap();
        resp.reader
    }

    /// Collect data from the reader.
    pub async fn collect_reader(&self, mut reader: ChunkReaderImpl) -> Vec<(i64, Option<i64>)> {
        let mut dst = Vec::new();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            let chunk = reader.project_chunk(chunk);
            append_chunk_to(&chunk, &mut dst);
        }

        dst
    }
}

pub type FileTesterBase = TesterBase<RaftEngineLogStore>;

fn new_write_batch_for_test(enable_version_column: bool) -> WriteBatch {
    if enable_version_column {
        write_batch_util::new_write_batch(
            &[
                (
                    test_util::TIMESTAMP_NAME,
                    LogicalTypeId::TimestampMillisecond,
                    false,
                ),
                ("v0", LogicalTypeId::Int64, true),
            ],
            Some(0),
            2,
        )
    } else {
        write_batch_util::new_write_batch(
            &[
                (
                    test_util::TIMESTAMP_NAME,
                    LogicalTypeId::TimestampMillisecond,
                    false,
                ),
                ("v0", LogicalTypeId::Int64, true),
            ],
            Some(0),
            1,
        )
    }
}

fn new_put_data(data: &[(TimestampMillisecond, Option<i64>)]) -> HashMap<String, VectorRef> {
    let mut put_data = HashMap::with_capacity(2);

    let timestamps =
        TimestampMillisecondVector::from_vec(data.iter().map(|v| v.0.into()).collect());
    let values = Int64Vector::from_owned_iterator(data.iter().map(|kv| kv.1));

    put_data.insert(
        test_util::TIMESTAMP_NAME.to_string(),
        Arc::new(timestamps) as VectorRef,
    );
    put_data.insert("v0".to_string(), Arc::new(values) as VectorRef);

    put_data
}

fn new_delete_data(keys: &[TimestampMillisecond]) -> HashMap<String, VectorRef> {
    let mut delete_data = HashMap::new();

    let timestamps =
        TimestampMillisecondVector::from_vec(keys.iter().map(|v| v.0.into()).collect());

    delete_data.insert(
        test_util::TIMESTAMP_NAME.to_string(),
        Arc::new(timestamps) as VectorRef,
    );

    delete_data
}

fn append_chunk_to(chunk: &Chunk, dst: &mut Vec<(i64, Option<i64>)>) {
    assert_eq!(2, chunk.columns.len());

    let timestamps = chunk.columns[0]
        .as_any()
        .downcast_ref::<TimestampMillisecondVector>()
        .unwrap();
    let values = chunk.columns[1]
        .as_any()
        .downcast_ref::<Int64Vector>()
        .unwrap();
    for (ts, value) in timestamps.iter_data().zip(values.iter_data()) {
        dst.push((ts.unwrap().into_native(), value));
    }
}

#[tokio::test]
async fn test_new_region() {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_field_column(("v0", LogicalTypeId::Float32, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();

    let dir = create_temp_dir("test_new_region");
    let store_dir = dir.path().to_str().unwrap();

    let store_config = config_util::new_store_config(region_name, store_dir).await;
    let placeholder_memtable = store_config
        .memtable_builder
        .build(metadata.schema().clone());

    let region = RegionImpl::new(
        Version::new(Arc::new(metadata), placeholder_memtable),
        store_config,
    );

    let expect_schema = schema_util::new_schema_ref(
        &[
            ("k1", LogicalTypeId::Int32, false),
            (
                test_util::TIMESTAMP_NAME,
                LogicalTypeId::TimestampMillisecond,
                false,
            ),
            ("v0", LogicalTypeId::Float32, true),
        ],
        Some(1),
    );

    assert_eq!(region_name, region.name());
    assert_eq!(expect_schema, *region.in_memory_metadata().schema());
}

#[tokio::test]
async fn test_recover_region_manifets_compress() {
    test_recover_region_manifets(true).await;
}

#[tokio::test]
async fn test_recover_region_manifets_uncompress() {
    test_recover_region_manifets(false).await;
}

async fn test_recover_region_manifets(compress: bool) {
    common_telemetry::init_default_ut_logging();
    let tmp_dir = create_temp_dir("test_recover_region_manifets");
    let memtable_builder = Arc::new(DefaultMemtableBuilder::default()) as _;

    let mut builder = Fs::default();
    builder.root(&tmp_dir.path().to_string_lossy());
    let object_store = ObjectStore::new(builder).unwrap().finish();

    let manifest = RegionManifest::with_checkpointer(
        "/manifest/",
        object_store.clone(),
        manifest_compress_type(compress),
        None,
        None,
    );
    let region_meta = Arc::new(build_region_meta());

    let sst_layer = Arc::new(FsAccessLayer::new("sst", object_store)) as _;
    let file_purger = Arc::new(LocalScheduler::new(
        SchedulerConfig::default(),
        NoopFilePurgeHandler,
    ));
    // Recover from empty
    assert!(RegionImpl::<NoopLogStore>::recover_from_manifest(
        &manifest,
        &memtable_builder,
        &sst_layer,
        &file_purger,
    )
    .await
    .unwrap()
    .0
    .is_none());

    let file_id_a = FileId::random();
    let file_id_b = FileId::random();
    let file_id_c = FileId::random();

    {
        // save some actions into region_meta
        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.as_ref().into(),
                    committed_sequence: 40,
                },
            )))
            .await
            .unwrap();

        manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Edit(build_region_edit(1, &[file_id_a], &[])),
                RegionMetaAction::Edit(build_region_edit(2, &[file_id_b, file_id_c], &[])),
            ]))
            .await
            .unwrap();

        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.as_ref().into(),
                    committed_sequence: 42,
                },
            )))
            .await
            .unwrap();
    }

    // try to recover
    let (version, recovered_metadata) = RegionImpl::<NoopLogStore>::recover_from_manifest(
        &manifest,
        &memtable_builder,
        &sst_layer,
        &file_purger,
    )
    .await
    .unwrap();

    assert_recovered_manifest(
        version,
        recovered_metadata,
        &file_id_a,
        &file_id_b,
        &file_id_c,
        &region_meta,
    );

    // do a manifest checkpoint
    let checkpoint = manifest.do_checkpoint().await.unwrap().unwrap();
    assert_eq!(1, checkpoint.last_version);
    assert_eq!(2, checkpoint.compacted_actions);
    assert_eq!(
        manifest.last_checkpoint().await.unwrap().unwrap(),
        checkpoint
    );
    // recover from checkpoint
    let (version, recovered_metadata) = RegionImpl::<NoopLogStore>::recover_from_manifest(
        &manifest,
        &memtable_builder,
        &sst_layer,
        &file_purger,
    )
    .await
    .unwrap();

    assert_recovered_manifest(
        version,
        recovered_metadata,
        &file_id_a,
        &file_id_b,
        &file_id_c,
        &region_meta,
    );

    // check manifest state
    assert_eq!(3, manifest.last_version());
    let mut iter = manifest.scan(0, MAX_VERSION).await.unwrap();
    let (version, action) = iter.next_action().await.unwrap().unwrap();
    assert_eq!(2, version);
    assert!(matches!(action.actions[0], RegionMetaAction::Change(..)));
    assert!(iter.next_action().await.unwrap().is_none());
}

fn assert_recovered_manifest(
    version: Option<Version>,
    recovered_metadata: RecoveredMetadataMap,
    file_id_a: &FileId,
    file_id_b: &FileId,
    file_id_c: &FileId,
    region_meta: &Arc<RegionMetadata>,
) {
    assert_eq!(42, *recovered_metadata.first_key_value().unwrap().0);
    let version = version.unwrap();
    assert_eq!(*version.metadata(), *region_meta);
    assert_eq!(version.flushed_sequence(), 2);
    assert_eq!(version.manifest_version(), 1);
    let ssts = version.ssts();
    let files = ssts.levels()[0]
        .files()
        .map(|f| f.file_name())
        .collect::<HashSet<_>>();
    assert_eq!(3, files.len());
    assert_eq!(
        HashSet::from([
            file_id_a.as_parquet(),
            file_id_b.as_parquet(),
            file_id_c.as_parquet()
        ]),
        files
    );
}

fn create_region_meta(region_name: &str) -> RegionMetadata {
    let desc = RegionDescBuilder::new(region_name)
        .push_field_column(("v0", LogicalTypeId::Int64, true))
        .push_field_column(("v1", LogicalTypeId::String, true))
        .push_field_column(("v2", LogicalTypeId::Boolean, true))
        .build();
    desc.try_into().unwrap()
}

async fn create_store_config(region_name: &str, root: &str) -> StoreConfig<NoopLogStore> {
    let mut builder = Fs::default();
    builder.root(root);
    let object_store = ObjectStore::new(builder).unwrap().finish();
    let parent_dir = "";
    let sst_dir = engine::region_sst_dir(parent_dir, region_name);
    let manifest_dir = engine::region_manifest_dir(parent_dir, region_name);

    let sst_layer = Arc::new(FsAccessLayer::new(&sst_dir, object_store.clone()));
    let manifest = RegionManifest::with_checkpointer(
        &manifest_dir,
        object_store,
        CompressionType::Uncompressed,
        None,
        None,
    );
    manifest.start().await.unwrap();

    let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());

    let regions = Arc::new(RegionMap::new());

    let flush_scheduler = Arc::new(
        FlushScheduler::new(
            SchedulerConfig::default(),
            compaction_scheduler.clone(),
            regions,
            PickerConfig::default(),
        )
        .unwrap(),
    );

    let log_store = Arc::new(NoopLogStore::default());

    let file_purger = Arc::new(LocalScheduler::new(
        SchedulerConfig::default(),
        NoopFilePurgeHandler,
    ));
    StoreConfig {
        log_store,
        sst_layer,
        manifest,
        memtable_builder: Arc::new(DefaultMemtableBuilder::default()),
        flush_scheduler,
        flush_strategy: Arc::new(SizeBasedStrategy::default()),
        compaction_scheduler,
        engine_config: Default::default(),
        file_purger,
        ttl: None,
        compaction_time_window: None,
        write_buffer_size: ReadableSize::mb(32).0 as usize,
    }
}

struct WindowedReaderTester {
    data_written: Vec<Vec<(i64, i64, String, bool)>>,
    expected: Vec<(i64, i64, String, bool)>,
    region: RegionImpl<NoopLogStore>,
    _temp_dir: TempDir,
}

impl WindowedReaderTester {
    async fn new(
        region_name: &'static str,
        data_written: Vec<Vec<(i64, i64, String, bool)>>,
        expected: Vec<(i64, i64, String, bool)>,
    ) -> Self {
        let temp_dir = create_temp_dir(&format!("write_and_read_windowed_{}", region_name));
        let root = temp_dir.path().to_str().unwrap();
        let metadata = create_region_meta(region_name);
        let store_config = create_store_config(region_name, root).await;
        let region = RegionImpl::create(metadata, store_config).await.unwrap();

        let tester = Self {
            data_written,
            expected,
            region,
            _temp_dir: temp_dir,
        };
        tester.prepare().await;
        tester
    }

    async fn prepare(&self) {
        for batch in &self.data_written {
            let mut write_batch = self.region.write_request();
            let ts = TimestampMillisecondVector::from_iterator(
                batch
                    .iter()
                    .map(|(v, _, _, _)| TimestampMillisecond::new(*v)),
            );
            let v0 = Int64Vector::from_iterator(batch.iter().map(|(_, v, _, _)| *v));
            let v1 = StringVector::from_iterator(batch.iter().map(|(_, _, v, _)| v.as_str()));
            let v2 = BooleanVector::from_iterator(batch.iter().map(|(_, _, _, v)| *v));

            let columns = [
                ("timestamp".to_string(), Arc::new(ts) as VectorRef),
                ("v0".to_string(), Arc::new(v0) as VectorRef),
                ("v1".to_string(), Arc::new(v1) as VectorRef),
                ("v2".to_string(), Arc::new(v2) as VectorRef),
            ]
            .into_iter()
            .collect::<HashMap<String, VectorRef>>();
            write_batch.put(columns).unwrap();

            self.region
                .write(&WriteContext {}, write_batch)
                .await
                .unwrap();

            // flush the region to ensure data resides across SST files.
            self.region
                .flush(&FlushContext {
                    wait: true,
                    reason: FlushReason::Others,
                    ..Default::default()
                })
                .await
                .unwrap();
        }
    }

    async fn check(&self, order_options: Vec<OrderOption>) {
        let read_context = ReadContext::default();
        let snapshot = self.region.snapshot(&read_context).unwrap();
        let response = snapshot
            .scan(
                &read_context,
                ScanRequest {
                    sequence: None,
                    projection: None,
                    filters: vec![],
                    limit: None,
                    output_ordering: Some(order_options),
                },
            )
            .await
            .unwrap();

        let mut timestamps = Vec::with_capacity(self.expected.len());
        let mut col1 = Vec::with_capacity(self.expected.len());
        let mut col2 = Vec::with_capacity(self.expected.len());
        let mut col3 = Vec::with_capacity(self.expected.len());

        let mut reader = response.reader;
        let ts_index = reader.user_schema().timestamp_index().unwrap();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            let ts_col = &chunk.columns[ts_index];
            let ts_col = ts_col
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap();
            let v1_col = chunk.columns[1]
                .as_any()
                .downcast_ref::<Int64Vector>()
                .unwrap();
            let v2_col = chunk.columns[2]
                .as_any()
                .downcast_ref::<StringVector>()
                .unwrap();
            let v3_col = chunk.columns[3]
                .as_any()
                .downcast_ref::<BooleanVector>()
                .unwrap();

            for ts in ts_col.iter_data() {
                timestamps.push(ts.unwrap().0.value());
            }
            for v in v1_col.iter_data() {
                col1.push(v.unwrap());
            }
            for v in v2_col.iter_data() {
                col2.push(v.unwrap().to_string());
            }
            for v in v3_col.iter_data() {
                col3.push(v.unwrap());
            }
        }

        assert_eq!(
            timestamps,
            self.expected
                .iter()
                .map(|(v, _, _, _)| *v)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            col1,
            self.expected
                .iter()
                .map(|(_, v, _, _)| *v)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            col2,
            self.expected
                .iter()
                .map(|(_, _, v, _)| v.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            col3,
            self.expected
                .iter()
                .map(|(_, _, _, v)| *v)
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn test_read_by_chunk_reader() {
    common_telemetry::init_default_ut_logging();

    WindowedReaderTester::new(
        "test_region",
        vec![vec![(1, 1, "1".to_string(), false)]],
        vec![(1, 1, "1".to_string(), false)],
    )
    .await
    .check(vec![OrderOption {
        name: "timestamp".to_string(),
        options: SortOptions {
            descending: true,
            nulls_first: true,
        },
    }])
    .await;

    WindowedReaderTester::new(
        "test_region",
        vec![
            vec![
                (1, 1, "1".to_string(), false),
                (2, 2, "2".to_string(), false),
            ],
            vec![
                (3, 3, "3".to_string(), false),
                (4, 4, "4".to_string(), false),
            ],
        ],
        vec![
            (4, 4, "4".to_string(), false),
            (3, 3, "3".to_string(), false),
            (2, 2, "2".to_string(), false),
            (1, 1, "1".to_string(), false),
        ],
    )
    .await
    .check(vec![OrderOption {
        name: "timestamp".to_string(),
        options: SortOptions {
            descending: true,
            nulls_first: true,
        },
    }])
    .await;

    WindowedReaderTester::new(
        "test_region",
        vec![
            vec![
                (1, 1, "1".to_string(), false),
                (2, 2, "2".to_string(), false),
                (60000, 60000, "60".to_string(), false),
            ],
            vec![
                (3, 3, "3".to_string(), false),
                (61000, 61000, "61".to_string(), false),
            ],
        ],
        vec![
            (61000, 61000, "61".to_string(), false),
            (60000, 60000, "60".to_string(), false),
            (3, 3, "3".to_string(), false),
            (2, 2, "2".to_string(), false),
            (1, 1, "1".to_string(), false),
        ],
    )
    .await
    .check(vec![OrderOption {
        name: "timestamp".to_string(),
        options: SortOptions {
            descending: true,
            nulls_first: true,
        },
    }])
    .await;

    WindowedReaderTester::new(
        "test_region",
        vec![
            vec![
                (1, 1, "1".to_string(), false),
                (2, 2, "2".to_string(), false),
                (60000, 60000, "60".to_string(), false),
            ],
            vec![
                (3, 3, "3".to_string(), false),
                (61000, 61000, "61".to_string(), false),
            ],
        ],
        vec![
            (1, 1, "1".to_string(), false),
            (2, 2, "2".to_string(), false),
            (3, 3, "3".to_string(), false),
            (60000, 60000, "60".to_string(), false),
            (61000, 61000, "61".to_string(), false),
        ],
    )
    .await
    .check(vec![OrderOption {
        name: "timestamp".to_string(),
        options: SortOptions {
            descending: false,
            nulls_first: true,
        },
    }])
    .await;
}
