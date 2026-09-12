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
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use api::v1::{ColumnSchema, Rows};
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use common_time::Timestamp;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::TimestampMillisecondType;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::AlterKind::SetRegionOptions;
use store_api::region_request::{
    EnterStagingRequest, PathType, RegionAlterRequest, RegionCloseRequest, RegionCompactRequest,
    RegionDeleteRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest,
    RegionTruncateRequest, SetRegionOption, StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::{Notify, Semaphore};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::flush_test::MockTimeProvider;
use crate::engine::listener::{CompactionListener, EventListener};
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows_for_key, column_metadata_to_column_schema, put_rows,
};

pub(crate) async fn put_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };
    put_rows(engine, region_id, rows).await;

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

async fn flush(engine: &MitoEngine, region_id: RegionId) {
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

pub(crate) async fn compact(engine: &MitoEngine, region_id: RegionId) {
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);
}

pub(crate) async fn delete_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let row_cnt = rows.len();
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest {
                rows,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(row_cnt, result.affected_rows);

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

async fn collect_stream_ts(stream: SendableRecordBatchStream) -> Vec<i64> {
    let mut res = Vec::new();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    for batch in batches {
        let ts_col = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        res.extend((0..ts_col.len()).map(|i| ts_col.value(i)));
    }
    res
}

/// Pauses the first unit at merge or commit while observing sibling progress.
struct UnitVisibilityGate {
    first: std::sync::atomic::AtomicU64,
    at_commit: bool,
    entered: Notify,
    resume: Semaphore,
    applied: Notify,
    cancel_requested: Notify,
}

/// Fails a selected SST close and records earlier outputs for cleanup assertions.
struct FailingUnitSstWriter {
    inner: object_store::layers::mock::Writer,
    path: String,
    remaining: Arc<std::sync::atomic::AtomicUsize>,
    finalized: Arc<std::sync::Mutex<Vec<String>>>,
}

impl object_store::layers::mock::Write for FailingUnitSstWriter {
    async fn write(
        &mut self,
        bytes: object_store::layers::mock::Buffer,
    ) -> object_store::layers::mock::Result<()> {
        self.inner.write(bytes).await
    }
    /// Injects failure at the configured close without publishing that SST.
    async fn close(
        &mut self,
    ) -> object_store::layers::mock::Result<object_store::layers::mock::Metadata> {
        let previous = self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        if previous == Ok(1) {
            return Err(object_store::layers::mock::Error::new(
                object_store::layers::mock::ErrorKind::Unexpected,
                "second unit SST failed",
            ));
        }
        let metadata = self.inner.close().await?;
        if previous.is_ok() {
            self.finalized.lock().unwrap().push(self.path.clone());
        }
        Ok(metadata)
    }
    async fn abort(&mut self) -> object_store::layers::mock::Result<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn test_compaction_unit_failed_dependency_group_keeps_input_and_cleans_output() {
    let remaining = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let finalized = Arc::new(std::sync::Mutex::new(Vec::new()));
    let count = remaining.clone();
    let written = finalized.clone();
    let layer = object_store::layers::mock::MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _, inner| {
            if path.ends_with(".parquet") {
                Box::new(FailingUnitSstWriter {
                    inner,
                    path: path.to_string(),
                    remaining: count.clone(),
                    finalized: written.clone(),
                })
            } else {
                inner
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(layer);
    let config = MitoConfig {
        min_compaction_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
    let region_id = RegionId::new(43, 1);
    let create = CreateRequestBuilder::new().build();
    let table_dir = create.table_dir.clone();
    let columns = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &columns, 0..122).await;
    let original = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap()
        .file_ids();
    remaining.store(2, Ordering::SeqCst);
    let error = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest {
                options: api::v1::region::compact_request::Options::StrictWindow(
                    api::v1::region::StrictWindow { window_seconds: 60 },
                ),
                parallelism: Some(2),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::StorageUnavailable, error.status_code());
    let current = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(original, current.file_ids());
    let expected = (0..122).map(|n| n * 1000).collect::<Vec<_>>();
    assert_eq!(
        expected,
        collect_stream_ts(current.scan().await.unwrap()).await
    );
    let paths = finalized.lock().unwrap().clone();
    assert_eq!(
        1,
        paths.len(),
        "one output must have finished before the failing output"
    );
    let store = env.get_object_store().unwrap();
    for path in paths {
        assert!(
            !store.exists(&path).await.unwrap(),
            "abandoned output {path}"
        );
    }
    let engine = env.reopen_engine(engine, config).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: Default::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    let reopened = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        expected,
        collect_stream_ts(reopened.scan().await.unwrap()).await
    );
}

impl UnitVisibilityGate {
    /// Blocks only the first arriving unit, leaving its siblings free to progress.
    async fn block_first(&self, id: u64) {
        if self
            .first
            .compare_exchange(u64::MAX, id, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.entered.notify_one();
            self.resume.acquire().await.unwrap().forget();
        }
    }
}

/// Releases the visibility gate even when a test exits through a panic.
struct UnitVisibilityGuard(Arc<UnitVisibilityGate>);

/// Holds one unit before merge and its sibling at commit for deterministic failure ordering.
struct UnitFailureGate {
    merge: Arc<UnitVisibilityGate>,
    commit: Arc<CompactionPlanningGate>,
}

#[async_trait]
impl EventListener for UnitFailureGate {
    async fn on_compaction_unit_merge_begin(&self, _region_id: RegionId, id: u64) {
        self.merge.block_first(id).await;
    }

    async fn on_compaction_commit_begin(&self, region_id: RegionId) {
        self.commit.on_compaction_commit_begin(region_id).await;
    }

    fn on_compaction_cancel_requested(&self, region_id: RegionId) {
        self.commit.on_compaction_cancel_requested(region_id);
    }

    async fn on_compaction_result_notified(&self, region_id: RegionId) {
        self.commit.on_compaction_result_notified(region_id).await;
    }
}

/// Distinguishes a rejected write from a lost reply after persistence.
#[derive(Clone, Copy)]
enum ManifestWriteFailure {
    BeforeWrite,
    AfterWrite,
}

/// Injects manifest failures around a real storage write to verify output ownership.
struct FailingManifestWriter {
    inner: object_store::layers::mock::Writer,
    failure: ManifestWriteFailure,
}

impl object_store::layers::mock::Write for FailingManifestWriter {
    /// Rejects the pre-write case before any bytes reach the real writer.
    async fn write(
        &mut self,
        bytes: object_store::layers::mock::Buffer,
    ) -> object_store::layers::mock::Result<()> {
        if matches!(self.failure, ManifestWriteFailure::BeforeWrite) {
            return Err(object_store::layers::mock::Error::new(
                object_store::layers::mock::ErrorKind::PermissionDenied,
                "manifest write rejected before persistence",
            ));
        }
        self.inner.write(bytes).await
    }
    /// Persists the manifest before simulating a lost success reply.
    async fn close(
        &mut self,
    ) -> object_store::layers::mock::Result<object_store::layers::mock::Metadata> {
        self.inner.close().await?;
        Err(object_store::layers::mock::Error::new(
            object_store::layers::mock::ErrorKind::Unexpected,
            "lost manifest response after persistence",
        ))
    }
    async fn abort(&mut self) -> object_store::layers::mock::Result<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn test_compaction_unit_manifest_failure_preserves_outputs() {
    for failure in [
        ManifestWriteFailure::BeforeWrite,
        ManifestWriteFailure::AfterWrite,
    ] {
        let armed = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ssts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (inject, writes, paths) = (armed.clone(), attempts.clone(), ssts.clone());
        let layer = object_store::layers::mock::MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| {
                if path.ends_with(".parquet") {
                    paths.lock().unwrap().push(path.to_string());
                }
                if path.contains("/manifest/") && path.contains(".json") {
                    writes.fetch_add(1, Ordering::SeqCst);
                    if inject.swap(false, Ordering::SeqCst) {
                        return Box::new(FailingManifestWriter { inner, failure });
                    }
                }
                inner
            }))
            .build()
            .unwrap();
        let region_id = RegionId::new(44, 1);
        let gate = Arc::new(CompactionPlanningGate::new(region_id));
        let merge_gate = Arc::new(UnitVisibilityGate {
            first: std::sync::atomic::AtomicU64::new(u64::MAX),
            at_commit: false,
            entered: Notify::new(),
            resume: Semaphore::new(0),
            applied: Notify::new(),
            cancel_requested: Notify::new(),
        });
        let merge_guard = UnitVisibilityGuard(merge_gate.clone());
        let mut env = TestEnv::new().await.with_mock_layer(layer);
        let engine = env
            .create_engine_with(
                MitoConfig {
                    max_background_compactions: 2,
                    min_compaction_interval: Duration::from_secs(3600),
                    ..Default::default()
                },
                None,
                Some(Arc::new(UnitFailureGate {
                    merge: merge_gate,
                    commit: gate.clone(),
                })),
                None,
            )
            .await;
        let create = CreateRequestBuilder::new().build();
        let columns = create
            .column_metadatas
            .iter()
            .map(column_metadata_to_column_schema)
            .collect::<Vec<_>>();
        engine
            .handle_request(region_id, RegionRequest::Create(create))
            .await
            .unwrap();
        put_and_flush(&engine, region_id, &columns, 0..2).await;
        put_and_flush(&engine, region_id, &columns, 120..122).await;
        let original = engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap()
            .file_ids();
        assert_eq!(2, original.len());
        let region = engine.get_region(region_id).unwrap();
        let version = region
            .manifest_ctx
            .manifest_manager
            .read()
            .await
            .manifest()
            .manifest_version;
        ssts.lock().unwrap().clear();
        attempts.store(0, Ordering::SeqCst);
        armed.store(true, Ordering::SeqCst);
        let guard = gate.arm_commit();
        let compact_engine = engine.clone();
        let compact = tokio::spawn(async move {
            compact_engine
                .handle_request(
                    region_id,
                    RegionRequest::Compact(RegionCompactRequest {
                        options: api::v1::region::compact_request::Options::StrictWindow(
                            api::v1::region::StrictWindow { window_seconds: 60 },
                        ),
                        parallelism: Some(2),
                        ..Default::default()
                    }),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), gate.wait_until_commit_entered())
            .await
            .unwrap();
        let sibling_guard = gate.arm_pending_ddl_dispatch();
        let ddl_engine = engine.clone();
        let truncate = tokio::spawn(async move {
            ddl_engine
                .handle_request(
                    region_id,
                    RegionRequest::Truncate(RegionTruncateRequest::All),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), gate.wait_until_cancel_requested())
            .await
            .unwrap();
        drop(merge_guard);
        // Observe the cancelled sibling's exit before releasing the failing commit.
        // Its cancellation must neither finish the request nor mask the later I/O error.
        tokio::time::timeout(
            Duration::from_secs(10),
            gate.wait_until_pending_ddl_dispatch(),
        )
        .await
        .unwrap();
        assert!(!compact.is_finished());
        assert!(!truncate.is_finished());
        sibling_guard.release();
        guard.release();
        let error = tokio::time::timeout(Duration::from_secs(10), compact)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(StatusCode::StorageUnavailable, error.status_code());
        let error = tokio::time::timeout(Duration::from_secs(10), truncate)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(StatusCode::StorageUnavailable, error.status_code());
        assert_eq!(
            1,
            attempts.load(Ordering::SeqCst),
            "one failed publication, without automatic retry"
        );
        assert!(!armed.load(Ordering::SeqCst));
        assert_eq!(
            version,
            region
                .manifest_ctx
                .manifest_manager
                .read()
                .await
                .manifest()
                .manifest_version
        );
        let scan = engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap();
        assert_eq!(
            original,
            scan.file_ids(),
            "failed edits must not be applied"
        );
        assert_eq!(
            vec![0, 1000, 120_000, 121_000],
            collect_stream_ts(scan.scan().await.unwrap()).await
        );
        assert!(
            region
                .version()
                .ssts
                .levels()
                .iter()
                .flat_map(|l| l.files())
                .all(|f| !f.compacting())
        );
        let paths = ssts.lock().unwrap().clone();
        assert!(!paths.is_empty());
        let store = env.get_object_store().unwrap();
        let mut retained = 0;
        for path in paths {
            retained += usize::from(store.exists(&path).await.unwrap());
        }
        assert_eq!(
            usize::from(matches!(failure, ManifestWriteFailure::AfterWrite)),
            retained
        );
    }
}

impl Drop for UnitVisibilityGuard {
    fn drop(&mut self) {
        self.0.resume.add_permits(1);
    }
}

#[async_trait]
impl EventListener for UnitVisibilityGate {
    fn on_compaction_cancel_requested(&self, _region_id: RegionId) {
        self.cancel_requested.notify_one();
    }
    async fn on_compaction_unit_merge_begin(&self, _region_id: RegionId, id: u64) {
        if !self.at_commit {
            self.block_first(id).await;
        }
    }
    async fn on_compaction_unit_committed(&self, _region_id: RegionId, id: u64) {
        if self.at_commit {
            self.block_first(id).await;
        }
    }
    fn on_compaction_unit_applied(&self, _region_id: RegionId, _id: u64) {
        self.applied.notify_one();
    }
}

#[tokio::test]
async fn test_compaction_unit_commit_and_apply_can_interleave() {
    enum Scenario {
        SlowMerge,
        PendingApply,
        Truncate,
        Reopen,
    }
    for scenario in [
        Scenario::SlowMerge,
        Scenario::PendingApply,
        Scenario::Truncate,
        Scenario::Reopen,
    ] {
        let at_commit = matches!(scenario, Scenario::PendingApply | Scenario::Reopen);
        let gate = Arc::new(UnitVisibilityGate {
            first: std::sync::atomic::AtomicU64::new(u64::MAX),
            at_commit,
            entered: Notify::new(),
            resume: Semaphore::new(0),
            applied: Notify::new(),
            cancel_requested: Notify::new(),
        });
        let guard = UnitVisibilityGuard(gate.clone());
        let mut env = TestEnv::new().await;
        let config = MitoConfig {
            max_background_compactions: 2,
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        };
        let engine = env
            .create_engine_with(config.clone(), None, Some(gate.clone()), None)
            .await;
        let region_id = RegionId::new(42, 1);
        let create = CreateRequestBuilder::new().build();
        let table_dir = create.table_dir.clone();
        let columns = create
            .column_metadatas
            .iter()
            .map(column_metadata_to_column_schema)
            .collect::<Vec<_>>();
        engine
            .handle_request(region_id, RegionRequest::Create(create))
            .await
            .unwrap();
        put_and_flush(&engine, region_id, &columns, 0..2).await;
        put_and_flush(&engine, region_id, &columns, 120..122).await;
        let old = engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap();
        let old_ids = old.file_ids();
        assert_eq!(2, old.num_files());
        let compact_engine = engine.clone();
        let task = tokio::spawn(async move {
            compact_engine
                .handle_request(
                    region_id,
                    RegionRequest::Compact(RegionCompactRequest {
                        options: api::v1::region::compact_request::Options::StrictWindow(
                            api::v1::region::StrictWindow { window_seconds: 60 },
                        ),
                        parallelism: Some(2),
                        ..Default::default()
                    }),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), gate.applied.notified())
            .await
            .expect("sibling unit waited for another unit's merge or apply");
        let current = engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap();
        let unchanged = current
            .file_ids()
            .iter()
            .filter(|id| old_ids.contains(id))
            .count();
        assert_eq!(1, unchanged, "the ungated unit must already be visible");
        assert!(
            !task.is_finished(),
            "manual request finished before all units"
        );
        let mut expected = vec![0, 1000, 120_000, 121_000];
        assert_eq!(
            expected,
            collect_stream_ts(current.scan().await.unwrap()).await
        );
        assert_eq!(expected, collect_stream_ts(old.scan().await.unwrap()).await);
        let truncate_task = if matches!(scenario, Scenario::Truncate) {
            let engine = engine.clone();
            let ddl = tokio::spawn(async move {
                engine
                    .handle_request(
                        region_id,
                        RegionRequest::Truncate(RegionTruncateRequest::All),
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(10), gate.cancel_requested.notified())
                .await
                .unwrap();
            assert!(!ddl.is_finished());
            assert!(!task.is_finished());
            Some(ddl)
        } else {
            None
        };
        if matches!(scenario, Scenario::Reopen) {
            engine
                .handle_request(
                    region_id,
                    RegionRequest::Close(RegionCloseRequest::default()),
                )
                .await
                .unwrap();
            engine
                .handle_request(
                    region_id,
                    RegionRequest::Open(RegionOpenRequest {
                        engine: String::new(),
                        table_dir: table_dir.clone(),
                        path_type: PathType::Bare,
                        options: Default::default(),
                        skip_wal_replay: false,
                        checkpoint: None,
                        requirements: Default::default(),
                    }),
                )
                .await
                .unwrap();
            let recovered = engine
                .scanner(region_id, ScanRequest::default())
                .await
                .unwrap();
            assert_eq!(
                expected,
                collect_stream_ts(recovered.scan().await.unwrap()).await
            );
        }
        drop(guard);
        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        if let Some(ddl) = truncate_task {
            assert_eq!(StatusCode::Cancelled, result.unwrap_err().status_code());
            tokio::time::timeout(Duration::from_secs(10), ddl)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            expected.clear();
        } else if matches!(scenario, Scenario::Reopen) {
            assert_eq!(StatusCode::Cancelled, result.unwrap_err().status_code());
        } else {
            result.unwrap();
        }
        let completed = engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap();
        assert!(completed.file_ids().iter().all(|id| !old_ids.contains(id)));
        assert_eq!(
            expected,
            collect_stream_ts(completed.scan().await.unwrap()).await,
            "applying the delayed edit must preserve its sibling's result"
        );
        let engine =
            tokio::time::timeout(Duration::from_secs(10), env.reopen_engine(engine, config))
                .await
                .unwrap();
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    table_dir,
                    path_type: PathType::Bare,
                    options: Default::default(),
                    skip_wal_replay: false,
                    checkpoint: None,
                    requirements: Default::default(),
                }),
            )
            .await
            .unwrap();
        let reopened = engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap();
        assert_eq!(
            expected,
            collect_stream_ts(reopened.scan().await.unwrap()).await
        );
    }
}

struct CompactionListenerGuard(Option<Arc<CompactionListener>>);

impl CompactionListenerGuard {
    fn new(listener: Arc<CompactionListener>) -> Self {
        Self(Some(listener))
    }

    fn release(mut self) {
        self.0.take().unwrap().wake();
    }
}

impl Drop for CompactionListenerGuard {
    fn drop(&mut self) {
        if let Some(listener) = self.0.take() {
            listener.wake();
        }
    }
}

/// Test gate that blocks selected compaction lifecycle phases for one region.
struct CompactionPlanningGate {
    region_id: RegionId,
    armed: AtomicBool,
    entered: Notify,
    cancel_requested: Notify,
    permits: Semaphore,
    merge_armed: AtomicBool,
    merge_entered: Notify,
    merge_permits: Semaphore,
    commit_armed: AtomicBool,
    commit_entered: Notify,
    commit_permits: Semaphore,
    pending_ddl_armed: AtomicBool,
    pending_ddl_entered: Notify,
    pending_ddl_permits: Semaphore,
}

/// Releases an armed [`CompactionPlanningGate`] when a test exits unexpectedly.
struct CompactionPlanningGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionPlanningGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release();
    }
}

impl Drop for CompactionPlanningGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release();
        }
    }
}

/// Releases an armed merge gate when a test exits unexpectedly.
struct CompactionMergeGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionMergeGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release_merge();
    }
}

impl Drop for CompactionMergeGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release_merge();
        }
    }
}

/// Releases an armed commit gate when a test exits unexpectedly.
struct CompactionCommitGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionCommitGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release_commit();
    }
}

impl Drop for CompactionCommitGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release_commit();
        }
    }
}

/// Releases an armed pending-DDL dispatch gate when a test exits unexpectedly.
struct CompactionPendingDdlGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionPendingDdlGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release_pending_ddl_dispatch();
    }
}

impl Drop for CompactionPendingDdlGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release_pending_ddl_dispatch();
        }
    }
}

impl CompactionPlanningGate {
    fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            cancel_requested: Notify::new(),
            permits: Semaphore::new(0),
            merge_armed: AtomicBool::new(false),
            merge_entered: Notify::new(),
            merge_permits: Semaphore::new(0),
            commit_armed: AtomicBool::new(false),
            commit_entered: Notify::new(),
            commit_permits: Semaphore::new(0),
            pending_ddl_armed: AtomicBool::new(false),
            pending_ddl_entered: Notify::new(),
            pending_ddl_permits: Semaphore::new(0),
        }
    }

    fn arm(self: &Arc<Self>) -> CompactionPlanningGateGuard {
        self.armed.store(true, Ordering::Relaxed);
        CompactionPlanningGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    async fn wait_until_cancel_requested(&self) {
        self.cancel_requested.notified().await;
    }

    fn arm_merge(self: &Arc<Self>) -> CompactionMergeGateGuard {
        self.merge_armed.store(true, Ordering::Relaxed);
        CompactionMergeGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_merge_entered(&self) {
        self.merge_entered.notified().await;
    }

    fn arm_commit(self: &Arc<Self>) -> CompactionCommitGateGuard {
        self.commit_armed.store(true, Ordering::Relaxed);
        CompactionCommitGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_commit_entered(&self) {
        self.commit_entered.notified().await;
    }

    fn arm_pending_ddl_dispatch(self: &Arc<Self>) -> CompactionPendingDdlGateGuard {
        self.pending_ddl_armed.store(true, Ordering::Relaxed);
        CompactionPendingDdlGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_pending_ddl_dispatch(&self) {
        self.pending_ddl_entered.notified().await;
    }

    fn release(&self) {
        self.permits.add_permits(1);
    }

    fn release_merge(&self) {
        self.merge_permits.add_permits(1);
    }

    fn release_commit(&self) {
        self.commit_permits.add_permits(1);
    }

    fn release_pending_ddl_dispatch(&self) {
        self.pending_ddl_permits.add_permits(1);
    }
}

#[async_trait]
impl EventListener for CompactionPlanningGate {
    async fn on_compaction_pick_begin(&self, region_id: RegionId) {
        if region_id != self.region_id {
            return;
        }

        if !self.armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.entered.notify_one();
        self.permits.acquire().await.unwrap().forget();
    }

    async fn on_merge_ssts_finished(&self, region_id: RegionId) {
        if region_id != self.region_id || !self.merge_armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.merge_entered.notify_one();
        self.merge_permits.acquire().await.unwrap().forget();
    }

    async fn on_compaction_commit_begin(&self, region_id: RegionId) {
        if region_id != self.region_id || !self.commit_armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.commit_entered.notify_one();
        self.commit_permits.acquire().await.unwrap().forget();
    }

    async fn on_compaction_result_notified(&self, region_id: RegionId) {
        if region_id != self.region_id || !self.pending_ddl_armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.pending_ddl_entered.notify_one();
        self.pending_ddl_permits.acquire().await.unwrap().forget();
    }

    fn on_compaction_cancel_requested(&self, region_id: RegionId) {
        if region_id == self.region_id {
            self.cancel_requested.notify_one();
        }
    }
}

#[tokio::test]
async fn test_planning_followup_updates_schedule_time() {
    assert_automatic_followup_updates_schedule_time(0).await;
}

#[tokio::test]
async fn test_execution_followup_updates_schedule_time() {
    assert_automatic_followup_updates_schedule_time(4).await;
}

async fn assert_automatic_followup_updates_schedule_time(preexisting_flushes: usize) {
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let interval = Duration::from_secs(60 * 60);
    let initial_time = 1_000;
    let time_provider = Arc::new(MockTimeProvider::new(initial_time));
    let engine = env
        .create_engine_with_time(
            MitoConfig {
                min_compaction_interval: interval,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            time_provider.clone(),
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "automatic_followup_interval",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let column_schemas = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();

    for offset in 0..preexisting_flushes {
        put_and_flush(
            &engine,
            region_id,
            &column_schemas,
            offset * 10..offset * 10 + 10,
        )
        .await;
    }

    let first_schedule_time = initial_time + interval.as_millis() as i64;
    time_provider.set_now(first_schedule_time);
    let gate_guard = gate.arm();
    let first_start = preexisting_flushes * 10;
    put_and_flush(
        &engine,
        region_id,
        &column_schemas,
        first_start..first_start + 10,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("initial automatic planning did not reach the gate");

    let trigger_time = first_schedule_time + interval.as_millis() as i64;
    time_provider.set_now(trigger_time);
    put_and_flush(
        &engine,
        region_id,
        &column_schemas,
        first_start + 10..first_start + 20,
    )
    .await;
    let followup_schedule_time = trigger_time + 1;
    time_provider.set_now(followup_schedule_time);
    gate_guard.release();

    let region = engine.get_region(region_id).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while region.last_schedule_compaction_millis() != followup_schedule_time {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("automatic follow-up did not update its schedule time");
}

#[tokio::test]
async fn test_region_b_progresses_while_same_worker_region_a_is_picking() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(2, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_a));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                min_compaction_interval: Duration::ZERO,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;

    for (region_id, table_name) in [(region_a, "region_a"), (region_b, "region_b")] {
        env.get_schema_metadata_manager()
            .register_region_table_info(
                region_id.table_id(),
                table_name,
                "test_catalog",
                "test_schema",
                None,
                env.get_kv_backend(),
            )
            .await;
        engine
            .handle_request(
                region_id,
                RegionRequest::Create(
                    CreateRequestBuilder::new()
                        .insert_option("compaction.type", "twcs")
                        .build(),
                ),
            )
            .await
            .unwrap();
    }

    let request = CreateRequestBuilder::new().build();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    let gate_guard = gate.arm();
    let engine_for_compaction = engine.clone();
    let region_a_compaction = tokio::spawn(async move {
        engine_for_compaction
            .handle_request(
                region_a,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("region A planning did not reach the gate");

    let engine_for_region_b = engine.clone();
    let mut region_b_work = tokio::spawn(async move {
        put_and_flush(&engine_for_region_b, region_b, &column_schemas, 0..10).await;
    });
    tokio::time::timeout(Duration::from_secs(5), &mut region_b_work)
        .await
        .expect("region B was blocked by region A compaction planning")
        .expect("region B work task panicked");
    gate_guard.release();
    tokio::time::timeout(Duration::from_secs(5), region_a_compaction)
        .await
        .expect("region A compaction task did not finish after gate release")
        .expect("region A compaction task panicked")
        .expect("region A compaction failed");
}

#[tokio::test]
async fn test_picking_close_reopen_ignores_old_plan() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(3, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "close_reopen",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = create.table_dir.clone();
    let options = create.options.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("closed region compaction waiter was not released")
        .expect("closed region compaction task panicked")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    gate_guard.release();
    tokio::time::timeout(Duration::from_secs(5), compact(&engine, region_id))
        .await
        .expect("replacement compaction was blocked by the stale plan");
    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_enter_staging_waits_for_picking_logical_cancellation_ack() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(4, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "enter_staging",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .insert_option("compaction.type", "twcs")
                    .build(),
            ),
        )
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");
    let staging_engine = engine.clone();
    let staging_task = tokio::spawn(async move {
        staging_engine
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_cancel_requested())
        .await
        .expect("enter-staging did not request picking cancellation");
    assert!(!compact_task.is_finished());
    assert!(!staging_task.is_finished());

    gate_guard.release();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("cancelled compaction waiter was not released")
        .expect("cancelled compaction task panicked")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    tokio::time::timeout(Duration::from_secs(5), staging_task)
        .await
        .expect("enter-staging did not finish after cancellation acknowledgment")
        .expect("enter-staging task panicked")
        .expect("enter-staging request failed");
    assert!(engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_truncate_waits_for_non_cancellable_compaction_commit() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(10, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "truncate_during_compaction_commit",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build();
    let column_schemas = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 30..40).await;

    let commit_guard = gate.arm_commit();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_commit_entered())
        .await
        .expect("compaction did not reach the non-cancellable commit gate");

    let truncate_engine = engine.clone();
    let mut truncate_task = tokio::spawn(async move {
        truncate_engine
            .handle_request(
                region_id,
                RegionRequest::Truncate(RegionTruncateRequest::ByTimeRanges {
                    time_ranges: vec![(
                        Timestamp::new_millisecond(0),
                        Timestamp::new_millisecond(39_000),
                    )],
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            biased;
            result = &mut truncate_task => {
                panic!("truncate completed before compaction terminal completion: {result:?}");
            }
            () = gate.wait_until_cancel_requested() => {}
        }
    })
    .await
    .expect("truncate was not queued behind non-cancellable compaction");
    assert!(!truncate_task.is_finished());

    let pending_ddl_guard = gate.arm_pending_ddl_dispatch();
    commit_guard.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        gate.wait_until_pending_ddl_dispatch(),
    )
    .await
    .expect("compaction terminal result did not reach pending truncate dispatch");
    tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("compaction waiter was not released at terminal completion")
        .expect("compaction task panicked")
        .expect("compaction failed");
    assert!(!truncate_task.is_finished());

    pending_ddl_guard.release();
    tokio::time::timeout(Duration::from_secs(5), truncate_task)
        .await
        .expect("queued truncate did not finish after compaction completion")
        .expect("truncate task panicked")
        .expect("queued truncate failed");

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(
        collect_stream_ts(scanner.scan().await.unwrap())
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn test_worker_shutdown_fails_picking_waiter() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(5, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "worker_shutdown",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .insert_option("compaction.type", "twcs")
                    .build(),
            ),
        )
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");

    tokio::time::timeout(Duration::from_secs(5), engine.stop())
        .await
        .expect("worker shutdown blocked on picking")
        .unwrap();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("worker shutdown did not release the compaction waiter")
        .expect("compaction task panicked during worker shutdown")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    gate_guard.release();
}

#[tokio::test]
async fn test_compaction_region() {
    test_compaction_region_with_format(false).await;
    test_compaction_region_with_format(true).await;
}

async fn test_compaction_region_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 5 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 15..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 15..25).await;

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Input:
    // [0..9]
    //       [10...19]
    //                [20....29]
    //          -[15.........29]- (delete)
    //           [15.....24]
    // Count-first compaction consumes the first 4 SSTs as soon as the trigger is reached.
    // The compacted output and final flush leave 2 SSTs.
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..25).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_infer_compaction_time_window() {
    test_infer_compaction_time_window_with_format(false).await;
    test_infer_compaction_time_window_with_format(true).await;
}

async fn test_infer_compaction_time_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // time window should be absent
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window
            .is_none()
    );

    put_and_flush(&engine, region_id, &column_schemas, 1..2).await;
    put_and_flush(&engine, region_id, &column_schemas, 2..3).await;
    put_and_flush(&engine, region_id, &column_schemas, 3..4).await;
    put_and_flush(&engine, region_id, &column_schemas, 4..5).await;

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    assert_eq!(
        Duration::from_secs(3600),
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window
            .unwrap()
    );

    // write two rows to trigger another flush.
    // note: this two rows still use the original part_duration (1day by default), so they are written
    // to the same time partition and flushed to one file.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 3601, 3602, 0),
        },
    )
    .await;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 7201, 7202, 0),
        },
    )
    .await;
    // this flush should update part_duration in TimePartitions.
    flush(&engine, region_id).await;
    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    // These data should use new part_duration in TimePartitions and get written to two different
    // time partitions so we end up with 4 ssts.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 3601, 3602, 0),
        },
    )
    .await;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 7201, 7202, 0),
        },
    )
    .await;
    flush(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        4,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
}

#[tokio::test]
async fn test_compaction_overlapping_files() {
    test_compaction_overlapping_files_with_format(false).await;
    test_compaction_overlapping_files_with_format(true).await;
}

async fn test_compaction_overlapping_files_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 5 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    delete_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 30..40).await;

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!(
        vec,
        (0..=9)
            .map(|v| v * 1000)
            .chain((20..=29).map(|v| v * 1000))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_compaction_region_with_overlapping() {
    test_compaction_region_with_overlapping_with_format(false).await;
    test_compaction_region_with_overlapping_with_format(true).await;
}

async fn test_compaction_region_with_overlapping_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 4 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 0..2400).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 3600..10800).await; // window 10800
    delete_and_flush(&engine, region_id, &column_schemas, 0..3600).await; // window 3600

    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((3600..10800).map(|i| { i * 1000 }).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_compaction_region_with_overlapping_delete_all() {
    test_compaction_region_with_overlapping_delete_all_with_format(false).await;
    test_compaction_region_with_overlapping_delete_all_with_format(true).await;
}

async fn test_compaction_region_with_overlapping_delete_all_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 4 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..2400).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 2400..3600).await; // window 3600
    delete_and_flush(&engine, region_id, &column_schemas, 0..10800).await; // window 10800
    tokio::time::sleep(Duration::from_millis(2)).await;
    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert!(vec.is_empty());
}

#[tokio::test]
async fn test_compaction_input_limit_keeps_rows_deleted() {
    test_compaction_input_limit_keeps_rows_deleted_with_format(false).await;
    test_compaction_input_limit_keeps_rows_deleted_with_format(true).await;
}

/// Creates a region that only compacts when asked, so that a test can build an exact file
/// layout with `put_and_flush` and `delete_and_flush`.
async fn env_for_manual_compaction(
    env: &mut TestEnv,
    region_id: RegionId,
    flat_format: bool,
) -> (MitoEngine, Vec<ColumnSchema>) {
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await;

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    (engine, column_schemas)
}

/// The picker caps a compaction at 16 input files and drops the largest file groups to get
/// there. A deletion marker among the picked files must not be filtered out while the file
/// holding the rows it masks stays behind, otherwise those rows become visible again.
async fn test_compaction_input_limit_keeps_rows_deleted_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let (engine, column_schemas) =
        env_for_manual_compaction(&mut env, region_id, flat_format).await;

    // One large file spanning the whole time window.
    put_and_flush(&engine, region_id, &column_schemas, 0..3000).await;
    // Deletes 6 rows of that file. The markers land in a tiny file that overlaps it.
    delete_and_flush(&engine, region_id, &column_schemas, 10..16).await;
    // 15 more tiny files that overlap the large one but not each other, so the window holds
    // 17 file groups forming 2 runs.
    for i in 2..17 {
        put_and_flush(&engine, region_id, &column_schemas, i * 10..i * 10 + 6).await;
    }

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        17,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // The 16 tiny files are merged into one; the large file exceeds the input file num limit
    // and is left behind.
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert!(
        !(10..16).any(|ts| vec.contains(&(ts * 1000))),
        "deleted rows are visible again after compaction"
    );
    assert_eq!(2994, vec.len());
}

#[tokio::test]
async fn test_compaction_of_part_of_a_run_keeps_rows_deleted() {
    test_compaction_of_part_of_a_run_keeps_rows_deleted_with_format(false).await;
    test_compaction_of_part_of_a_run_keeps_rows_deleted_with_format(true).await;
}

/// A run is supposed to hold no overlapping files, which is what lets `merge_seq_files`
/// compact part of a run and still filter deleted rows. Run detection compares time ranges
/// exclusively though, so a file covering a single timestamp lands in the same run as the
/// file it deletes rows from. The rows must stay deleted when only one of the two is picked.
async fn test_compaction_of_part_of_a_run_keeps_rows_deleted_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let (engine, column_schemas) =
        env_for_manual_compaction(&mut env, region_id, flat_format).await;

    // One large file spanning the whole time window.
    put_and_flush(&engine, region_id, &column_schemas, 0..3000).await;
    // Deletes a single row of that file, so the marker lands in a file covering one timestamp.
    delete_and_flush(&engine, region_id, &column_schemas, 1..2).await;
    // 31 more single row files, each holding another key at its own timestamp.
    for ts in 1..32 {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("b", ts * 10, ts * 10 + 1, 0),
        };
        put_rows(&engine, region_id, rows).await;
        flush(&engine, region_id).await;
    }

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert!(
        !vec.contains(&1000),
        "deleted row is visible again after compaction"
    );
    assert_eq!(3030, vec.len());
}

// For issue https://github.com/GreptimeTeam/greptimedb/issues/3633
#[tokio::test]
async fn test_readonly_during_compaction() {
    test_readonly_during_compaction_with_format(false).await;
    test_readonly_during_compaction_with_format(true).await;
}

async fn test_readonly_during_compaction_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CompactionListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                // Ensure there is only one background worker for purge task.
                max_background_purges: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let listener_guard = CompactionListenerGuard::new(listener.clone());
    // Flush 4 balanced SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    // Waits until the engine receives compaction finished request.
    listener.wait_handle_finished().await;

    // Converts region to follower.
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    // Wakes up the listener.
    listener_guard.release();

    let notify = Arc::new(Notify::new());
    // We already sets max background purges to 1, so we can submit a task to the
    // purge scheduler to ensure all purge tasks are finished.
    let job_notify = notify.clone();
    engine
        .purge_scheduler()
        .schedule(Box::pin(async move {
            job_notify.notify_one();
        }))
        .unwrap();
    notify.notified().await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        4,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..20).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_local_compaction_cancellation_notifies_before_pending_ddl_dispatch() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(2049, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                max_background_purges: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let merge_guard = gate.arm_merge();
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_merge_entered())
        .await
        .expect("local compaction did not reach its cancellable merge gate");

    let pending_ddl_guard = gate.arm_pending_ddl_dispatch();
    let staging_engine = engine.clone();
    let mut staging_task = tokio::spawn(async move {
        staging_engine
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_cancel_requested())
        .await
        .expect("enter-staging did not request local compaction cancellation");
    assert!(!staging_task.is_finished());

    merge_guard.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        gate.wait_until_pending_ddl_dispatch(),
    )
    .await
    .expect("cancelled compaction did not notify before pending DDL dispatch");
    assert!(!staging_task.is_finished());

    pending_ddl_guard.release();
    tokio::time::timeout(Duration::from_secs(5), &mut staging_task)
        .await
        .expect("enter-staging did not finish after cancellation notification")
        .expect("enter-staging task panicked")
        .expect("enter-staging request failed");
    assert!(engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_compaction_update_time_window() {
    test_compaction_update_time_window_with_format(false).await;
    test_compaction_update_time_window_with_format(true).await;
}

async fn test_compaction_update_time_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 3 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..900).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 900..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2700).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 2700..3600).await; // window 3600

    compact(&engine, region_id).await;
    assert_eq!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window,
        Some(Duration::from_secs(3600))
    );
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(0, scanner.num_memtables());
    // We keep all 3 files because no enough file to merge
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    // Flush a new SST and the time window is applied.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600

    // Puts window 7200.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 3600, 4000, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(1, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..4000).map(|v| v * 1000).collect::<Vec<_>>(), vec);

    // Puts window 3600.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 2400, 3600, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(2, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..4000).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_change_region_compaction_window() {
    test_change_region_compaction_window_with_format(false).await;
    test_change_region_compaction_window_with_format(true).await;
}

async fn test_change_region_compaction_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = request.table_dir.clone();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 2 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..600).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 600..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2400).await; // window 3600

    compact(&engine, region_id).await;

    // Put window 7200
    put_and_flush(&engine, region_id, &column_schemas, 4000..5000).await;

    // Check compaction window.
    let region = engine.get_region(region_id).unwrap();
    {
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(3600)),
            version.compaction_time_window,
        );
        assert!(version.options.compaction.time_window().is_none());
    }

    // Change compaction window.
    let request = RegionRequest::Alter(RegionAlterRequest {
        kind: SetRegionOptions {
            options: vec![SetRegionOption::Twsc(
                "compaction.twcs.time_window".to_string(),
                "2h".to_string(),
            )],
        },
    });
    engine.handle_request(region_id, request).await.unwrap();
    assert_eq!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .options
            .compaction
            .time_window(),
        Some(Duration::from_secs(7200))
    );

    put_and_flush(&engine, region_id, &column_schemas, 5000..5100).await;
    put_and_flush(&engine, region_id, &column_schemas, 5100..5200).await;
    put_and_flush(&engine, region_id, &column_schemas, 5200..5300).await;

    // Compaction again. It should compacts window 3600 and 7200
    // into 7200.
    compact(&engine, region_id).await;
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.options.compaction.time_window()
        );
    }

    // Reopen region.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: Default::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        // We open the region without options, so the time window should be None.
        assert!(version.options.compaction.time_window().is_none());
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
    }
}

#[tokio::test]
async fn test_open_overwrite_compaction_window() {
    test_open_overwrite_compaction_window_with_format(false).await;
    test_open_overwrite_compaction_window_with_format(true).await;
}

async fn test_open_overwrite_compaction_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = request.table_dir.clone();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 2 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..600).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 600..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2400).await; // window 3600

    compact(&engine, region_id).await;

    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(3600)),
            version.compaction_time_window,
        );
        assert!(version.options.compaction.time_window().is_none());
    }

    // Reopen region.
    let options = HashMap::from([
        ("compaction.type".to_string(), "twcs".to_string()),
        ("compaction.twcs.time_window".to_string(), "2h".to_string()),
    ]);
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.options.compaction.time_window()
        );
    }
}
