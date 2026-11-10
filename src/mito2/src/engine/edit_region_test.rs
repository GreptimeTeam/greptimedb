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

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::{ArrowIpc, Rows};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::DfRecordBatch;
use common_test_util::flight::encode_to_flight_data;
use common_time::Timestamp;
use common_time::util::current_time_millis;
use datatypes::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::ObjectStore;
use store_api::region_engine::{RegionEngine, RegionManifestInfo, RegionRole};
use store_api::region_request::{
    RegionBulkInsertsRequest, RegionCloseRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{FileId, RegionId};
use tokio::sync::{Barrier, mpsc, oneshot};

use crate::cache::file_cache::{FileType, IndexKey};
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::EventListener;
use crate::manifest::action::RegionEdit;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{RegionEditRequest, WorkerRequest};
use crate::sst::file::{ColumnIndexMetadata, FileMeta, IndexType};
use crate::sst::location;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, rows_schema};
use crate::time_provider::mock::MockTimeProvider;

#[tokio::test]
async fn test_edit_region_schedule_compaction() {
    for flat_format in [true, false] {
        for schedule_compaction_after_edit in [true, false] {
            test_edit_region_schedule_compaction_with_format(
                flat_format,
                schedule_compaction_after_edit,
            )
            .await;
        }
    }
}

async fn test_edit_region_schedule_compaction_with_format(
    flat_format: bool,
    schedule_compaction_after_edit: bool,
) {
    let mut env = TestEnv::new().await;

    struct EditRegionListener {
        tx: Mutex<Option<oneshot::Sender<RegionId>>>,
    }

    impl EventListener for EditRegionListener {
        fn on_compaction_scheduled(&self, region_id: RegionId) {
            let mut tx = self.tx.lock().unwrap();
            tx.take().unwrap().send(region_id).unwrap();
        }
    }

    let (tx, mut rx) = oneshot::channel();
    let config = MitoConfig {
        min_compaction_interval: Duration::from_secs(60 * 60),
        schedule_compaction_after_edit,
        default_flat_format: flat_format,
        ..Default::default()
    };
    let initial_time = current_time_millis();
    let time_provider = Arc::new(MockTimeProvider::new(initial_time));
    let engine = env
        .create_engine_with_time(
            config.clone(),
            None,
            Some(Arc::new(EditRegionListener {
                tx: Mutex::new(Some(tx)),
            })),
            time_provider.clone(),
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
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let initial_schedule_time = region.last_schedule_compaction_millis();
    assert_eq!(initial_time, initial_schedule_time);

    let new_edit = |file_starts: &[i64]| RegionEdit {
        files_to_add: file_starts
            .iter()
            .map(|start| FileMeta {
                region_id: region.region_id,
                file_id: FileId::random(),
                time_range: (
                    Timestamp::new_millisecond(*start),
                    Timestamp::new_millisecond(1000 * 1000),
                ),
                ..Default::default()
            })
            .collect(),
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
        wal_recovery: Default::default(),
    };
    engine
        .edit_region(region.region_id, new_edit(&[0, 10, 50, 80]))
        .await
        .unwrap();
    // Asserts that the compaction of the region is not scheduled,
    // because the minimum time interval between two compactions is not passed.
    assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    assert_eq!(
        initial_schedule_time,
        region.last_schedule_compaction_millis()
    );

    // Simulates the time has passed the min compaction interval,
    let next_schedule_time = initial_time + config.min_compaction_interval.as_millis() as i64;
    time_provider.set_now(next_schedule_time);
    // ... then edits the region again,
    engine
        .edit_region(region.region_id, new_edit(&[90]))
        .await
        .unwrap();

    // ... finally asserts that the compaction of the region is scheduled, or not: controlled by the
    // "schedule_compaction_after_edit" config.
    let recv_or_timeout = tokio::time::timeout(Duration::from_secs(2), rx).await;
    if schedule_compaction_after_edit {
        let actual = recv_or_timeout.unwrap().unwrap();
        assert_eq!(region_id, actual);
        // Wait for the `last_schedule_compaction_millis` to update.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(next_schedule_time, region.last_schedule_compaction_millis());
    } else {
        assert!(recv_or_timeout.is_err());
    }
}

#[rstest::rstest]
#[case::without_index(None)]
#[case::known_index_size(Some(7))]
#[case::unknown_index_size(Some(0))]
#[tokio::test]
async fn test_edit_region_fill_cache(
    #[values(false, true)] flat_format: bool,
    #[case] index_file_size: Option<u64>,
) {
    let mut env = TestEnv::new().await;

    struct EditRegionListener {
        tx: Mutex<Option<oneshot::Sender<FileId>>>,
    }

    impl EventListener for EditRegionListener {
        fn on_file_cache_filled(&self, file_id: FileId) {
            let mut tx = self.tx.lock().unwrap();
            tx.take().unwrap().send(file_id).unwrap();
        }
    }

    let (tx, rx) = oneshot::channel();
    let engine = env
        .create_engine_with(
            MitoConfig {
                // Write cache must be enabled to download the ingested SST file.
                enable_write_cache: true,
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(Arc::new(EditRegionListener {
                tx: Mutex::new(Some(tx)),
            })),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    let file_id = FileId::random();
    // Simulating the ingestion of an SST file.
    env.get_object_store()
        .unwrap()
        .write(
            &format!("{}/{}.parquet", region.table_dir(), file_id),
            b"x".as_slice(),
        )
        .await
        .unwrap();

    let mut meta = FileMeta {
        region_id,
        file_id,
        index_version: 3,
        ..Default::default()
    };
    if let Some(size) = index_file_size {
        meta.index_file_size = size;
        meta.indexes.push(ColumnIndexMetadata {
            column_id: 0,
            created_indexes: smallvec::smallvec![IndexType::HllIndex],
            ..Default::default()
        });
    }
    let index_path = location::index_file_path(
        region.table_dir(),
        meta.index_id(),
        region.access_layer.path_type(),
    );
    region
        .access_layer
        .object_store()
        .write(&index_path, b"payload".as_slice())
        .await
        .unwrap();
    let edit = RegionEdit {
        files_to_add: vec![meta],
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
        wal_recovery: Default::default(),
    };
    engine.edit_region(region.region_id, edit).await.unwrap();

    // Asserts that the background downloading of the SST is succeeded.
    let actual = tokio::time::timeout(Duration::from_secs(9), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file_id, actual);
    // The listener runs after both download attempts, including the skipped-index path.
    let cache_manager = engine.cache_manager();
    let file_cache = cache_manager.write_cache().unwrap().file_cache();
    let index_key = IndexKey::new(region_id, file_id, FileType::Puffin(3));
    let expected_cached = index_file_size.is_some_and(|size| size > 0);
    assert_eq!(file_cache.contains_key(&index_key), expected_cached);
    let path = file_cache.cache_file_path(index_key);
    if expected_cached {
        assert_eq!(
            file_cache.local_store().read(&path).await.unwrap().to_vec(),
            b"payload"
        );
    } else {
        assert!(!file_cache.local_store().exists(&path).await.unwrap());
    }
}

#[tokio::test]
async fn test_write_during_region_editing_is_queued() {
    let mut env = TestEnv::new().await;
    let (engine, mut request_rx) = create_engine_with_request_listener(&mut env).await;

    let region_id = RegionId::new(1, 1);
    let create_request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&create_request);
    engine
        .handle_request(region_id, RegionRequest::Create(create_request))
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    let manifest_guard = region.manifest_ctx.manifest_manager.write().await;
    let edit = test_region_edit(region.region_id, FileId::random());

    let edit_engine = engine.clone();
    let edit_task = tokio::spawn(async move { edit_engine.edit_region(region_id, edit).await });

    wait_until_region_is_in_editing(&region).await;

    drain_worker_recv_events(&mut request_rx);

    let write_engine = engine.clone();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 1),
    };
    let write_task = tokio::spawn(async move {
        write_engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    skip_wal: false,
                    rows,
                    hint: None,
                    partition_expr_version: None,
                }),
            )
            .await
    });
    wait_worker_recv_event(&mut request_rx).await;

    let second_file_id = FileId::random();
    let second_edit = test_region_edit(region.region_id, second_file_id);
    let second_edit_engine = engine.clone();
    let second_edit_task =
        tokio::spawn(async move { second_edit_engine.edit_region(region_id, second_edit).await });
    wait_worker_recv_event(&mut request_rx).await;

    drop(manifest_guard);
    edit_task.await.unwrap().unwrap();
    let output = write_task.await.unwrap().unwrap();
    assert_eq!(1, output.affected_rows);
    second_edit_task.await.unwrap().unwrap();

    let second_file_sequence = region_file_sequence(&region, second_file_id);
    assert_eq!(Some(3), second_file_sequence);
}

#[tokio::test]
async fn test_bulk_insert_during_region_editing_is_queued() {
    let mut env = TestEnv::new().await;
    let (engine, mut request_rx) = create_engine_with_request_listener(&mut env).await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    let manifest_guard = region.manifest_ctx.manifest_manager.write().await;
    let edit = test_region_edit(region.region_id, FileId::random());

    let edit_engine = engine.clone();
    let edit_task = tokio::spawn(async move { edit_engine.edit_region(region_id, edit).await });

    wait_until_region_is_in_editing(&region).await;

    drain_worker_recv_events(&mut request_rx);

    let bulk_engine = engine.clone();
    let bulk_task = tokio::spawn(async move {
        bulk_engine
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 0, 1)),
            )
            .await
    });

    wait_worker_recv_event(&mut request_rx).await;

    let second_file_id = FileId::random();
    let second_edit = test_region_edit(region.region_id, second_file_id);
    let second_edit_engine = engine.clone();
    let second_edit_task =
        tokio::spawn(async move { second_edit_engine.edit_region(region_id, second_edit).await });
    wait_worker_recv_event(&mut request_rx).await;

    drop(manifest_guard);
    edit_task.await.unwrap().unwrap();
    let output = bulk_task.await.unwrap().unwrap();
    assert_eq!(1, output.affected_rows);
    second_edit_task.await.unwrap().unwrap();

    let second_file_sequence = region_file_sequence(&region, second_file_id);
    assert_eq!(Some(3), second_file_sequence);
}

#[tokio::test]
async fn test_stalled_write_fails_fast_if_region_closed_during_editing() {
    let mut env = TestEnv::new().await;
    let (engine, mut request_rx) = create_engine_with_request_listener(&mut env).await;

    let region_id = RegionId::new(1, 1);
    let create_request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&create_request);
    engine
        .handle_request(region_id, RegionRequest::Create(create_request))
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    let manifest_guard = region.manifest_ctx.manifest_manager.write().await;
    let edit = test_region_edit(region.region_id, FileId::random());

    let edit_engine = engine.clone();
    let edit_task = tokio::spawn(async move { edit_engine.edit_region(region_id, edit).await });

    wait_until_region_is_in_editing(&region).await;

    drain_worker_recv_events(&mut request_rx);

    let write_engine = engine.clone();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 1),
    };
    let write_task = tokio::spawn(async move {
        write_engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    skip_wal: false,
                    rows,
                    hint: None,
                    partition_expr_version: None,
                }),
            )
            .await
    });

    wait_worker_recv_event(&mut request_rx).await;

    let second_edit_engine = engine.clone();
    let second_edit = test_region_edit(region.region_id, FileId::random());
    let second_edit_task =
        tokio::spawn(async move { second_edit_engine.edit_region(region_id, second_edit).await });
    wait_worker_recv_event(&mut request_rx).await;

    let close_engine = engine.clone();
    let close_task = tokio::spawn(async move {
        close_engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
    });

    wait_worker_recv_event(&mut request_rx).await;

    drop(manifest_guard);
    close_task.await.unwrap().unwrap();

    let write_result = tokio::time::timeout(Duration::from_secs(3), write_task)
        .await
        .expect("stalled write should fail after region is closed")
        .unwrap();
    assert_eq!(
        StatusCode::RegionNotFound,
        write_result.unwrap_err().status_code()
    );
    assert_eq!(
        StatusCode::RegionNotFound,
        second_edit_task.await.unwrap().unwrap_err().status_code()
    );
    assert!(edit_task.await.unwrap().is_err());
}

#[tokio::test]
async fn test_raw_add_only_region_edits_are_merged() {
    let mut env = TestEnv::new().await;
    let (engine, mut request_rx) = create_engine_with_request_listener(&mut env).await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    // Hold the manifest lock so the first edit keeps the region in `Editing`.
    // The following two edits will be queued in `RegionEditQueue`.
    let manifest_guard = region.manifest_ctx.manifest_manager.write().await;

    let first_file_id = FileId::random();
    let first_edit = test_region_edit(region.region_id, first_file_id);
    let first_edit_engine = engine.clone();
    let first_edit_task =
        tokio::spawn(async move { first_edit_engine.edit_region(region_id, first_edit).await });

    wait_until_region_is_in_editing(&region).await;

    drain_worker_recv_events(&mut request_rx);

    // Both queued edits are raw add-only edits, so they should be merged into
    // a single edit and share the same committed sequence.
    let second_file_id = FileId::random();
    let second_edit = test_region_edit(region.region_id, second_file_id);
    let second_edit_engine = engine.clone();
    let second_edit_task =
        tokio::spawn(async move { second_edit_engine.edit_region(region_id, second_edit).await });
    wait_worker_recv_event(&mut request_rx).await;

    let third_file_id = FileId::random();
    let third_edit = test_region_edit(region.region_id, third_file_id);
    let third_edit_engine = engine.clone();
    let third_edit_task =
        tokio::spawn(async move { third_edit_engine.edit_region(region_id, third_edit).await });
    wait_worker_recv_event(&mut request_rx).await;

    drop(manifest_guard);

    first_edit_task.await.unwrap().unwrap();
    second_edit_task.await.unwrap().unwrap();
    third_edit_task.await.unwrap().unwrap();

    // The first edit gets sequence 1. The merged queued edit gets sequence 2,
    // so both files from the second and third requests share sequence 2.
    assert_eq!(Some(1), region_file_sequence(&region, first_file_id));
    assert_eq!(Some(2), region_file_sequence(&region, second_file_id));
    assert_eq!(Some(2), region_file_sequence(&region, third_file_id));
}

#[tokio::test]
async fn test_region_edit_with_file_sequence_is_not_merged() {
    let mut env = TestEnv::new().await;
    let (engine, mut request_rx) = create_engine_with_request_listener(&mut env).await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    // Hold the manifest lock so the first edit keeps the region in `Editing`.
    // The following two edits will be queued in `RegionEditQueue`.
    let manifest_guard = region.manifest_ctx.manifest_manager.write().await;

    let first_file_id = FileId::random();
    let first_edit = test_region_edit(region.region_id, first_file_id);
    let first_edit_engine = engine.clone();
    let first_edit_task =
        tokio::spawn(async move { first_edit_engine.edit_region(region_id, first_edit).await });

    wait_until_region_is_in_editing(&region).await;

    drain_worker_recv_events(&mut request_rx);

    let second_file_id = FileId::random();
    let mut second_edit = test_region_edit(region.region_id, second_file_id);
    // A file that already carries a sequence is not a raw add-only edit and
    // must not be merged with the next queued edit.
    second_edit.files_to_add[0].sequence = NonZeroU64::new(99);
    let second_edit_engine = engine.clone();
    let second_edit_task =
        tokio::spawn(async move { second_edit_engine.edit_region(region_id, second_edit).await });
    wait_worker_recv_event(&mut request_rx).await;

    let third_file_id = FileId::random();
    let third_edit = test_region_edit(region.region_id, third_file_id);
    let third_edit_engine = engine.clone();
    let third_edit_task =
        tokio::spawn(async move { third_edit_engine.edit_region(region_id, third_edit).await });
    wait_worker_recv_event(&mut request_rx).await;

    drop(manifest_guard);

    first_edit_task.await.unwrap().unwrap();
    second_edit_task.await.unwrap().unwrap();
    third_edit_task.await.unwrap().unwrap();

    // Because the second edit is not mergeable, the third edit runs separately
    // and receives the next committed sequence.
    assert_eq!(Some(1), region_file_sequence(&region, first_file_id));
    assert_eq!(Some(2), region_file_sequence(&region, second_file_id));
    assert_eq!(Some(3), region_file_sequence(&region, third_file_id));
}

#[tokio::test]
async fn test_failed_region_edit_preserves_sequence() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    region.version_control.update_last_sequence(2);
    let manifest_guard = region.manifest_ctx.manifest_manager.write().await;
    let (tx, rx) = oneshot::channel();
    engine
        .inner
        .workers
        .submit_to_worker(
            region_id,
            WorkerRequest::EditRegion(RegionEditRequest::new(
                region_id,
                test_region_edit(region_id, FileId::random()),
                false,
                tx,
            )),
        )
        .await
        .unwrap();
    // Sync fails before taking the manifest lock, after the edit starts.
    assert!(
        engine
            .sync_region(region_id, RegionManifestInfo::mito(0, 0, 0).into())
            .await
            .is_err()
    );
    assert_eq!(
        region.state(),
        RegionRoleState::Leader(RegionLeaderState::Editing)
    );
    assert_eq!(region.version_control.current().last_sequence, 3);
    assert_eq!(region.version_control.committed_sequence(), 0);
    region.set_role(RegionRole::Follower);
    drop(manifest_guard);
    assert!(rx.await.unwrap().is_err());
    assert_eq!(region.version_control.current().last_sequence, 3);
    assert_eq!(region.version_control.committed_sequence(), 0);

    region.set_role(RegionRole::Leader);
    let file_id = FileId::random();
    engine
        .edit_region(region_id, test_region_edit(region_id, file_id))
        .await
        .unwrap();
    assert_eq!(region_file_sequence(&region, file_id), Some(4));
    let current = region.version_control.current();
    assert_eq!((current.last_sequence, current.committed_sequence), (4, 4));
}

#[tokio::test]
async fn test_region_edit_clears_preserve_row_sequence() {
    let mut env = TestEnv::new().await;
    let (engine, _) = create_engine_with_request_listener(&mut env).await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    let file_id = FileId::random();
    let mut edit = test_region_edit(region.region_id, file_id);
    // The caller claims the file preserves per-row sequences...
    edit.files_to_add[0].preserve_row_sequence = true;

    engine.edit_region(region.region_id, edit).await.unwrap();

    // ...but a generic region edit assigns a new destination sequence domain
    // without proving or rewriting the physical per-row sequence column, so the
    // marker must be cleared while the assigned sequence stays committed + 1.
    let version = region.version();
    let file = version.ssts.levels()[0]
        .files
        .iter()
        .find(|(id, _)| **id == file_id)
        .unwrap()
        .1;
    assert!(!file.meta_ref().preserve_row_sequence);
    assert_eq!(Some(1), file.meta_ref().sequence.map(|s| s.get()));
}

async fn wait_until_region_is_in_editing(region: &MitoRegionRef) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while region.state() != RegionRoleState::Leader(RegionLeaderState::Editing) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

fn region_file_sequence(region: &MitoRegionRef, file_id: FileId) -> Option<u64> {
    region.version().ssts.levels()[0]
        .files
        .iter()
        .find(|(id, _)| **id == file_id)
        .and_then(|(_, file)| file.meta_ref().sequence)
        .map(|sequence| sequence.get())
}

struct RecvRequestListener {
    tx: mpsc::UnboundedSender<usize>,
}

impl EventListener for RecvRequestListener {
    fn on_recv_requests(&self, request_num: usize) {
        let _ = self.tx.send(request_num);
    }
}

async fn create_engine_with_request_listener(
    env: &mut TestEnv,
) -> (MitoEngine, mpsc::UnboundedReceiver<usize>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            None,
            Some(Arc::new(RecvRequestListener { tx })),
            None,
        )
        .await;
    (engine, rx)
}

fn drain_worker_recv_events(rx: &mut mpsc::UnboundedReceiver<usize>) {
    while rx.try_recv().is_ok() {}
}

async fn wait_worker_recv_event(rx: &mut mpsc::UnboundedReceiver<usize>) {
    tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
}

fn test_region_edit(region_id: RegionId, file_id: FileId) -> RegionEdit {
    RegionEdit {
        files_to_add: vec![FileMeta {
            region_id,
            file_id,
            level: 0,
            ..Default::default()
        }],
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
        wal_recovery: Default::default(),
    }
}

fn build_bulk_insert_request(
    region_id: RegionId,
    start: usize,
    end: usize,
) -> RegionBulkInsertsRequest {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_0", DataType::Utf8, true),
        Field::new("field_0", DataType::Float64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let tag = Arc::new(StringArray::from_iter_values(
        (start..end).map(|value| value.to_string()),
    )) as ArrayRef;
    let field = Arc::new(Float64Array::from_iter_values(
        (start..end).map(|value| value as f64),
    )) as ArrayRef;
    let ts = Arc::new(TimestampMillisecondArray::from_iter_values(
        (start..end).map(|value| value as i64 * 1000),
    )) as ArrayRef;
    let payload = DfRecordBatch::try_new(schema, vec![tag, field, ts]).unwrap();
    let (schema, record_batch) = encode_to_flight_data(payload.clone());

    RegionBulkInsertsRequest {
        skip_wal: false,
        region_id,
        payload,
        raw_data: ArrowIpc {
            schema: schema.data_header,
            data_header: record_batch.data_header,
            payload: record_batch.data_body,
        },
        partition_expr_version: None,
        aligned_schema_version: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_edit_region_concurrently() {
    test_edit_region_concurrently_with_format(false).await;
    test_edit_region_concurrently_with_format(true).await;
}

async fn test_edit_region_concurrently_with_format(flat_format: bool) {
    const EDITS_PER_TASK: usize = 10;
    let tasks_count = 10;

    // A task that creates SST files and edits the region with them.
    struct Task {
        region: MitoRegionRef,
        ssts: Vec<FileMeta>,
    }

    impl Task {
        async fn create_ssts(&mut self, object_store: &ObjectStore) {
            for _ in 0..EDITS_PER_TASK {
                let file = FileMeta {
                    region_id: self.region.region_id,
                    file_id: FileId::random(),
                    level: 0,
                    ..Default::default()
                };
                object_store
                    .write(
                        &format!("{}/{}.parquet", self.region.table_dir(), file.file_id),
                        b"x".as_slice(),
                    )
                    .await
                    .unwrap();
                self.ssts.push(file);
            }
        }

        async fn edit_region(self, engine: MitoEngine) {
            for sst in self.ssts {
                let edit = RegionEdit {
                    files_to_add: vec![sst],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                    wal_recovery: Default::default(),
                };
                engine
                    .edit_region(self.region.region_id, edit)
                    .await
                    .unwrap();
            }
        }
    }

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            // Suppress the compaction to not impede the speed of this kinda stress testing.
            min_compaction_interval: Duration::from_secs(60 * 60),
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();

    let mut tasks = Vec::with_capacity(tasks_count);
    let object_store = env.get_object_store().unwrap();
    for _ in 0..tasks_count {
        let mut task = Task {
            region: region.clone(),
            ssts: Vec::new(),
        };
        task.create_ssts(&object_store).await;
        tasks.push(task);
    }

    let mut futures = Vec::with_capacity(tasks_count);
    let barrier = Arc::new(Barrier::new(tasks_count));
    for task in tasks {
        futures.push(tokio::spawn({
            let barrier = barrier.clone();
            let engine = engine.clone();
            async move {
                barrier.wait().await;
                task.edit_region(engine).await;
            }
        }));
    }
    futures::future::join_all(futures).await;

    assert_eq!(
        region.version().ssts.levels()[0].files.len(),
        tasks_count * EDITS_PER_TASK
    );
}

#[tokio::test]
async fn test_alter_precedes_edit_backlog() {
    use store_api::region_request::{AddColumn, AlterKind, RegionAlterRequest};
    use tokio::sync::Notify;
    struct Listener {
        tx: mpsc::UnboundedSender<usize>,
        altering: Notify,
        release: Notify,
        flushing: Notify,
        release_flush: Notify,
    }
    #[async_trait::async_trait]
    impl EventListener for Listener {
        fn on_recv_requests(&self, count: usize) {
            let _ = self.tx.send(count);
        }
        async fn on_flush_begin(&self, _: RegionId) {
            self.flushing.notify_one();
            self.release_flush.notified().await;
        }
        async fn on_notify_region_change_result_begin(&self, _: RegionId) {
            self.altering.notify_one();
            self.release.notified().await;
        }
    }
    for (dirty, invalid) in [(false, false), (true, false), (false, true)] {
        let mut env = TestEnv::new().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let listener = Arc::new(Listener {
            tx,
            altering: Notify::new(),
            release: Notify::new(),
            flushing: Notify::new(),
            release_flush: Notify::new(),
        });
        let engine = env
            .create_engine_with(
                MitoConfig {
                    schedule_compaction_after_edit: false,
                    ..Default::default()
                },
                None,
                Some(listener.clone()),
                None,
            )
            .await;
        let region_id = RegionId::new(1, 1);
        let create = CreateRequestBuilder::new().build();
        let schema = rows_schema(&create);
        engine
            .handle_request(region_id, RegionRequest::Create(create))
            .await
            .unwrap();
        if dirty {
            crate::test_util::put_rows(
                &engine,
                region_id,
                Rows {
                    schema,
                    rows: build_rows(0, 1),
                },
            )
            .await;
        }
        let region = engine.get_region(region_id).unwrap();
        let guard = region.manifest_ctx.manifest_manager.write().await;
        let e = engine.clone();
        let first = tokio::spawn(async move {
            e.edit_region(region_id, test_region_edit(region_id, FileId::random()))
                .await
        });
        wait_until_region_is_in_editing(&region).await;
        drain_worker_recv_events(&mut rx);
        let file_id = FileId::random();
        let e = engine.clone();
        let queued = tokio::spawn(async move {
            e.edit_region(region_id, test_region_edit(region_id, file_id))
                .await
        });
        wait_worker_recv_event(&mut rx).await;
        let e = engine.clone();
        let alter = tokio::spawn(async move {
            e.handle_request(
                region_id,
                RegionRequest::Alter(RegionAlterRequest {
                    kind: AlterKind::AddColumns {
                        columns: vec![AddColumn {
                            column_metadata: store_api::metadata::ColumnMetadata {
                                column_schema: datatypes::schema::ColumnSchema::new(
                                    if invalid { "ts" } else { "new_field" },
                                    datatypes::data_type::ConcreteDataType::float64_datatype(),
                                    true,
                                ),
                                semantic_type: api::v1::SemanticType::Field,
                                column_id: 100,
                            },
                            location: None,
                        }],
                    },
                }),
            )
            .await
        });
        wait_worker_recv_event(&mut rx).await;
        drop(guard);
        let late = if dirty {
            tokio::time::timeout(Duration::from_secs(10), listener.flushing.notified())
                .await
                .unwrap();
            drain_worker_recv_events(&mut rx);
            let e = engine.clone();
            let task = tokio::spawn(async move {
                e.edit_region(region_id, test_region_edit(region_id, FileId::random()))
                    .await
            });
            wait_worker_recv_event(&mut rx).await;
            listener.release_flush.notify_one();
            Some(task)
        } else {
            None
        };
        if invalid {
            assert!(alter.await.unwrap().is_err());
            first.await.unwrap().unwrap();
            tokio::time::timeout(Duration::from_secs(10), queued)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            continue;
        }
        tokio::time::timeout(Duration::from_secs(10), listener.altering.notified())
            .await
            .unwrap();
        assert_eq!(None, region_file_sequence(&region, file_id));
        assert!(!queued.is_finished());
        assert!(late.as_ref().is_none_or(|task| !task.is_finished()));
        // Queue another ALTER while the first is waiting to notify the worker.
        drain_worker_recv_events(&mut rx);
        let e = engine.clone();
        let second_alter = tokio::spawn(async move {
            e.handle_request(
                region_id,
                RegionRequest::Alter(RegionAlterRequest {
                    kind: AlterKind::AddColumns {
                        columns: vec![AddColumn {
                            column_metadata: store_api::metadata::ColumnMetadata {
                                column_schema: datatypes::schema::ColumnSchema::new(
                                    "new_field_2",
                                    datatypes::data_type::ConcreteDataType::float64_datatype(),
                                    true,
                                ),
                                semantic_type: api::v1::SemanticType::Field,
                                column_id: 101,
                            },
                            location: None,
                        }],
                    },
                }),
            )
            .await
        });
        wait_worker_recv_event(&mut rx).await;
        listener.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), alter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), listener.altering.notified())
            .await
            .unwrap();
        // Completing the first ALTER must hand off to the next ALTER before edits.
        assert_eq!(None, region_file_sequence(&region, file_id));
        assert!(!queued.is_finished());
        assert!(late.as_ref().is_none_or(|task| !task.is_finished()));
        listener.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), second_alter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let metadata = region.metadata();
        assert_eq!(metadata.column_by_name("new_field").unwrap().column_id, 100);
        assert_eq!(
            metadata.column_by_name("new_field_2").unwrap().column_id,
            101
        );
        first.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(10), queued)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(region_file_sequence(&region, file_id).is_some());
        if let Some(late) = late {
            tokio::time::timeout(Duration::from_secs(10), late)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn test_drop_rejects_alter_and_edit_queues_after_flush_commit() {
    use store_api::region_request::{
        AddColumn, AlterKind, RegionAlterRequest, RegionDropRequest, RegionFlushRequest,
    };
    use tokio::sync::Notify;

    struct Listener {
        tx: mpsc::UnboundedSender<usize>,
        committing: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl EventListener for Listener {
        fn on_recv_requests(&self, count: usize) {
            let _ = self.tx.send(count);
        }

        async fn on_flush_commit_begin(&self, _: RegionId) {
            self.committing.notify_one();
            self.release.notified().await;
        }
    }

    let mut env = TestEnv::new().await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let listener = Arc::new(Listener {
        tx,
        committing: Notify::new(),
        release: Notify::new(),
    });
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1, 1);
    let create = CreateRequestBuilder::new().build();
    let schema = rows_schema(&create);
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    crate::test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: build_rows(0, 1),
        },
    )
    .await;

    let e = engine.clone();
    let flush = tokio::spawn(async move {
        e.handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), listener.committing.notified())
        .await
        .unwrap();
    drain_worker_recv_events(&mut rx);

    // DROP can no longer cancel the flush, so it waits for successful completion.
    let e = engine.clone();
    let drop = tokio::spawn(async move {
        e.handle_request(
            region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
    });
    wait_worker_recv_event(&mut rx).await;

    // The first ALTER waits in the flush scheduler; the second uses pending_alters.
    let mut alters = Vec::new();
    for _ in 0..2 {
        let e = engine.clone();
        alters.push(tokio::spawn(async move {
            e.handle_request(
                region_id,
                RegionRequest::Alter(RegionAlterRequest {
                    kind: AlterKind::AddColumns {
                        columns: vec![AddColumn {
                            column_metadata: store_api::metadata::ColumnMetadata {
                                column_schema: datatypes::schema::ColumnSchema::new(
                                    "new_field",
                                    datatypes::data_type::ConcreteDataType::float64_datatype(),
                                    true,
                                ),
                                semantic_type: api::v1::SemanticType::Field,
                                column_id: 100,
                            },
                            location: None,
                        }],
                    },
                }),
            )
            .await
        }));
        wait_worker_recv_event(&mut rx).await;
    }
    let e = engine.clone();
    let edit = tokio::spawn(async move {
        e.edit_region(region_id, test_region_edit(region_id, FileId::random()))
            .await
    });
    wait_worker_recv_event(&mut rx).await;
    listener.release.notify_one();

    tokio::time::timeout(Duration::from_secs(10), async {
        flush.await.unwrap().unwrap();
        drop.await.unwrap().unwrap();
        assert!(engine.get_region(region_id).is_none());
        for alter in alters {
            let err = alter.await.unwrap().unwrap_err();
            assert_eq!(err.status_code(), StatusCode::RegionNotFound);
        }
        let err = edit.await.unwrap().unwrap_err();
        assert_eq!(err.status_code(), StatusCode::RegionNotFound);
    })
    .await
    .expect("DROP left queued ALTER or edit requests unresolved");
}
