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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::{ArrowIpc, Rows};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::DfRecordBatch;
use common_test_util::flight::encode_to_flight_data;
use common_time::util::current_time_millis;
use datatypes::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::ObjectStore;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionBulkInsertsRequest, RegionCloseRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{FileId, RegionId};
use tokio::sync::{Barrier, mpsc, oneshot};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::flush_test::MockTimeProvider;
use crate::engine::listener::EventListener;
use crate::manifest::action::RegionEdit;
use crate::region::MitoRegionRef;
use crate::region::state::RegionRequestPolicy;
use crate::sst::file::FileMeta;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, rows_schema};

#[tokio::test]
async fn test_edit_region_schedule_compaction() {
    test_edit_region_schedule_compaction_with_format(false).await;
    test_edit_region_schedule_compaction_with_format(true).await;
}

async fn test_edit_region_schedule_compaction_with_format(flat_format: bool) {
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
        default_flat_format: flat_format,
        ..Default::default()
    };
    let time_provider = Arc::new(MockTimeProvider::new(current_time_millis()));
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

    let new_edit = || RegionEdit {
        files_to_add: vec![FileMeta {
            region_id: region.region_id,
            file_id: FileId::random(),
            level: 0,
            ..Default::default()
        }],
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
    };
    engine
        .edit_region(region.region_id, new_edit())
        .await
        .unwrap();
    // Asserts that the compaction of the region is not scheduled,
    // because the minimum time interval between two compactions is not passed.
    assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

    // Simulates the time has passed the min compaction interval,
    time_provider
        .set_now(current_time_millis() + config.min_compaction_interval.as_millis() as i64);
    // ... then edits the region again,
    engine
        .edit_region(region.region_id, new_edit())
        .await
        .unwrap();
    // ... finally asserts that the compaction of the region is scheduled.
    let actual = tokio::time::timeout(Duration::from_secs(9), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(region_id, actual);
}

#[tokio::test]
async fn test_edit_region_fill_cache() {
    test_edit_region_fill_cache_with_format(false).await;
    test_edit_region_fill_cache_with_format(true).await;
}

async fn test_edit_region_fill_cache_with_format(flat_format: bool) {
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

    let edit = RegionEdit {
        files_to_add: vec![FileMeta {
            region_id: region.region_id,
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
    };
    engine.edit_region(region.region_id, edit).await.unwrap();

    // Asserts that the background downloading of the SST is succeeded.
    let actual = tokio::time::timeout(Duration::from_secs(9), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file_id, actual);
}

#[tokio::test]
async fn test_write_during_region_edit_is_queued() {
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

    wait_region_request_policy(&region, RegionRequestPolicy::Stall).await;
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

    let second_file_sequence = region.version().ssts.levels()[0]
        .files
        .iter()
        .find(|(file_id, _)| **file_id == second_file_id)
        .and_then(|(_, file)| file.meta_ref().sequence)
        .map(|sequence| sequence.get());
    assert_eq!(Some(3), second_file_sequence);
}

#[tokio::test]
async fn test_bulk_insert_during_region_edit_is_queued() {
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

    wait_region_request_policy(&region, RegionRequestPolicy::Stall).await;
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

    let second_file_sequence = region.version().ssts.levels()[0]
        .files
        .iter()
        .find(|(file_id, _)| **file_id == second_file_id)
        .and_then(|(_, file)| file.meta_ref().sequence)
        .map(|sequence| sequence.get());
    assert_eq!(Some(3), second_file_sequence);
}

#[tokio::test]
async fn test_stalled_write_fails_fast_if_region_closed_during_region_edit() {
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

    wait_region_request_policy(&region, RegionRequestPolicy::Stall).await;

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
            .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
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

async fn wait_region_request_policy(region: &MitoRegionRef, policy: RegionRequestPolicy) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while region.request_policy() != policy {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
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
        region_id,
        payload,
        raw_data: ArrowIpc {
            schema: schema.data_header,
            data_header: record_batch.data_header,
            payload: record_batch.data_body,
        },
        partition_expr_version: None,
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
