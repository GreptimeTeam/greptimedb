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

use std::sync::Arc;
use std::sync::atomic::Ordering;

use api::v1::Rows;
use common_base::Plugins;
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::flush_test::MockRegionHook;
use crate::engine::listener::AlterFlushListener;
use crate::engine::region_hook::RegionHookRef;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

#[tokio::test]
async fn test_engine_close_region() {
    test_engine_close_region_with_format(false).await;
    test_engine_close_region_with_format(true).await;
}

async fn test_engine_close_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("close").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    // It's okay to close a region doesn't exist.
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the created region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    // It's okay to close this region again.
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_region_hook_on_close() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;

    let hook = Arc::new(MockRegionHook::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(MitoConfig::default(), plugins)
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Sanity: no lifecycle events before closing besides the create open.
    assert_eq!(hook.opened_count.load(Ordering::Relaxed), 1);
    assert_eq!(hook.closed_count.load(Ordering::Relaxed), 0);
    assert_eq!(hook.dropped_count.load(Ordering::Relaxed), 0);

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    // Closing fires on_region_closed exactly once.
    assert_eq!(hook.closed_count.load(Ordering::Relaxed), 1);
    // Close must not be confused with drop.
    assert_eq!(hook.dropped_count.load(Ordering::Relaxed), 0);
    assert_eq!(hook.files_removed_count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn test_engine_close_region_flush_on_close() {
    let mut env = TestEnv::with_prefix("close-flush-on-close").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;
    assert!(
        !engine
            .get_region(region_id)
            .unwrap()
            .version()
            .memtables
            .is_empty()
    );

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest {
                flush_on_close: true,
            }),
        )
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    engine
        .handle_request(
            region_id,
            RegionRequest::Open(store_api::region_request::RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir.clone(),
                path_type: store_api::region_request::PathType::Bare,
                options: request.options.clone(),
                skip_wal_replay: true,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(3, batches.iter().map(|b| b.num_rows()).sum::<usize>());
}

#[tokio::test]
async fn test_engine_close_region_flush_on_close_while_flushing() {
    let mut env = TestEnv::with_prefix("close-flush-on-close-while-flushing").await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;

    let flush_engine = engine.clone();
    let flush_task = tokio::spawn(async move {
        flush_region(&flush_engine, region_id, None).await;
    });
    listener.wait_flush_begin().await;

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(3, 6),
        },
    )
    .await;

    let request_count = listener.request_count();
    let close_engine = engine.clone();
    let close_task = tokio::spawn(async move {
        close_engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest {
                    flush_on_close: true,
                }),
            )
            .await
            .unwrap();
    });
    listener.wait_request_count(request_count + 1).await;

    let second_flush_listener = listener.clone();
    let second_flush_task = tokio::spawn(async move {
        second_flush_listener.wait_flush_begin().await;
        second_flush_listener.wake_flush();
    });
    listener.wake_flush();

    flush_task.await.unwrap();
    close_task.await.unwrap();
    second_flush_task.abort();
    assert!(!engine.is_region_exists(region_id));

    engine
        .handle_request(
            region_id,
            RegionRequest::Open(store_api::region_request::RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options: request.options,
                skip_wal_replay: true,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(6, batches.iter().map(|b| b.num_rows()).sum::<usize>());
}

#[rstest::rstest]
#[case::skip_wal(true, [false, false])]
#[case::flush_on_close(false, [true, false])]
#[tokio::test]
async fn test_repeated_close_during_edit_preserves_flush_requirement(
    #[case] skip_wal: bool,
    #[case] flush_on_close: [bool; 2],
) {
    use crate::manifest::action::RegionEdit;
    use crate::region::{RegionLeaderState, RegionRoleState};
    use crate::request::{DdlRequest, SenderDdlRequest, WorkerRequest};
    use store_api::region_request::RegionPutRequest;

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            num_workers: 1,
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let other_id = RegionId::new(1, 2);
    let create = CreateRequestBuilder::new().build();
    let schema = rows_schema(&create);
    for id in [region_id, other_id] {
        engine
            .handle_request(id, RegionRequest::Create(create.clone()))
            .await
            .unwrap();
    }
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows {
                    schema,
                    rows: build_rows(0, 3),
                },
                hint: None,
                partition_expr_version: None,
                skip_wal,
            }),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let guard = region.manifest_ctx.manifest_manager.write().await;
    // A removal of a nonexistent file exercises manifest serialization without fake SSTs.
    let e = engine.clone();
    let edit = tokio::spawn(async move {
        e.edit_region(
            region_id,
            RegionEdit {
                files_to_remove: vec![crate::sst::file::FileMeta {
                    region_id,
                    file_id: store_api::storage::FileId::random(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while region.state() != RegionRoleState::Leader(RegionLeaderState::Editing) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut closes = Vec::new();
    for flush_on_close in flush_on_close {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        engine
            .inner
            .workers
            .submit_to_worker(
                region_id,
                WorkerRequest::Ddl(SenderDdlRequest {
                    region_id,
                    sender: sender.into(),
                    request: DdlRequest::Close(RegionCloseRequest { flush_on_close }),
                }),
            )
            .await
            .unwrap();
        closes.push(receiver);
    }
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        flush_region(&engine, other_id, None),
    )
    .await
    .unwrap();
    for close in &mut closes {
        assert!(matches!(
            close.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
    }
    drop(guard);
    tokio::time::timeout(std::time::Duration::from_secs(5), edit)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for close in closes {
        assert_eq!(
            0,
            tokio::time::timeout(std::time::Duration::from_secs(5), close)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
    }
    assert!(!engine.is_region_exists(region_id));
    // WAL replay could hide a skipped flush after reopening. Check the committed SST first.
    assert!(!region.manifest_ctx.manifest().await.files.is_empty());
    crate::test_util::reopen_region(&engine, region_id, create.table_dir, true, create.options)
        .await;
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        3,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    engine.stop().await.unwrap();
}
