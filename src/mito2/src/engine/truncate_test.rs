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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_telemetry::{info, init_default_ut_logging};
use common_wal::options::{WAL_OPTIONS_KEY, WalOptions};
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    PathType, RegionFlushRequest, RegionOpenRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::RegionId;
use tokio::sync::Notify;

use super::ScanRequest;
use crate::config::MitoConfig;
use crate::engine::listener::{EventListener, FlushCancellationListener};
use crate::test_util::{
    CreateRequestBuilder, MockWriteBufferManager, TestEnv, build_rows, put_rows, rows_schema,
};

#[derive(Default)]
struct FlushCommitListener {
    commit_started: Notify,
    cancel_requested: Notify,
    resume_flush: Notify,
}

impl FlushCommitListener {
    async fn wait_commit_started(&self) {
        self.commit_started.notified().await;
    }

    async fn wait_cancel_requested(&self) {
        self.cancel_requested.notified().await;
    }

    fn resume_flush(&self) {
        self.resume_flush.notify_one();
    }
}

#[async_trait::async_trait]
impl EventListener for FlushCommitListener {
    async fn on_flush_commit_begin(&self, _region_id: RegionId) {
        self.commit_started.notify_one();
        self.resume_flush.notified().await;
    }

    fn on_flush_cancel_requested(&self, _region_id: RegionId) {
        self.cancel_requested.notify_one();
    }
}

#[derive(Default)]
struct CountingStallListener {
    count: AtomicUsize,
    notify: Notify,
}

impl CountingStallListener {
    async fn wait_for_count(&self, expected: usize) {
        loop {
            let notified = self.notify.notified();
            if self.count.load(Ordering::Relaxed) >= expected {
                return;
            }
            notified.await;
        }
    }
}

#[async_trait::async_trait]
impl EventListener for CountingStallListener {
    fn on_write_stall(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}

#[tokio::test]
async fn test_engine_truncate_region_basic() {
    test_engine_truncate_region_basic_with_format(false).await;
    test_engine_truncate_region_basic_with_format(true).await;
}

async fn test_engine_truncate_region_basic_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("truncate-basic").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Truncate the region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+----+
| tag_0 | field_0 | ts |
+-------+---------+----+
+-------+---------+----+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_put_data_after_truncate() {
    test_engine_put_data_after_truncate_with_format(false).await;
    test_engine_put_data_after_truncate_with_format(true).await;
}

async fn test_engine_put_data_after_truncate_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("truncate-put").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Truncate the region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    // Put data to the region again.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(5, 8),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_truncate_after_flush() {
    test_engine_truncate_after_flush_with_format(false).await;
    test_engine_truncate_after_flush_with_format(true).await;
}

async fn test_engine_truncate_after_flush_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("truncate-flush").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    // Create the region.
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

    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flush the region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request.clone()).await.unwrap();
    assert_eq!(1, scanner.num_files());

    // Truncate the region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(5, 8),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(0, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_truncate_reopen() {
    test_engine_truncate_reopen_with_format(false).await;
    test_engine_truncate_reopen_with_format(true).await;
}

async fn test_engine_truncate_reopen_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("truncate-reopen").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let region = engine.get_region(region_id).unwrap();
    let last_entry_id = region.version_control.current().last_entry_id;

    // Truncate the region
    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    // Reopen the region again.
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
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(last_entry_id, region.version().flushed_entry_id);

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+----+
| tag_0 | field_0 | ts |
+-------+---------+----+
+-------+---------+----+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_discard_unflushed() {
    test_engine_discard_unflushed_with_format(false).await;
    test_engine_discard_unflushed_with_format(true).await;
}

async fn test_engine_discard_unflushed_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("discard-unflushed").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Persist the first batch and leave the second batch in memtables and WAL.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(3, 6),
        },
    )
    .await;

    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    let discarded_entry_id = version_data.last_entry_id;
    let discarded_sequence = version_data.committed_sequence;
    assert!(!version_data.version.memtables.is_empty());
    assert_eq!(
        1,
        engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap()
            .num_files()
    );

    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::Unflushed),
        )
        .await
        .unwrap();

    let version = region.version();
    assert!(version.memtables.is_empty());
    assert_eq!(discarded_entry_id, version.flushed_entry_id);
    assert_eq!(discarded_sequence, version.flushed_sequence);
    assert_eq!(
        1,
        engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap()
            .num_files()
    );

    // Repeating the request is a no-op and also retries WAL obsoletion if needed.
    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::Unflushed),
        )
        .await
        .unwrap();

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // The replay boundary is durable: reopening must not restore discarded rows.
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
                options: HashMap::default(),
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

    let region = engine.get_region(region_id).unwrap();
    assert!(region.version().memtables.is_empty());
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());

    // The region remains writable after discarding its unflushed data.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(6, 8),
        },
    )
    .await;
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        5,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

#[tokio::test]
async fn test_engine_discard_unflushed_skip_wal() {
    let mut env = TestEnv::with_prefix("discard-unflushed-skip-wal").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&WalOptions::Noop).unwrap(),
    );
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(3, 6),
        },
    )
    .await;

    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    let discarded_sequence = version_data.committed_sequence;
    // Entry ids stay at 0 without a WAL, so only the sequence advances.
    assert_eq!(0, version_data.last_entry_id);
    assert!(discarded_sequence > version_data.version.flushed_sequence);

    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::Unflushed),
        )
        .await
        .unwrap();

    let version = region.version();
    assert!(version.memtables.is_empty());
    assert_eq!(0, version.flushed_entry_id);
    assert_eq!(discarded_sequence, version.flushed_sequence);
    assert_eq!(
        1,
        engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap()
            .num_files()
    );

    // The flushed rows survive and the region stays writable.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(6, 8),
        },
    )
    .await;
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        5,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

#[tokio::test]
async fn test_engine_discard_unflushed_wakes_stalled_writes() {
    let mut env = TestEnv::with_prefix("discard-unflushed-wake-stalled").await;
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(CountingStallListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 2,
                ..Default::default()
            },
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
            None,
        )
        .await;

    // These region IDs are routed to different workers when num_workers is 2.
    let discarded_region_id = RegionId::new(1, 1);
    let peer_region_id = RegionId::new(2, 1);
    let discarded_request = CreateRequestBuilder::new().build();
    let discarded_schema = rows_schema(&discarded_request);
    engine
        .handle_request(
            discarded_region_id,
            RegionRequest::Create(discarded_request),
        )
        .await
        .unwrap();
    let peer_request = CreateRequestBuilder::new().table_dir("peer").build();
    let peer_schema = rows_schema(&peer_request);
    engine
        .handle_request(peer_region_id, RegionRequest::Create(peer_request))
        .await
        .unwrap();

    put_rows(
        &engine,
        discarded_region_id,
        Rows {
            schema: discarded_schema.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;

    write_buffer_manager.set_should_stall(true);
    let discarded_engine = engine.clone();
    let discarded_write = tokio::spawn(async move {
        put_rows(
            &discarded_engine,
            discarded_region_id,
            Rows {
                schema: discarded_schema,
                rows: build_rows(3, 6),
            },
        )
        .await;
    });
    let peer_engine = engine.clone();
    let peer_write = tokio::spawn(async move {
        put_rows(
            &peer_engine,
            peer_region_id,
            Rows {
                schema: peer_schema,
                rows: build_rows(6, 8),
            },
        )
        .await;
    });

    tokio::time::timeout(Duration::from_secs(10), listener.wait_for_count(2))
        .await
        .expect("writes should stall on both workers");
    assert!(!discarded_write.is_finished());
    assert!(!peer_write.is_finished());

    write_buffer_manager.set_should_stall(false);
    engine
        .handle_request(
            discarded_region_id,
            RegionRequest::Truncate(RegionTruncateRequest::Unflushed),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        discarded_write.await.unwrap();
        peer_write.await.unwrap();
    })
    .await
    .expect("discard should wake stalled writes on both workers");

    let stream = engine
        .scan_to_stream(discarded_region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        3,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );

    let stream = engine
        .scan_to_stream(peer_region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        2,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

#[tokio::test]
async fn test_engine_truncate_during_flush() {
    test_engine_truncate_during_flush_with_format(false, false).await;
    test_engine_truncate_during_flush_with_format(true, false).await;
}

#[tokio::test]
async fn test_engine_discard_unflushed_during_flush() {
    test_engine_truncate_during_flush_with_format(false, true).await;
    test_engine_truncate_during_flush_with_format(true, true).await;
}

#[tokio::test]
async fn test_engine_discard_unflushed_after_flush_commit_started() {
    test_engine_discard_unflushed_after_flush_commit_started_with_format(false).await;
    test_engine_discard_unflushed_after_flush_commit_started_with_format(true).await;
}

async fn test_engine_discard_unflushed_after_flush_commit_started_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("discard-unflushed-after-flush-commit").await;
    let listener = Arc::new(FlushCommitListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 3),
        },
    )
    .await;

    let flush_engine = engine.clone();
    let flush_task = tokio::spawn(async move {
        flush_engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), listener.wait_commit_started())
        .await
        .expect("flush did not reach the commit gate");

    let discard_engine = engine.clone();
    let discard_task = tokio::spawn(async move {
        discard_engine
            .handle_request(
                region_id,
                RegionRequest::Truncate(RegionTruncateRequest::Unflushed),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), listener.wait_cancel_requested())
        .await
        .expect("discard did not queue behind the committing flush");
    assert!(!discard_task.is_finished());

    listener.resume_flush();
    tokio::time::timeout(Duration::from_secs(5), flush_task)
        .await
        .expect("flush did not finish")
        .expect("flush task panicked")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), discard_task)
        .await
        .expect("discard did not finish")
        .expect("discard task panicked")
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert!(region.version().memtables.is_empty());
    assert_eq!(
        1,
        engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap()
            .num_files()
    );
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        3,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

async fn test_engine_truncate_during_flush_with_format(flat_format: bool, unflushed_only: bool) {
    init_default_ut_logging();
    let mut env = TestEnv::with_prefix("truncate-during-flush").await;
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(FlushCancellationListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            Some(write_buffer_manager),
            Some(listener.clone()),
            None,
        )
        .await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let region = engine.get_region(region_id).unwrap();

    let version_data = region.version_control.current();
    let entry_id = version_data.last_entry_id;
    let sequence = version_data.committed_sequence;

    // Flush region.
    let engine_cloned = engine.clone();
    let flush_task = tokio::spawn(async move {
        info!("do flush task!!!!");
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
    });

    // Ensure the flush is running before submitting truncate.
    tokio::time::timeout(Duration::from_secs(5), listener.wait_flush_started())
        .await
        .expect("flush did not reach the cancellation gate");

    // Truncate cancels the flush, releases the listener gate, and runs after cancellation.
    tokio::time::timeout(
        Duration::from_secs(5),
        engine.handle_request(
            region_id,
            RegionRequest::Truncate(if unflushed_only {
                RegionTruncateRequest::Unflushed
            } else {
                RegionTruncateRequest::All
            }),
        ),
    )
    .await
    .expect("truncate did not finish after cancelling flush")
    .unwrap();

    let flush_err = tokio::time::timeout(Duration::from_secs(5), flush_task)
        .await
        .expect("cancelled flush did not finish")
        .expect("flush task panicked")
        .unwrap_err();
    assert_eq!(flush_err.status_code(), StatusCode::Cancelled);

    // Check sequences and entry id.
    let version_data = region.version_control.current();
    let truncated_entry_id = version_data.version.truncated_entry_id;
    let truncated_sequence = version_data.version.flushed_sequence;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request.clone()).await.unwrap();
    assert_eq!(0, scanner.num_files());
    assert_eq!((!unflushed_only).then_some(entry_id), truncated_entry_id);
    assert_eq!(sequence, truncated_sequence);

    // Reopen the engine.
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
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let current_version = region.version_control.current().version;
    assert_eq!(
        current_version.truncated_entry_id,
        (!unflushed_only).then_some(entry_id)
    );

    // The cancelled flush wrote no files, so both kinds leave the region empty and the
    // replay must not bring the discarded rows back.
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        0,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}
