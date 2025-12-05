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

use std::assert_matches::assert_matches;
use std::collections::HashMap;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::options::{KafkaWalOptions, WAL_OPTIONS_KEY, WalOptions};
use rstest::rstest;
use rstest_reuse::{self, apply};
use store_api::logstore::provider::RaftEngineProvider;
use store_api::region_engine::{RegionEngine, RegionRole, SetRegionRoleStateResponse};
use store_api::region_request::{
    PathType, RegionCatchupRequest, RegionCloseRequest, RegionOpenRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::error::Error;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, TestEnv, build_rows, flush_region,
    kafka_log_store_factory, prepare_test_for_kafka_log_store, put_rows,
    raft_engine_log_store_factory, rows_schema, single_kafka_log_store_factory,
    single_raft_engine_log_store_factory,
};
use crate::wal::EntryId;

fn get_last_entry_id(resp: SetRegionRoleStateResponse) -> Option<EntryId> {
    if let SetRegionRoleStateResponse::Success(success) = resp {
        success.last_entry_id()
    } else {
        unreachable!();
    }
}

#[apply(single_kafka_log_store_factory)]

async fn test_catchup_with_last_entry_id(factory: Option<LogStoreFactory>) {
    use store_api::region_engine::SettableRegionRoleState;

    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };

    let mut env = TestEnv::with_prefix("last_entry_id")
        .await
        .with_log_store_factory(factory.clone());
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .kafka_topic(topic.clone())
        .build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
            }))
            .unwrap(),
        );
    };
    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    // Ensures the mutable is empty.
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.version().memtables.mutable.is_empty());

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };

    put_rows(&leader_engine, region_id, rows).await;

    let resp = leader_engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();

    let last_entry_id = get_last_entry_id(resp);
    assert!(last_entry_id.is_some());

    // Replays the memtable.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: last_entry_id,
                ..Default::default()
            }),
        )
        .await;
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(!region.is_writable());
    assert!(resp.is_ok());
    assert!(!follower_engine.is_region_catching_up(region_id));

    // Scans
    let request = ScanRequest::default();
    let stream = follower_engine
        .scan_to_stream(region_id, request)
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

    // Replays the memtable again, should be ok.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                entry_id: last_entry_id,
                ..Default::default()
            }),
        )
        .await;
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.is_writable());
    assert!(resp.is_ok());
}

#[apply(single_kafka_log_store_factory)]
async fn test_catchup_with_incorrect_last_entry_id(factory: Option<LogStoreFactory>) {
    use store_api::region_engine::SettableRegionRoleState;

    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };

    let mut env = TestEnv::with_prefix("incorrect_last_entry_id")
        .await
        .with_log_store_factory(factory.clone());
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .kafka_topic(topic.clone())
        .build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
            }))
            .unwrap(),
        );
    };
    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    // Ensures the mutable is empty.
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.version().memtables.mutable.is_empty());

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };

    put_rows(&leader_engine, region_id, rows).await;

    let resp = leader_engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();

    let last_entry_id = get_last_entry_id(resp);
    assert!(last_entry_id.is_some());

    let incorrect_last_entry_id = last_entry_id.map(|e| e + 1);

    // Replays the memtable.
    let err = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: incorrect_last_entry_id,
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
    let err = err.as_any().downcast_ref::<Error>().unwrap();
    assert!(!follower_engine.is_region_catching_up(region_id));
    assert_matches!(err, Error::Unexpected { .. });

    // It should ignore requests to writable regions.
    region.set_role(RegionRole::Leader);
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: incorrect_last_entry_id,
                ..Default::default()
            }),
        )
        .await;
    assert!(resp.is_ok());
}

#[apply(single_kafka_log_store_factory)]
async fn test_catchup_without_last_entry_id(factory: Option<LogStoreFactory>) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };

    let mut env = TestEnv::with_prefix("without_last_entry_id")
        .await
        .with_log_store_factory(factory.clone());
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .kafka_topic(topic.clone())
        .build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
            }))
            .unwrap(),
        );
    };
    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&leader_engine, region_id, rows).await;

    // Replays the memtable.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                ..Default::default()
            }),
        )
        .await;
    assert!(resp.is_ok());
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(!region.is_writable());

    let request = ScanRequest::default();
    let stream = follower_engine
        .scan_to_stream(region_id, request)
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

    // Replays the memtable again, should be ok.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                ..Default::default()
            }),
        )
        .await;
    assert!(resp.is_ok());
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.is_writable());
}

#[apply(single_kafka_log_store_factory)]
async fn test_catchup_with_manifest_update(factory: Option<LogStoreFactory>) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };

    let mut env = TestEnv::with_prefix("without_manifest_update")
        .await
        .with_log_store_factory(factory.clone());
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .kafka_topic(topic.clone())
        .build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
            }))
            .unwrap(),
        );
    };
    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&leader_engine, region_id, rows).await;

    // Triggers to create a new manifest file.
    flush_region(&leader_engine, region_id, None).await;

    // Puts to WAL.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(3, 5),
    };
    put_rows(&leader_engine, region_id, rows).await;

    // Triggers to create a new manifest file.
    flush_region(&leader_engine, region_id, None).await;

    let region = follower_engine.get_region(region_id).unwrap();
    // Ensures the mutable is empty.
    assert!(region.version().memtables.mutable.is_empty());

    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.manifest_version, 0);

    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                ..Default::default()
            }),
        )
        .await;
    assert!(resp.is_ok());

    // The inner region was replaced. We must get it again.
    let region = follower_engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.manifest_version, 2);
    assert!(!region.is_writable());

    let request = ScanRequest::default();
    let stream = follower_engine
        .scan_to_stream(region_id, request)
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
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Replays the memtable again, should be ok.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                ..Default::default()
            }),
        )
        .await;
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(resp.is_ok());
    assert!(region.is_writable());
}

async fn close_region(engine: &MitoEngine, region_id: RegionId) {
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
}

async fn open_region(
    engine: &MitoEngine,
    region_id: RegionId,
    table_dir: String,
    skip_wal_replay: bool,
) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: table_dir.clone(),
                options: HashMap::new(),
                skip_wal_replay,
                path_type: PathType::Bare,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
}

async fn scan_region(engine: &MitoEngine, region_id: RegionId) -> RecordBatches {
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    RecordBatches::try_collect(stream).await.unwrap()
}

#[apply(single_raft_engine_log_store_factory)]
async fn test_local_catchup(factory: Option<LogStoreFactory>) {
    use store_api::region_engine::SettableRegionRoleState;

    use crate::test_util::LogStoreImpl;

    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };

    let mut env = TestEnv::with_prefix("local_catchup")
        .await
        .with_log_store_factory(factory.clone());
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let Some(LogStoreImpl::RaftEngine(log_store)) = env.get_log_store() else {
        unreachable!()
    };

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&leader_engine, region_id, rows).await;
    flush_region(&leader_engine, region_id, None).await;

    // Ensure the last entry id is 1.
    let resp = leader_engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    let last_entry_id = get_last_entry_id(resp);
    assert_eq!(last_entry_id.unwrap(), 1);

    // Close the region, and open it again.
    close_region(&leader_engine, region_id).await;
    open_region(&leader_engine, region_id, table_dir.clone(), false).await;

    // Set the region to leader.
    leader_engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    // Write more rows
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(4, 7),
    };
    put_rows(&leader_engine, region_id, rows).await;
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(8, 9),
    };
    put_rows(&leader_engine, region_id, rows).await;
    let resp = leader_engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    let last_entry_id = get_last_entry_id(resp);
    assert_eq!(last_entry_id.unwrap(), 3);

    close_region(&leader_engine, region_id).await;
    // Reopen the region, and skip the wal replay.
    leader_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: table_dir.clone(),
                options: HashMap::new(),
                skip_wal_replay: true,
                path_type: PathType::Bare,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    // The last entry id should be 1.
    let region = leader_engine.get_region(region_id).unwrap();
    assert_eq!(region.version_control.current().last_entry_id, 1);

    // There are 2 entries in the log store.
    let (start, end) = log_store.span(&RaftEngineProvider::new(region_id.into()));
    assert_eq!(start.unwrap(), 2);
    assert_eq!(end.unwrap(), 3);

    // Try to catchup the region.
    let resp = leader_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                ..Default::default()
            }),
        )
        .await;
    assert!(resp.is_ok());
    // After catchup, the last entry id should be 3.
    let region = leader_engine.get_region(region_id).unwrap();
    assert_eq!(region.version_control.current().last_entry_id, 3);

    // The log store has been obsoleted these 2 entries.
    let (start, end) = log_store.span(&RaftEngineProvider::new(region_id.into()));
    assert_eq!(start, None);
    assert_eq!(end, None);

    // For local WAL, entries are not replayed during catchup.
    // Therefore, any rows that were not flushed before closing the region will not be visible.
    let batches = scan_region(&leader_engine, region_id).await;
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Write more rows
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(4, 7),
    };
    put_rows(&leader_engine, region_id, rows).await;

    let batches = scan_region(&leader_engine, region_id).await;
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_catchup_not_exist() {
    test_catchup_not_exist_with_format(false).await;
    test_catchup_not_exist_with_format(true).await;
}

async fn test_catchup_not_exist_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let non_exist_region_id = RegionId::new(1, 1);

    let err = engine
        .handle_request(
            non_exist_region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(err.status_code(), StatusCode::RegionNotFound);
}

#[tokio::test]
async fn test_catchup_region_busy() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    let worker = engine.inner.workers.worker(region_id);
    let catchup_regions = worker.catchup_regions();
    catchup_regions.insert_region(region_id);
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(err.status_code(), StatusCode::RegionBusy);
}
