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
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Row, Rows, SemanticType};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextOptions};
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    AddColumn, AddColumnLocation, AlterKind, RegionAlterRequest, RegionOpenRequest, RegionRequest,
    SetRegionOption,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::listener::{AlterFlushListener, NotifyRegionChangeResultListener};
use crate::engine::MitoEngine;
use crate::test_util::{
    build_rows, build_rows_for_key, flush_region, put_rows, rows_schema, CreateRequestBuilder,
    TestEnv,
};

async fn scan_check_after_alter(engine: &MitoEngine, region_id: RegionId, expected: &str) {
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

fn add_tag1() -> RegionAlterRequest {
    RegionAlterRequest {
        schema_version: 0,
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_1",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 3,
                },
                location: Some(AddColumnLocation::First),
            }],
        },
    }
}

fn alter_column_fulltext_options() -> RegionAlterRequest {
    RegionAlterRequest {
        schema_version: 0,
        kind: AlterKind::SetColumnFulltext {
            column_name: "tag_0".to_string(),
            options: FulltextOptions {
                enable: true,
                analyzer: FulltextAnalyzer::English,
                case_sensitive: false,
            },
        },
    }
}

fn check_region_version(
    engine: &MitoEngine,
    region_id: RegionId,
    last_entry_id: u64,
    committed_sequence: u64,
    flushed_entry_id: u64,
    flushed_sequence: u64,
) {
    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    assert_eq!(last_entry_id, version_data.last_entry_id);
    assert_eq!(committed_sequence, version_data.committed_sequence);
    assert_eq!(flushed_entry_id, version_data.version.flushed_entry_id);
    assert_eq!(flushed_sequence, version_data.version.flushed_sequence);
}

#[tokio::test]
async fn test_alter_region() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;

    let column_schemas = rows_schema(&request);
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let request = add_tag1();
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 0     | 0.0     | 1970-01-01T00:00:00 |
|       | 1     | 1.0     | 1970-01-01T00:00:01 |
|       | 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    scan_check_after_alter(&engine, region_id, expected).await;
    check_region_version(&engine, region_id, 1, 3, 1, 3);

    // Reopen region.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();
    scan_check_after_alter(&engine, region_id, expected).await;
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

/// Build rows with schema (string, f64, ts_millis, string).
fn build_rows_for_tags(
    tag0: &str,
    tag1: &str,
    start: usize,
    end: usize,
    value_start: usize,
) -> Vec<Row> {
    (start..end)
        .enumerate()
        .map(|(idx, ts)| Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(tag0.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value((value_start + idx) as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(ts as i64 * 1000)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(tag1.to_string())),
                },
            ],
        })
        .collect()
}

#[tokio::test]
async fn test_put_after_alter() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;

    let mut column_schemas = rows_schema(&request);
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    let request = add_tag1();
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | b     | 0.0     | 1970-01-01T00:00:00 |
|       | b     | 1.0     | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    scan_check_after_alter(&engine, region_id, expected).await;

    // Reopen region.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();
    // Convert region to leader.
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    // Put with old schema.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 2, 3, 2),
    };
    put_rows(&engine, region_id, rows).await;

    // Push tag_1 to schema.
    column_schemas.push(api::v1::ColumnSchema {
        column_name: "tag_1".to_string(),
        datatype: ColumnDataType::String as i32,
        semantic_type: SemanticType::Tag as i32,
        ..Default::default()
    });
    // Put with new schema.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_tags("a", "a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan again.
    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
| a     | a     | 0.0     | 1970-01-01T00:00:00 |
| a     | a     | 1.0     | 1970-01-01T00:00:01 |
|       | b     | 0.0     | 1970-01-01T00:00:00 |
|       | b     | 1.0     | 1970-01-01T00:00:01 |
|       | b     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_retry() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    let request = add_tag1();
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();
    // Retries request.
    let request = add_tag1();
    let err = engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::RequestOutdated);

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | a     | 0.0     | 1970-01-01T00:00:00 |
|       | a     | 1.0     | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    scan_check_after_alter(&engine, region_id, expected).await;
    check_region_version(&engine, region_id, 1, 2, 1, 2);
}

#[tokio::test]
async fn test_alter_on_flushing() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()))
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Spawns a task to flush the engine.
    let engine_cloned = engine.clone();
    let flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits for flush begin.
    listener.wait_flush_begin().await;

    // Consumes the notify permit in the listener.
    listener.wait_request_begin().await;

    // Submits an alter request to the region. The region should add the request
    // to the pending ddl request list.
    let request = add_tag1();
    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(request))
            .await
            .unwrap();
    });
    // Waits until the worker handles the alter request.
    listener.wait_request_begin().await;

    // Spawns two task to flush the engine. The flush scheduler should put them to the
    // pending task list.
    let engine_cloned = engine.clone();
    let pending_flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits until the worker handles the flush request.
    listener.wait_request_begin().await;

    // Wake up flush.
    listener.wake_flush();
    // Wait for the flush job.
    tokio::time::timeout(Duration::from_secs(5), flush_job)
        .await
        .unwrap()
        .unwrap();
    // Wait for pending flush job.
    tokio::time::timeout(Duration::from_secs(5), pending_flush_job)
        .await
        .unwrap()
        .unwrap();
    // Wait for the write job.
    tokio::time::timeout(Duration::from_secs(5), alter_job)
        .await
        .unwrap()
        .unwrap();

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | a     | 0.0     | 1970-01-01T00:00:00 |
|       | a     | 1.0     | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_column_fulltext_options() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()))
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;

    let column_schemas = rows_schema(&request);
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Spawns a task to flush the engine.
    let engine_cloned = engine.clone();
    let flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits for flush begin.
    listener.wait_flush_begin().await;

    // Consumes the notify permit in the listener.
    listener.wait_request_begin().await;

    // Submits an alter request to the region. The region should add the request
    // to the pending ddl request list.
    let request = alter_column_fulltext_options();
    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(request))
            .await
            .unwrap();
    });
    // Waits until the worker handles the alter request.
    listener.wait_request_begin().await;

    // Spawns two task to flush the engine. The flush scheduler should put them to the
    // pending task list.
    let engine_cloned = engine.clone();
    let pending_flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    // Waits until the worker handles the flush request.
    listener.wait_request_begin().await;

    // Wake up flush.
    listener.wake_flush();
    // Wait for the flush job.
    flush_job.await.unwrap();
    // Wait for pending flush job.
    pending_flush_job.await.unwrap();
    // Wait for the write job.
    alter_job.await.unwrap();

    let expect_fulltext_options = FulltextOptions {
        enable: true,
        analyzer: FulltextAnalyzer::English,
        case_sensitive: false,
    };
    let check_fulltext_options = |engine: &MitoEngine, expected: &FulltextOptions| {
        let current_fulltext_options = engine
            .get_region(region_id)
            .unwrap()
            .metadata()
            .column_by_name("tag_0")
            .unwrap()
            .column_schema
            .fulltext_options()
            .unwrap()
            .unwrap();
        assert_eq!(*expected, current_fulltext_options);
    };
    check_fulltext_options(&engine, &expect_fulltext_options);
    check_region_version(&engine, region_id, 1, 3, 1, 3);

    // Reopen region.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();
    check_fulltext_options(&engine, &expect_fulltext_options);
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

#[tokio::test]
async fn test_alter_region_ttl_options() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()))
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let engine_cloned = engine.clone();
    let alter_ttl_request = RegionAlterRequest {
        schema_version: 0,
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::TTL(Duration::from_secs(500))],
        },
    };
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(alter_ttl_request))
            .await
            .unwrap();
    });

    alter_job.await.unwrap();

    let check_ttl = |engine: &MitoEngine, expected: &Duration| {
        let current_ttl = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .ttl
            .unwrap();
        assert_eq!(*expected, current_ttl);
    };
    // Verify the ttl.
    check_ttl(&engine, &Duration::from_secs(500));
}

#[tokio::test]
async fn test_write_stall_on_altering() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let listener = Arc::new(NotifyRegionChangeResultListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()))
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        let request = add_tag1();
        engine_cloned
            .handle_request(region_id, RegionRequest::Alter(request))
            .await
            .unwrap();
    });

    let column_schemas_cloned = column_schemas.clone();
    let engine_cloned = engine.clone();
    let put_job = tokio::spawn(async move {
        let rows = Rows {
            schema: column_schemas_cloned,
            rows: build_rows(0, 3),
        };
        put_rows(&engine_cloned, region_id, rows).await;
    });

    listener.wake_notify();
    alter_job.await.unwrap();
    put_job.await.unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 0     | 0.0     | 1970-01-01T00:00:00 |
|       | 1     | 1.0     | 1970-01-01T00:00:01 |
|       | 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}
