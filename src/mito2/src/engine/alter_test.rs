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

use std::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::bulk_wal_entry::Body;
use api::v1::helper::{row, tag_column_schema};
use api::v1::value::ValueData;
use api::v1::{ArrowIpc, BulkWalEntry, ColumnDataType, Row, Rows, SemanticType, Value, WalEntry};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::ddl::utils::{parse_column_metadatas, parse_manifest_infos_from_extensions};
use common_recordbatch::{DfRecordBatch, RecordBatches};
use common_test_util::flight::encode_to_flight_data;
use datafusion_expr::col;
use datatypes::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextBackend, FulltextOptions};
use store_api::logstore::provider::Provider;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::TABLE_COLUMN_METADATA_EXTENSION_KEY;
use store_api::region_engine::{RegionEngine, RegionManifestInfo, RegionRole};
use store_api::region_request::{
    AddColumn, AddColumnLocation, AlterKind, PathType, RegionAlterRequest,
    RegionBulkInsertsRequest, RegionOpenRequest, RegionRequest, SetIndexOption, SetRegionOption,
    UnsetRegionOption,
};
use store_api::storage::{ColumnId, RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::{AlterFlushListener, NotifyRegionChangeResultListener};
use crate::error;
use crate::sst::FormatType;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, LogStoreImpl, TestEnv, build_rows, build_rows_for_key,
    column_metadata_to_column_schema, flush_region, put_rows, reopen_region, rows_schema,
};
use crate::wal::Wal;

async fn scan_check_after_alter(engine: &MitoEngine, region_id: RegionId, expected: &str) {
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

fn add_tag1() -> RegionAlterRequest {
    RegionAlterRequest {
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

fn alter_column_inverted_index() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Inverted {
                column_name: "tag_0".to_string(),
            }],
        },
    }
}

fn alter_column_fulltext_options() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Fulltext {
                column_name: "tag_0".to_string(),
                options: FulltextOptions::new_unchecked(
                    true,
                    FulltextAnalyzer::English,
                    false,
                    FulltextBackend::Bloom,
                    1000,
                    0.01,
                ),
            }],
        },
    }
}

fn add_nullable_field1() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "field_1",
                        ConcreteDataType::float64_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Field,
                    column_id: 3,
                },
                location: None,
            }],
        },
    }
}

fn build_row_with_added_field(
    metadata: &[ColumnMetadata],
    tag_0: &str,
    field_0: f64,
    field_1: Option<f64>,
    ts_millis: i64,
) -> Row {
    let values = metadata
        .iter()
        .map(|column| match column.column_schema.name.as_str() {
            "tag_0" => Value {
                value_data: Some(ValueData::StringValue(tag_0.to_string())),
            },
            "field_0" => Value {
                value_data: Some(ValueData::F64Value(field_0)),
            },
            "field_1" => Value {
                value_data: field_1.map(ValueData::F64Value),
            },
            "ts" => Value {
                value_data: Some(ValueData::TimestampMillisecondValue(ts_millis)),
            },
            name => panic!("unexpected column {name}"),
        })
        .collect();

    Row { values }
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

fn assert_column_metadatas(column_name: &[(&str, ColumnId)], column_metadatas: &[ColumnMetadata]) {
    assert_eq!(column_name.len(), column_metadatas.len());
    for (name, id) in column_name {
        let column_metadata = column_metadatas
            .iter()
            .find(|c| c.column_id == *id)
            .unwrap();
        assert_eq!(column_metadata.column_schema.name, *name);
    }
}

#[tokio::test]
async fn test_alter_region() {
    test_alter_region_with_format(false).await;
    test_alter_region_with_format(true).await;
}

async fn test_alter_region_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    let response = engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let column_metadatas =
        parse_column_metadatas(&response.extensions, TABLE_COLUMN_METADATA_EXTENSION_KEY).unwrap();
    assert_column_metadatas(
        &[("tag_0", 0), ("field_0", 1), ("ts", 2)],
        &column_metadatas,
    );

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let request = add_tag1();
    let response = engine
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

    let mut manifests = parse_manifest_infos_from_extensions(&response.extensions).unwrap();
    assert_eq!(manifests.len(), 1);
    let (return_region_id, manifest) = manifests.remove(0);
    assert_eq!(return_region_id, region_id);
    assert_eq!(manifest, RegionManifestInfo::mito(2, 1, 0));
    let column_metadatas =
        parse_column_metadatas(&response.extensions, TABLE_COLUMN_METADATA_EXTENSION_KEY).unwrap();
    assert_column_metadatas(
        &[("tag_0", 0), ("field_0", 1), ("ts", 2), ("tag_1", 3)],
        &column_metadatas,
    );

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
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    scan_check_after_alter(&engine, region_id, expected).await;
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

#[tokio::test]
async fn test_filter_is_null_after_alter_add_field() {
    test_filter_is_null_after_alter_add_field_with_format(false).await;
    test_filter_is_null_after_alter_add_field_with_format(true).await;
}

async fn test_filter_is_null_after_alter_add_field_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
            env.get_kv_backend(),
        )
        .await;

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
            rows: vec![build_rows_for_key("a", 0, 1, 1).into_iter().next().unwrap()],
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    engine
        .handle_request(region_id, RegionRequest::Alter(add_nullable_field1()))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let metadata = region.metadata().column_metadatas.clone();
    let schema = metadata
        .iter()
        .map(column_metadata_to_column_schema)
        .collect();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: vec![build_row_with_added_field(
                &metadata,
                "a",
                1.0,
                Some(10.0),
                0,
            )],
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    // We skip field filters under merge mode because the flushed field values may be stale before
    // the row is merged with newer field data.
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                filters: vec![col("field_1").is_null()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+---------+
| tag_0 | field_0 | ts                  | field_1 |
+-------+---------+---------------------+---------+
| a     | 1.0     | 1970-01-01T00:00:00 | 10.0    |
+-------+---------+---------------------+---------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
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
        .map(|(idx, ts)| {
            row(vec![
                ValueData::StringValue(tag0.to_string()),
                ValueData::F64Value((value_start + idx) as f64),
                ValueData::TimestampMillisecondValue(ts as i64 * 1000),
                ValueData::StringValue(tag1.to_string()),
            ])
        })
        .collect()
}

#[tokio::test]
async fn test_put_after_alter() {
    test_put_after_alter_with_format(false).await;
    test_put_after_alter_with_format(true).await;
}

async fn test_put_after_alter_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
            env.get_kv_backend(),
        )
        .await;

    let mut column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
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
    column_schemas.push(tag_column_schema("tag_1", ColumnDataType::String));
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
    test_alter_region_retry_with_format(false).await;
    test_alter_region_retry_with_format(true).await;
}

async fn test_alter_region_retry_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
            env.get_kv_backend(),
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
    let err = err.as_any().downcast_ref::<error::Error>().unwrap();
    assert_matches!(err, &error::Error::InvalidRegionRequest { .. });

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
    test_alter_on_flushing_with_format(false).await;
    test_alter_on_flushing_with_format(true).await;
}

async fn test_alter_on_flushing_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
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
    let scanner = engine.scanner(region_id, request).await.unwrap();
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
    test_alter_column_fulltext_options_with_format(false).await;
    test_alter_column_fulltext_options_with_format(true).await;
}

async fn test_alter_column_fulltext_options_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
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

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
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

    let expect_fulltext_options = FulltextOptions::new_unchecked(
        true,
        FulltextAnalyzer::English,
        false,
        FulltextBackend::Bloom,
        1000,
        0.01,
    );
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
    check_fulltext_options(&engine, &expect_fulltext_options);
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

#[tokio::test]
async fn test_alter_column_set_inverted_index() {
    test_alter_column_set_inverted_index_with_format(false).await;
    test_alter_column_set_inverted_index_with_format(true).await;
}

async fn test_alter_column_set_inverted_index_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
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

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
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
    let request = alter_column_inverted_index();
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

    let check_inverted_index_set = |engine: &MitoEngine| {
        assert!(
            engine
                .get_region(region_id)
                .unwrap()
                .metadata()
                .column_by_name("tag_0")
                .unwrap()
                .column_schema
                .is_inverted_indexed()
        )
    };
    check_inverted_index_set(&engine);
    check_region_version(&engine, region_id, 1, 3, 1, 3);

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
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    check_inverted_index_set(&engine);
    check_region_version(&engine, region_id, 1, 3, 1, 3);
}

#[tokio::test]
async fn test_alter_region_ttl_options() {
    test_alter_region_ttl_options_with_format(false).await;
    test_alter_region_ttl_options_with_format(true).await;
}

async fn test_alter_region_ttl_options_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
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
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let engine_cloned = engine.clone();
    let alter_ttl_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Ttl(Some(Duration::from_secs(500).into()))],
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
        let current_ttl = engine.get_region(region_id).unwrap().version().options.ttl;
        assert_eq!(current_ttl, Some((*expected).into()));
    };
    // Verify the ttl.
    check_ttl(&engine, &Duration::from_secs(500));
}

#[tokio::test]
async fn test_mixed_region_options_are_published_after_flush() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
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
            env.get_kv_backend(),
        )
        .await;
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

    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Alter(RegionAlterRequest {
                    kind: AlterKind::SetRegionOptions {
                        options: vec![
                            SetRegionOption::Ttl(Some(Duration::from_secs(500).into())),
                            SetRegionOption::MaxRowGroupRowCount(Some(1024)),
                        ],
                    },
                }),
            )
            .await
            .unwrap();
    });

    listener.wait_flush_begin().await;
    let version = engine.get_region(region_id).unwrap().version();
    assert_eq!(None, version.options.ttl);
    assert_eq!(None, version.options.max_row_group_row_count);

    listener.wake_flush();
    alter_job.await.unwrap();

    let version = engine.get_region(region_id).unwrap().version();
    assert_eq!(Some(Duration::from_secs(500).into()), version.options.ttl);
    assert_eq!(Some(1024), version.options.max_row_group_row_count);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_stall_on_altering() {
    common_telemetry::init_default_ut_logging();

    test_write_stall_on_altering_with_format(false).await;
    test_write_stall_on_altering_with_format(true).await;
}

async fn test_write_stall_on_altering_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(NotifyRegionChangeResultListener::default());
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
    // Make sure the loop is handling the alter request.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let column_schemas_cloned = column_schemas.clone();
    let engine_cloned = engine.clone();
    let put_job = tokio::spawn(async move {
        let rows = Rows {
            schema: column_schemas_cloned,
            rows: build_rows(0, 3),
        };
        put_rows(&engine_cloned, region_id, rows).await;
    });
    // Make sure the loop is handling the put request.
    tokio::time::sleep(Duration::from_millis(100)).await;

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
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_with_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: false,
            ..Default::default()
        })
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
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Inserts some data before alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with primary_key format
    flush_region(&engine, region_id, None).await;

    let expected_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_data, batches.pretty_print().unwrap());

    // Alters sst_format from primary_key to flat
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("flat".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with flat format
    flush_region(&engine, region_id, None).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
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

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_without_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: false,
            ..Default::default()
        })
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
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_format = |engine: &MitoEngine, expected: Option<FormatType>| {
        let current_format = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .sst_format;
        assert_eq!(current_format, expected);
    };
    check_format(&engine, Some(FormatType::PrimaryKey));

    // Inserts some data before alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Alters sst_format from primary_key to flat
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("flat".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    check_format(&engine, Some(FormatType::Flat));

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
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

    check_format(&engine, Some(FormatType::Flat));

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_flat_to_pk_with_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            ..Default::default()
        })
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
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Inserts some data with flat format
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with flat format
    flush_region(&engine, region_id, None).await;

    let expected_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_data, batches.pretty_print().unwrap());

    // Alters sst_format from flat to primary_key
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("primary_key".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes to create SST files with primary_key format
    flush_region(&engine, region_id, None).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
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

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_sst_format_flat_to_pk_without_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            ..Default::default()
        })
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
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_format = |engine: &MitoEngine, expected: Option<FormatType>| {
        let current_format = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .sst_format;
        assert_eq!(current_format, expected);
    };
    check_format(&engine, Some(FormatType::Flat));

    // Inserts some data with flat format
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Alters sst_format from flat to primary_key
    let alter_format_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::Format("primary_key".to_string())],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_format_request))
        .await
        .unwrap();

    check_format(&engine, Some(FormatType::PrimaryKey));

    // Inserts more data after alter
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    put_rows(&engine, region_id, rows).await;

    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());

    // Reopens region to verify format persists
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: false,
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

    check_format(&engine, Some(FormatType::PrimaryKey));

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected_all_data, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_alter_region_append_mode_with_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // Create a region with append_mode=false (default)
    let request = CreateRequestBuilder::new().build();

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

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_append_mode = |engine: &MitoEngine, expected: bool| {
        let append_mode = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .append_mode;
        assert_eq!(append_mode, expected);
    };
    check_append_mode(&engine, false);

    // Inserts some data before alter (memtable not empty, alter will trigger flush)
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Alters append_mode from false to true (this triggers internal flush)
    let alter_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::AppendMode(true)],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await
        .unwrap();

    check_append_mode(&engine, true);

    // Inserts duplicate data after alter (same as rows 0, 1, 2)
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes again
    flush_region(&engine, region_id, None).await;

    // After append_mode=true, duplicates should be preserved
    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );

    // Reopens region to verify append_mode persists
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
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

    check_append_mode(&engine, true);

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );
}

#[tokio::test]
async fn test_alter_region_append_mode_without_flush() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // Create a region with append_mode=false (default)
    let request = CreateRequestBuilder::new().build();

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

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_append_mode = |engine: &MitoEngine, expected: bool| {
        let append_mode = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .append_mode;
        assert_eq!(append_mode, expected);
    };
    check_append_mode(&engine, false);

    // Alters append_mode from false to true immediately (no data, no flush needed)
    let alter_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::AppendMode(true)],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await
        .unwrap();

    check_append_mode(&engine, true);

    // Inserts duplicate data
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Insert same data again
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flushes
    flush_region(&engine, region_id, None).await;

    // Duplicates should be preserved
    let expected_all_data = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );

    // Reopens region to verify append_mode persists
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
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

    check_append_mode(&engine, true);

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        expected_all_data,
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    );
}

#[tokio::test]
async fn test_alter_region_append_mode_invalid() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // Create a region with append_mode=true
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let check_append_mode = |engine: &MitoEngine, expected: bool| {
        let append_mode = engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .append_mode;
        assert_eq!(append_mode, expected);
    };
    check_append_mode(&engine, true);

    // Try to alter append_mode from true to false (should fail)
    let alter_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::AppendMode(false)],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await
        .unwrap_err();

    // append_mode should still be true
    check_append_mode(&engine, true);
}

#[tokio::test]
async fn test_alter_region_preserve_row_sequence_lifecycle() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("test_alter_region_preserve_row_sequence_lifecycle").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let alter = |kind: AlterKind| {
        engine.handle_request(region_id, RegionRequest::Alter(RegionAlterRequest { kind }))
    };

    // Fill the memtable while preserve_row_sequence is disabled.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;
    let version = engine.get_region(region_id).unwrap().version();
    assert!(version.memtables.num_rows() > 0);
    let flushed_sequence = version.flushed_sequence;

    // Enable alone on the non-empty memtable: applies in memory through the
    // fast path, neither flushing nor losing rows.
    alter(AlterKind::SetRegionOptions {
        options: vec![SetRegionOption::PreserveRowSequence(true)],
    })
    .await
    .unwrap();
    let version = engine.get_region(region_id).unwrap().version();
    assert!(version.options.preserve_row_sequence);
    assert_eq!(0, version.ssts.levels()[0].files.len());
    assert!(version.memtables.num_rows() > 0);
    assert_eq!(flushed_sequence, version.flushed_sequence);

    // Rows written after the ALTER share the pre-ALTER memtable; a later flush
    // writes both into one SST marked with preserve_row_sequence.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(3, 5),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    let version = engine.get_region(region_id).unwrap().version();
    assert!(version.options.preserve_row_sequence);
    let level0 = &version.ssts.levels()[0].files;
    assert_eq!(1, level0.len());
    assert!(
        level0
            .values()
            .next()
            .unwrap()
            .meta_ref()
            .preserve_row_sequence
    );
    let flushed_sequence = version.flushed_sequence;

    // Exact scan (2, 5] returns pre-ALTER row "2" (seq 3) and post-ALTER rows
    // "3" and "4" (seqs 4 and 5): distinct row sequences survive the flush.
    let scan_exact = async |min: Option<u64>, max: Option<u64>| -> String {
        let stream = engine
            .scan_to_stream(
                region_id,
                ScanRequest {
                    memtable_min_sequence: min,
                    memtable_max_sequence: max,
                    exact_sequence_range: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        sort_batches_and_print(&batches, &["tag_0", "ts"])
    };
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, scan_exact(Some(2), Some(5)).await);

    // Fill the memtable again and unset on the non-empty memtable: applies in
    // memory without creating another SST.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(5, 7),
        },
    )
    .await;
    alter(AlterKind::UnsetRegionOptions {
        keys: vec![UnsetRegionOption::PreserveRowSequence],
    })
    .await
    .unwrap();
    let version = engine.get_region(region_id).unwrap().version();
    assert!(!version.options.preserve_row_sequence);
    assert_eq!(1, version.ssts.levels()[0].files.len());
    assert!(version.memtables.num_rows() > 0);

    // The exact capability is unsupported immediately: both bounds are still
    // enforceable against the flushed frontier, yet row-level filtering is off.
    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(flushed_sequence),
                memtable_max_sequence: Some(flushed_sequence),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected sequence-range unsupported error");
    assert!(matches!(err, error::Error::SequenceRangeUnsupported { .. }));

    // A subsequent flush under the disabled state writes an unmarked SST while
    // the earlier marked SST stays untouched.
    flush_region(&engine, region_id, None).await;
    let version = engine.get_region(region_id).unwrap().version();
    let level0 = &version.ssts.levels()[0].files;
    assert_eq!(2, level0.len());
    let newest = level0
        .values()
        .max_by_key(|f| f.meta_ref().sequence)
        .unwrap();
    assert!(!newest.meta_ref().preserve_row_sequence);
    assert_eq!(
        1,
        level0
            .values()
            .filter(|f| f.meta_ref().preserve_row_sequence)
            .count()
    );
}

#[tokio::test]
async fn test_alter_region_append_mode_preserve_combined_flushes_in_both_orders() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix(
        "test_alter_region_append_mode_preserve_combined_flushes_in_both_orders",
    )
    .await;
    let engine = env.create_engine(MitoConfig::default()).await;

    // Both orders of the combined ALTER must behave identically: append_mode's
    // requirement for an empty memtable is not bypassed by preserve_row_sequence.
    for (region_id, options) in [
        (
            RegionId::new(1, 1),
            vec![
                SetRegionOption::AppendMode(true),
                SetRegionOption::PreserveRowSequence(true),
            ],
        ),
        (
            RegionId::new(1, 2),
            vec![
                SetRegionOption::PreserveRowSequence(true),
                SetRegionOption::AppendMode(true),
            ],
        ),
    ] {
        let request = CreateRequestBuilder::new().build();
        let column_schemas = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        // Non-empty memtable: the combined ALTER must flush before applying.
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: column_schemas,
                rows: build_rows(0, 3),
            },
        )
        .await;

        engine
            .handle_request(
                region_id,
                RegionRequest::Alter(RegionAlterRequest {
                    kind: AlterKind::SetRegionOptions { options },
                }),
            )
            .await
            .unwrap();

        let version = engine.get_region(region_id).unwrap().version();
        assert!(version.options.append_mode && version.options.preserve_row_sequence);
        // The append_mode change forced a flush: the memtable is now empty and
        // the pre-ALTER rows live in the flushed SST.
        assert_eq!(0, version.memtables.num_rows());

        let request = ScanRequest::default();
        let stream = engine.scan_to_stream(region_id, request).await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(3, batches.iter().map(|b| b.num_rows()).sum::<usize>());
    }
}

#[tokio::test]
async fn test_alter_region_preserve_row_sequence_requires_append_mode() {
    common_telemetry::init_default_ut_logging();

    let mut env =
        TestEnv::with_prefix("test_alter_region_preserve_row_sequence_requires_append_mode").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();

    let alter = |options: Vec<SetRegionOption>| {
        engine.handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions { options },
            }),
        )
    };

    let err = alter(vec![SetRegionOption::PreserveRowSequence(true)])
        .await
        .unwrap_err();
    let err = err.as_any().downcast_ref::<error::Error>().unwrap();
    assert_matches!(
        err,
        error::Error::InvalidMetadata { source, .. }
            if source.to_string().contains("preserve_row_sequence is only supported for append-only tables")
    );
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);

    // Disabling on a non-append region is a no-op success and leaves the
    // option off.
    alter(vec![SetRegionOption::PreserveRowSequence(false)])
        .await
        .unwrap();
    assert!(
        !engine
            .get_region(region_id)
            .unwrap()
            .version()
            .options
            .preserve_row_sequence
    );
}

/// Builds a batch against the schema before [add_nullable_field1]
/// (schema version 0): `(tag_0, field_0, ts)`.
fn build_schema_v0_batch() -> DfRecordBatch {
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
        (0..3).map(|v| format!("a{v}")),
    )) as ArrayRef;
    let field = Arc::new(Float64Array::from_iter_values((0..3).map(|v| v as f64))) as ArrayRef;
    let ts = Arc::new(TimestampMillisecondArray::from_iter_values(
        (0..3).map(|v| v as i64 * 1000),
    )) as ArrayRef;
    DfRecordBatch::try_new(schema, vec![tag, field, ts]).unwrap()
}

fn encode_arrow_ipc(batch: &DfRecordBatch) -> ArrowIpc {
    let (schema, record_batch) = encode_to_flight_data(batch.clone());
    ArrowIpc {
        schema: schema.data_header,
        data_header: record_batch.data_header,
        payload: record_batch.data_body,
    }
}

/// Builds a bulk insert request whose batch was built against the schema
/// before [add_nullable_field1] (schema version 0): `(tag_0, field_0, ts)`.
fn build_schema_v0_bulk_request(region_id: RegionId) -> RegionBulkInsertsRequest {
    let payload = build_schema_v0_batch();
    let raw_data = encode_arrow_ipc(&payload);

    RegionBulkInsertsRequest {
        region_id,
        payload,
        raw_data,
        partition_expr_version: None,
        // The writer only knows schema version 0 while the region is already
        // at version 1, so the worker has to fill the missing columns.
        aligned_schema_version: Some(0),
    }
}

async fn scan_all_sorted(engine: &MitoEngine, region_id: RegionId) -> String {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    sort_batches_and_print(&batches, &["tag_0", "ts"])
}

const SCHEMA_V0_BULK_ROWS: &str = "\
+-------+---------+---------------------+---------+
| tag_0 | field_0 | ts                  | field_1 |
+-------+---------+---------------------+---------+
| a0    | 0.0     | 1970-01-01T00:00:00 |         |
| a1    | 1.0     | 1970-01-01T00:00:01 |         |
| a2    | 2.0     | 1970-01-01T00:00:02 |         |
+-------+---------+---------------------+---------+";

#[tokio::test]
async fn test_bulk_insert_stale_schema_wal_replay() {
    test_bulk_insert_stale_schema_wal_replay_with_format(false).await;
    test_bulk_insert_stale_schema_wal_replay_with_format(true).await;
}

async fn test_bulk_insert_stale_schema_wal_replay_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Bumps the region schema version to 1.
    engine
        .handle_request(region_id, RegionRequest::Alter(add_nullable_field1()))
        .await
        .unwrap();

    // Bulk insert whose batch misses `field_1`. The worker fills the missing
    // column before writing to the memtable and the WAL.
    let response = engine
        .handle_request(
            region_id,
            RegionRequest::BulkInserts(build_schema_v0_bulk_request(region_id)),
        )
        .await
        .unwrap();
    assert_eq!(3, response.affected_rows);

    assert_eq!(
        SCHEMA_V0_BULK_ROWS,
        scan_all_sorted(&engine, region_id).await,
        "flat_format: {flat_format}"
    );

    // Reopens the region to replay the WAL. The rows must survive the replay.
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    assert_eq!(
        SCHEMA_V0_BULK_ROWS,
        scan_all_sorted(&engine, region_id).await,
        "flat_format: {flat_format}"
    );
}

/// Replays a bulk WAL entry that misses columns added by an alter. This
/// simulates entries written by old versions that keep the stale raw data
/// when filling missing columns.
#[tokio::test]
async fn test_bulk_insert_stale_wal_entry_replay() {
    test_bulk_insert_stale_wal_entry_replay_with_format(false).await;
    test_bulk_insert_stale_wal_entry_replay_with_format(true).await;
}

async fn test_bulk_insert_stale_wal_entry_replay_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Bumps the region schema version to 1.
    engine
        .handle_request(region_id, RegionRequest::Alter(add_nullable_field1()))
        .await
        .unwrap();

    // Writes a bulk entry whose batch misses `field_1` to the WAL directly.
    let batch = build_schema_v0_batch();
    let entry = WalEntry {
        mutations: vec![],
        bulk_entries: vec![BulkWalEntry {
            sequence: 1,
            max_ts: 2000,
            min_ts: 0,
            timestamp_index: 2,
            body: Some(Body::ArrowIpc(encode_arrow_ipc(&batch))),
        }],
    };
    let LogStoreImpl::RaftEngine(log_store) = env.get_log_store().unwrap() else {
        unreachable!()
    };
    let wal = Wal::new(log_store);
    let mut writer = wal.writer();
    writer
        .add_entry(
            region_id,
            1,
            &entry,
            &Provider::raft_engine_provider(region_id.as_u64()),
        )
        .unwrap();
    writer.write_to_wal().await.unwrap();

    // Reopens the region to replay the WAL. The replay must fill the missing
    // column instead of losing the rows.
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    assert_eq!(
        SCHEMA_V0_BULK_ROWS,
        scan_all_sorted(&engine, region_id).await,
        "flat_format: {flat_format}"
    );
}
