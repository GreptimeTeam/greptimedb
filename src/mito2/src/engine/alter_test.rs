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
use std::sync::Arc;
use std::time::Duration;

use api::v1::helper::{row, tag_column_schema};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Row, Rows, SemanticType};
use common_error::ext::ErrorExt;
use common_meta::ddl::utils::{parse_column_metadatas, parse_manifest_infos_from_extensions};
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextBackend, FulltextOptions};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::TABLE_COLUMN_METADATA_EXTENSION_KEY;
use store_api::region_engine::{RegionEngine, RegionManifestInfo, RegionRole};
use store_api::region_request::{
    AddColumn, AddColumnLocation, AlterKind, PathType, RegionAlterRequest, RegionOpenRequest,
    RegionRequest, SetIndexOption, SetRegionOption,
};
use store_api::storage::{ColumnId, RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::{AlterFlushListener, NotifyRegionChangeResultListener};
use crate::error;
use crate::sst::FormatType;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, build_rows_for_key, flush_region, put_rows,
    rows_schema,
};

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
            default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
            default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
            default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
                default_experimental_flat_format: flat_format,
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
            default_experimental_flat_format: false,
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
                default_experimental_flat_format: false,
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
            default_experimental_flat_format: false,
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
                default_experimental_flat_format: false,
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
