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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema as PbColumnSchema, Row, Rows, SemanticType, WriteHint};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::ValueRef;
use either::Either;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING,
};
use store_api::region_engine::{RegionEngine, RegionRole, SettableRegionRoleState};
use store_api::region_request::{
    PathType, RegionCloseRequest, RegionCreateRequest, RegionOpenRequest, RegionPutRequest,
    RegionRequest,
};
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, ReservedColumnId};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::oneshot;

use crate::compaction::compactor::{OpenCompactionRegionRequest, open_compaction_region};
use crate::config::MitoConfig;
use crate::engine::MITO_ENGINE_NAME;
use crate::error;
use crate::region::opener::{PartitionExprFetcher, PartitionExprFetcherRef};
use crate::region::options::RegionOptions;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, reopen_region, rows_schema,
};

#[tokio::test]
async fn test_engine_open_empty() {
    test_engine_open_empty_with_format(false).await;
    test_engine_open_empty_with_format(true).await;
}

async fn test_engine_open_empty_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("open-empty").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: "empty".to_string(),
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let err = engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let role = engine.role(region_id);
    assert_eq!(role, None);
}

#[tokio::test]
async fn test_engine_open_existing() {
    test_engine_open_existing_with_format(false).await;
    test_engine_open_existing_with_format(true).await;
}

async fn test_engine_open_existing_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("open-exiting").await;
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
}

#[tokio::test]
async fn test_engine_reopen_region() {
    test_engine_reopen_region_with_format(false).await;
    test_engine_reopen_region_with_format(true).await;
}

async fn test_engine_reopen_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("reopen-region").await;
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

    reopen_region(&engine, region_id, table_dir, false, Default::default()).await;
    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_reopen_time_series_sparse_memtable_with_bulk() {
    let mut env = TestEnv::with_prefix("reopen-time-series-sparse-with-bulk").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: false,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let table_dir = "test_reopen_partition_tree_sparse".to_string();

    // Metric-engine-shaped schema. The first PK column must be `__table_id` so
    // PartitionTree marks the region as partitioned, which is required for its
    // sparse primary-key write path.
    let column_metadatas = vec![
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::table_id(),
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                ConcreteDataType::uint64_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::tsid(),
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag_0".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "field_0".to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ),
            semantic_type: SemanticType::Field,
            column_id: 1,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        },
    ];
    let primary_key_column_ids = vec![ReservedColumnId::table_id(), ReservedColumnId::tsid(), 0];
    let request = RegionCreateRequest {
        engine: MITO_ENGINE_NAME.to_string(),
        column_metadatas,
        primary_key: primary_key_column_ids.clone(),
        options: HashMap::from([
            ("memtable.type".to_string(), "partition_tree".to_string()),
            (
                MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING.to_string(),
                "sparse".to_string(),
            ),
        ]),
        table_dir: table_dir.clone(),
        path_type: PathType::Bare,
        partition_expr_json: None,
    };
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    assert_eq!(
        PrimaryKeyEncoding::Sparse,
        engine
            .get_region(region_id)
            .unwrap()
            .metadata()
            .primary_key_encoding
    );

    // Write sparse-shaped rows (the layout metric-engine sends in production):
    // `[__primary_key (Binary), ts, field_0]` with `WriteHint::Sparse`.
    let pk_codec = SparsePrimaryKeyCodec::from_columns(primary_key_column_ids.iter().copied());
    put_sparse_rows(&engine, region_id, &pk_codec, 0, 3).await;

    // Reopen with `memtable.type=bulk` *without* flushing. The unflushed rows
    // must replay from WAL into the new BulkMemtable and stay readable after
    // the migration.
    reopen_region(
        &engine,
        region_id,
        table_dir,
        true,
        HashMap::from([("memtable.type".to_string(), "bulk".to_string())]),
    )
    .await;

    let expected = "\
+------------+--------+-------+---------+---------------------+
| __table_id | __tsid | tag_0 | field_0 | ts                  |
+------------+--------+-------+---------+---------------------+
| 1          | 0      | 0     | 0.0     | 1970-01-01T00:00:00 |
| 1          | 1      | 1     | 1.0     | 1970-01-01T00:00:01 |
| 1          | 2      | 2     | 2.0     | 1970-01-01T00:00:02 |
+------------+--------+-------+---------+---------------------+";
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

/// Pushes sparse-shaped rows for the metric-engine schema. Each row has the
/// shape `[__primary_key (Binary), ts (timestamp_ms), field_0 (f64)]`. The
/// primary key encodes `(__table_id=1, __tsid=i, tag_0=i.to_string())`.
async fn put_sparse_rows(
    engine: &crate::engine::MitoEngine,
    region_id: RegionId,
    pk_codec: &SparsePrimaryKeyCodec,
    start: usize,
    end: usize,
) {
    let schema = vec![
        PbColumnSchema {
            column_name: PRIMARY_KEY_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Binary as i32,
            semantic_type: SemanticType::Tag as i32,
            datatype_extension: None,
            options: None,
        },
        PbColumnSchema {
            column_name: "ts".to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            datatype_extension: None,
            options: None,
        },
        PbColumnSchema {
            column_name: "field_0".to_string(),
            datatype: ColumnDataType::Float64 as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: None,
            options: None,
        },
    ];
    let mut rows = Vec::with_capacity(end - start);
    for i in start..end {
        let tag_value = i.to_string();
        let mut buffer = Vec::new();
        pk_codec
            .encode_to_vec(
                [
                    (ReservedColumnId::table_id(), ValueRef::UInt32(1)),
                    (ReservedColumnId::tsid(), ValueRef::UInt64(i as u64)),
                    (0, ValueRef::String(&tag_value)),
                ]
                .into_iter(),
                &mut buffer,
            )
            .unwrap();
        rows.push(Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::BinaryValue(buffer)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(i as i64 * 1000)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(i as f64)),
                },
            ],
        });
    }
    let num_rows = rows.len();
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows { schema, rows },
                hint: Some(WriteHint {
                    primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse as i32,
                }),
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(num_rows, result.affected_rows);
}

#[tokio::test]
async fn test_engine_open_readonly() {
    test_engine_open_readonly_with_format(false).await;
    test_engine_open_readonly_with_format(true).await;
}

async fn test_engine_open_readonly_with_format(flat_format: bool) {
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
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    reopen_region(&engine, region_id, table_dir, false, Default::default()).await;

    // Region is readonly.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 2),
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: rows.clone(),
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotReady, err.status_code());

    assert_eq!(Some(RegionRole::Follower), engine.role(region_id));
    // Converts region to leader.
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    assert_eq!(Some(RegionRole::Leader), engine.role(region_id));

    put_rows(&engine, region_id, rows).await;
}

#[tokio::test]
async fn test_engine_region_open_with_options() {
    test_engine_region_open_with_options_with_format(false).await;
    test_engine_region_open_with_options_with_format(true).await;
}

async fn test_engine_region_open_with_options_with_format(flat_format: bool) {
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

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the region again with options.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::from([("ttl".to_string(), "4d".to_string())]),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        region.version().options.ttl,
        Some(Duration::from_secs(3600 * 24 * 4).into())
    );
}

#[tokio::test]
async fn test_engine_region_open_with_custom_store() {
    test_engine_region_open_with_custom_store_with_format(false).await;
    test_engine_region_open_with_custom_store_with_format(true).await;
}

async fn test_engine_region_open_with_custom_store_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine_with_multiple_object_stores(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            None,
            &["Gcs"],
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("storage", "Gcs")
        .build();
    let table_dir = request.table_dir.clone();

    // Create a custom region.
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    // Close the custom region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the custom region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::from([("storage".to_string(), "Gcs".to_string())]),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    // The region should not be opened with the default object store.
    let region = engine.get_region(region_id).unwrap();
    let object_store_manager = env.get_object_store_manager().unwrap();
    assert!(
        !object_store_manager
            .default_object_store()
            .exists(&region.access_layer.build_region_dir(region_id))
            .await
            .unwrap()
    );
    assert!(
        object_store_manager
            .find("Gcs")
            .unwrap()
            .exists(&region.access_layer.build_region_dir(region_id))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_open_region_skip_wal_replay() {
    test_open_region_skip_wal_replay_with_format(false).await;
    test_open_region_skip_wal_replay_with_format(true).await;
}

async fn test_open_region_skip_wal_replay_with_format(flat_format: bool) {
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

    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 5),
    };
    put_rows(&engine, region_id, rows).await;

    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    // Skip the WAL replay .
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: table_dir.clone(),
                path_type: PathType::Bare,
                options: Default::default(),
                skip_wal_replay: true,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

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

    // Replay the WAL.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    // Open the region again with options.
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
            }),
        )
        .await
        .unwrap();

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
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_ok() {
    test_open_region_wait_for_opening_region_ok_with_format(false).await;
    test_open_region_wait_for_opening_region_ok_with_format(true).await;
}

async fn test_open_region_wait_for_opening_region_ok_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-ok").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    table_dir: "empty".to_string(),
                    path_type: PathType::Bare,
                    options: HashMap::default(),
                    skip_wal_replay: false,
                    checkpoint: None,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Ok(0));
    }

    assert_eq!(handle_open.await.unwrap().unwrap().affected_rows, 0);
    assert_eq!(rx.await.unwrap().unwrap(), 0);
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_err() {
    test_open_region_wait_for_opening_region_err_with_format(false).await;
    test_open_region_wait_for_opening_region_err_with_format(true).await;
}

async fn test_open_region_wait_for_opening_region_err_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-err").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    table_dir: "empty".to_string(),
                    path_type: PathType::Bare,
                    options: HashMap::default(),
                    skip_wal_replay: false,
                    checkpoint: None,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Err(error::RegionNotFoundSnafu { region_id }.build()));
    }

    assert_eq!(
        handle_open.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
    assert_eq!(
        rx.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
}

#[tokio::test]
async fn test_open_compaction_region() {
    test_open_compaction_region_with_format(false).await;
    test_open_compaction_region_with_format(true).await;
}

async fn test_open_compaction_region_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let mut mito_config = MitoConfig {
        default_flat_format: flat_format,
        ..Default::default()
    };
    mito_config
        .sanitize(&env.data_home().display().to_string())
        .unwrap();

    let engine = env.create_engine(mito_config.clone()).await;

    let region_id = RegionId::new(1, 1);
    let schema_metadata_manager = env.get_schema_metadata_manager();
    schema_metadata_manager
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
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    let object_store_manager = env.get_object_store_manager().unwrap();

    let req = OpenCompactionRegionRequest {
        region_id,
        table_dir: table_dir.clone(),
        path_type: PathType::Bare,
        region_options: RegionOptions::default(),
        max_parallelism: 1,
    };

    let compaction_region = open_compaction_region(
        &req,
        &mito_config,
        object_store_manager.clone(),
        Either::Right(schema_metadata_manager),
    )
    .await
    .unwrap();

    assert_eq!(region_id, compaction_region.region_id);
}

/// Returns a dummy fetcher that returns the expr_json only once.
fn new_dummy_fetcher(region_id: RegionId, expr_json: String) -> PartitionExprFetcherRef {
    struct DummyFetcher {
        region_id: RegionId,
        expr_json: String,
        fetch_count: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl PartitionExprFetcher for DummyFetcher {
        async fn fetch_expr(&self, region_id: RegionId) -> Option<String> {
            if region_id == self.region_id {
                let count = self.fetch_count.fetch_add(1, Ordering::Relaxed);
                (count == 0).then(|| self.expr_json.clone())
            } else {
                None
            }
        }
    }

    Arc::new(DummyFetcher {
        region_id,
        expr_json,
        fetch_count: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn test_open_backfills_partition_expr_with_fetcher() {
    let mut env = TestEnv::with_prefix("open-backfill-fetcher").await;

    // prepare plugins with dummy fetcher
    let region_id = RegionId::new(100, 1);
    let expr_json = r#"{\"Expr\":{\"lhs\":{\"Column\":\"a\"},\"op\":\"GtEq\",\"rhs\":{\"Value\":{\"UInt32\":10}}}}"#.to_string();
    let fetcher = new_dummy_fetcher(region_id, expr_json.clone());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, None, Some(fetcher))
        .await;

    // create region with partition_expr=None (legacy style)
    let request = CreateRequestBuilder::new()
        .table_dir("t")
        .partition_expr_json(None)
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let meta = engine.get_region(region_id).unwrap().metadata();
    assert!(meta.partition_expr.is_none());

    // close and reopen to trigger backfill in opener
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir.clone(),
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    // verify partition_expr is backfilled and persisted
    let meta = engine.get_region(region_id).unwrap().metadata();
    assert_eq!(meta.partition_expr.as_deref(), Some(expr_json.as_str()));

    // Set leader and trigger backfill
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();

    // reopen again to ensure no further changes and still Some
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir.clone(),
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    let meta = engine.get_region(region_id).unwrap().metadata();
    assert_eq!(meta.partition_expr.as_deref(), Some(expr_json.as_str()));
}

#[tokio::test]
async fn test_open_keeps_none_without_fetcher() {
    let mut env = TestEnv::with_prefix("open-no-fetcher").await;
    // engine without fetcher
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(200, 1);
    let request = CreateRequestBuilder::new()
        .table_dir("t2")
        .partition_expr_json(None)
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let meta = engine.get_region(region_id).unwrap().metadata();
    assert!(meta.partition_expr.is_none());

    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir.clone(),
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();

    let meta = engine.get_region(region_id).unwrap().metadata();
    assert!(meta.partition_expr.is_none());
}
