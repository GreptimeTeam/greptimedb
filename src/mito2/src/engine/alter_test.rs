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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Row, Rows, SemanticType};
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AddColumn, AddColumnLocation, AlterKind, RegionAlterRequest, RegionOpenRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::{
    build_rows, build_rows_for_key, put_rows, rows_schema, CreateRequestBuilder, TestEnv,
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

#[tokio::test]
async fn test_alter_region() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

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
    let check_region = |engine: &MitoEngine| {
        let region = engine.get_region(region_id).unwrap();
        let version_data = region.version_control.current();
        assert_eq!(1, version_data.last_entry_id);
        assert_eq!(3, version_data.committed_sequence);
        assert_eq!(1, version_data.version.flushed_entry_id);
        assert_eq!(3, version_data.version.flushed_sequence);
    };
    check_region(&engine);

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
    check_region(&engine);
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
    // Set writable.
    engine.set_writable(region_id, true).unwrap();

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
    let stream = engine.handle_query(region_id, request).await.unwrap();
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
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | a     | 0.0     | 1970-01-01T00:00:00 |
|       | a     | 1.0     | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    scan_check_after_alter(&engine, region_id, expected).await;
    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    assert_eq!(1, version_data.last_entry_id);
    assert_eq!(2, version_data.committed_sequence);
    assert_eq!(1, version_data.version.flushed_entry_id);
    assert_eq!(2, version_data.version.flushed_sequence);
}
