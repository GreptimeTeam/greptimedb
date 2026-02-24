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

//! Tests for append mode.

use std::collections::HashMap;

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AlterKind, PathType, RegionAlterRequest, RegionCompactRequest, RegionOpenRequest,
    RegionRequest, SetRegionOption,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, build_rows_for_key, flush_region, put_rows,
    reopen_region, rows_schema,
};

#[tokio::test]
async fn test_append_mode_write_query() {
    test_append_mode_write_query_with_format(false).await;
    test_append_mode_write_query_with_format(true).await;
}

async fn test_append_mode_write_query_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // rows 1, 2
    let rows = build_rows(1, 3);
    let rows = Rows {
        schema: column_schemas.clone(),
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let mut rows = build_rows(0, 2);
    rows.append(&mut build_rows(1, 2));
    // rows 0, 1, 1
    let rows = Rows {
        schema: column_schemas,
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));

    // Tries to use seq scan to test it under append mode.
    let scan = engine
        .scan_region(region_id, ScanRequest::default())
        .unwrap();
    let seq_scan = scan.seq_scan().await.unwrap();
    let stream = seq_scan.build_stream().unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_append_mode_compaction() {
    test_append_mode_compaction_with_format(false).await;
    test_append_mode_compaction_with_format(true).await;
}

async fn test_append_mode_compaction_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("append_mode", "true")
        .build();
    let table_dir = request.table_dir.clone();
    let region_opts = request.options.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 SSTs for compaction.
    // a, field 1, 2
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 1, 3, 1),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    // a, field 0, 1
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    // b, field 0, 1
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    let output = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(output.affected_rows, 0);

    // a, field 2, 3
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 2, 4, 2),
    };
    put_rows(&engine, region_id, rows).await;

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| a     | 2.0     | 1970-01-01T00:00:02 |
| a     | 2.0     | 1970-01-01T00:00:02 |
| a     | 3.0     | 1970-01-01T00:00:03 |
| b     | 0.0     | 1970-01-01T00:00:00 |
| b     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    // Scans in parallel.
    let mut scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(1, scanner.num_files());
    assert_eq!(1, scanner.num_memtables());
    scanner.set_target_partitions(2);
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));

    // Reopens engine.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    // Reopens the region.
    reopen_region(&engine, region_id, table_dir, false, region_opts).await;
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));
}

#[tokio::test]
async fn test_alter_append_mode_clears_merge_mode() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

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

    // Create a region with merge_mode=last_non_null.
    let request = CreateRequestBuilder::new()
        .insert_option("merge_mode", "last_non_null")
        .build();
    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Verify initial options.
    let options = &engine.get_region(region_id).unwrap().version().options;
    assert!(!options.append_mode);
    assert!(options.merge_mode.is_some());

    // Insert some data.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Alter append_mode to true (triggers flush since memtable is not empty).
    let alter_request = RegionAlterRequest {
        kind: AlterKind::SetRegionOptions {
            options: vec![SetRegionOption::AppendMode(true)],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await
        .unwrap();

    // Verify append_mode is true and merge_mode is cleared.
    let options = &engine.get_region(region_id).unwrap().version().options;
    assert!(options.append_mode);
    assert!(options.merge_mode.is_none());

    // Insert duplicate data (should be preserved in append mode).
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flush to persist.
    flush_region(&engine, region_id, None).await;

    let expected = "\
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

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));

    // Reopen engine and region to verify persistence.
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
            }),
        )
        .await
        .unwrap();

    // Verify options persist after reopen.
    let options = &engine.get_region(region_id).unwrap().version().options;
    assert!(options.append_mode);
    assert!(options.merge_mode.is_none());

    // Verify data persists (duplicates preserved).
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));
}

#[tokio::test]
async fn test_put_single_range() {
    test_put_single_range_with_format(false).await;
    test_put_single_range_with_format(true).await;
}

async fn test_put_single_range_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("append_mode", "true")
        .build();
    let table_dir = request.table_dir.clone();
    let region_opts = request.options.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // a, field 1, 2
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 1, 3, 1),
    };
    put_rows(&engine, region_id, rows).await;
    // a, field 0, 1
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    // b, field 0, 1
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    // a, field 2, 3
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 2, 4, 2),
    };
    put_rows(&engine, region_id, rows).await;

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| a     | 2.0     | 1970-01-01T00:00:02 |
| a     | 2.0     | 1970-01-01T00:00:02 |
| a     | 3.0     | 1970-01-01T00:00:03 |
| b     | 0.0     | 1970-01-01T00:00:00 |
| b     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    // Scans in parallel.
    let mut scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(0, scanner.num_files());
    assert_eq!(1, scanner.num_memtables());
    scanner.set_target_partitions(2);
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));

    // Flushes and scans.
    flush_region(&engine, region_id, None).await;
    let mut scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(1, scanner.num_files());
    assert_eq!(0, scanner.num_memtables());
    scanner.set_target_partitions(2);
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));

    // Reopens engine.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    // Reopens the region.
    reopen_region(&engine, region_id, table_dir, false, region_opts).await;
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));
}
