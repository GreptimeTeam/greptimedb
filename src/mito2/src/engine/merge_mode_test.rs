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

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_delete_rows_for_key, build_rows_with_fields, delete_rows,
    delete_rows_schema, flush_region, put_rows, reopen_region, rows_schema,
};

#[tokio::test]
async fn test_merge_mode_write_query() {
    test_merge_mode_write_query_with_format(false).await;
    test_merge_mode_write_query_with_format(true).await;
}

async fn test_merge_mode_write_query_with_format(flat_format: bool) {
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
        .field_num(2)
        .insert_option("merge_mode", "last_non_null")
        .build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = build_rows_with_fields(
        "a",
        &[1, 2, 3],
        &[(Some(1), None), (None, None), (None, Some(3))],
    );
    let rows = Rows {
        schema: column_schemas.clone(),
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let rows = build_rows_with_fields("a", &[2, 3], &[(Some(12), None), (Some(13), None)]);
    let rows = Rows {
        schema: column_schemas.clone(),
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let rows = build_rows_with_fields("a", &[1, 2], &[(Some(11), None), (Some(22), Some(222))]);
    let rows = Rows {
        schema: column_schemas,
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------+---------------------+
| tag_0 | field_0 | field_1 | ts                  |
+-------+---------+---------+---------------------+
| a     | 11.0    |         | 1970-01-01T00:00:01 |
| a     | 22.0    | 222.0   | 1970-01-01T00:00:02 |
| a     | 13.0    | 3.0     | 1970-01-01T00:00:03 |
+-------+---------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_merge_mode_compaction() {
    test_merge_mode_compaction_with_format(false).await;
    test_merge_mode_compaction_with_format(true).await;
}

async fn test_merge_mode_compaction_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

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
        .field_num(2)
        .insert_option("compaction.type", "twcs")
        .insert_option("merge_mode", "last_non_null")
        .build();
    let table_dir = request.table_dir.clone();
    let region_opts = request.options.clone();
    let delete_schema = delete_rows_schema(&request);
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 SSTs for compaction.
    // a, 1 => (1, null), 2 => (null, null), 3 => (null, 3), 4 => (4, 4)
    let rows = build_rows_with_fields(
        "a",
        &[1, 2, 3, 4],
        &[
            (Some(1), None),
            (None, None),
            (None, Some(3)),
            (Some(4), Some(4)),
        ],
    );
    let rows = Rows {
        schema: column_schemas.clone(),
        rows,
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // a, 1 => (null, 11), 2 => (2, null), 3 => (null, 13)
    let rows = build_rows_with_fields(
        "a",
        &[1, 2, 3],
        &[(None, Some(11)), (Some(2), None), (None, Some(13))],
    );
    let rows = Rows {
        schema: column_schemas.clone(),
        rows,
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // Delete a, 4
    let rows = Rows {
        schema: delete_schema.clone(),
        rows: build_delete_rows_for_key("a", 4, 5),
    };
    delete_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    let output = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(output.affected_rows, 0);

    // a, 1 => (21, null), 2 => (22, null)
    let rows = build_rows_with_fields("a", &[1, 2], &[(Some(21), None), (Some(22), None)]);
    let rows = Rows {
        schema: column_schemas.clone(),
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let expected = "\
+-------+---------+---------+---------------------+
| tag_0 | field_0 | field_1 | ts                  |
+-------+---------+---------+---------------------+
| a     | 21.0    | 11.0    | 1970-01-01T00:00:01 |
| a     | 22.0    |         | 1970-01-01T00:00:02 |
| a     |         | 13.0    | 1970-01-01T00:00:03 |
+-------+---------+---------+---------------------+";
    // Scans in parallel.
    let mut scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(2, scanner.num_files());
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
