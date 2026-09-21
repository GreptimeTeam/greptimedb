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
use datafusion_expr::{col, lit};
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::UInt64Type;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::compaction::compactor::CompactionVersion;
use crate::config::MitoConfig;
use crate::manifest::action::RegionEdit;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_delete_rows_for_key, build_rows_with_fields, delete_rows,
    delete_rows_schema, flush_region, put_rows, reopen_region, rows_schema,
};

#[rstest::rstest]
#[tokio::test]
async fn test_admitted_sst_compaction_preserves_intermediate_memtable(
    #[values(false, true)] flat_format: bool,
    #[values(false, true)] strict_window: bool,
    #[values(false, true)] immutable: bool,
) {
    // Issue #9157: A@1 and admitted C@3 must not merge across memtable B@2.
    let mut env = TestEnv::new().await;
    let config = MitoConfig {
        default_flat_format: flat_format,
        schedule_compaction_after_edit: false,
        min_compaction_interval: std::time::Duration::from_secs(3600),
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
    let region_id = RegionId::new(1, 1);
    let source_id = RegionId::new(1, 2);
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
        .insert_option("compaction.twcs.time_window", "1h")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .insert_option("merge_mode", "last_non_null")
        .build();
    let table_dir = request.table_dir.clone();
    let region_opts = request.options.clone();
    let schema = rows_schema(&request);
    engine
        .handle_request(source_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    for (id, fields) in [(region_id, (Some(1), None)), (source_id, (None, Some(3)))] {
        put_rows(
            &engine,
            id,
            Rows {
                schema: schema.clone(),
                rows: build_rows_with_fields("a", &[10], &[fields]),
            },
        )
        .await;
        flush_region(&engine, id, None).await;
    }
    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: build_rows_with_fields("a", &[10], &[(Some(2), None)]),
        },
    )
    .await;
    let region = engine.get_region(region_id).unwrap();
    if immutable {
        region.version_control.freeze_mutable().unwrap();
    }
    let foreign = engine
        .get_region(source_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .next()
        .unwrap()
        .meta_ref()
        .clone();
    assert_eq!(source_id, foreign.region_id);
    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![foreign],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, region.version_control.committed_sequence());
    let before_ids = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap()
        .file_ids();
    assert_eq!(2, before_ids.len());
    let expected = "\
+-------+---------+---------+---------------------+
| tag_0 | field_0 | field_1 | ts                  |
+-------+---------+---------+---------------------+
| a     | 2.0     | 3.0     | 1970-01-01T00:00:10 |
+-------+---------+---------+---------------------+";
    let before = RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .pretty_print()
    .unwrap();
    assert_eq!(expected, before);
    let compact_request = || {
        if strict_window {
            RegionCompactRequest {
                options: api::v1::region::compact_request::Options::StrictWindow(
                    api::v1::region::StrictWindow {
                        window_seconds: 3600,
                    },
                ),
                ..Default::default()
            }
        } else {
            RegionCompactRequest::default()
        }
    };
    engine
        .handle_request(region_id, RegionRequest::Compact(compact_request()))
        .await
        .unwrap();
    let after = RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .pretty_print()
    .unwrap();
    assert_eq!(
        expected, after,
        "compaction must not promote A's x above memtable B"
    );
    let after_ids = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap()
        .file_ids();
    assert_eq!(before_ids, after_ids, "the unsafe closure must be deferred");

    let old_version = region.version();
    let snapshot = CompactionVersion::from(old_version.clone());
    assert_eq!(Some(2), snapshot.memtable_min_sequence);
    flush_region(&engine, region_id, None).await;
    // Flush publishes a new Version. Reusing the old snapshot cannot observe
    // empty memtables paired with its pre-flush SST set.
    assert_eq!(
        Some(2),
        CompactionVersion::from(old_version).memtable_min_sequence
    );
    assert_eq!(Some(2), snapshot.memtable_min_sequence);
    assert_eq!(
        None,
        CompactionVersion::from(region.version()).memtable_min_sequence
    );
    engine
        .handle_request(region_id, RegionRequest::Compact(compact_request()))
        .await
        .unwrap();
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "flush makes the complete A+B+C merge eligible"
    );
    let merged = RecordBatches::try_collect(scanner.scan().await.unwrap())
        .await
        .unwrap()
        .pretty_print()
        .unwrap();
    assert_eq!(expected, merged);
    let engine = env.reopen_engine(engine, config).await;
    reopen_region(&engine, region_id, table_dir, false, region_opts).await;
    let reopened = RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .pretty_print()
    .unwrap();
    assert_eq!(expected, reopened);
}

#[rstest::rstest]
#[tokio::test]
async fn test_partial_compaction_preserves_put_versions(
    #[values(false, true)] flat_format: bool,
    #[values("last_row", "last_non_null")] merge_mode: &str,
) {
    let mut env = TestEnv::new().await;
    let config = MitoConfig {
        default_flat_format: flat_format,
        min_compaction_interval: std::time::Duration::from_secs(3600),
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
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
        .insert_option("compaction.twcs.time_window", "1h")
        .insert_option("merge_mode", merge_mode)
        .build();
    let table_dir = request.table_dir.clone();
    let region_opts = request.options.clone();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Issue #9146: A(x=1), B(x=2) share a key and timestamp. For LastNonNull,
    // C(y=3) also shares that key: merging A+C must not promote A's x above B.
    // For LastRow, C is unrelated and B must remain outside the partial pick.
    let a = build_rows_with_fields("a", &[10], &[(Some(1), None)]);
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: schema.clone(),
            rows: a,
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    let mut b = build_rows_with_fields("a", &[10], &[(Some(2), None)]);
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(0);
    for _ in 0..3000 {
        b.extend(build_rows_with_fields(
            &format!("{:032x}", rand::Rng::random::<u128>(&mut rng)),
            &[1],
            &[(Some(2), None)],
        ));
    }
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: schema.clone(),
            rows: b,
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    let b_id = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .find(|file| file.meta_ref().num_rows == 3001)
        .unwrap()
        .file_id();
    let last_non_null = merge_mode == "last_non_null";
    let c = build_rows_with_fields(
        if last_non_null { "a" } else { "z" },
        &[10],
        &[(None, Some(3))],
    );
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: schema.clone(),
            rows: c,
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    // Sixteen small inputs plus the large middle-version file B. The normal
    // byte-balanced candidate excludes B.
    for i in 0..14 {
        let rows = build_rows_with_fields("z", &[100 + i], &[(Some(4), None)]);
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows,
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;
    }
    let scan = || ScanRequest {
        filters: vec![col("tag_0").eq(lit("a"))],
        ..Default::default()
    };
    let before =
        RecordBatches::try_collect(engine.scan_to_stream(region_id, scan()).await.unwrap())
            .await
            .unwrap()
            .pretty_print()
            .unwrap();
    let expected = if last_non_null {
        "\
+-------+---------+---------+---------------------+
| tag_0 | field_0 | field_1 | ts                  |
+-------+---------+---------+---------------------+
| a     | 2.0     | 3.0     | 1970-01-01T00:00:10 |
+-------+---------+---------+---------------------+"
    } else {
        "\
+-------+---------+---------+---------------------+
| tag_0 | field_0 | field_1 | ts                  |
+-------+---------+---------+---------------------+
| a     | 2.0     |         | 1970-01-01T00:00:10 |
+-------+---------+---------+---------------------+"
    };
    assert_eq!(expected, before);
    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    let after = RecordBatches::try_collect(engine.scan_to_stream(region_id, scan()).await.unwrap())
        .await
        .unwrap()
        .pretty_print()
        .unwrap();
    assert_eq!(
        expected, after,
        "flat_format={flat_format}, mode={merge_mode}"
    );
    let version = engine.get_region(region_id).unwrap().version();
    let files: Vec<_> = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .collect();
    assert_eq!(if last_non_null { 1 } else { 2 }, files.len());
    assert_eq!(
        !last_non_null,
        files.iter().any(|file| file.file_id() == b_id)
    );
    assert!(
        files
            .iter()
            .all(|file| !file.meta_ref().preserve_row_sequence)
    );
    let region = engine.get_region(region_id).unwrap();
    let mut output_sequences = std::collections::HashSet::new();
    for file in files.iter().filter(|file| file.file_id() != b_id) {
        let mut reader = region
            .access_layer
            .read_sst((*file).clone())
            .build()
            .await
            .unwrap()
            .unwrap();
        while let Some(batch) = reader.next_record_batch().await.unwrap() {
            output_sequences.extend(
                batch
                    .column(batch.num_columns() - 2)
                    .as_primitive::<UInt64Type>()
                    .values()
                    .iter()
                    .copied(),
            );
        }
    }
    assert!(
        output_sequences.len() > 1,
        "compaction must retain effective input sequences"
    );
    let engine = env.reopen_engine(engine, config).await;
    reopen_region(&engine, region_id, table_dir, false, region_opts).await;
    let reopened =
        RecordBatches::try_collect(engine.scan_to_stream(region_id, scan()).await.unwrap())
            .await
            .unwrap()
            .pretty_print()
            .unwrap();
    assert_eq!(expected, reopened);
}

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
            default_flat_format: flat_format,
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

    let request = CreateRequestBuilder::new()
        .field_num(2)
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "3")
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
                default_flat_format: flat_format,
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
