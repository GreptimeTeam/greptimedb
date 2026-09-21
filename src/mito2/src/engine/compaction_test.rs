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

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use api::v1::{ColumnSchema, Rows};
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use common_time::Timestamp;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::TimestampMillisecondType;
use parquet::basic::{Encoding, Type as PhysicalType};
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::AlterKind::SetRegionOptions;
use store_api::region_request::{
    EnterStagingRequest, PathType, RegionAlterRequest, RegionCloseRequest, RegionCompactRequest,
    RegionDeleteRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest,
    RegionTruncateRequest, SetRegionOption, StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::{Notify, Semaphore};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::{CompactionListener, EventListener};
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows_for_key, column_metadata_to_column_schema, put_rows,
};
use crate::time_provider::mock::MockTimeProvider;

pub(crate) async fn put_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };
    put_rows(engine, region_id, rows).await;

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

async fn flush(engine: &MitoEngine, region_id: RegionId) {
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

pub(crate) async fn compact(engine: &MitoEngine, region_id: RegionId) {
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);
}

pub(crate) async fn delete_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let row_cnt = rows.len();
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest {
                rows,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(row_cnt, result.affected_rows);

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

async fn collect_stream_ts(stream: SendableRecordBatchStream) -> Vec<i64> {
    let mut res = Vec::new();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    for batch in batches {
        let ts_col = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        res.extend((0..ts_col.len()).map(|i| ts_col.value(i)));
    }
    res
}

#[rstest::rstest]
#[case::split("1B", true)]
#[case::unlimited("0B", false)]
#[tokio::test]
async fn test_strict_window_output_file_size(
    #[values(false, true)] flat_format: bool,
    #[case] max_output_file_size: &str,
    #[case] expect_split: bool,
) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.max_output_file_size", max_output_file_size)
        .insert_option("max_row_group_row_count", "50")
        .build();
    let columns = crate::test_util::rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Each input SST spans both windows. Each series is large enough to exceed
    // the tiny threshold, and must remain intact within its output window.
    for key in ["a", "b", "c"] {
        let rows = [0, 3600]
            .into_iter()
            .flat_map(|start| build_rows_for_key(key, start, start + 1000, start))
            .collect();
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: columns.clone(),
                rows,
            },
        )
        .await;
        flush(&engine, region_id).await;
    }
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let before = RecordBatches::try_collect(stream).await.unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest {
                options: api::v1::region::compact_request::Options::StrictWindow(
                    api::v1::region::StrictWindow {
                        window_seconds: 3600,
                    },
                ),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    let version = engine.get_region(region_id).unwrap().version();
    assert!(version.ssts.levels()[0].files.is_empty());
    let primary_key_mapper = version.ssts.primary_key_mapper();
    let files = version.ssts.levels()[1].files().collect::<Vec<_>>();
    assert_eq!(
        6000,
        files
            .iter()
            .map(|file| file.meta_ref().num_rows)
            .sum::<u64>()
    );
    for start in [0, 3600] {
        let window_files = files
            .iter()
            .filter(|file| {
                let (min, max) = file.time_range();
                min >= Timestamp::new_second(start) && max < Timestamp::new_second(start + 3600)
            })
            .collect::<Vec<_>>();
        if expect_split {
            assert!(window_files.len() > 1, "each window must split by size");
        } else {
            assert_eq!(1, window_files.len(), "zero must disable size splitting");
        }
        let mut ranges = window_files
            .iter()
            .map(|file| file.primary_key_range(&primary_key_mapper).unwrap())
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        assert!(ranges.windows(2).all(|pair| pair[0].1 < pair[1].0));
    }
    assert!(files.iter().all(|file| {
        let (min, max) = file.time_range();
        min.value() / 3_600_000 == max.value() / 3_600_000
    }));

    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let after = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        sort_batches_and_print(&before, &["tag_0", "ts"]),
        sort_batches_and_print(&after, &["tag_0", "ts"])
    );
}

/// Flush may collapse versions within one file, but compaction must not promote
/// them, even with no external overlaps or after rewriting a previous output.
#[rstest::rstest]
#[case(false, false)]
#[case(true, false)]
#[case(true, true)]
#[tokio::test]
async fn test_compaction_preserves_flush_sequences_across_generations(
    #[values(false, true)] flat_format: bool,
    #[case] append_mode: bool,
    #[case] preserve_row_sequence: bool,
) {
    let mut env = TestEnv::new().await;
    let config = MitoConfig {
        default_flat_format: flat_format,
        min_compaction_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", &append_mode.to_string())
        .insert_option("preserve_row_sequence", &preserve_row_sequence.to_string())
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();
    let table_dir = request.table_dir.clone();
    let options = request.options.clone();
    let columns = crate::test_util::rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    async fn read_sequences(engine: &MitoEngine, region_id: RegionId) -> Vec<u64> {
        let region = engine.get_region(region_id).unwrap();
        let mut sequences = Vec::new();
        for file in region
            .version()
            .ssts
            .levels()
            .iter()
            .flat_map(|l| l.files())
        {
            let mut reader = region
                .access_layer
                .read_sst(file.clone())
                .build()
                .await
                .unwrap()
                .unwrap();
            while let Some(batch) = reader.next_record_batch().await.unwrap() {
                sequences.extend_from_slice(
                    batch
                        .column(batch.num_columns() - 2)
                        .as_primitive::<datatypes::arrow::datatypes::UInt64Type>()
                        .values(),
                );
            }
        }
        sequences.sort_unstable();
        sequences
    }

    let mut expected = Vec::new();
    for generation in 0..=2 {
        // Interleaving time ranges exercise a merge without duplicate rows.
        let rows = [generation, generation + 10]
            .into_iter()
            .flat_map(|ts| build_rows_for_key("a", ts, ts + 1, 0))
            .collect();
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: columns.clone(),
                rows,
            },
        )
        .await;
        flush(&engine, region_id).await;
        let max = (generation * 2 + 2) as u64;
        expected.extend(if preserve_row_sequence {
            [max - 1, max]
        } else {
            [max, max]
        });
        assert_eq!(expected, read_sequences(&engine, region_id).await);
        if generation == 0 {
            continue;
        }
        engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest {
                    options: api::v1::region::compact_request::Options::StrictWindow(
                        api::v1::region::StrictWindow {
                            window_seconds: 3600,
                        },
                    ),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let version = engine.get_region(region_id).unwrap().version();
        let files: Vec<_> = version
            .ssts
            .levels()
            .iter()
            .flat_map(|l| l.files())
            .collect();
        assert_eq!(
            1,
            files.len(),
            "generation {generation} must merge all inputs"
        );
        assert_eq!(
            preserve_row_sequence,
            files[0].meta_ref().preserve_row_sequence
        );
        assert_eq!(Some(max), files[0].meta_ref().sequence.map(|s| s.get()));
        assert_eq!(
            expected,
            read_sequences(&engine, region_id).await,
            "generation {generation}"
        );
    }
    let engine = env.reopen_engine(engine, config).await;
    crate::test_util::reopen_region(&engine, region_id, table_dir, false, options).await;
    assert_eq!(expected, read_sequences(&engine, region_id).await);
}

#[tokio::test]
async fn test_partial_compaction_preserves_delete_order_flat() {
    assert_partial_compaction_preserves_delete_order(true).await;
}

#[tokio::test]
async fn test_partial_compaction_preserves_delete_order_primary_key() {
    assert_partial_compaction_preserves_delete_order(false).await;
}

/// A newer unrelated input must not promote an old Put above an unselected Delete.
/// All files fit in the same one-hour window; no cross-window SST is required.
async fn assert_partial_compaction_preserves_delete_order(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let (engine, columns) = env_for_manual_compaction(&mut env, region_id, flat_format).await;

    put_and_flush(&engine, region_id, &columns, 10..16).await;
    let mut deletes = build_rows_for_key("a", 10, 16, 0);
    // Distinct keys keep the Delete SST large even after compression, so the
    // byte-balance rule excludes it without fabricating FileMeta sizes.
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(0);
    for _ in 0..3000 {
        deletes.extend(build_rows_for_key(
            &format!("{:032x}", rand::Rng::random::<u128>(&mut rng)),
            1,
            2,
            0,
        ));
    }
    engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest {
                rows: Rows {
                    schema: columns.clone(),
                    rows: deletes,
                },
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    flush(&engine, region_id).await;
    // These newer writes are legitimate, but must not change the version of 10..16.
    for i in 10..25 {
        put_and_flush(&engine, region_id, &columns, i * 10..i * 10 + 6).await;
    }

    let region = engine.get_region(region_id).unwrap();
    let table_dir = region.table_dir().to_string();
    let version = region.version();
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .collect::<Vec<_>>();
    assert_eq!(17, files.len());
    assert!(files.iter().all(|file| {
        file.time_range().0 >= Timestamp::new_millisecond(0)
            && file.time_range().1 < Timestamp::new_millisecond(3_600_000)
    }));
    let delete_file = files
        .iter()
        .find(|file| file.meta_ref().num_rows == 3006)
        .unwrap();
    let delete_id = delete_file.file_id();
    let delete_sequence = delete_file.meta_ref().sequence;
    let old_ids = files
        .iter()
        .map(|file| file.file_id())
        .collect::<HashSet<_>>();
    let before = collect_stream_ts(
        engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap()
            .scan()
            .await
            .unwrap(),
    )
    .await;
    let expected = (10..25)
        .flat_map(|i| (i * 10..i * 10 + 6).map(|ts| ts * 1000))
        .collect::<Vec<i64>>();
    assert_eq!(
        expected, before,
        "the Delete must hide old rows before compaction"
    );

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let current_ids = scanner.file_ids();
    assert_eq!(
        2,
        current_ids.len(),
        "the 16 small inputs must merge, leaving the large Delete SST"
    );
    assert!(current_ids.contains(&delete_id));
    assert_eq!(
        1,
        current_ids.iter().filter(|id| old_ids.contains(id)).count()
    );
    let after = collect_stream_ts(scanner.scan().await.unwrap()).await;

    let current = region.version();
    let output = current
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .find(|file| !old_ids.contains(&file.file_id()))
        .unwrap();
    let mut reader = region
        .access_layer
        .read_sst(output.clone())
        .build()
        .await
        .unwrap()
        .unwrap();
    let batch = reader.next_record_batch().await.unwrap().unwrap();
    let output_sequences = batch
        .column(batch.num_columns() - 2)
        .as_primitive::<datatypes::arrow::datatypes::UInt64Type>();
    let first_output_sequence = output_sequences.value(0);

    crate::test_util::reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    let reopened = collect_stream_ts(
        engine
            .scanner(region_id, ScanRequest::default())
            .await
            .unwrap()
            .scan()
            .await
            .unwrap(),
    )
    .await;
    let resurrected = after
        .iter()
        .filter(|ts| (10_000..16_000).contains(*ts))
        .collect::<Vec<_>>();
    assert!(
        after == expected && reopened == expected,
        "partial compaction changed delete ordering (flat_format={flat_format}, unselected Delete sequence={delete_sequence:?}, first output row sequence={first_output_sequence}); expected rows={}, after rows={}, reopened rows={}, resurrected timestamps={resurrected:?}",
        expected.len(),
        after.len(),
        reopened.len()
    );
}

/// Real-engine regression for the tombstone resurrection chain described in
/// https://greptime.feishu.cn/wiki/L2WYwIM40iG9tckowrnctyw8nQc.
/// No synthetic file sizes, PK ranges, or manually selected compaction inputs.
#[rstest::rstest]
#[tokio::test]
async fn test_cross_schema_compaction_keeps_rows_deleted(
    #[values(false, true)] flat_format: bool,
    #[values(false, true)] cross_window: bool,
    #[values(None, Some(""))] tag_default: Option<&str>,
) {
    use api::v1::SemanticType;
    use api::v1::value::ValueData;
    use datatypes::prelude::{ConcreteDataType, Value};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema as DataColumnSchema};
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::{AddColumn, AddColumnLocation, AlterKind};

    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let (engine, mut columns) = env_for_manual_compaction_with_window(
        &mut env,
        region_id,
        flat_format,
        if cross_window { "2h" } else { "1h" },
    )
    .await;
    let (target_ts, other_ts) = if cross_window { (3500, 3700) } else { (10, 1) };

    // Incompressible, distinct a* keys keep the old SST large enough for a
    // partial pick to leave it behind. Its maximum PK is exactly the victim b.
    let mut old_rows = build_rows_for_key("b", target_ts, target_ts + 1, 0);
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(0);
    for _ in 0..3000 {
        old_rows.extend(build_rows_for_key(
            &format!("a{:032x}", rand::Rng::random::<u128>(&mut rng)),
            other_ts,
            other_ts + 1,
            0,
        ));
    }
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: columns.clone(),
            rows: old_rows,
        },
    )
    .await;
    flush(&engine, region_id).await;
    let old_version = engine.get_region(region_id).unwrap().version();
    let old_files = old_version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .collect::<Vec<_>>();
    assert_eq!(1, old_files.len());
    let old_file = old_files[0];
    if cross_window {
        // This real SST crosses the new one-hour boundary. TWCS assigns it by
        // max_ts=3700 to window 7200, unlike Delete(b,3500) in window 3600.
        set_compaction_window(&engine, region_id, "1h").await;
    }

    let added = ColumnMetadata {
        column_id: 3,
        semantic_type: SemanticType::Tag,
        column_schema: DataColumnSchema::new("tag_1", ConcreteDataType::string_datatype(), true)
            .with_default_constraint(
                tag_default.map(|value| ColumnDefaultConstraint::Value(Value::from(value))),
            )
            .unwrap(),
    };
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::AddColumns {
                    columns: vec![AddColumn {
                        column_metadata: added.clone(),
                        location: Some(AddColumnLocation::First),
                    }],
                },
            }),
        )
        .await
        .unwrap();
    columns.push(column_metadata_to_column_schema(&added));
    let append_tag = |mut rows: Vec<api::v1::Row>| {
        for row in &mut rows {
            row.values.push(api::v1::Value {
                value_data: tag_default.map(|value| ValueData::StringValue(value.to_string())),
            });
        }
        Rows {
            schema: columns.clone(),
            rows,
        }
    };
    let deletes = ["b", "c"]
        .into_iter()
        .flat_map(|key| build_rows_for_key(key, target_ts, target_ts + 1, 0))
        .collect();
    engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest {
                rows: append_tag(deletes),
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    flush(&engine, region_id).await;
    for ts in target_ts + 10..target_ts + 25 {
        put_rows(
            &engine,
            region_id,
            append_tag(build_rows_for_key("c", ts, ts + 1, 0)),
        )
        .await;
        flush(&engine, region_id).await;
    }

    let region = engine.get_region(region_id).unwrap();
    let table_dir = region.table_dir().to_string();
    let version = region.version();
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .collect::<Vec<_>>();
    assert_eq!(17, files.len());
    assert_eq!(vec![0, 3], version.metadata.primary_key);
    let new_ids = files
        .iter()
        .filter(|file| file.file_id() != old_file.file_id())
        .map(|file| file.file_id())
        .collect::<HashSet<_>>();
    for file in files
        .iter()
        .filter(|file| new_ids.contains(&file.file_id()))
    {
        assert!(old_file.meta_ref().primary_key_max < file.meta_ref().primary_key_min);
        assert!(file.time_range().1 < Timestamp::new_second(3600));
    }
    assert_eq!(
        cross_window,
        old_file.time_range().1 > Timestamp::new_second(3600)
    );

    let picked = preview_regular_compaction(&engine, region_id).await;
    assert_eq!(1, picked.outputs.len());
    let output = &picked.outputs[0];
    assert_eq!(
        new_ids,
        output.inputs.iter().map(|file| file.file_id()).collect()
    );
    let filter_deleted = output.filter_deleted;
    let before = sorted_scan_timestamps(&engine, region_id).await;
    assert_eq!(3015, before.len());
    assert!(!before.contains(&(target_ts as i64 * 1000)));

    compact(&engine, region_id).await;

    let after_version = region.version();
    let after_files = after_version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .collect::<Vec<_>>();
    assert_eq!(2, after_files.len());
    assert!(
        after_files
            .iter()
            .any(|file| file.file_id() == old_file.file_id())
    );
    let output_file = after_files
        .into_iter()
        .find(|file| file.file_id() != old_file.file_id())
        .unwrap();
    assert!(!new_ids.contains(&output_file.file_id()));
    let output_rows = output_file.meta_ref().num_rows;
    let output_deletes = count_sst_deletes(&engine, region_id, output_file).await;
    let after = sorted_scan_timestamps(&engine, region_id).await;

    let config = MitoConfig {
        default_flat_format: flat_format,
        min_compaction_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let engine = env.reopen_engine(engine, config).await;
    crate::test_util::reopen_region(&engine, region_id, table_dir, false, HashMap::new()).await;
    let reopened = sorted_scan_timestamps(&engine, region_id).await;
    // Do not fail on filter_deleted before actually executing compaction/reopen:
    // the negative-control run must demonstrate data resurrection, not just a bad plan.
    assert!(
        !filter_deleted
            && output_rows == 17
            && output_deletes == 2
            && after == before
            && reopened == before,
        "cross-schema tombstone loss: flat_format={flat_format}, cross_window={cross_window}, default={tag_default:?}, selected={}, filter_deleted={filter_deleted}, output_rows={output_rows}, output_deletes={output_deletes}, before={}, after={}, reopened={}, resurrected_after={}, resurrected_reopen={}",
        new_ids.len(),
        before.len(),
        after.len(),
        reopened.len(),
        after.contains(&(target_ts as i64 * 1000)),
        reopened.contains(&(target_ts as i64 * 1000)),
    );
}

async fn count_sst_deletes(
    engine: &MitoEngine,
    region_id: RegionId,
    file: &crate::sst::file::FileHandle,
) -> usize {
    use api::v1::OpType;
    use datatypes::arrow::datatypes::UInt8Type;

    let region = engine.get_region(region_id).unwrap();
    let mut reader = region
        .access_layer
        .read_sst(file.clone())
        .build()
        .await
        .unwrap()
        .unwrap();
    let mut deletes = 0;
    while let Some(batch) = reader.next_record_batch().await.unwrap() {
        deletes += batch
            .column(batch.num_columns() - 1)
            .as_primitive::<UInt8Type>()
            .values()
            .iter()
            .filter(|op| **op == OpType::Delete as u8)
            .count();
    }
    deletes
}

async fn set_compaction_window(engine: &MitoEngine, region_id: RegionId, window: &str) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: SetRegionOptions {
                    options: vec![SetRegionOption::Twsc(
                        "compaction.twcs.time_window".into(),
                        window.into(),
                    )],
                },
            }),
        )
        .await
        .unwrap();
}

async fn sorted_scan_timestamps(engine: &MitoEngine, region_id: RegionId) -> Vec<i64> {
    let stream = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap()
        .scan()
        .await
        .unwrap();
    let mut timestamps = collect_stream_ts(stream).await;
    timestamps.sort_unstable();
    timestamps
}

async fn preview_regular_compaction(
    engine: &MitoEngine,
    region_id: RegionId,
) -> crate::compaction::picker::PickerOutput {
    use crate::compaction::compactor::CompactionRegion;
    use crate::compaction::picker::new_picker;

    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let picker = new_picker(
        &RegionCompactRequest::default().options,
        &version.options,
        None,
        None,
    );
    let compaction_region = CompactionRegion {
        region_id,
        region_options: version.options.clone(),
        engine_config: Arc::new(MitoConfig::default()),
        region_metadata: version.metadata.clone(),
        cache_manager: engine.cache_manager(),
        access_layer: region.access_layer.clone(),
        manifest_ctx: region.manifest_ctx.clone(),
        current_version: version.into(),
        file_purger: None,
        ttl: None,
        max_parallelism: 1,
        plugins: common_base::Plugins::new(),
    };
    picker.pick(&compaction_region).await.unwrap().unwrap()
}

struct CompactionListenerGuard(Option<Arc<CompactionListener>>);

impl CompactionListenerGuard {
    fn new(listener: Arc<CompactionListener>) -> Self {
        Self(Some(listener))
    }

    fn release(mut self) {
        self.0.take().unwrap().wake();
    }
}

impl Drop for CompactionListenerGuard {
    fn drop(&mut self) {
        if let Some(listener) = self.0.take() {
            listener.wake();
        }
    }
}

/// Test gate that blocks selected compaction lifecycle phases for one region.
struct CompactionPlanningGate {
    region_id: RegionId,
    armed: AtomicBool,
    entered: Notify,
    cancel_requested: Notify,
    permits: Semaphore,
    merge_armed: AtomicBool,
    merge_entered: Notify,
    merge_permits: Semaphore,
    commit_armed: AtomicBool,
    commit_entered: Notify,
    commit_permits: Semaphore,
    pending_ddl_armed: AtomicBool,
    pending_ddl_entered: Notify,
    pending_ddl_permits: Semaphore,
}

/// Releases an armed [`CompactionPlanningGate`] when a test exits unexpectedly.
struct CompactionPlanningGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionPlanningGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release();
    }
}

impl Drop for CompactionPlanningGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release();
        }
    }
}

/// Releases an armed merge gate when a test exits unexpectedly.
struct CompactionMergeGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionMergeGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release_merge();
    }
}

impl Drop for CompactionMergeGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release_merge();
        }
    }
}

/// Releases an armed commit gate when a test exits unexpectedly.
struct CompactionCommitGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionCommitGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release_commit();
    }
}

impl Drop for CompactionCommitGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release_commit();
        }
    }
}

/// Releases an armed pending-DDL dispatch gate when a test exits unexpectedly.
struct CompactionPendingDdlGateGuard {
    gate: Option<Arc<CompactionPlanningGate>>,
}

impl CompactionPendingDdlGateGuard {
    fn release(mut self) {
        self.gate.take().unwrap().release_pending_ddl_dispatch();
    }
}

impl Drop for CompactionPendingDdlGateGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release_pending_ddl_dispatch();
        }
    }
}

impl CompactionPlanningGate {
    fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            cancel_requested: Notify::new(),
            permits: Semaphore::new(0),
            merge_armed: AtomicBool::new(false),
            merge_entered: Notify::new(),
            merge_permits: Semaphore::new(0),
            commit_armed: AtomicBool::new(false),
            commit_entered: Notify::new(),
            commit_permits: Semaphore::new(0),
            pending_ddl_armed: AtomicBool::new(false),
            pending_ddl_entered: Notify::new(),
            pending_ddl_permits: Semaphore::new(0),
        }
    }

    fn arm(self: &Arc<Self>) -> CompactionPlanningGateGuard {
        self.armed.store(true, Ordering::Relaxed);
        CompactionPlanningGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    async fn wait_until_cancel_requested(&self) {
        self.cancel_requested.notified().await;
    }

    fn arm_merge(self: &Arc<Self>) -> CompactionMergeGateGuard {
        self.merge_armed.store(true, Ordering::Relaxed);
        CompactionMergeGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_merge_entered(&self) {
        self.merge_entered.notified().await;
    }

    fn arm_commit(self: &Arc<Self>) -> CompactionCommitGateGuard {
        self.commit_armed.store(true, Ordering::Relaxed);
        CompactionCommitGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_commit_entered(&self) {
        self.commit_entered.notified().await;
    }

    fn arm_pending_ddl_dispatch(self: &Arc<Self>) -> CompactionPendingDdlGateGuard {
        self.pending_ddl_armed.store(true, Ordering::Relaxed);
        CompactionPendingDdlGateGuard {
            gate: Some(self.clone()),
        }
    }

    async fn wait_until_pending_ddl_dispatch(&self) {
        self.pending_ddl_entered.notified().await;
    }

    fn release(&self) {
        self.permits.add_permits(1);
    }

    fn release_merge(&self) {
        self.merge_permits.add_permits(1);
    }

    fn release_commit(&self) {
        self.commit_permits.add_permits(1);
    }

    fn release_pending_ddl_dispatch(&self) {
        self.pending_ddl_permits.add_permits(1);
    }
}

#[async_trait]
impl EventListener for CompactionPlanningGate {
    async fn on_compaction_pick_begin(&self, region_id: RegionId) {
        if region_id != self.region_id {
            return;
        }

        if !self.armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.entered.notify_one();
        self.permits.acquire().await.unwrap().forget();
    }

    async fn on_merge_ssts_finished(&self, region_id: RegionId) {
        if region_id != self.region_id || !self.merge_armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.merge_entered.notify_one();
        self.merge_permits.acquire().await.unwrap().forget();
    }

    async fn on_compaction_commit_begin(&self, region_id: RegionId) {
        if region_id != self.region_id || !self.commit_armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.commit_entered.notify_one();
        self.commit_permits.acquire().await.unwrap().forget();
    }

    async fn on_compaction_result_notified(&self, region_id: RegionId) {
        if region_id != self.region_id || !self.pending_ddl_armed.swap(false, Ordering::Relaxed) {
            return;
        }

        self.pending_ddl_entered.notify_one();
        self.pending_ddl_permits.acquire().await.unwrap().forget();
    }

    fn on_compaction_cancel_requested(&self, region_id: RegionId) {
        if region_id == self.region_id {
            self.cancel_requested.notify_one();
        }
    }
}

#[tokio::test]
async fn test_planning_followup_updates_schedule_time() {
    assert_automatic_followup_updates_schedule_time(0).await;
}

#[tokio::test]
async fn test_execution_followup_updates_schedule_time() {
    assert_automatic_followup_updates_schedule_time(4).await;
}

async fn assert_automatic_followup_updates_schedule_time(preexisting_flushes: usize) {
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let interval = Duration::from_secs(60 * 60);
    let initial_time = 1_000;
    let time_provider = Arc::new(MockTimeProvider::new(initial_time));
    let engine = env
        .create_engine_with_time(
            MitoConfig {
                min_compaction_interval: interval,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            time_provider.clone(),
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "automatic_followup_interval",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let column_schemas = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();

    for offset in 0..preexisting_flushes {
        put_and_flush(
            &engine,
            region_id,
            &column_schemas,
            offset * 10..offset * 10 + 10,
        )
        .await;
    }

    let first_schedule_time = initial_time + interval.as_millis() as i64;
    time_provider.set_now(first_schedule_time);
    let gate_guard = gate.arm();
    let first_start = preexisting_flushes * 10;
    put_and_flush(
        &engine,
        region_id,
        &column_schemas,
        first_start..first_start + 10,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("initial automatic planning did not reach the gate");

    let trigger_time = first_schedule_time + interval.as_millis() as i64;
    time_provider.set_now(trigger_time);
    put_and_flush(
        &engine,
        region_id,
        &column_schemas,
        first_start + 10..first_start + 20,
    )
    .await;
    let followup_schedule_time = trigger_time + 1;
    time_provider.set_now(followup_schedule_time);
    gate_guard.release();

    let region = engine.get_region(region_id).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while region.last_schedule_compaction_millis() != followup_schedule_time {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("automatic follow-up did not update its schedule time");
}

#[tokio::test]
async fn test_region_b_progresses_while_same_worker_region_a_is_picking() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(2, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_a));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                min_compaction_interval: Duration::ZERO,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;

    for (region_id, table_name) in [(region_a, "region_a"), (region_b, "region_b")] {
        env.get_schema_metadata_manager()
            .register_region_table_info(
                region_id.table_id(),
                table_name,
                "test_catalog",
                "test_schema",
                None,
                env.get_kv_backend(),
            )
            .await;
        engine
            .handle_request(
                region_id,
                RegionRequest::Create(
                    CreateRequestBuilder::new()
                        .insert_option("compaction.type", "twcs")
                        .build(),
                ),
            )
            .await
            .unwrap();
    }

    let request = CreateRequestBuilder::new().build();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    let gate_guard = gate.arm();
    let engine_for_compaction = engine.clone();
    let region_a_compaction = tokio::spawn(async move {
        engine_for_compaction
            .handle_request(
                region_a,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("region A planning did not reach the gate");

    let engine_for_region_b = engine.clone();
    let mut region_b_work = tokio::spawn(async move {
        put_and_flush(&engine_for_region_b, region_b, &column_schemas, 0..10).await;
    });
    tokio::time::timeout(Duration::from_secs(5), &mut region_b_work)
        .await
        .expect("region B was blocked by region A compaction planning")
        .expect("region B work task panicked");
    gate_guard.release();
    tokio::time::timeout(Duration::from_secs(5), region_a_compaction)
        .await
        .expect("region A compaction task did not finish after gate release")
        .expect("region A compaction task panicked")
        .expect("region A compaction failed");
}

#[tokio::test]
async fn test_picking_close_reopen_ignores_old_plan() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(3, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "close_reopen",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = create.table_dir.clone();
    let options = create.options.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("closed region compaction waiter was not released")
        .expect("closed region compaction task panicked")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options,
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

    gate_guard.release();
    tokio::time::timeout(Duration::from_secs(5), compact(&engine, region_id))
        .await
        .expect("replacement compaction was blocked by the stale plan");
    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_enter_staging_waits_for_picking_logical_cancellation_ack() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(4, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "enter_staging",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .insert_option("compaction.type", "twcs")
                    .build(),
            ),
        )
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");
    let staging_engine = engine.clone();
    let staging_task = tokio::spawn(async move {
        staging_engine
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_cancel_requested())
        .await
        .expect("enter-staging did not request picking cancellation");
    assert!(!compact_task.is_finished());
    assert!(!staging_task.is_finished());

    gate_guard.release();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("cancelled compaction waiter was not released")
        .expect("cancelled compaction task panicked")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    tokio::time::timeout(Duration::from_secs(5), staging_task)
        .await
        .expect("enter-staging did not finish after cancellation acknowledgment")
        .expect("enter-staging task panicked")
        .expect("enter-staging request failed");
    assert!(engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_truncate_waits_for_non_cancellable_compaction_commit() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(10, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "truncate_during_compaction_commit",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build();
    let column_schemas = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 30..40).await;

    let commit_guard = gate.arm_commit();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_commit_entered())
        .await
        .expect("compaction did not reach the non-cancellable commit gate");

    let truncate_engine = engine.clone();
    let mut truncate_task = tokio::spawn(async move {
        truncate_engine
            .handle_request(
                region_id,
                RegionRequest::Truncate(RegionTruncateRequest::ByTimeRanges {
                    time_ranges: vec![(
                        Timestamp::new_millisecond(0),
                        Timestamp::new_millisecond(39_000),
                    )],
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            biased;
            result = &mut truncate_task => {
                panic!("truncate completed before compaction terminal completion: {result:?}");
            }
            () = gate.wait_until_cancel_requested() => {}
        }
    })
    .await
    .expect("truncate was not queued behind non-cancellable compaction");
    assert!(!truncate_task.is_finished());

    let pending_ddl_guard = gate.arm_pending_ddl_dispatch();
    commit_guard.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        gate.wait_until_pending_ddl_dispatch(),
    )
    .await
    .expect("compaction terminal result did not reach pending truncate dispatch");
    tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("compaction waiter was not released at terminal completion")
        .expect("compaction task panicked")
        .expect("compaction failed");
    assert!(!truncate_task.is_finished());

    pending_ddl_guard.release();
    tokio::time::timeout(Duration::from_secs(5), truncate_task)
        .await
        .expect("queued truncate did not finish after compaction completion")
        .expect("truncate task panicked")
        .expect("queued truncate failed");

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(
        collect_stream_ts(scanner.scan().await.unwrap())
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn test_worker_shutdown_fails_picking_waiter() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(5, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "worker_shutdown",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .insert_option("compaction.type", "twcs")
                    .build(),
            ),
        )
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");

    tokio::time::timeout(Duration::from_secs(5), engine.stop())
        .await
        .expect("worker shutdown blocked on picking")
        .unwrap();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("worker shutdown did not release the compaction waiter")
        .expect("compaction task panicked during worker shutdown")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    gate_guard.release();
}

#[tokio::test]
async fn test_compaction_region() {
    test_compaction_region_with_format(false).await;
    test_compaction_region_with_format(true).await;
}

async fn test_compaction_region_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .insert_option("experimental_sst_float_field_encoding", "byte_stream_split")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 5 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 15..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 15..25).await;

    compact(&engine, region_id).await;

    let region = engine.get_region(region_id).unwrap();
    let file = region.version().ssts.levels()[1]
        .files
        .values()
        .next()
        .expect("compaction output SST")
        .clone();
    let reader = region
        .access_layer
        .read_sst(file)
        .build()
        .await
        .unwrap()
        .expect("compaction output SST reader");
    assert!(
        reader
            .parquet_metadata()
            .row_groups()
            .iter()
            .flat_map(|row_group| row_group.columns())
            .any(|column| {
                column.column_path().string() == "field_0"
                    && column.column_type() == PhysicalType::DOUBLE
                    && column
                        .encodings()
                        .any(|encoding| encoding == Encoding::BYTE_STREAM_SPLIT)
            })
    );

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Input:
    // [0..9]
    //       [10...19]
    //                [20....29]
    //          -[15.........29]- (delete)
    //           [15.....24]
    // Count-first compaction consumes the first 4 SSTs as soon as the trigger is reached.
    // The compacted output and final flush leave 2 SSTs.
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..25).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_infer_compaction_time_window() {
    test_infer_compaction_time_window_with_format(false).await;
    test_infer_compaction_time_window_with_format(true).await;
}

async fn test_infer_compaction_time_window_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // time window should be absent
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window
            .is_none()
    );

    put_and_flush(&engine, region_id, &column_schemas, 1..2).await;
    put_and_flush(&engine, region_id, &column_schemas, 2..3).await;
    put_and_flush(&engine, region_id, &column_schemas, 3..4).await;
    put_and_flush(&engine, region_id, &column_schemas, 4..5).await;

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    assert_eq!(
        Duration::from_secs(3600),
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window
            .unwrap()
    );

    // write two rows to trigger another flush.
    // note: this two rows still use the original part_duration (1day by default), so they are written
    // to the same time partition and flushed to one file.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 3601, 3602, 0),
        },
    )
    .await;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 7201, 7202, 0),
        },
    )
    .await;
    // this flush should update part_duration in TimePartitions.
    flush(&engine, region_id).await;
    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    // These data should use new part_duration in TimePartitions and get written to two different
    // time partitions so we end up with 4 ssts.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 3601, 3602, 0),
        },
    )
    .await;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 7201, 7202, 0),
        },
    )
    .await;
    flush(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        4,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
}

#[tokio::test]
async fn test_compaction_overlapping_files() {
    test_compaction_overlapping_files_with_format(false).await;
    test_compaction_overlapping_files_with_format(true).await;
}

async fn test_compaction_overlapping_files_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 5 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    delete_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 30..40).await;

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!(
        vec,
        (0..=9)
            .map(|v| v * 1000)
            .chain((20..=29).map(|v| v * 1000))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_compaction_region_with_overlapping() {
    test_compaction_region_with_overlapping_with_format(false).await;
    test_compaction_region_with_overlapping_with_format(true).await;
}

async fn test_compaction_region_with_overlapping_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 4 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 0..2400).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 3600..10800).await; // window 10800
    delete_and_flush(&engine, region_id, &column_schemas, 0..3600).await; // window 3600

    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((3600..10800).map(|i| { i * 1000 }).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_compaction_region_with_overlapping_delete_all() {
    test_compaction_region_with_overlapping_delete_all_with_format(false).await;
    test_compaction_region_with_overlapping_delete_all_with_format(true).await;
}

async fn test_compaction_region_with_overlapping_delete_all_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 4 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..2400).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 2400..3600).await; // window 3600
    delete_and_flush(&engine, region_id, &column_schemas, 0..10800).await; // window 10800
    tokio::time::sleep(Duration::from_millis(2)).await;
    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert!(vec.is_empty());
}

#[tokio::test]
async fn test_compaction_input_limit_keeps_rows_deleted() {
    test_compaction_input_limit_keeps_rows_deleted_with_format(false).await;
    test_compaction_input_limit_keeps_rows_deleted_with_format(true).await;
}

/// Creates a region that only compacts when asked, so that a test can build an exact file
/// layout with `put_and_flush` and `delete_and_flush`.
async fn env_for_manual_compaction(
    env: &mut TestEnv,
    region_id: RegionId,
    flat_format: bool,
) -> (MitoEngine, Vec<ColumnSchema>) {
    env_for_manual_compaction_with_window(env, region_id, flat_format, "1h").await
}

async fn env_for_manual_compaction_with_window(
    env: &mut TestEnv,
    region_id: RegionId,
    flat_format: bool,
    time_window: &str,
) -> (MitoEngine, Vec<ColumnSchema>) {
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            min_compaction_interval: Duration::from_secs(3600),
            ..Default::default()
        })
        .await;

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
        .insert_option("compaction.twcs.time_window", time_window)
        .build();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    (engine, column_schemas)
}

/// The picker caps a compaction at 16 input files and drops the largest file groups to get
/// there. A deletion marker among the picked files must not be filtered out while the file
/// holding the rows it masks stays behind, otherwise those rows become visible again.
async fn test_compaction_input_limit_keeps_rows_deleted_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let (engine, column_schemas) =
        env_for_manual_compaction(&mut env, region_id, flat_format).await;

    // One large file spanning the whole time window.
    put_and_flush(&engine, region_id, &column_schemas, 0..3000).await;
    // Deletes 6 rows of that file. The markers land in a tiny file that overlaps it.
    delete_and_flush(&engine, region_id, &column_schemas, 10..16).await;
    // 15 more tiny files that overlap the large one but not each other, so the window holds
    // 17 file groups forming 2 runs.
    for i in 2..17 {
        put_and_flush(&engine, region_id, &column_schemas, i * 10..i * 10 + 6).await;
    }

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        17,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // The 16 tiny files are merged into one; the large file exceeds the input file num limit
    // and is left behind.
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert!(
        !(10..16).any(|ts| vec.contains(&(ts * 1000))),
        "deleted rows are visible again after compaction"
    );
    assert_eq!(2994, vec.len());
}

#[tokio::test]
async fn test_compaction_of_part_of_a_run_keeps_rows_deleted() {
    test_compaction_of_part_of_a_run_keeps_rows_deleted_with_format(false).await;
    test_compaction_of_part_of_a_run_keeps_rows_deleted_with_format(true).await;
}

/// A run is supposed to hold no overlapping files, which is what lets `merge_seq_files`
/// compact part of a run and still filter deleted rows. Run detection compares time ranges
/// exclusively though, so a file covering a single timestamp lands in the same run as the
/// file it deletes rows from. The rows must stay deleted when only one of the two is picked.
async fn test_compaction_of_part_of_a_run_keeps_rows_deleted_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(1, 1);
    let (engine, column_schemas) =
        env_for_manual_compaction(&mut env, region_id, flat_format).await;

    // One large file spanning the whole time window.
    put_and_flush(&engine, region_id, &column_schemas, 0..3000).await;
    // Deletes a single row of that file, so the marker lands in a file covering one timestamp.
    delete_and_flush(&engine, region_id, &column_schemas, 1..2).await;
    // 31 more single row files, each holding another key at its own timestamp.
    for ts in 1..32 {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("b", ts * 10, ts * 10 + 1, 0),
        };
        put_rows(&engine, region_id, rows).await;
        flush(&engine, region_id).await;
    }

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert!(
        !vec.contains(&1000),
        "deleted row is visible again after compaction"
    );
    assert_eq!(3030, vec.len());
}

// For issue https://github.com/GreptimeTeam/greptimedb/issues/3633
#[tokio::test]
async fn test_readonly_during_compaction() {
    test_readonly_during_compaction_with_format(false).await;
    test_readonly_during_compaction_with_format(true).await;
}

async fn test_readonly_during_compaction_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CompactionListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                // Ensure there is only one background worker for purge task.
                max_background_purges: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
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
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let listener_guard = CompactionListenerGuard::new(listener.clone());
    // Flush 4 balanced SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    // Waits until the engine receives compaction finished request.
    listener.wait_handle_finished().await;

    // Converts region to follower.
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    // Wakes up the listener.
    listener_guard.release();

    let notify = Arc::new(Notify::new());
    // We already sets max background purges to 1, so we can submit a task to the
    // purge scheduler to ensure all purge tasks are finished.
    let job_notify = notify.clone();
    engine
        .purge_scheduler()
        .schedule(Box::pin(async move {
            job_notify.notify_one();
        }))
        .unwrap();
    notify.notified().await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        4,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..20).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_local_compaction_cancellation_notifies_before_pending_ddl_dispatch() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(2049, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                max_background_purges: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
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
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let merge_guard = gate.arm_merge();
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_merge_entered())
        .await
        .expect("local compaction did not reach its cancellable merge gate");

    let pending_ddl_guard = gate.arm_pending_ddl_dispatch();
    let staging_engine = engine.clone();
    let mut staging_task = tokio::spawn(async move {
        staging_engine
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_cancel_requested())
        .await
        .expect("enter-staging did not request local compaction cancellation");
    assert!(!staging_task.is_finished());

    merge_guard.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        gate.wait_until_pending_ddl_dispatch(),
    )
    .await
    .expect("cancelled compaction did not notify before pending DDL dispatch");
    assert!(!staging_task.is_finished());

    pending_ddl_guard.release();
    tokio::time::timeout(Duration::from_secs(5), &mut staging_task)
        .await
        .expect("enter-staging did not finish after cancellation notification")
        .expect("enter-staging task panicked")
        .expect("enter-staging request failed");
    assert!(engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_compaction_update_time_window() {
    test_compaction_update_time_window_with_format(false).await;
    test_compaction_update_time_window_with_format(true).await;
}

async fn test_compaction_update_time_window_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 3 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..900).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 900..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2700).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 2700..3600).await; // window 3600

    compact(&engine, region_id).await;
    assert_eq!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window,
        Some(Duration::from_secs(3600))
    );
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(0, scanner.num_memtables());
    // We keep all 3 files because no enough file to merge
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    // Flush a new SST and the time window is applied.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600

    // Puts window 7200.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 3600, 4000, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(1, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..4000).map(|v| v * 1000).collect::<Vec<_>>(), vec);

    // Puts window 3600.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 2400, 3600, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(2, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..4000).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_change_region_compaction_window() {
    test_change_region_compaction_window_with_format(false).await;
    test_change_region_compaction_window_with_format(true).await;
}

async fn test_change_region_compaction_window_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = request.table_dir.clone();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 2 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..600).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 600..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2400).await; // window 3600

    compact(&engine, region_id).await;

    // Put window 7200
    put_and_flush(&engine, region_id, &column_schemas, 4000..5000).await;

    // Check compaction window.
    let region = engine.get_region(region_id).unwrap();
    {
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(3600)),
            version.compaction_time_window,
        );
        assert!(version.options.compaction.time_window().is_none());
    }

    // Change compaction window.
    let request = RegionRequest::Alter(RegionAlterRequest {
        kind: SetRegionOptions {
            options: vec![SetRegionOption::Twsc(
                "compaction.twcs.time_window".to_string(),
                "2h".to_string(),
            )],
        },
    });
    engine.handle_request(region_id, request).await.unwrap();
    assert_eq!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .options
            .compaction
            .time_window(),
        Some(Duration::from_secs(7200))
    );

    put_and_flush(&engine, region_id, &column_schemas, 5000..5100).await;
    put_and_flush(&engine, region_id, &column_schemas, 5100..5200).await;
    put_and_flush(&engine, region_id, &column_schemas, 5200..5300).await;

    // Compaction again. It should compacts window 3600 and 7200
    // into 7200.
    compact(&engine, region_id).await;
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.options.compaction.time_window()
        );
    }

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
                options: Default::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        // We open the region without options, so the time window should be None.
        assert!(version.options.compaction.time_window().is_none());
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
    }
}

#[tokio::test]
async fn test_open_overwrite_compaction_window() {
    test_open_overwrite_compaction_window_with_format(false).await;
    test_open_overwrite_compaction_window_with_format(true).await;
}

async fn test_open_overwrite_compaction_window_with_format(flat_format: bool) {
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
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = request.table_dir.clone();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 2 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..600).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 600..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2400).await; // window 3600

    compact(&engine, region_id).await;

    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(3600)),
            version.compaction_time_window,
        );
        assert!(version.options.compaction.time_window().is_none());
    }

    // Reopen region.
    let options = HashMap::from([
        ("compaction.type".to_string(), "twcs".to_string()),
        ("compaction.twcs.time_window".to_string(), "2h".to_string()),
    ]);
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
                options,
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.options.compaction.time_window()
        );
    }
}
