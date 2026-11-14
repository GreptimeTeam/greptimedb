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

//! Tests for partition expression filtering in SST readers.

use api::v1::{Rows, SemanticType};
use common_recordbatch::RecordBatches;
use common_time::Timestamp;
use datafusion_expr::Expr;
use datatypes::arrow::array::AsArray;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use datatypes::value::Value;
use partition::expr::{Operand, col};
use partition::function::IcebergHash;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{RegionEngine, RemapManifestsRequest};
use store_api::region_request::{
    AddColumn, AlterKind, ApplyStagingManifestRequest, EnterStagingRequest, RegionAlterRequest,
    RegionCompactRequest, RegionFlushRequest, RegionRequest, StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

/// Helper to create a partition expression for testing.
/// Creates `col_name >= start AND col_name < end`.
fn range_expr_string(col_name: &str, start: f64, end: f64) -> String {
    col(col_name)
        .gt_eq(Value::Float64(start.into()))
        .and(col(col_name).lt(Value::Float64(end.into())))
        .as_json_str()
        .unwrap()
}

/// Ends staging the way repartition does, importing the region's own files.
async fn apply_staging(engine: &MitoEngine, region_id: RegionId, partition_expr: String) {
    let result = engine
        .remap_manifests(RemapManifestsRequest {
            region_id,
            input_regions: vec![region_id],
            region_mapping: [(region_id, vec![region_id])].into_iter().collect(),
            new_partition_exprs: [(region_id, partition_expr.clone())].into_iter().collect(),
        })
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr,
                central_region_id: region_id,
                manifest_path: result.manifest_paths[&region_id].clone(),
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_partition_filter_basic() {
    test_partition_filter_basic_with_format(false).await;
    test_partition_filter_basic_with_format(true).await;
}

/// Test that partition filter correctly filters rows from SST.
///
/// This test:
/// 1. Creates a region with partition expr `tag_0 >= "0"` (covers all data)
/// 2. Writes data (tag_0 = "0"..="9") and flushes - SST has old partition expr
/// 3. Enters staging mode with partition expr `tag_0 >= "5"` (narrower)
/// 4. Writes more data in staging mode
/// 5. Exits staging mode - region now has new partition expr
/// 6. Scans all data - old SST data not matching new expr should be filtered
async fn test_partition_filter_basic_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1024, 0);

    // Create region with initial partition expr: tag_0 >= "0" (covers all data)
    let initial_partition_expr = range_expr_string("field_0", 0., 99.);
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(initial_partition_expr))
        .build();
    let column_schemas = rows_schema(&request);

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Write initial data (tag_0 = "0".."4") and flush to SST
    // This SST will have partition_expr = "tag_0 >= 0"
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 5),
    };
    put_rows(&engine, region_id, rows_data).await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    // Enter staging mode with narrower partition expr: tag_0 >= "5"
    // After staging, region will only accept data where tag_0 >= "5"
    let new_partition_expr = range_expr_string("field_0", 5., 99.);
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    new_partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    // Write data in staging mode (tag_0 = "5".."11")
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(5, 11),
    };
    put_rows(&engine, region_id, rows_data).await;

    // Scan data in staging mode - rows written in staging stay readable in the memtable
    let projection = Some(vec![1]);
    let request = ScanRequest {
        projection,
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(
        total_rows, 11,
        "Should see 11 rows before exiting staging mode"
    );

    // Exit staging mode
    apply_staging(&engine, region_id, new_partition_expr).await;

    // Scan after exiting staging - the old SST (tag_0 = "0".."4") should have
    // rows filtered by partition expr (tag_0 >= "5"), which means none of them pass.
    // But the memtable rows (tag_0 = "5".."10") satisfy the partition expr.
    let projection = Some(vec![1]);
    let request = ScanRequest {
        projection,
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    // After exit:
    // - Old SST (rows 0-4): partition expr is "tag_0 >= 5", so these are filtered out
    // - Memtable (rows 5-10): These satisfy partition expr, so they're visible
    assert_eq!(
        total_rows, 6,
        "Should see 6 rows after exiting staging (rows 5-10 from the memtable), flat_format: {}",
        flat_format
    );
}

#[tokio::test]
async fn test_partition_filter_iceberg_hash() {
    test_partition_filter_iceberg_hash_with_format(false).await;
    test_partition_filter_iceberg_hash_with_format(true).await;
}

/// Narrows a region to `iceberg_hash(tag_0) < 0` and checks rows flushed under
/// the old partition expr are filtered by the hash.
async fn test_partition_filter_iceberg_hash_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(range_expr_string("field_0", 0., 99.)))
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let flush = || async {
        engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
            .unwrap();
    };
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 20),
        },
    )
    .await;
    flush().await;

    let in_partition = |i: &usize| {
        IcebergHash::evaluate_value(&Value::String(i.to_string().as_str().into())).unwrap()
            < Value::Int32(0)
    };
    let hash_expr = Operand::IcebergHash(IcebergHash::new("tag_0"))
        .lt(Value::Int32(0))
        .as_json_str()
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    hash_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: (20..40)
                .filter(in_partition)
                .flat_map(|i| build_rows(i, i + 1))
                .collect(),
        },
    )
    .await;
    apply_staging(&engine, region_id, hash_expr).await;

    let request = ScanRequest {
        projection: Some(vec![0]),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(scanner.scan().await.unwrap())
        .await
        .unwrap();
    let mut actual = vec![];
    for batch in batches.iter() {
        let tags = datatypes::arrow::compute::cast(
            batch.column(0),
            &datatypes::arrow::datatypes::DataType::Utf8,
        )
        .unwrap();
        actual.extend(
            tags.as_string::<i32>()
                .iter()
                .map(|v| v.unwrap().to_string()),
        );
    }
    actual.sort();
    let mut expected = (0..40)
        .filter(in_partition)
        .map(|i| i.to_string())
        .collect::<Vec<_>>();
    expected.sort();
    // Rows flushed before staging fall on both sides of the hash bound.
    assert!((0..20).any(|i| in_partition(&i)) && !(0..20).all(|i| in_partition(&i)));
    assert_eq!(actual, expected, "flat_format: {flat_format}");
}

#[tokio::test]
async fn test_partition_filter_keeps_rows_of_sst_without_partition_column() {
    test_partition_filter_keeps_rows_of_sst_without_partition_column_with_format(false).await;
    test_partition_filter_keeps_rows_of_sst_without_partition_column_with_format(true).await;
}

/// Regression test for #10246.
///
/// Flushes an SST before the partition column `zn` exists, then adds `zn` and partitions the
/// region on `zn < 'm'`. The old SST has no `zn`, so its rows hold NULL: a *known* value, and
/// `zn < 'm'` is one of the expressions that own NULL (`PartitionExpr::try_as_logical_expr`
/// adds `IS NULL`). The scan must therefore keep the rows instead of failing, while genuine
/// `WHERE` predicates keep their own semantics.
async fn test_partition_filter_keeps_rows_of_sst_without_partition_column_with_format(
    flat_format: bool,
) {
    common_telemetry::init_default_ut_logging();

    let (_env, engine, region_id) = setup_old_sst_partitioned_on_zn(
        flat_format,
        zn_metadata(),
        col("zn").lt(Value::String("m".into())),
        1,
    )
    .await;

    // The region owns the NULL rows of the old SST, which holds no `zn`.
    let rows = scan_row_count(&engine, region_id, vec![]).await;
    assert_eq!(
        rows, 5,
        "rows of an SST without the partition column must be kept by the region that owns \
         NULL, flat_format: {flat_format}"
    );

    // A genuine predicate on the partition column still filters: the old rows hold NULL `zn`.
    let rows = scan_row_count(
        &engine,
        region_id,
        vec![datafusion_expr::col("zn").eq(datafusion_expr::lit("a"))],
    )
    .await;
    assert_eq!(
        rows, 0,
        "user predicate on the missing column must still filter, flat_format: {flat_format}"
    );

    // A genuine predicate on a column the old SST contains still filters.
    let rows = scan_row_count(
        &engine,
        region_id,
        vec![datafusion_expr::col("field_0").lt(datafusion_expr::lit(3.0_f64))],
    )
    .await;
    assert_eq!(
        rows, 3,
        "user predicate on a stored column must still filter, flat_format: {flat_format}"
    );
}

#[tokio::test]
async fn test_partition_filter_prunes_sst_without_partition_column_in_peer_region() {
    test_partition_filter_prunes_sst_without_partition_column_in_peer_region_with_format(false)
        .await;
    test_partition_filter_prunes_sst_without_partition_column_in_peer_region_with_format(true)
        .await;
}

/// The peer of [`test_partition_filter_keeps_rows_of_sst_without_partition_column`]: the same
/// old SST, whose `zn` is a known NULL, evaluated against the disjoint destination region
/// `zn >= 'm'`. Partition routing orders NULL below non-NULL values, so this region doesn't own
/// those rows and must prune the file instead of retaining them.
async fn test_partition_filter_prunes_sst_without_partition_column_in_peer_region_with_format(
    flat_format: bool,
) {
    common_telemetry::init_default_ut_logging();

    let (_env, engine, region_id) = setup_old_sst_partitioned_on_zn(
        flat_format,
        zn_metadata(),
        col("zn").gt_eq(Value::String("m".into())),
        1,
    )
    .await;

    let rows = scan_row_count(&engine, region_id, vec![]).await;
    assert_eq!(
        rows, 0,
        "a region that doesn't own NULL must prune an SST without the partition column, \
         flat_format: {flat_format}"
    );
}

#[tokio::test]
async fn test_compaction_keeps_rows_of_sst_without_partition_column() {
    test_compaction_keeps_rows_of_sst_without_partition_column_with_format(false).await;
    test_compaction_keeps_rows_of_sst_without_partition_column_with_format(true).await;
}

/// Regression test for #10246 on the compaction path.
///
/// Compaction reads through the same `ScanInput`/`PredicateGroup` machinery as a query, so the
/// region partition expression is pushed down as a filter there too. Compacting a region
/// partitioned on `zn < 'm'` (which owns NULL) must keep every row of the SSTs that were
/// written before `zn` existed, and the rewrite must not drop the file.
async fn test_compaction_keeps_rows_of_sst_without_partition_column_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    // A single input file makes the compaction read use `PreFilterMode::All`, so field
    // predicates (including the partition expression on `zn`) are applied there.
    let (_env, engine, region_id) = setup_old_sst_partitioned_on_zn(
        flat_format,
        zn_metadata(),
        col("zn").lt(Value::String("m".into())),
        1,
    )
    .await;

    assert_eq!(5, scan_row_count(&engine, region_id, vec![]).await);
    let level0_files = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()[0]
        .files
        .len();
    assert_eq!(
        1, level0_files,
        "expected the flushed SST, flat_format: {flat_format}"
    );

    // A strict-window compaction rewrites the whole window, so these SSTs are really read and
    // rewritten instead of being left untouched.
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
    assert!(
        version.ssts.levels()[0].files.is_empty(),
        "compaction must rewrite the SSTs written before the column existed, flat_format: \
         {flat_format}"
    );
    let rows_in_files: u64 = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .map(|file| file.meta_ref().num_rows)
        .sum();
    assert_eq!(
        5, rows_in_files,
        "compaction must keep every row of the SSTs without the partition column, \
         flat_format: {flat_format}"
    );
    assert_eq!(
        5,
        scan_row_count(&engine, region_id, vec![]).await,
        "rows must stay readable after compaction, flat_format: {flat_format}"
    );
}

#[tokio::test]
async fn test_partition_filter_keeps_rows_of_sst_without_impure_default_column() {
    test_partition_filter_keeps_rows_of_sst_without_impure_default_column_with_format(false).await;
    test_partition_filter_keeps_rows_of_sst_without_impure_default_column_with_format(true).await;
}

/// Regression test for #10246 with an impure column default.
///
/// The partition column `zn` is added with a `now()` default, so the value the old SST's rows
/// hold can't be reproduced at read time. Neither the partition predicate pushed down as a
/// simple filter nor the range-level partition filter may prune the file on an evaluated
/// default.
async fn test_partition_filter_keeps_rows_of_sst_without_impure_default_column_with_format(
    flat_format: bool,
) {
    common_telemetry::init_default_ut_logging();

    let zn_column = ColumnSchema::new(
        "zn",
        ConcreteDataType::timestamp_millisecond_datatype(),
        true,
    )
    .with_default_constraint(Some(ColumnDefaultConstraint::Function("now()".to_string())))
    .unwrap();
    let (_env, engine, region_id) = setup_old_sst_partitioned_on_zn(
        flat_format,
        ColumnMetadata {
            column_schema: zn_column,
            semantic_type: SemanticType::Field,
            column_id: 3,
        },
        // `zn < 2000-01-01` owns NULL, but the default is impure, so nothing may be assumed.
        col("zn").lt(Value::Timestamp(Timestamp::new_millisecond(
            946_684_800_000,
        ))),
        1,
    )
    .await;

    let rows = scan_row_count(&engine, region_id, vec![]).await;
    assert_eq!(
        rows, 5,
        "rows of an SST without an impure-default partition column must be kept, \
         flat_format: {flat_format}"
    );

    // A genuine predicate on a stored column still filters.
    let rows = scan_row_count(
        &engine,
        region_id,
        vec![datafusion_expr::col("field_0").lt(datafusion_expr::lit(2.0_f64))],
    )
    .await;
    assert_eq!(
        rows, 2,
        "user predicate on a stored column must still filter, flat_format: {flat_format}"
    );
}

/// The `zn` column added by the regression tests: nullable, no default.
fn zn_metadata() -> ColumnMetadata {
    ColumnMetadata {
        column_schema: ColumnSchema::new("zn", ConcreteDataType::string_datatype(), true),
        semantic_type: SemanticType::Field,
        column_id: 3,
    }
}

/// Creates a region with `sst_count` SSTs written before the `zn` column existed (so they don't
/// contain `zn`), adds `zn` and then partitions the region on `partition_expr`.
async fn setup_old_sst_partitioned_on_zn(
    flat_format: bool,
    zn_column: ColumnMetadata,
    partition_expr: partition::expr::PartitionExpr,
    sst_count: usize,
) -> (TestEnv, MitoEngine, RegionId) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush rows while the region has neither the `zn` column nor a partition expression, so
    // none of the SSTs contains `zn`.
    for index in 0..sst_count {
        let start = index * 5 / sst_count;
        let end = (index + 1) * 5 / sst_count;
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: column_schemas.clone(),
                rows: build_rows(start, end),
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;
    }

    // `ALTER TABLE ... ADD COLUMN zn`, then partition the region.
    alter_add_column(&engine, region_id, zn_column).await;
    let partition_expr = partition_expr.as_json_str().unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    // Exit staging and apply the new partition expression to the region.
    apply_staging(&engine, region_id, partition_expr).await;
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .metadata()
            .partition_expr
            .is_some(),
        "the region must be partitioned on `zn`"
    );

    (env, engine, region_id)
}

/// Adds a column to `region_id`.
async fn alter_add_column(engine: &MitoEngine, region_id: RegionId, column: ColumnMetadata) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::AddColumns {
                    columns: vec![AddColumn {
                        column_metadata: column,
                        location: None,
                    }],
                },
            }),
        )
        .await
        .unwrap();
}

async fn scan_row_count(engine: &MitoEngine, region_id: RegionId, filters: Vec<Expr>) -> usize {
    let request = ScanRequest {
        filters,
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(scanner.scan().await.unwrap())
        .await
        .unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}
