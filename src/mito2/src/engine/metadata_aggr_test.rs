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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common_function::aggrs::aggr_wrapper::{aggr_state_func_name, is_builtin_count_state};
use common_function::function_registry::FUNCTION_REGISTRY;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use datatypes::arrow::datatypes::SchemaRef;
use object_store::layers::mock::MockLayerBuilder;
use store_api::region_engine::{RegionEngine, RegionScannerRef};
use store_api::region_request::{RegionDeleteRequest, RegionPutRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use table::table::scan::{PartialAggrRule, RegionScanExec};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{compact, delete_and_flush, put_and_flush};
use crate::test_util::{self, CreateRequestBuilder, TestEnv};

/// Exercises DataFusion's normal SQL physical optimizer over a real Mito scan.
/// GreptimeDB's SQL-to-count hint is covered separately by ScanHintRule tests.
#[derive(Debug)]
struct CountTable {
    plan: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl TableProvider for CountTable {
    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if let Some(projection) = projection {
            let schema = self.plan.schema();
            let exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> = projection
                .iter()
                .map(|index| {
                    let name = schema.field(*index).name();
                    (Arc::new(Column::new(name, *index)) as _, name.clone())
                })
                .collect();
            Ok(Arc::new(ProjectionExec::try_new(exprs, self.plan.clone())?))
        } else {
            Ok(self.plan.clone())
        }
    }
}

/// A session whose physical optimizer wraps COUNT over a split scan like the
/// query engine.
fn count_context() -> SessionContext {
    let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
    let index = rules
        .iter()
        .position(|rule| rule.name() == "aggregate_statistics")
        .unwrap();
    rules.insert(
        index,
        Arc::new(PartialAggrRule::with_count_state(is_builtin_count_state)),
    );
    let state = datafusion::execution::session_state::SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udaf(
        FUNCTION_REGISTRY
            .get_aggr_func(&aggr_state_func_name("count"))
            .unwrap(),
    );
    ctx
}

/// Plans and runs `SELECT count(*)` over a scan of the region with the query
/// engine's COUNT rule. Returns the physical plan and the count.
async fn sql_count(
    engine: &MitoEngine,
    region_id: RegionId,
    request: ScanRequest,
) -> (String, i64) {
    let (plan, result) = sql_aggregate(engine, region_id, request, "SELECT count(*) FROM t").await;
    let ScalarValue::Int64(Some(count)) = result else {
        unreachable!()
    };
    (plan, count)
}

/// Plans once: creating the physical plan can consume the scan's one-shot split.
async fn sql_aggregate(
    engine: &MitoEngine,
    region_id: RegionId,
    request: ScanRequest,
    sql: &str,
) -> (String, ScalarValue) {
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    let plan = Arc::new(RegionScanExec::new(scanner, request, None).unwrap());
    let ctx = count_context();
    ctx.register_table("t", Arc::new(CountTable { plan }))
        .unwrap();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    let result = ScalarValue::try_from_array(batches[0].column(0), 0).unwrap();
    (text, result)
}

#[tokio::test]
async fn test_metadata_aggr_compaction_sql_and_fallback() {
    run_compaction_sql_and_fallback("last_row").await;
}

/// Both dedup merge modes keep one row per primary key and timestamp, so
/// field merging does not change which files are exact.
#[tokio::test]
async fn test_metadata_aggr_compaction_sql_and_fallback_last_non_null() {
    run_compaction_sql_and_fallback("last_non_null").await;
}

async fn run_compaction_sql_and_fallback(merge_mode: &str) {
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            if path.ends_with(".parquet") {
                observed.fetch_add(1, Ordering::Relaxed);
            }
            inner
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(layer);
    let config = MitoConfig {
        experimental_metadata_aggr: true,
        min_compaction_interval: Duration::from_secs(3600),
        schedule_compaction_after_edit: false,
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "count_test",
            "greptime",
            "public",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("merge_mode", merge_mode)
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .build();
    let schema = test_util::rows_schema(&create);
    let table_dir = create.table_dir.clone();
    let options = create.options.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    for range in [0..5, 2..7, 0..3] {
        put_and_flush(&engine, region_id, &schema, range).await;
    }
    delete_and_flush(&engine, region_id, &schema, 1..2).await;
    let request = ScanRequest {
        metadata_aggr_hint: true,
        ..Default::default()
    };
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(
        scanner.properties().exact_num_rows(),
        None,
        "overlapping flush outputs may share rows"
    );
    compact(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].meta_ref().num_rows, 6);
    drop(version);
    drop(region);

    // Verify the capability survives manifest persistence and reopening.
    test_util::reopen_region(&engine, region_id, table_dir, true, options).await;
    // Only hinted COUNT scans prepare a count.
    let ordinary = engine
        .handle_query(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(ordinary.properties().exact_num_rows(), None);
    drop(ordinary);
    // The first count may load the file's Parquet metadata. Later ones are
    // served from the SST metadata cache, and no count reads SST data.
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(scanner.properties().exact_num_rows(), Some(6));
    drop(scanner);
    reads.store(0, Ordering::Relaxed);
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(scanner.properties().exact_num_rows(), Some(6));
    let scan: Arc<dyn ExecutionPlan> =
        Arc::new(RegionScanExec::new(scanner, request.clone(), None).unwrap());
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(CountTable { plan: scan }))
        .unwrap();
    let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(!text.contains("RegionScanExec"), "{text}");
    let batches = df.collect().await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Int64(Some(6))
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "optimized COUNT must not open any SST"
    );

    let ts = |v| lit(ScalarValue::TimestampMillisecond(Some(v), None));
    for (filters, expected) in [
        (
            vec![col("ts").gt_eq(ts(0)).and(col("ts").lt(ts(7000)))],
            Some(6),
        ),
        (vec![col("ts").lt(ts(3000))], None),
        (vec![col("ts").gt(ts(7000))], Some(0)),
    ] {
        let scanner = engine
            .handle_query(
                region_id,
                ScanRequest {
                    filters,
                    ..request.clone()
                },
            )
            .await
            .unwrap();
        assert_eq!(scanner.properties().exact_num_rows(), expected);
    }

    // A new deletion in memory invalidates the complete count; ordinary scan
    // still resolves the tombstone against the compacted file.
    engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest {
                rows: api::v1::Rows {
                    schema: schema.clone(),
                    rows: test_util::build_rows_for_key("a", 2, 3, 0),
                },
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(scanner.properties().exact_num_rows(), None);
    let baseline = common_recordbatch::RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        baseline.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        5
    );
    // Ordinary writes retain their original behavior.
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: api::v1::Rows {
                    schema,
                    rows: test_util::build_rows_for_key("a", 2, 3, 0),
                },
                hint: None,
                partition_expr_version: None,
                skip_wal: false,
            }),
        )
        .await
        .unwrap();
    engine.stop().await.unwrap();
}

/// Deletions are read from the SST's `__op_type` statistics: a flushed file
/// without overlaps is exact unless it holds a delete marker.
#[tokio::test]
async fn test_metadata_aggr_reads_deletes_from_sst_statistics() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            experimental_metadata_aggr: true,
            ..Default::default()
        })
        .await;
    let request = ScanRequest {
        metadata_aggr_hint: true,
        ..Default::default()
    };
    for (region_id, delete) in [(RegionId::new(1, 1), false), (RegionId::new(1, 2), true)] {
        let create = CreateRequestBuilder::new().build();
        let schema = test_util::rows_schema(&create);
        engine
            .handle_request(region_id, RegionRequest::Create(create))
            .await
            .unwrap();
        test_util::put_rows(
            &engine,
            region_id,
            api::v1::Rows {
                schema: schema.clone(),
                rows: test_util::build_rows_for_key("a", 0, 5, 0),
            },
        )
        .await;
        if delete {
            // The tombstone of a missing row is flushed into the same file.
            engine
                .handle_request(
                    region_id,
                    RegionRequest::Delete(RegionDeleteRequest {
                        rows: api::v1::Rows {
                            schema: schema.clone(),
                            rows: test_util::build_rows_for_key("a", 10, 11, 0),
                        },
                        hint: None,
                        partition_expr_version: None,
                    }),
                )
                .await
                .unwrap();
        }
        test_util::flush_region(&engine, region_id, None).await;
        let region = engine.get_region(region_id).unwrap();
        let num_files = region.version().ssts.levels()[0].files.len();
        assert_eq!(num_files, 1);

        let scanner = engine
            .handle_query(region_id, request.clone())
            .await
            .unwrap();
        assert_eq!(
            scanner.properties().exact_num_rows(),
            (!delete).then_some(5),
            "delete={delete}"
        );
        let rows = common_recordbatch::RecordBatches::try_collect(
            engine
                .scan_to_stream(region_id, ScanRequest::default())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 5);
    }
    engine.stop().await.unwrap();
}

/// A strict lower bound past the largest timestamp matches no row. The count
/// must agree with a plain scan rather than cover the file.
#[tokio::test]
async fn test_metadata_aggr_strict_bound_past_max() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            experimental_metadata_aggr: true,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let create = CreateRequestBuilder::new().build();
    let schema = test_util::rows_schema(&create);
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    test_util::put_rows(
        &engine,
        region_id,
        api::v1::Rows {
            schema,
            rows: test_util::build_rows_for_key("a", 0, 5, 0),
        },
    )
    .await;
    test_util::flush_region(&engine, region_id, None).await;

    let max = || lit(ScalarValue::TimestampMillisecond(Some(i64::MAX), None));
    for filter in [col("ts").gt(max()), max().lt(col("ts"))] {
        let filters = vec![filter];
        let scanner = engine
            .handle_query(
                region_id,
                ScanRequest {
                    filters: filters.clone(),
                    metadata_aggr_hint: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let rows = common_recordbatch::RecordBatches::try_collect(
            engine
                .scan_to_stream(
                    region_id,
                    ScanRequest {
                        filters: filters.clone(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let scanned = rows.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(scanned, 0, "{filters:?}");
        assert_eq!(
            scanner.properties().exact_num_rows(),
            Some(scanned),
            "{filters:?}"
        );
    }
    engine.stop().await.unwrap();
}

/// Rows a scanner yields.
async fn scanned_rows(scanner: RegionScannerRef, request: ScanRequest) -> usize {
    let plan = Arc::new(RegionScanExec::new(scanner, request, None).unwrap());
    datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

/// Rows a plain scan of the region yields for `request`.
async fn plain_rows(engine: &MitoEngine, region_id: RegionId, request: ScanRequest) -> usize {
    let request = ScanRequest {
        metadata_aggr_hint: false,
        ..request
    };
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    scanned_rows(scanner, request).await
}

/// The rows a hinted scan counts from metadata and the rows its residual scan
/// yields, or `None` if it does not split.
async fn split_rows(
    engine: &MitoEngine,
    region_id: RegionId,
    request: ScanRequest,
) -> Option<(usize, usize)> {
    let mut scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    let split = scanner.take_statistics_split()?;
    Some((
        split.exact_rows,
        scanned_rows(split.residual, request).await,
    ))
}

/// Metadata rows and residual rows of the hinted scans of
/// [`test_metadata_aggr_fourteen_day_workload`], in its order.
struct FourteenDaySplits {
    whole: Option<(usize, usize)>,
    from_day_7: Option<(usize, usize)>,
    days_5_to_10: Option<(usize, usize)>,
    after_put: Option<(usize, usize)>,
    after_delete: Option<(usize, usize)>,
}

/// Days 0..7 are compacted into one file, days 7..13 are separate flushes,
/// and day 13 is still in the memtable. The memtable partition follows the
/// 7-day compaction window, `[7d, 14d)`.
///
/// - A time-series memtable range is bounded by that partition, so it keeps
///   the files of days 7..13 residual. Only the compacted file is counted
///   from metadata.
/// - A bulk memtable range is bounded by the part it captured, day 13, so
///   every file is counted from metadata.
#[rstest::rstest]
#[case::time_series("primary_key", "time_series", FourteenDaySplits {
    whole: Some((7_000, 7_000)),
    from_day_7: None,
    // The memtable lies outside the query range, so it is not captured and
    // bounds nothing; the compacted file is only partly covered.
    days_5_to_10: Some((3_000, 2_000)),
    // The put creates partition `[14d, 21d)`, the delete `[0, 7d)`.
    after_put: Some((7_000, 7_001)),
    after_delete: None,
})]
#[case::bulk("flat", "bulk", FourteenDaySplits {
    whole: Some((13_000, 1_000)),
    from_day_7: Some((6_000, 1_000)),
    days_5_to_10: Some((3_000, 2_000)),
    after_put: Some((13_000, 1_001)),
    // The deletion's part overlaps the compacted file only.
    after_delete: Some((6_000, 8_000)),
})]
#[tokio::test]
async fn test_metadata_aggr_fourteen_day_workload(
    #[case] sst_format: &str,
    #[case] memtable: &str,
    #[case] splits: FourteenDaySplits,
) {
    use api::v1::value::ValueData;
    use store_api::region_request::RegionFlushRequest;

    const DAY_SECONDS: usize = 86_400;
    let reads = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            if path.ends_with(".parquet") {
                observed.lock().unwrap().push(path.to_string());
            }
            inner
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(layer);
    let engine = env
        .create_engine(MitoConfig {
            experimental_metadata_aggr: true,
            min_compaction_interval: Duration::from_secs(3600),
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(2, 1);
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "7d")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .insert_option("sst_format", sst_format)
        .insert_option("memtable.type", memtable)
        .build();
    let schema = test_util::rows_schema(&create);
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    let day_rows = |day: usize| {
        let mut rows = test_util::build_rows_for_key("a", 0, 1000, 0);
        for (minute, row) in rows.iter_mut().enumerate() {
            row.values[2].value_data = Some(ValueData::TimestampMillisecondValue(
                ((day * DAY_SECONDS + minute * 60) * 1000) as i64,
            ));
        }
        api::v1::Rows {
            schema: schema.clone(),
            rows,
        }
    };
    for day in 0..14 {
        test_util::put_rows(&engine, region_id, day_rows(day)).await;
        if day < 13 {
            engine
                .handle_request(
                    region_id,
                    RegionRequest::Flush(RegionFlushRequest::default()),
                )
                .await
                .unwrap();
        }
        if day == 6 {
            compact(&engine, region_id).await;
        }
    }

    // Ordinary scans are never split, even with the option enabled.
    let mut scanner = engine
        .handle_query(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(scanner.take_statistics_split().is_none());

    // The residual scanner keeps the properties the metric engine sets after
    // Mito returns the scanner. This hinted scan also loads the files' metadata.
    let hinted = ScanRequest {
        metadata_aggr_hint: true,
        ..Default::default()
    };
    let mut scanner = engine
        .handle_query(region_id, hinted.clone())
        .await
        .unwrap();
    scanner.set_logical_region(true);
    scanner.set_query_load_region_id(RegionId::new(2, 7));
    let split = scanner.take_statistics_split().unwrap();
    assert_eq!(Some(split.exact_rows), splits.whole.map(|(exact, _)| exact));
    assert!(split.residual.properties().is_logical_region());
    assert_eq!(
        split.residual.properties().query_load_region_id(),
        Some(RegionId::new(2, 7))
    );
    assert_eq!(split.residual.properties().exact_num_rows(), None);
    drop(split);

    // A dynamic predicate invalidates the prepared split.
    let mut scanner = engine
        .handle_query(region_id, hinted.clone())
        .await
        .unwrap();
    scanner.add_dyn_filter_to_predicate(vec![datafusion::physical_plan::expressions::lit(true)]);
    assert!(scanner.take_statistics_split().is_none());

    let ts = |day: i64| {
        lit(ScalarValue::TimestampMillisecond(
            Some(day * 86_400_000),
            None,
        ))
    };
    // (filters, count, exact count, metadata and residual rows of a split)
    for (filters, expected_count, exact, split) in [
        (vec![], 14_000, None, splits.whole),
        // The memtable lies outside the query range, so it is not scanned.
        (vec![col("ts").lt(ts(7))], 7_000, Some(7_000), None),
        (vec![col("ts").gt_eq(ts(7))], 7_000, None, splits.from_day_7),
        (
            vec![col("ts").gt_eq(ts(5)).and(col("ts").lt(ts(10)))],
            5_000,
            None,
            splits.days_5_to_10,
        ),
    ] {
        reads.lock().unwrap().clear();
        let request = ScanRequest {
            filters,
            ..hinted.clone()
        };
        let scanner = engine
            .handle_query(region_id, request.clone())
            .await
            .unwrap();
        assert_eq!(scanner.properties().exact_num_rows(), exact, "{request:?}");
        // The files' metadata is cached, so neither proof opens an SST.
        let paths = reads.lock().unwrap().clone();
        assert!(paths.is_empty(), "COUNT opened SSTs: {paths:?}");
        assert_eq!(
            split_rows(&engine, region_id, request.clone()).await,
            split,
            "{request:?}"
        );
        assert_eq!(
            plain_rows(&engine, region_id, request).await,
            expected_count
        );
    }

    for (timestamp, delete, expected, split) in [
        (15 * 86_400_000, false, 14_001, splits.after_put),
        (86_400_000, true, 14_000, splits.after_delete),
    ] {
        let mut rows = test_util::build_rows_for_key("a", 0, 1, 0);
        rows[0].values[2].value_data = Some(ValueData::TimestampMillisecondValue(timestamp));
        let rows = api::v1::Rows {
            schema: schema.clone(),
            rows,
        };
        let request = if delete {
            RegionRequest::Delete(RegionDeleteRequest {
                rows,
                hint: None,
                partition_expr_version: None,
            })
        } else {
            RegionRequest::Put(RegionPutRequest {
                rows,
                hint: None,
                partition_expr_version: None,
                skip_wal: false,
            })
        };
        engine.handle_request(region_id, request).await.unwrap();
        assert_eq!(split_rows(&engine, region_id, hinted.clone()).await, split);
        assert_eq!(
            plain_rows(&engine, region_id, hinted.clone()).await,
            expected
        );
    }
    engine.stop().await.unwrap();
}

/// A captured memtable range bounds its rows by its container, not by the
/// rows present when it was captured. File A holds `a@100`, file B holds
/// `b@0` and `b@200`, and the memtable holds `a@memtable_second` when the
/// split is prepared. `a@100` is written again before the split executes.
///
/// - A time-series range reads its live series set, so it sees the later
///   write. In the same daily partition A overlaps the container and stays
///   residual, although it is disjoint from the memtable's rows. In another
///   partition A is exact, and the later write lands in a partition the scan
///   never captured.
/// - A bulk range reads the parts captured with it, so A is exact even in the
///   same partition, and the later write lands in a part the scan never read.
#[rstest::rstest]
#[case::time_series_same_partition("primary_key", "time_series", 0, None)]
#[case::time_series_other_partition("primary_key", "time_series", 2 * 86_400, Some((1, 1)))]
#[case::bulk_same_partition("flat", "bulk", 0, Some((1, 1)))]
#[tokio::test]
async fn test_metadata_aggr_split_bounds_memtables_by_container(
    #[case] sst_format: &str,
    #[case] memtable: &str,
    #[case] memtable_second: usize,
    #[case] split: Option<(usize, usize)>,
) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            experimental_metadata_aggr: true,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let create = CreateRequestBuilder::new()
        .insert_option("sst_format", sst_format)
        .insert_option("memtable.type", memtable)
        .build();
    let schema = test_util::rows_schema(&create);
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    let rows = |key: &str, seconds: &[usize]| api::v1::Rows {
        schema: schema.clone(),
        rows: seconds
            .iter()
            .flat_map(|&second| test_util::build_rows_for_key(key, second, second + 1, 0))
            .collect(),
    };
    put_and_flush_rows(&engine, region_id, rows("a", &[100])).await;
    put_and_flush_rows(&engine, region_id, rows("b", &[0, 200])).await;
    test_util::put_rows(&engine, region_id, rows("a", &[memtable_second])).await;

    let request = ScanRequest {
        filters: vec![col("tag_0").eq(lit("a"))],
        metadata_aggr_hint: true,
        ..Default::default()
    };
    let mut scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    let prepared = scanner.take_statistics_split();
    // Written after the split is prepared, before it executes.
    test_util::put_rows(&engine, region_id, rows("a", &[100])).await;

    let actual = match prepared {
        Some(prepared) => Some((
            prepared.exact_rows,
            scanned_rows(prepared.residual, request.clone()).await,
        )),
        None => None,
    };
    assert_eq!(actual, split);
    // Whether or not the scan splits, `a@100` is counted once.
    assert_eq!(plain_rows(&engine, region_id, request).await, 2);
    engine.stop().await.unwrap();
}

async fn put_and_flush_rows(engine: &MitoEngine, region_id: RegionId, rows: api::v1::Rows) {
    test_util::put_rows(engine, region_id, rows).await;
    test_util::flush_region(engine, region_id, None).await;
}

/// The COUNT rule wraps the partial COUNT over a split scan and merges the
/// metadata rows once. Without a split the plan is unchanged.
#[tokio::test]
async fn test_metadata_aggr_exec_merges_metadata_rows() {
    use api::v1::value::ValueData;

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            experimental_metadata_aggr: true,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let create = CreateRequestBuilder::new().build();
    let schema = test_util::rows_schema(&create);
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    let day_rows = |day: i64, rows: usize| {
        let mut rows = test_util::build_rows_for_key("a", 0, rows, 0);
        for (second, row) in rows.iter_mut().enumerate() {
            row.values[2].value_data = Some(ValueData::TimestampMillisecondValue(
                day * 86_400_000 + second as i64 * 1000,
            ));
        }
        api::v1::Rows {
            schema: schema.clone(),
            rows,
        }
    };
    put_and_flush_rows(&engine, region_id, day_rows(0, 7)).await;
    put_and_flush_rows(&engine, region_id, day_rows(1, 5)).await;
    // The memtable partition of day 2 is disjoint from both files.
    test_util::put_rows(&engine, region_id, day_rows(2, 3)).await;

    let hinted = ScanRequest {
        metadata_aggr_hint: true,
        ..Default::default()
    };
    let (plan, count) = sql_count(&engine, region_id, hinted.clone()).await;
    assert!(plan.contains("MetadataAggrExec: exact_rows=12"), "{plan}");
    assert_eq!(count, 15);

    // Distributed planning uses the registered state wrapper; its final
    // aggregate returns a struct whose first field is the merged COUNT state.
    let (plan, state) = sql_aggregate(
        &engine,
        region_id,
        hinted.clone(),
        "SELECT __count_state(1) FROM t",
    )
    .await;
    assert!(plan.contains("MetadataAggrExec: exact_rows=12"), "{plan}");
    let ScalarValue::Struct(state) = state else {
        panic!("expected a COUNT state struct");
    };
    assert_eq!(
        ScalarValue::try_from_array(state.column(0), 0).unwrap(),
        ScalarValue::Int64(Some(15)),
    );

    // Without the hint the scan carries no split.
    let (plan, count) = sql_count(&engine, region_id, ScanRequest::default()).await;
    assert!(!plan.contains("MetadataAggrExec"), "{plan}");
    assert_eq!(count, 15);

    // A row in the partition of day 1 overlaps the second file; the first
    // file is still exact.
    test_util::put_rows(&engine, region_id, day_rows(1, 1)).await;
    let (plan, count) = sql_count(&engine, region_id, hinted.clone()).await;
    assert!(plan.contains("MetadataAggrExec: exact_rows=7"), "{plan}");
    assert_eq!(count, 15);

    // A row in the partition of day 0 leaves no exact file.
    test_util::put_rows(&engine, region_id, day_rows(0, 1)).await;
    let (plan, count) = sql_count(&engine, region_id, hinted).await;
    assert!(!plan.contains("MetadataAggrExec"), "{plan}");
    assert_eq!(count, 15);
    engine.stop().await.unwrap();
}
