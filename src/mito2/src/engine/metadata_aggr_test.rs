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
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use datatypes::arrow::datatypes::SchemaRef;
use object_store::layers::mock::MockLayerBuilder;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionDeleteRequest, RegionPutRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use table::table::scan::{PartialAggrRule, statistics_scan_plan};

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

/// A session whose physical optimizer splits COUNT states like the query engine.
fn count_context() -> SessionContext {
    let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
    let index = rules
        .iter()
        .position(|rule| rule.name() == "aggregate_statistics")
        .unwrap();
    rules.insert(index, Arc::new(PartialAggrRule::default()));
    let state = datafusion::execution::session_state::SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rules(rules)
        .build();
    SessionContext::new_with_state(state)
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
        statistics_scan_plan(scanner, request.clone(), None, false).unwrap();
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

/// Days 0..7 are compacted into one file, days 7..13 are separate flushes,
/// and day 13 is still in the memtable. A whole-range COUNT counts every file
/// from metadata and only scans the memtable.
#[tokio::test]
async fn test_metadata_aggr_fourteen_day_workload() {
    use api::v1::value::ValueData;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion_common::stats::Precision;
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

    // Both parts keep the properties the metric engine sets after Mito
    // returns the scanner. This hinted scan also loads the files' metadata.
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
    for part in [&split.exact, &split.inexact] {
        assert!(part.properties().is_logical_region());
        assert_eq!(
            part.properties().query_load_region_id(),
            Some(RegionId::new(2, 7))
        );
    }
    assert_eq!(split.exact.properties().exact_num_rows(), Some(13_000));
    assert_eq!(split.inexact.properties().exact_num_rows(), None);
    drop(split);

    let ts = |day: i64| {
        lit(ScalarValue::TimestampMillisecond(
            Some(day * 86_400_000),
            None,
        ))
    };
    // (filters, count, exact count, split into exact and residual scans)
    for (filters, expected_count, exact, split) in [
        (vec![], 14_000, None, true),
        (vec![col("ts").lt(ts(7))], 7_000, Some(7_000), false),
        // Days 7..13 are exact; day 13 is in the memtable.
        (vec![col("ts").gt_eq(ts(7))], 7_000, None, true),
        // The compacted file is only partly covered; days 7..10 are exact.
        (
            vec![col("ts").gt_eq(ts(5)).and(col("ts").lt(ts(10)))],
            5_000,
            None,
            true,
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
        let scan = statistics_scan_plan(scanner, request.clone(), None, false).unwrap();
        if request.filters.is_empty() {
            let children = scan.children();
            assert_eq!(children.len(), 2);
            let stats = |plan: &Arc<dyn ExecutionPlan>| {
                StatisticsContext::new()
                    .compute(plan.as_ref(), &StatisticsArgs::new())
                    .unwrap()
                    .num_rows
            };
            assert_eq!(stats(children[0]), Precision::Exact(13_000));
            assert_eq!(stats(children[1]), Precision::Inexact(1_000));
            assert_eq!(stats(&scan), Precision::Inexact(14_000));
        }
        let ctx = count_context();
        ctx.register_table("t", Arc::new(CountTable { plan: scan }))
            .unwrap();
        let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert_eq!(text.contains("SeqScan"), exact.is_none(), "{text}");
        assert_eq!(text.contains("InexactRegionScanExec"), split, "{text}");
        assert!(
            !text.contains("ExactRegionScanExec: "),
            "exact branch must be folded: {text}"
        );
        let batches = df.collect().await.unwrap();
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
            ScalarValue::Int64(Some(expected_count))
        );
        // The files' metadata is cached, and the residual of the whole-range
        // COUNT is the memtable, so neither query opens an SST.
        if exact.is_some() || request.filters.is_empty() {
            let paths = reads.lock().unwrap();
            assert!(paths.is_empty(), "COUNT opened SSTs: {paths:?}");
        }
    }
    let raw = common_recordbatch::RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        raw.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        14_000
    );

    for (timestamp, delete, expected, retains_exact) in [
        // A newer row leaves every file isolated.
        (15 * 86_400_000, false, 14_001, true),
        // A deletion in memory overlaps every file in time.
        (86_400_000, true, 14_000, false),
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
        let scanner = engine
            .handle_query(region_id, hinted.clone())
            .await
            .unwrap();
        let plan = statistics_scan_plan(scanner, hinted.clone(), None, false).unwrap();
        let ctx = count_context();
        ctx.register_table("t", Arc::new(CountTable { plan }))
            .unwrap();
        let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert_eq!(text.contains("13000 as"), retains_exact, "{text}");
        let batches = df.collect().await.unwrap();
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
            ScalarValue::Int64(Some(expected))
        );
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

/// A row written between preparing a split and executing it is counted once.
/// File A is exact, file B stays residual, and the memtable is disjoint from A
/// when the split is prepared. Rewriting A's row into the memtable afterwards
/// must not let the residual scan count it a second time. A time-series
/// memtable reads its live series set, so it sees rows written after its
/// ranges are built.
#[rstest::rstest]
#[case::bulk("flat", "bulk")]
#[case::time_series("primary_key", "time_series")]
#[tokio::test]
async fn test_metadata_aggr_split_ignores_later_memtable_writes(
    #[case] sst_format: &str,
    #[case] memtable: &str,
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
    test_util::put_rows(&engine, region_id, rows("a", &[0])).await;

    let request = ScanRequest {
        filters: vec![col("tag_0").eq(lit("a"))],
        metadata_aggr_hint: true,
        ..Default::default()
    };
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    // Written after the split is prepared, before it executes.
    test_util::put_rows(&engine, region_id, rows("a", &[100])).await;

    let plan = statistics_scan_plan(scanner, request, None, false).unwrap();
    let ctx = count_context();
    ctx.register_table("t", Arc::new(CountTable { plan }))
        .unwrap();
    let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(text.contains("InexactRegionScanExec"), "{text}");
    let batches = df.collect().await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Int64(Some(2))
    );
    engine.stop().await.unwrap();
}

async fn put_and_flush_rows(engine: &MitoEngine, region_id: RegionId, rows: api::v1::Rows) {
    test_util::put_rows(engine, region_id, rows).await;
    test_util::flush_region(engine, region_id, None).await;
}
