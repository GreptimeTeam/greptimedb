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

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::{SessionConfig, TaskContext};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{Column, IsNotNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::Partitioning;
use futures::StreamExt;
use promql::extension_plan::RangeManipulate;
use promql::functions::Rate;

use super::PromqlRateFusion;

const PARTITIONS: usize = 4;
const GROUPS: usize = 64;
const EVALS: usize = 256;
const RAW_POINTS: usize = 512;
const STEP_MS: i64 = 15_000;
const RANGE_MS: i64 = 300_000;
// Calibrated by the owner of the physical-rule integration; both plans intentionally
// receive the same finite, spill-capable runtime, not a deny-all spill pool.
const MEMORY_LIMIT: usize = 1024 * 1024;

struct Fixture {
    root: Arc<dyn ExecutionPlan>,
}

struct Run {
    runtime: Arc<RuntimeEnv>,
    pool: Arc<FairSpillPool>,
}

fn fixture(eval_end: usize) -> Fixture {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("val", DataType::Float64, true),
        Field::new("dc", DataType::Utf8, false),
    ]));
    let mut partitions = vec![Vec::new(); PARTITIONS];
    for group in 0..GROUPS {
        // Two source series per group exercise partial aggregation across partitions.
        for series in 0..2 {
            let partition = (group + series) % PARTITIONS;
            let times = (0..RAW_POINTS)
                .map(|i| i as i64 * STEP_MS)
                .collect::<Vec<_>>();
            let values = (0..RAW_POINTS)
                .map(|i| {
                    // Integer slopes and one counter reset produce exactly representable
                    // rates; nulls exercise the projection filter.
                    if i % 97 == 0 {
                        None
                    } else if (300..304).contains(&i) {
                        Some((i - 300) as f64)
                    } else {
                        Some((i as i64 * (group as i64 % 7 + 2) + series as i64 * 20) as f64)
                    }
                })
                .collect::<Vec<_>>();
            let labels = vec![format!("datacenter-{group:03}"); RAW_POINTS];
            partitions[partition].push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(times)),
                        Arc::new(Float64Array::from(values)),
                        Arc::new(StringArray::from(labels)),
                    ],
                )
                .unwrap(),
            );
        }
    }
    let source = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&partitions, schema.clone(), None).unwrap(),
    ))) as Arc<dyn ExecutionPlan>;
    let empty = datafusion::logical_expr::LogicalPlan::EmptyRelation(
        datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                datafusion_common::DFSchema::try_from(schema.as_ref().clone()).unwrap(),
            ),
        },
    );
    let range = RangeManipulate::new(
        (RAW_POINTS as i64 - eval_end as i64) * STEP_MS,
        (RAW_POINTS as i64 - 1) * STEP_MS,
        STEP_MS,
        0,
        RANGE_MS,
        "ts".to_string(),
        vec!["val".to_string()],
        empty,
    )
    .unwrap()
    .to_execution_plan(Arc::clone(&source));

    let ts = Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>;
    let values = Arc::new(Column::new("val", 1)) as Arc<dyn PhysicalExpr>;
    let eval_time = Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>;
    let range_literal =
        Arc::new(Literal::new(ScalarValue::Int64(Some(RANGE_MS)))) as Arc<dyn PhysicalExpr>;
    let rate = Arc::new(ScalarFunctionExpr::new(
        "prom_rate",
        Arc::new(Rate::scalar_udf()),
        vec![
            Arc::new(Column::new("ts_range", 3)),
            values,
            eval_time,
            range_literal,
        ],
        Arc::new(Field::new("rate", DataType::Float64, true)),
        Arc::new(ConfigOptions::default()),
    )) as Arc<dyn PhysicalExpr>;
    let dc = Arc::new(Column::new("dc", 2)) as Arc<dyn PhysicalExpr>;
    let projection = Arc::new(
        ProjectionExec::try_new(
            vec![
                (ts, "ts".to_string()),
                (rate, "rate".to_string()),
                (dc, "dc".to_string()),
            ],
            Arc::clone(&range),
        )
        .unwrap(),
    );
    let filter = Arc::new(
        FilterExec::try_new(
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("rate", 1)))),
            projection.clone(),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let group = PhysicalGroupBy::new_single(vec![
        (
            Arc::new(Column::new("dc", 2)) as Arc<dyn PhysicalExpr>,
            "dc".to_string(),
        ),
        (
            Arc::new(Column::new("ts", 0)) as Arc<dyn PhysicalExpr>,
            "ts".to_string(),
        ),
    ]);
    let sum = Arc::new(
        AggregateExprBuilder::new(
            datafusion::functions_aggregate::sum::sum_udaf(),
            vec![Arc::new(Column::new("rate", 1))],
        )
        .schema(filter.schema())
        .alias("sum_rate")
        .build()
        .unwrap(),
    );
    let partial = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group.clone(),
            vec![sum.clone()],
            vec![None],
            filter,
            projection.schema(),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let repartition = Arc::new(
        RepartitionExec::try_new(
            partial,
            Partitioning::Hash(
                vec![
                    Arc::new(Column::new("dc", 0)),
                    Arc::new(Column::new("ts", 1)),
                ],
                PARTITIONS,
            ),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let final_sum = Arc::new(
        AggregateExprBuilder::new(
            datafusion::functions_aggregate::sum::sum_udaf(),
            vec![Arc::new(Column::new("sum_rate", 2))],
        )
        .schema(repartition.schema())
        .alias("sum_rate")
        .build()
        .unwrap(),
    );
    let final_aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group.as_final(),
            vec![final_sum],
            vec![None],
            repartition.clone(),
            repartition.schema(),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    let coalesced = Arc::new(CoalescePartitionsExec::new(final_aggregate));
    let sort = SortExec::new(
        [
            PhysicalSortExpr::new(Arc::new(Column::new("dc", 0)), Default::default()),
            PhysicalSortExpr::new(Arc::new(Column::new("ts", 1)), Default::default()),
        ]
        .into(),
        coalesced,
    );
    Fixture {
        root: Arc::new(sort),
    }
}

fn run_env() -> Run {
    let pool = Arc::new(FairSpillPool::new(MEMORY_LIMIT));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
            )
            .build()
            .unwrap(),
    );
    Run { runtime, pool }
}

fn collect_spill_stats(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize, usize) {
    let mut stats = (0, 0, 0);
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && matches!(
            aggregate.mode(),
            AggregateMode::Final | AggregateMode::FinalPartitioned
        )
        && let Some(metrics) = aggregate.metrics()
    {
        stats.0 += metrics.spill_count().unwrap_or(0);
        stats.1 += metrics.spilled_bytes().unwrap_or(0);
        stats.2 += metrics.spilled_rows().unwrap_or(0);
    }
    for child in plan.children() {
        let child_stats = collect_spill_stats(child);
        stats.0 += child_stats.0;
        stats.1 += child_stats.1;
        stats.2 += child_stats.2;
    }
    stats
}

fn task_context(runtime: &Arc<RuntimeEnv>, migration: bool) -> Arc<TaskContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(PARTITIONS)
        .with_batch_size(128)
        .with_sort_spill_reservation_bytes(32 * 1024);
    config.options_mut().execution.enable_migration_aggregate = migration;
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::clone(runtime))
        .build();
    Arc::new(TaskContext::from(&state))
}

async fn execute_full_with_held_batch(
    plan: Arc<dyn ExecutionPlan>,
    runtime: &Arc<RuntimeEnv>,
    migration: bool,
) -> Vec<RecordBatch> {
    let mut stream = plan.execute(0, task_context(runtime, migration)).unwrap();
    let held = stream.next().await.unwrap().unwrap();
    let held_snapshot = rows(std::slice::from_ref(&held));
    let mut batches = vec![held.clone()];
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    if batches.iter().map(RecordBatch::num_rows).sum::<usize>() > 128 {
        assert!(batches.len() >= 2, "expected multiple output batches");
    }
    assert_eq!(rows(std::slice::from_ref(&held)), held_snapshot);
    batches
}

fn rows(batches: &[RecordBatch]) -> BTreeMap<(String, i64), f64> {
    let mut output = BTreeMap::new();
    for batch in batches {
        let dcs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = (dcs.value(row).to_string(), times.value(row));
            assert!(
                !output.contains_key(&key),
                "duplicate output key {key:?}; partitioned sort lost rows or duplicated groups"
            );
            output.insert(
                key,
                if sums.is_null(row) {
                    f64::NAN
                } else {
                    sums.value(row)
                },
            );
        }
    }
    output
}

fn assert_results_equal(
    actual: &BTreeMap<(String, i64), f64>,
    expected: &BTreeMap<(String, i64), f64>,
    migration: bool,
) {
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    for (key, expected_value) in expected {
        let actual_value = actual.get(key).unwrap();
        assert!(
            actual_value.to_bits() == expected_value.to_bits()
                || (actual_value - expected_value).abs() <= 1e-12,
            "{key:?}: actual={actual_value}, expected={expected_value}, migration={migration}"
        );
    }
}

#[tokio::test]
async fn bounded_full_pipeline_rate_fusion_matches_spilling_baseline_and_releases_resources() {
    for migration in [true, false] {
        for eval_end in [1, EVALS] {
            let fixture = fixture(eval_end);
            let baseline_run = run_env();
            let fused_run = run_env();
            let mut config = ConfigOptions::default();
            config.execution.enable_migration_aggregate = migration;
            let optimized = PromqlRateFusion
                .optimize(Arc::clone(&fixture.root), &config)
                .unwrap();
            let before = displayable(fixture.root.as_ref()).indent(true).to_string();
            let after = displayable(optimized.as_ref()).indent(true).to_string();
            assert_ne!(after, before, "migration={migration}, eval_end={eval_end}");
            assert!(after.contains("PromRangeRateExec"), "{after}");
            for retained in [
                "FilterExec",
                "Partial",
                "FinalPartitioned",
                "RepartitionExec",
                "SortExec",
            ] {
                assert!(after.contains(retained), "missing {retained} in {after}");
            }

            let expected = execute_full_with_held_batch(
                Arc::clone(&fixture.root),
                &baseline_run.runtime,
                migration,
            )
            .await;
            let actual =
                execute_full_with_held_batch(Arc::clone(&optimized), &fused_run.runtime, migration)
                    .await;
            let expected_rows = rows(&expected);
            let actual_rows = rows(&actual);
            assert_results_equal(&actual_rows, &expected_rows, migration);
            assert_eq!(expected_rows.len(), GROUPS * eval_end);
            assert_eq!(actual_rows.len(), GROUPS * eval_end);
            for group in 0..GROUPS {
                for eval in 0..eval_end {
                    let key = (
                        format!("datacenter-{group:03}"),
                        (RAW_POINTS as i64 - eval_end as i64 + eval as i64) * STEP_MS,
                    );
                    assert!(
                        expected_rows.contains_key(&key),
                        "missing baseline key {key:?}"
                    );
                    assert!(actual_rows.contains_key(&key), "missing fused key {key:?}");
                }
            }
            let spill = collect_spill_stats(&optimized);
            let baseline_spill = collect_spill_stats(&fixture.root);
            if eval_end == EVALS {
                assert!(
                    spill.0 > 0 && spill.1 > 0 && spill.2 > 0,
                    "fused final aggregate spill={spill:?}"
                );
                assert!(
                    baseline_spill.0 > 0 && baseline_spill.1 > 0 && baseline_spill.2 > 0,
                    "baseline final aggregate spill={baseline_spill:?}"
                );
            }
            assert!(baseline_run.pool.reserved() <= MEMORY_LIMIT);
            assert!(fused_run.pool.reserved() <= MEMORY_LIMIT);
            assert_eq!(
                baseline_run.pool.reserved(),
                0,
                "baseline reservations remain"
            );
            assert_eq!(fused_run.pool.reserved(), 0, "fused reservations remain");
            for run in [&baseline_run, &fused_run] {
                for dir in run.runtime.disk_manager.temp_dir_paths() {
                    if dir.exists() {
                        assert_eq!(
                            std::fs::read_dir(dir).unwrap().count(),
                            0,
                            "spill files leaked"
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn early_drop_preserves_held_output_and_releases_memory_reservations() {
    for migration in [true, false] {
        let fixture = fixture(EVALS);
        let run = run_env();
        let mut config = ConfigOptions::default();
        config.execution.enable_migration_aggregate = migration;
        let optimized = PromqlRateFusion
            .optimize(Arc::clone(&fixture.root), &config)
            .unwrap();
        let mut stream = optimized
            .execute(0, task_context(&run.runtime, migration))
            .unwrap();
        let held = stream.next().await.unwrap().unwrap();
        let held_snapshot = rows(std::slice::from_ref(&held));
        assert!(!held_snapshot.is_empty());
        drop(stream);
        tokio::task::yield_now().await;
        assert_eq!(rows(std::slice::from_ref(&held)), held_snapshot);
        drop((optimized, fixture));
        assert!(held.num_rows() > 0);
        assert_eq!(run.pool.reserved(), 0);
        for dir in run.runtime.disk_manager.temp_dir_paths() {
            if dir.exists() {
                assert_eq!(std::fs::read_dir(dir).unwrap().count(), 0);
            }
        }
    }
}
