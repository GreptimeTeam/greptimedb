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

mod correlation;

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::{JoinType, NullEquality, Result, Statistics};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_optimizer::PhysicalOptimizerContext;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::join_selection::JoinSelection;
use datafusion_physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
use datafusion_physical_plan::operator_statistics::{
    ExtendedStatistics, StatisticsProvider, StatisticsRegistry, StatisticsResult,
};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, collect};

#[derive(Debug, Clone, Copy)]
enum RowStats {
    Absent,
    Exact,
}

#[derive(Debug)]
struct FixtureStatistics {
    rows: RowStats,
    fact_rows: usize,
    dim_rows: usize,
}

impl StatisticsProvider for FixtureStatistics {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        _children: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        let Some(source) = plan.downcast_ref::<DataSourceExec>() else {
            return Ok(StatisticsResult::Delegate);
        };
        if source
            .data_source()
            .downcast_ref::<MemorySourceConfig>()
            .is_none()
        {
            return Ok(StatisticsResult::Delegate);
        }
        let schema = plan.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let rows = if names.contains(&"fact_key") {
            self.fact_rows
        } else if names.contains(&"dim_key") {
            self.dim_rows
        } else {
            return Ok(StatisticsResult::Delegate);
        };
        let stats = match self.rows {
            RowStats::Absent => Statistics::new_unknown(&schema),
            RowStats::Exact => {
                Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(rows))
            }
        };
        Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
    }
}

struct Context {
    config: ConfigOptions,
    registry: StatisticsRegistry,
}

impl PhysicalOptimizerContext for Context {
    fn config_options(&self) -> &ConfigOptions {
        &self.config
    }
    fn statistics_registry(&self) -> Option<&StatisticsRegistry> {
        Some(&self.registry)
    }
}

fn source(schema: SchemaRef, columns: Vec<Vec<Option<i64>>>) -> Result<Arc<dyn ExecutionPlan>> {
    let arrays: Vec<Arc<dyn Array>> = columns
        .into_iter()
        .map(|v| Arc::new(Int64Array::from(v)) as _)
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
    Ok(MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        schema,
        None,
    )?)
}

fn schema(key: &str, value: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(key, DataType::Int64, true),
        Field::new(value, DataType::Int64, true),
    ]))
}

fn join(fact_first: bool, fact_rows: usize, dim_rows: usize) -> Result<Arc<dyn ExecutionPlan>> {
    let fact = source(
        schema("fact_key", "fact_value"),
        vec![
            (0..fact_rows)
                .map(|i| {
                    if i % 13 == 0 {
                        None
                    } else {
                        Some((i % 16) as i64)
                    }
                })
                .collect(),
            (0..fact_rows)
                .map(|i| {
                    if i % 11 == 0 {
                        None
                    } else {
                        Some((i % 7) as i64)
                    }
                })
                .collect(),
        ],
    )?;
    let dim = source(
        schema("dim_key", "dim_weight"),
        vec![
            (0..dim_rows)
                .map(|i| {
                    if i % 7 == 0 {
                        None
                    } else {
                        Some((i / 4) as i64)
                    }
                })
                .collect(),
            (0..dim_rows)
                .map(|i| {
                    if i % 9 == 0 {
                        None
                    } else {
                        Some((i % 3) as i64)
                    }
                })
                .collect(),
        ],
    )?;
    let (left, right, left_key, right_key) = if fact_first {
        (fact, dim, "fact_key", "dim_key")
    } else {
        (dim, fact, "dim_key", "fact_key")
    };
    let on: JoinOn = vec![(
        Arc::new(Column::new(left_key, 0)),
        Arc::new(Column::new(right_key, 0)),
    )];
    let hash = Arc::new(HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Auto,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let exprs = ["fact_key", "fact_value", "dim_key", "dim_weight"]
        .into_iter()
        .map(|name| {
            let index = hash.schema().index_of(name)?;
            Ok((Arc::new(Column::new(name, index)) as _, name.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(ProjectionExec::try_new(exprs, hash)?))
}

fn hash_join(plan: &dyn ExecutionPlan) -> Option<&HashJoinExec> {
    if let Some(hash) = plan.downcast_ref::<HashJoinExec>() {
        return Some(hash);
    }
    plan.children()
        .into_iter()
        .find_map(|child| hash_join(child.as_ref()))
}

fn fact_build(plan: &Arc<dyn ExecutionPlan>) -> bool {
    hash_join(plan.as_ref())
        .expect("expected hash join")
        .left()
        .schema()
        .field(0)
        .name()
        == "fact_key"
}

type Row = (Option<i64>, Option<i64>, Option<i64>, Option<i64>);

async fn output(plan: Arc<dyn ExecutionPlan>, expected_schema: &SchemaRef) -> Result<Vec<Row>> {
    let batches = collect(plan, Arc::new(TaskContext::default())).await?;
    let mut rows = Vec::new();
    for batch in batches {
        assert_eq!(batch.schema().as_ref(), expected_schema.as_ref());
        for i in 0..batch.num_rows() {
            let value = |column: usize| -> Option<i64> {
                let array = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (!array.is_null(i)).then(|| array.value(i))
            };
            rows.push((value(0), value(1), value(2), value(3)));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

fn oracle(facts: usize, dims: usize) -> Vec<Row> {
    let fact = (0..facts)
        .map(|i| {
            (
                if i % 13 == 0 {
                    None
                } else {
                    Some((i % 16) as i64)
                },
                if i % 11 == 0 {
                    None
                } else {
                    Some((i % 7) as i64)
                },
            )
        })
        .collect::<Vec<_>>();
    let dim = (0..dims)
        .map(|i| {
            (
                if i % 7 == 0 {
                    None
                } else {
                    Some((i / 4) as i64)
                },
                if i % 9 == 0 {
                    None
                } else {
                    Some((i % 3) as i64)
                },
            )
        })
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for (fk, fv) in &fact {
        for (dk, dw) in &dim {
            if fk.is_some() && fk == dk {
                rows.push((*fk, *fv, *dk, *dw));
            }
        }
    }
    rows.sort_unstable();
    rows
}

fn source_stats(
    plan: &dyn ExecutionPlan,
    registry: &StatisticsRegistry,
) -> Result<Vec<Statistics>> {
    let mut sources = Vec::new();
    for child in plan.children() {
        sources.extend(source_stats(child.as_ref(), registry)?);
    }
    if plan.downcast_ref::<DataSourceExec>().is_some() {
        sources.push(registry.compute_base(plan)?);
    }
    Ok(sources)
}

async fn check_case(fact_first: bool, facts: usize, dims: usize) -> Result<()> {
    let mut config = ConfigOptions::default();
    config.optimizer.use_statistics_registry = true;
    config.optimizer.join_reordering = true;
    config.optimizer.hash_join_single_partition_threshold = 0;
    config.optimizer.hash_join_single_partition_threshold_rows = 0;
    let original = join(fact_first, facts, dims)?;
    let expected_schema = original.schema();
    let expected = oracle(facts, dims);
    for stats_kind in [RowStats::Absent, RowStats::Exact] {
        let registry = StatisticsRegistry::with_providers(vec![Arc::new(FixtureStatistics {
            rows: stats_kind,
            fact_rows: facts,
            dim_rows: dims,
        })]);
        let context = Context {
            config: config.clone(),
            registry,
        };
        let stats = source_stats(original.as_ref(), &context.registry)?;
        assert_eq!(stats.len(), 2);
        for stat in stats {
            assert_eq!(stat.total_byte_size, Precision::Absent);
            match stats_kind {
                RowStats::Absent => assert_eq!(stat.num_rows, Precision::Absent),
                RowStats::Exact => assert!(matches!(stat.num_rows, Precision::Exact(_))),
            }
        }
        let plan = JoinSelection::new().optimize_with_context(Arc::clone(&original), &context)?;
        assert_eq!(
            hash_join(plan.as_ref()).unwrap().mode,
            PartitionMode::Partitioned
        );
        assert_eq!(plan.schema().as_ref(), expected_schema.as_ref());
        let build_is_fact = fact_build(&plan);
        assert_eq!(
            build_is_fact,
            match stats_kind {
                RowStats::Absent => fact_first,
                RowStats::Exact => facts <= dims,
            }
        );
        let result = output(plan, &expected_schema).await?;
        assert_eq!(result, expected);
        println!(
            "fact={facts} dim={dims} orientation={} stats={stats_kind:?} build={} rows={}",
            if fact_first {
                "fact-first"
            } else {
                "dim-first"
            },
            if build_is_fact { "fact" } else { "dim" },
            result.len()
        );
    }
    Ok(())
}

async fn run_cases() -> Result<()> {
    for (facts, dims) in [(256, 64), (16, 64), (256, 16)] {
        check_case(true, facts, dims).await?;
        check_case(false, facts, dims).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().skip(1).any(|arg| arg == "--bench") {
        run_cases().await?;
        correlation::run_cases().await?;
        return correlation::run_bench().await;
    }
    run_cases().await?;
    correlation::run_cases().await
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn absent_to_exact_statistics_change_build_side_and_preserve_bag()
    -> datafusion_common::Result<()> {
        super::run_cases().await
    }
}
