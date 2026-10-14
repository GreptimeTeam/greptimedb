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
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common_query::request::{PreparedSeriesRead, SeriesSourceRequest};
use datafusion::catalog::Session;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use futures::{StreamExt, TryStreamExt, stream};
use session::context::QueryContext;

use crate::dist_plan::DistExtensionPlanner;
use crate::research_series_grid::evaluate::VectorResult;
use crate::research_series_grid::online_execution::evaluate_collected;
use crate::research_series_grid::online_scan::CollectedSeries;
use crate::research_series_grid::query_program::{ProgramNode, QueryProgram, SourceOperation};
use crate::research_series_grid::scan_max::InstantMaxProcessor;
use crate::research_series_grid::scan_range::RangeProcessor;
use crate::research_series_grid::vector::{Labels, SeriesVector};

/// Dense join inputs above this size are left to the row-streaming plan.
const JOIN_INPUT_LIMIT: usize = 2 * 1024 * 1024 * 1024;
pub(crate) async fn try_bind(
    node: &ProgramNode,
    planner: &DistExtensionPlanner,
    session: &dyn Session,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let state = session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| invalid("Query program requires a SessionState"))?;
    let query_ctx = state
        .config()
        .get_extension()
        .unwrap_or_else(QueryContext::arc);
    let header = api::v1::region::RegionRequestHeader {
        tracing_context: common_telemetry::tracing_context::TracingContext::from_json(
            &state.session_id(),
        )
        .to_w3c(),
        dbname: query_ctx.get_db_string(),
        query_context: Some(query_ctx.as_ref().into()),
    };
    let mut requests = Vec::new();
    let mut input_indices = Vec::new();
    let mut target = None;
    for (index, (source, scan)) in node.program.sources.iter().zip(&node.scans).enumerate() {
        let Some(name) = DistExtensionPlanner::extract_full_table_name(scan)? else {
            return Ok(None);
        };
        let regions = planner.get_regions(&name, scan).await?;
        for region_id in regions {
            let current = planner
                .region_query_handler
                .select_target(query_ctx.read_preference(), region_id)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if target.as_ref().is_some_and(|target| *target != current) {
                return Ok(None);
            }
            target = Some(current);
            input_indices.push(index);
            requests.push(SeriesSourceRequest {
                header: Some(header.clone()),
                region_id,
                schema: source.schema.clone(),
                projection: source.projection.clone(),
                filters: source.filters.clone(),
                ordering: source.ordering.clone(),
                selector: None,
            });
        }
    }
    let Some(target) = target else {
        return Ok(None);
    };
    let Some(sources) = planner
        .region_query_handler
        .prepare_series_sources(&target, requests)
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?
    else {
        return Ok(None);
    };
    if sources.len() != input_indices.len() {
        return Err(invalid("Prepared query program source count mismatch"));
    }
    // A join keeps every input series as a dense grid until it is matched. With many
    // short-lived (churned) series those grids are mostly empty yet exceed the query
    // pool; the row-streaming plan handles them within budget.
    if node.program.join_sum.is_some() {
        let points = node.program.query.grid.instants.len();
        let bytes = sources
            .iter()
            .map(|source| source.source.num_series())
            .sum::<usize>()
            .saturating_mul(points)
            .saturating_mul(size_of::<Option<f64>>());
        if bytes > JOIN_INPUT_LIMIT {
            common_telemetry::debug!(
                bytes,
                "Research join program declined: dense inputs too large"
            );
            return Ok(None);
        }
    }
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(node.program.query.schema.clone()),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ));
    common_telemetry::debug!(
        sources = sources.len(),
        "Research complete query program bound"
    );
    Ok(Some(Arc::new(ProgramExec {
        program: node.program.clone(),
        sources: sources.into_iter().map(Arc::new).collect(),
        input_indices,
        properties,
    })))
}

struct ProgramExec {
    program: Arc<QueryProgram>,
    sources: Vec<Arc<PreparedSeriesRead>>,
    input_indices: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for ProgramExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgramExec")
            .field("query", &self.program.query.expr)
            .finish()
    }
}
impl DisplayAs for ProgramExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResearchQueryProgram: {}", self.program.query.expr)
    }
}
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
impl ExecutionPlan for ProgramExec {
    fn name(&self) -> &str {
        "ResearchQueryProgram"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(invalid("Query program has no physical children"));
        }
        Ok(self)
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(invalid("Query program has one output partition"));
        }
        let program = self.program.clone();
        let sources = self.sources.clone();
        let indices = self.input_indices.clone();
        let reservation = Arc::new(
            MemoryConsumer::new("ResearchQueryProgramInputs").register(context.memory_pool()),
        );
        let memory = MemoryConsumer::new("ResearchOnlineEvaluator").register(context.memory_pool());
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let work = stream::once(async move {
            let specs = program
                .query
                .fragments
                .clone()
                .ok_or_else(|| invalid("Query program requires fragments"))?;
            let workers = program.query.workers.clamp(1, 32);
            let points = program.query.grid.instants.len();
            // A global top-one can stay in the existing single source task.
            // Larger or multi-source reads retain the parallel program path.
            if let Some(top) = &program.top_one
                && sources.len() == 1
                && read_tasks(sources[0].source.num_series(), points, workers) == 1
            {
                common_telemetry::debug!(tasks = 1, "Research complete query program executing");
                let source = sources[0].clone();
                let tracker = source.memory_tracker.clone();
                let SourceOperation::Range(spec) = &program.sources[indices[0]].operation else {
                    return Err(invalid("Topk program requires an instant source"));
                };
                let spec = spec.clone();
                let top = top.clone();
                let cancelled = guard.0.clone();
                let (batches, memory, bytes) = common_runtime::spawn_blocking_query(move || {
                    let mut processor =
                        super::top_one::Processor::new(top, spec, memory, cancelled.clone())?;
                    source
                        .source
                        .read_series(0..source.source.num_series(), &mut processor)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    let (series, memory) = processor.finish()?;
                    memory.try_grow(
                        crate::research_series_grid::online_execution::output_memory_bound(
                            &series,
                            &program.query.schema,
                        )?,
                    )?;
                    let batches = crate::research_series_grid::execution::into_batches(
                        series,
                        &program.query.grid,
                        &program.query.schema,
                        &cancelled,
                    )?;
                    let bytes = batches
                        .iter()
                        .try_fold(0usize, |size, batch| {
                            size.checked_add(batch.get_array_memory_size())
                        })
                        .ok_or_else(|| invalid("Topk output size overflow"))?;
                    common_telemetry::debug!("Research native top-one program activated");
                    Ok::<_, DataFusionError>((batches, memory, bytes))
                })
                .await
                .map_err(|error| invalid(&error.to_string()))??;
                let quota = if let Some(tracker) = tracker {
                    Some(
                        tracker
                            .reserve_output_bytes(bytes)
                            .await
                            .map_err(|error| DataFusionError::External(Box::new(error)))?,
                    )
                } else {
                    None
                };
                drop(guard);
                return Ok((batches, memory, vec![quota]));
            }
            let mut tasks = Vec::new();
            for (source, index) in sources.into_iter().zip(indices) {
                let count = source.source.num_series();
                let task_count = read_tasks(count, points, workers);
                let size = count.div_ceil(task_count).max(1);
                for task in 0..task_count {
                    let start = (task * size).min(count);
                    tasks.push((
                        source.clone(),
                        index,
                        start..start.saturating_add(size).min(count),
                    ));
                }
            }
            common_telemetry::debug!(
                tasks = tasks.len(),
                "Research complete query program executing"
            );
            let cancelled = guard.0.clone();
            let mut pending = stream::iter(tasks)
                .map(|(source, index, range)| {
                    let operation = program.sources[index].operation.clone();
                    let spec = specs[index].clone();
                    let grid = program.query.grid.clone();
                    let retained = reservation.clone();
                    let scratch = reservation.new_empty();
                    let cancelled = cancelled.clone();
                    async move {
                        let tracker = source.memory_tracker.clone();
                        let output = common_runtime::spawn_blocking_query(move || {
                            let (series, _kernel_memory) = match operation {
                                SourceOperation::Range(spec) => {
                                    let mut processor = RangeProcessor::new_series(
                                        spec,
                                        scratch,
                                        cancelled.clone(),
                                    )?;
                                    source.source.read_series(range, &mut processor).map_err(
                                        |error| DataFusionError::External(Box::new(error)),
                                    )?;
                                    processor.finish_series()?
                                }
                                SourceOperation::Max(spec) => {
                                    let mut processor = InstantMaxProcessor::new_series(
                                        spec,
                                        scratch,
                                        cancelled.clone(),
                                    )?;
                                    source.source.read_series(range, &mut processor).map_err(
                                        |error| DataFusionError::External(Box::new(error)),
                                    )?;
                                    processor.finish_series()?
                                }
                            };
                            let mut rows = BTreeMap::new();
                            spec.collect_series(series, &grid, &cancelled, &retained, &mut rows)?;
                            let vectors = rows
                                .into_iter()
                                .map(|(labels, values)| SeriesVector { labels, values })
                                .collect::<Vec<_>>();
                            let bytes = vectors
                                .iter()
                                .try_fold(0usize, |bytes, series| {
                                    bytes.checked_add(series.memory_size())
                                })
                                .ok_or_else(|| invalid("Query program output size overflow"))?;
                            Ok::<_, DataFusionError>((vectors, bytes))
                        })
                        .await
                        .map_err(|error| invalid(&error.to_string()))??;
                        let quota = if let Some(tracker) = tracker {
                            Some(
                                tracker
                                    .reserve_output_bytes(output.1)
                                    .await
                                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
                            )
                        } else {
                            None
                        };
                        Ok::<_, DataFusionError>((index, output.0, quota))
                    }
                })
                .buffer_unordered(workers);
            let mut outputs = Vec::new();
            while let Some(output) = pending.try_next().await? {
                outputs.push(output);
            }
            drop(pending);
            let cancelled = guard.0.clone();
            let result = common_runtime::spawn_blocking_query(move || {
                let mut rows: Vec<BTreeMap<Labels, Vec<Option<f64>>>> =
                    specs.iter().map(|_| BTreeMap::new()).collect();
                let mut quotas = Vec::with_capacity(outputs.len());
                for (index, output, quota) in outputs {
                    quotas.push(quota);
                    for series in output {
                        if cancelled.load(Ordering::Relaxed) {
                            return Err(invalid("Query program cancelled"));
                        }
                        match rows[index].entry(series.labels) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert(series.values);
                            }
                            std::collections::btree_map::Entry::Occupied(mut entry) => {
                                for (point, (destination, value)) in
                                    entry.get_mut().iter_mut().zip(series.values).enumerate()
                                {
                                    if point % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                                        return Err(invalid("Query program cancelled"));
                                    }
                                    if let Some(value) = value {
                                        specs[index].merge_point(destination, value)?;
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(join) = &program.join_sum {
                    let series = join.evaluate_series(&program.query, rows, &memory, &cancelled)?;
                    memory.try_grow(
                        crate::research_series_grid::online_execution::output_memory_bound(
                            &series,
                            &program.query.schema,
                        )?,
                    )?;
                    let batches = crate::research_series_grid::execution::into_batches(
                        series,
                        &program.query.grid,
                        &program.query.schema,
                        &cancelled,
                    )?;
                    common_telemetry::debug!("Research native join sum activated");
                    return Ok((batches, memory, quotas));
                }
                let inputs = specs
                    .iter()
                    .zip(rows)
                    .map(|(spec, rows)| {
                        (
                            spec.key.clone(),
                            VectorResult {
                                series: rows
                                    .into_iter()
                                    .map(|(labels, values)| SeriesVector { labels, values })
                                    .collect(),
                                tag_order: spec.tags.clone(),
                                reservation: None,
                            },
                        )
                    })
                    .collect();
                let (batches, memory) = evaluate_collected(
                    &program.query,
                    CollectedSeries {
                        inputs,
                        reservation,
                    },
                    memory,
                    &cancelled,
                )?;
                Ok::<_, DataFusionError>((batches, memory, quotas))
            })
            .await
            .map_err(|error| invalid(&error.to_string()))??;
            drop(guard);
            Ok::<_, DataFusionError>(result)
        });
        let batches = work
            .map_ok(|(batches, memory, quotas)| {
                stream::unfold(
                    (batches.into_iter(), memory, quotas),
                    |(mut batches, memory, quotas)| async move {
                        batches
                            .next()
                            .map(|batch| (Ok(batch), (batches, memory, quotas)))
                    },
                )
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.program.query.schema.clone(),
            batches,
        )))
    }
}

/// Read tasks for `series` over a grid of `points`. Work grows with the grid, so the
/// split is sized by series x points, calibrated to 128 series per task at 73 points.
fn read_tasks(series: usize, points: usize, workers: usize) -> usize {
    series
        .saturating_mul(points.max(1))
        .div_ceil(128 * 73)
        .clamp(1, workers)
        .min(series.max(1))
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::datasource::{DefaultTableSource, TableProvider};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_common::{ScalarValue, TableReference};
    use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableSource, col};
    use promql::extension_plan::{InstantManipulate, SeriesDivide};
    use promql_parser::parser::parse;
    use store_api::region_engine::{
        PreparedSeriesSource, SeriesBlockProcessor, VisibleSeriesBlock,
    };
    use store_api::storage::RegionId;

    use super::*;
    use crate::dist_plan::MergeScanLogicalPlan;

    #[test]
    fn read_tasks_scale_with_grid_length() {
        // Six-hour grid keeps the 128-series granularity.
        assert_eq!(read_tasks(32, 73, 8), 1);
        assert_eq!(read_tasks(1000, 73, 8), 8);
        // Q06 on a 7-day grid: 32 series no longer collapse into one task.
        assert_eq!(read_tasks(32, 2017, 8), 7);
        // Never more tasks than series or workers.
        assert_eq!(read_tasks(2, 20_000, 8), 2);
        assert_eq!(read_tasks(0, 2017, 8), 1);
    }
    use crate::optimizer::test_util::mock_table_provider_with_tsid;
    use crate::research_series_grid::SeriesInstantMax;
    use crate::research_series_grid::evaluate::SelectorKey;
    use crate::research_series_grid::grid::TimeGrid;
    use crate::research_series_grid::online_execution::OnlineQuery;
    use crate::research_series_grid::online_frontier::FragmentInput;
    use crate::research_series_grid::online_plan::OnlineNode;

    struct Source {
        start: i64,
        base: f64,
    }
    impl PreparedSeriesSource for Source {
        fn num_series(&self) -> usize {
            2
        }
        fn read_series(
            &self,
            range: std::ops::Range<usize>,
            processor: &mut dyn SeriesBlockProcessor,
        ) -> std::result::Result<(), common_error::ext::BoxedError> {
            for series in range {
                processor.process_series(
                    &[
                        (
                            "__tsid".into(),
                            datatypes::value::Value::UInt64(series as u64),
                        ),
                        (
                            "k0".into(),
                            datatypes::value::Value::String(format!("host{series}").into()),
                        ),
                    ],
                    &mut [Ok(VisibleSeriesBlock {
                        first_timestamp: self.start,
                        timestamp_step: 10,
                        values: Float64Array::from(vec![
                            self.base + series as f64 + 1.0,
                            self.base + series as f64 + 2.0,
                        ])
                        .into(),
                    })]
                    .into_iter(),
                )?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn compiled_program_rebinds_sources_and_merges_partial_maxima_before_sum() {
        let provider: Arc<dyn TableProvider> =
            Arc::new(mock_table_provider_with_tsid(RegionId::new(1, 1)));
        let weak = Arc::downgrade(&provider);
        let timestamp = |value| {
            datafusion_expr::Expr::Literal(
                ScalarValue::TimestampMillisecond(Some(value), None),
                None,
            )
        };
        let source = LogicalPlanBuilder::scan(
            "physical",
            Arc::new(DefaultTableSource::new(provider.clone())),
            None,
        )
        .unwrap()
        .filter(col("ts").gt(timestamp(-10)))
        .unwrap()
        .project(vec![
            col("__tsid"),
            col("k0"),
            col("ts"),
            col("v0").alias("greptime_value"),
        ])
        .unwrap()
        .sort(vec![
            col("__tsid").sort(true, true),
            col("ts").sort(true, true),
        ])
        .unwrap()
        .build()
        .unwrap();
        let divided = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec!["__tsid".into()],
                "ts".into(),
                source,
            )),
        });
        let instant = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                0,
                10,
                10,
                10,
                0,
                "ts".into(),
                vec!["k0".into()],
                Some("greptime_value".into()),
                divided,
            )),
        });
        let aggregate = LogicalPlanBuilder::from(instant)
            .aggregate(
                vec![col("k0"), col("ts")],
                vec![
                    datafusion::functions_aggregate::min_max::max_udaf()
                        .call(vec![col("greptime_value")])
                        .alias("value"),
                ],
            )
            .unwrap()
            .build()
            .unwrap();
        let fragment = SeriesInstantMax::recognize(&aggregate, &["k0".into()], "ts", "value")
            .unwrap()
            .unwrap()
            .plan();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let plan = OnlineNode::plan(
            OnlineQuery {
                fragments: Some(Arc::new(vec![FragmentInput {
                    key: SelectorKey {
                        selector: "fragment".into(),
                        function: None,
                    },
                    time: "ts".into(),
                    value: "value".into(),
                    tags: vec!["k0".into()],
                    metric: None,
                    packed: true,
                    dense: None,
                    partial_max: true,
                }])),
                expr: parse("sum(fragment)").unwrap(),
                grid: Arc::new(TimeGrid::new(0, 10, 10).unwrap()),
                specs: Arc::new(Vec::new()),
                tag_orders: Arc::new(BTreeMap::new()),
                schema: schema.clone(),
                lookback: 10,
                workers: 2,
            },
            vec![MergeScanLogicalPlan::new(fragment, false, BTreeMap::new()).into_logical_plan()],
        )
        .unwrap();
        let program = QueryProgram::compile(&plan).unwrap().unwrap();
        drop(plan);
        drop(aggregate);
        drop(provider);
        assert!(weak.upgrade().is_none());
        let fresh: Arc<dyn TableSource> = Arc::new(DefaultTableSource::new(Arc::new(
            mock_table_provider_with_tsid(RegionId::new(1, 1)),
        )));
        let sources =
            std::collections::HashMap::from([(TableReference::bare("physical"), fresh.clone())]);
        for delta in [0, 100] {
            let (bound, scans) = program.bind(delta, &sources).unwrap();
            let LogicalPlan::TableScan(scan) = &scans[0] else {
                panic!("routing scan")
            };
            assert!(Arc::ptr_eq(&scan.source, &fresh));
            assert_eq!(
                bound.sources[0].filters,
                [col("ts").gt(timestamp(delta - 10))]
            );
            assert_eq!(bound.query.grid.instants, [delta, delta + 10]);
            let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build()
                .unwrap();
            let context = Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
            let exec = Arc::new(ProgramExec {
                program: Arc::new(bound),
                sources: [0.0, 10.0]
                    .into_iter()
                    .map(|base| {
                        Arc::new(PreparedSeriesRead {
                            source: Arc::new(Source { start: delta, base }),
                            memory_tracker: None,
                        })
                    })
                    .collect(),
                input_indices: vec![0, 0],
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Final,
                    Boundedness::Bounded,
                )),
            });
            let mut stream = exec.execute(0, context).unwrap();
            let batch = stream.next().await.unwrap().unwrap();
            assert_eq!(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                [23.0, 25.0]
            );
            assert!(pool.reserved() >= batch.get_array_memory_size());
            drop(stream);
            assert_eq!(pool.reserved(), 0);
        }
        assert!(program.bind(0, &std::collections::HashMap::new()).is_err());
        let incompatible: Arc<dyn TableSource> = Arc::new(
            datafusion_expr::LogicalTableSource::new(Arc::new(Schema::empty())),
        );
        assert!(
            program
                .bind(
                    0,
                    &std::collections::HashMap::from([(
                        TableReference::bare("physical"),
                        incompatible
                    )])
                )
                .is_err()
        );
        assert!(program.bind(i64::MAX, &sources).is_err());
    }
}
