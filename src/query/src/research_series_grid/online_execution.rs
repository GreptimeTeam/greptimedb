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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use futures::{TryStreamExt, stream};
use promql_parser::parser::Expr;

use crate::research_series_grid::evaluate::{Evaluator, SelectorInputs, Value};
use crate::research_series_grid::execution::into_batches;
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::online_frontier::join_sum::JoinSum;
use crate::research_series_grid::online_frontier::{self, FragmentInput};
use crate::research_series_grid::online_input::SeriesInput;
use crate::research_series_grid::online_scan::collect_series;
use crate::research_series_grid::vector::SeriesVector;

#[derive(Debug, Clone)]
pub(crate) struct OnlineQuery {
    pub fragments: Option<Arc<Vec<FragmentInput>>>,
    pub expr: Expr,
    pub grid: Arc<TimeGrid>,
    pub specs: Arc<Vec<SeriesInput>>,
    pub tag_orders: Arc<BTreeMap<String, Vec<String>>>,
    pub schema: SchemaRef,
    pub lookback: i64,
    pub workers: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct OnlineExec {
    query: Arc<OnlineQuery>,
    children: Vec<Arc<dyn ExecutionPlan>>,
    join_sum: Option<JoinSum>,
    properties: Arc<PlanProperties>,
}

impl OnlineExec {
    pub(crate) fn try_new(
        query: Arc<OnlineQuery>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        if children.len()
            != query
                .fragments
                .as_ref()
                .map_or(query.specs.len(), |inputs| inputs.len())
        {
            return Err(DataFusionError::Plan(
                "Online series child count mismatch".into(),
            ));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(query.schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        let join_sum = JoinSum::recognize(&query);
        Ok(Self {
            join_sum,
            query,
            children,
            properties,
        })
    }
}

impl DisplayAs for OnlineExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ResearchOnlineSeries: arrow_join_sum={}, {}",
            self.join_sum.is_some(),
            self.query.expr
        )
    }
}

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl ExecutionPlan for OnlineExec {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn name(&self) -> &str {
        "ResearchOnlineSeries"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![self.join_sum.is_none(); self.children.len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(self.query.clone(), children)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "Online series has one output partition".into(),
            ));
        }
        if let Some(program) = &self.join_sum {
            return program.execute(self.query.clone(), self.children.clone(), context);
        }
        let query = self.query.clone();
        let children = self.children.clone();
        let memory = MemoryConsumer::new("ResearchOnlineEvaluator").register(context.memory_pool());
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let work = stream::once(async move {
            let input = if let Some(inputs) = &query.fragments {
                online_frontier::collect(
                    inputs.clone(),
                    children,
                    query.grid.clone(),
                    context,
                    guard.0.clone(),
                )
                .await?
            } else {
                collect_series(
                    query.specs.clone(),
                    children,
                    query.grid.clone(),
                    query.lookback,
                    query.workers,
                    context,
                    guard.0.clone(),
                )
                .await?
            };
            let cancelled = guard.0.clone();
            let result = common_runtime::spawn_blocking_query(move || {
                evaluate_collected(&query, input, memory, &cancelled)
            })
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            drop(guard);
            result
        });
        let batches = work
            .map_ok(|(batches, reservation)| {
                stream::unfold(
                    (batches.into_iter(), reservation),
                    |(mut batches, reservation)| async move {
                        batches
                            .next()
                            .map(|batch| (Ok(batch), (batches, reservation)))
                    },
                )
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.query.schema.clone(),
            batches,
        )))
    }
}

pub(crate) fn evaluate_collected(
    query: &OnlineQuery,
    input: crate::research_series_grid::online_scan::CollectedSeries,
    memory: datafusion::execution::memory_pool::MemoryReservation,
    cancelled: &AtomicBool,
) -> Result<(
    Vec<datafusion::arrow::record_batch::RecordBatch>,
    datafusion::execution::memory_pool::MemoryReservation,
)> {
    // The blocking task can outlive cancellation of the response stream.
    let _input_reservation = input.reservation;
    let mut evaluator = Evaluator {
        inputs: SelectorInputs::Prepared(&input.inputs),
        workers: 1,
        block_evaluation: false,
        grid: &query.grid,
        lookback: query.lookback,
        tag_orders: &query.tag_orders,
        cancelled,
        memory: Some(&memory),
    };
    let (series, _result_reservation) = match evaluator.evaluate(&query.expr)? {
        Value::Vector(result) => (result.series, result.reservation),
        Value::Scalar(value) => {
            let retained = memory.new_empty();
            retained.try_grow(
                query
                    .grid
                    .instants
                    .len()
                    .saturating_mul(size_of::<Option<f64>>()),
            )?;
            (
                vec![SeriesVector {
                    labels: BTreeMap::new(),
                    values: vec![Some(value); query.grid.instants.len()],
                }],
                Some(retained),
            )
        }
    };
    let output_reservation = memory.new_empty();
    let bytes = output_memory_bound(&series, &query.schema)?;
    output_reservation.try_grow(bytes)?;
    let batches = into_batches(series, &query.grid, &query.schema, cancelled)?;
    Ok((batches, output_reservation))
}

pub(super) fn output_memory_bound(series: &[SeriesVector], schema: &SchemaRef) -> Result<usize> {
    let bytes = series.iter().fold(0usize, |total, series| {
        let count = series.values.iter().filter(|value| value.is_some()).count();
        let row = schema
            .fields()
            .iter()
            .skip(2)
            .fold(16usize, |bytes, field| {
                bytes
                    .saturating_add(8)
                    .saturating_add(series.labels.get(field.name()).map_or(0, String::len))
            });
        // Builders can grow while input vectors and previously built columns remain live.
        total
            .saturating_add(count.saturating_mul(row).saturating_mul(4))
            .saturating_add(schema.fields().len().saturating_mul(512))
    });
    if bytes == usize::MAX {
        return Err(DataFusionError::Execution(
            "Online output memory size overflow".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research_series_grid::evaluate::SelectorKey;
    use datafusion::arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, MemoryReservation};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use futures::StreamExt;
    use promql_parser::parser::parse;

    #[tokio::test]
    async fn physical_children_supply_current_samples() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
            Field::new("host", DataType::Utf8, true),
        ]));
        let source = |values: [f64; 2]| -> Arc<dyn ExecutionPlan> {
            let batches = [("a", values), ("b", [3.0, 4.0])]
                .into_iter()
                .map(|(host, values)| {
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(TimestampMillisecondArray::from(vec![0, 10])),
                            Arc::new(Float64Array::from(values.to_vec())),
                            Arc::new(StringArray::from(vec![host; 2])),
                        ],
                    )
                    .unwrap()
                })
                .collect();
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[batches], schema.clone(), None).unwrap(),
            )))
        };
        let Expr::VectorSelector(selector) = parse("x").unwrap() else {
            panic!("selector")
        };
        let query = Arc::new(OnlineQuery {
            fragments: None,
            expr: parse("sum(x)").unwrap(),
            grid: Arc::new(TimeGrid::new(0, 10, 10).unwrap()),
            specs: Arc::new(vec![SeriesInput {
                key: SelectorKey::new(&selector, None),
                metric: "x".into(),
                time_column: "ts".into(),
                value_column: "value".into(),
                tags: vec!["host".into()],
                offset: 0,
            }]),
            tag_orders: Arc::new(BTreeMap::from([("x".into(), vec!["host".into()])])),
            schema: schema.clone(),
            lookback: 30,
            workers: 2,
        });
        let plan = Arc::new(OnlineExec::try_new(query, vec![source([2.0, 3.0])]).unwrap());
        assert_eq!(plan.children().len(), 1);
        let replacement = plan
            .clone()
            .with_new_children(vec![source([10.0, 20.0])])
            .unwrap();
        for (plan, expected) in [
            (plan.clone() as Arc<dyn ExecutionPlan>, vec![5.0, 7.0]),
            (replacement, vec![13.0, 24.0]),
        ] {
            let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build()
                .unwrap();
            let context = Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
            let mut stream = plan.execute(0, context.clone()).unwrap();
            let first = stream.next().await.unwrap().unwrap();
            assert!(pool.reserved() >= first.get_array_memory_size());
            drop(stream);
            assert_eq!(pool.reserved(), 0);
            let batches = datafusion::physical_plan::collect(plan, context)
                .await
                .unwrap();
            assert_eq!(batches.len(), 1);
            let values = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.values().as_ref(), expected);
            let times = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            assert_eq!(times.values().as_ref(), [0, 10]);
            assert_eq!(pool.reserved(), 0);
        }

        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let pool = Arc::new(BlockEvaluation {
            pool: GreedyMemoryPool::new(1 << 20),
            entered: std::sync::Mutex::new(Some(entered)),
            release: std::sync::Mutex::new(blocked),
        });
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()
            .unwrap();
        let context = Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
        let task =
            tokio::spawn(async move { datafusion::physical_plan::collect(plan, context).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        let cancelled = task.await.unwrap_err().is_cancelled();
        let retained_while_blocked = pool.reserved();
        release.send(()).unwrap();
        assert!(cancelled);
        assert!(retained_while_blocked > 0);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.reserved() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[derive(Debug)]
    struct BlockEvaluation {
        pool: GreedyMemoryPool,
        entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl fmt::Display for BlockEvaluation {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("BlockEvaluation")
        }
    }

    impl MemoryPool for BlockEvaluation {
        fn name(&self) -> &str {
            "BlockEvaluation"
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.pool.grow(reservation, additional);
        }

        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.pool.shrink(reservation, shrink);
        }

        fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
            if reservation.consumer().name() == "ResearchOnlineEvaluator"
                && let Some(entered) = self.entered.lock().unwrap().take()
            {
                entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            self.pool.try_grow(reservation, additional)
        }

        fn reserved(&self) -> usize {
            self.pool.reserved()
        }
    }
}
