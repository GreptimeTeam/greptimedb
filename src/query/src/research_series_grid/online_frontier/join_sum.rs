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
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::{Array, Float64Array, ListArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::{DataFusionError, Result};
use datatypes::arrow_array::string_array_value_at_index;
use futures::{StreamExt, TryStreamExt, stream};
use promql_parser::parser::{Expr, LabelModifier, VectorMatchCardinality};

use super::{PACKED_POINTS, collect_packed};
use crate::research_series_grid::evaluate::SelectorKey;
use crate::research_series_grid::execution::into_batches;
use crate::research_series_grid::online_execution::OnlineQuery;
use crate::research_series_grid::vector::{Labels, SeriesVector, group_labels};

#[derive(Debug, Clone)]
pub(crate) struct JoinSum {
    left: usize,
    right: usize,
    on: Vec<String>,
    groups: Vec<String>,
    without: bool,
}

fn bare(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(paren) => bare(&paren.expr),
        _ => expr,
    }
}

fn same_labels(left: &[String], right: &[String]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_unstable();
    right.sort_unstable();
    left == right
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

impl JoinSum {
    pub(crate) fn recognize(query: &OnlineQuery) -> Option<Self> {
        let specs = query.fragments.as_ref()?;
        if specs.len() != 2 {
            return None;
        }
        let Expr::Aggregate(sum) = bare(&query.expr) else {
            return None;
        };
        if sum.op.to_string() != "sum" || sum.param.is_some() {
            return None;
        }
        let Expr::Binary(binary) = bare(&sum.expr) else {
            return None;
        };
        if binary.op.to_string() != "*" {
            return None;
        }
        let modifier = binary.modifier.as_ref()?;
        if modifier.return_bool || modifier.fill_values != Default::default() {
            return None;
        }
        let VectorMatchCardinality::ManyToOne(include) = &modifier.card else {
            return None;
        };
        if !include.labels.is_empty() {
            return None;
        }
        let Some(LabelModifier::Include(on)) = &modifier.matching else {
            return None;
        };
        if on.labels.is_empty() || on.labels.iter().any(|name| name == "__name__") {
            return None;
        }
        let source = |expr: &Expr| {
            let Expr::VectorSelector(selector) = bare(expr) else {
                return None;
            };
            let key = SelectorKey::new(selector, None);
            specs.iter().position(|spec| spec.key == key)
        };
        let left = source(&binary.lhs)?;
        let right_expr = match bare(&binary.rhs) {
            Expr::Aggregate(topk) => {
                let Some(Expr::NumberLiteral(k)) = topk.param.as_deref().map(bare) else {
                    return None;
                };
                let Some(LabelModifier::Include(labels)) = &topk.modifier else {
                    return None;
                };
                if topk.op.to_string() != "topk"
                    || k.val != 1.0
                    || !same_labels(&labels.labels, &on.labels)
                {
                    return None;
                }
                topk.expr.as_ref()
            }
            expr => expr,
        };
        let right = source(right_expr)?;
        if left == right
            || specs[left].dense != Some(false)
            || !specs[left].packed
            || !specs[right].partial_max
            || !specs[right].packed
            || !same_labels(&specs[right].tags, &on.labels)
            || specs[left].tags.is_empty()
            || specs.iter().any(|spec| {
                spec.metric.is_some() || spec.tags.iter().any(|name| name == "__name__")
            })
        {
            return None;
        }
        let (groups, without) = match &sum.modifier {
            Some(LabelModifier::Include(labels)) => (labels.labels.clone(), false),
            Some(LabelModifier::Exclude(labels)) => (labels.labels.clone(), true),
            None => (Vec::new(), false),
        };
        Some(Self {
            left,
            right,
            on: on.labels.clone(),
            groups,
            without,
        })
    }

    pub(crate) fn evaluate_series(
        &self,
        query: &OnlineQuery,
        mut inputs: Vec<BTreeMap<Labels, Vec<Option<f64>>>>,
        memory: &MemoryReservation,
        cancelled: &AtomicBool,
    ) -> Result<Vec<SeriesVector>> {
        let left = std::mem::take(
            inputs
                .get_mut(self.left)
                .ok_or_else(|| invalid("Missing left join input"))?,
        );
        let right = inputs
            .get(self.right)
            .ok_or_else(|| invalid("Missing right join input"))?;
        let count = query.grid.instants.len();
        if right.values().any(|values| values.len() != count) {
            return Err(invalid("Join grid length mismatch"));
        }
        let scratch = memory.new_empty();
        let mut output: BTreeMap<Labels, Vec<Option<f64>>> = BTreeMap::new();
        // The ordered input preserves the existing evaluator's floating-point sum order.
        for (labels, values) in left {
            if cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Join cancelled"));
            }
            if values.len() != count {
                return Err(invalid("Join grid length mismatch"));
            }
            let bytes = labels.iter().fold(0usize, |bytes, (name, value)| {
                bytes.saturating_add((name.len() + value.len() + 256).saturating_mul(2))
            });
            if bytes > scratch.size() {
                scratch.try_grow(bytes - scratch.size())?;
            }
            let Some(matches) = right.get(&group_labels(&labels, &self.on, false)) else {
                continue;
            };
            let groups = group_labels(&labels, &self.groups, self.without);
            let group = match output.entry(groups) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let bytes = count
                        .saturating_mul(size_of::<Option<f64>>())
                        .saturating_add(512)
                        .saturating_add(
                            entry
                                .key()
                                .iter()
                                .map(|(name, value)| name.len() + value.len() + 256)
                                .sum::<usize>(),
                        );
                    memory.try_grow(bytes)?;
                    entry.insert(vec![None; count])
                }
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            };
            for (index, (left, right)) in values.into_iter().zip(matches).enumerate() {
                if index % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                    return Err(invalid("Join cancelled"));
                }
                if let (Some(left), Some(right)) = (left, right) {
                    let product = left * right;
                    group[index] =
                        Some(group[index].map_or(product, |previous| previous + product));
                }
            }
        }
        Ok(output
            .into_iter()
            .filter(|(_, values)| values.iter().any(Option::is_some))
            .map(|(labels, values)| SeriesVector { labels, values })
            .collect())
    }

    pub(crate) fn execute(
        &self,
        query: Arc<OnlineQuery>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let program = self.clone();
        let schema = query.schema.clone();
        let reservation =
            MemoryConsumer::new("ResearchArrowJoinSum").register(context.memory_pool());
        let guard = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let work = stream::once(async move {
            let mut streams = Vec::new();
            for (index, child) in children.iter().enumerate() {
                for partition in 0..child.output_partitioning().partition_count() {
                    streams.push(
                        child
                            .execute(partition, context.clone())?
                            .map_ok(move |batch| (index, batch))
                            .boxed(),
                    );
                }
            }
            let mut input = stream::select_all(streams);
            let mut batches: [Vec<RecordBatch>; 2] = Default::default();
            while let Some((index, batch)) = input.try_next().await? {
                reservation.try_grow(batch.get_array_memory_size().saturating_add(256))?;
                batches[index].push(batch);
            }
            let cancelled = guard.0.clone();
            common_telemetry::debug!("Research Arrow join sum activated");
            let result = common_runtime::spawn_blocking_query(move || {
                let series = program.evaluate(&query, &batches, &reservation, &cancelled)?;
                reservation.try_grow(
                    crate::research_series_grid::online_execution::output_memory_bound(
                        &series,
                        &query.schema,
                    )?,
                )?;
                let batches = into_batches(series, &query.grid, &query.schema, &cancelled)?;
                Ok::<_, DataFusionError>((batches, reservation))
            })
            .await
            .map_err(|error| invalid(&error.to_string()))?;
            drop(guard);
            result
        })
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
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, work)))
    }

    fn evaluate(
        &self,
        query: &OnlineQuery,
        batches: &[Vec<RecordBatch>; 2],
        memory: &MemoryReservation,
        cancelled: &AtomicBool,
    ) -> Result<Vec<SeriesVector>> {
        let specs = query
            .fragments
            .as_ref()
            .ok_or_else(|| invalid("Missing join fragments"))?;
        let mut right = BTreeMap::new();
        for batch in &batches[self.right] {
            collect_packed(
                &specs[self.right],
                batch,
                &query.grid,
                cancelled,
                memory,
                &mut right,
            )?;
        }
        // Preserve the existing evaluator's label order for floating-point sums.
        // Each slice shares its Arrow buffers with the retained input batches.
        let mut left: BTreeMap<Labels, Vec<Float64Array>> = BTreeMap::new();
        for batch in &batches[self.left] {
            let points = batch
                .column_by_name(PACKED_POINTS)
                .and_then(|array| array.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| invalid("Join requires packed series"))?;
            for row in 0..batch.num_rows() {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(invalid("Join cancelled"));
                }
                if points.is_null(row) {
                    return Err(invalid("Null packed join series"));
                }
                let values = points.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| invalid("Join requires Float64 grids"))?;
                if values.len() != query.grid.instants.len() {
                    return Err(invalid("Join grid length mismatch"));
                }
                if values.null_count() == values.len() {
                    continue;
                }
                let mut labels = BTreeMap::new();
                memory.try_grow(512)?;
                for name in &specs[self.left].tags {
                    let column = batch
                        .column_by_name(name)
                        .ok_or_else(|| invalid("Missing join label"))?;
                    if let Some(value) = string_array_value_at_index(column, row) {
                        memory
                            .try_grow(name.len().saturating_add(value.len()).saturating_add(256))?;
                        labels.insert(name.clone(), value.to_owned());
                    }
                }
                left.entry(labels).or_default().push(values.clone());
            }
        }
        let count = query.grid.instants.len();
        memory.try_grow(count)?;
        let mut seen = vec![false; count];
        let mut output: BTreeMap<Labels, Vec<Option<f64>>> = BTreeMap::new();
        for (labels, segments) in left {
            if cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Join cancelled"));
            }
            seen.fill(false);
            let scratch = memory.new_empty();
            scratch.try_grow(labels.iter().fold(0usize, |bytes, (name, value)| {
                bytes.saturating_add(
                    name.len()
                        .saturating_add(value.len())
                        .saturating_add(256)
                        .saturating_mul(2),
                )
            }))?;
            let matches = right.get(&group_labels(&labels, &self.on, false));
            let groups = group_labels(&labels, &self.groups, self.without);
            let mut group = if matches.is_some() {
                if !output.contains_key(&groups) {
                    let bytes = count
                        .saturating_mul(size_of::<Option<f64>>())
                        .saturating_add(512)
                        .saturating_add(
                            groups
                                .iter()
                                .map(|(name, value)| name.len() + value.len() + 256)
                                .sum::<usize>(),
                        );
                    memory.try_grow(bytes)?;
                    output.insert(groups.clone(), vec![None; count]);
                }
                Some(
                    output
                        .get_mut(&groups)
                        .ok_or_else(|| invalid("Missing join sum group"))?,
                )
            } else {
                None
            };
            for values in segments {
                for index in 0..count {
                    if index % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                        return Err(invalid("Join cancelled"));
                    }
                    if values.is_null(index) {
                        continue;
                    }
                    if std::mem::replace(&mut seen[index], true) {
                        return Err(invalid("Duplicate packed fragment series timestamp"));
                    }
                    let Some(rhs) = matches.and_then(|values| values[index]) else {
                        continue;
                    };
                    let sum = group
                        .as_mut()
                        .ok_or_else(|| invalid("Missing join sum group"))?;
                    let product = values.value(index) * rhs;
                    sum[index] = Some(sum[index].map_or(product, |previous| previous + product));
                }
            }
        }
        Ok(output
            .into_iter()
            .filter(|(_, values)| values.iter().any(Option::is_some))
            .map(|(labels, values)| SeriesVector { labels, values })
            .collect())
    }
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::SessionContext;

    use super::super::FragmentInput;
    use super::*;
    use crate::research_series_grid::grid::TimeGrid;
    use crate::research_series_grid::vector::{self, Aggregate, Arithmetic};
    use datafusion::arrow::array::{StructArray, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use promql_parser::parser::parse;

    fn query() -> OnlineQuery {
        OnlineQuery {
            fragments: Some(Arc::new(vec![
                FragmentInput {
                    key: SelectorKey {
                        selector: "left".into(),
                        function: None,
                    },
                    time: "ts".into(),
                    value: "v".into(),
                    tags: vec!["pod".into(), "replica".into()],
                    metric: None,
                    packed: true,
                    dense: Some(false),
                    partial_max: false,
                },
                FragmentInput {
                    key: SelectorKey {
                        selector: "right".into(),
                        function: None,
                    },
                    time: "ts".into(),
                    value: "v".into(),
                    tags: vec!["pod".into()],
                    metric: None,
                    packed: true,
                    dense: None,
                    partial_max: true,
                },
            ])),
            expr: parse("sum by(pod) (left * on(pod) group_left() topk by(pod)(1, right))")
                .unwrap(),
            grid: Arc::new(TimeGrid::new(0, 30, 10).unwrap()),
            specs: Arc::new(Vec::new()),
            tag_orders: Arc::new(BTreeMap::new()),
            schema: Arc::new(Schema::new(vec![
                Field::new(
                    "__research_grid_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("__research_grid_value", DataType::Float64, false),
                Field::new("pod", DataType::Utf8, true),
            ])),
            lookback: 0,
            workers: 8,
        }
    }

    fn batch(replica: Option<&str>, values: Vec<Option<f64>>) -> RecordBatch {
        let end = values.len() as i32;
        let values: ArrayRef = Arc::new(Float64Array::from(values));
        let points: ArrayRef = if replica.is_some() {
            values
        } else {
            let times: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..end).map(|i| i64::from(i) * 10),
            ));
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("time", times.data_type().clone(), false)),
                    times,
                ),
                (
                    Arc::new(Field::new("value", DataType::Float64, true)),
                    values,
                ),
            ]))
        };
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                points.data_type().clone(),
                replica.is_some(),
            )),
            OffsetBuffer::new(vec![0, end].into()),
            points,
            None,
        ));
        let mut columns = vec![
            (PACKED_POINTS, list),
            ("pod", Arc::new(StringArray::from(vec!["p"])) as ArrayRef),
        ];
        if let Some(replica) = replica {
            columns.push(("replica", Arc::new(StringArray::from(vec![replica]))));
        }
        RecordBatch::try_from_iter(columns).unwrap()
    }

    fn inputs() -> [Vec<RecordBatch>; 2] {
        [
            vec![
                batch(Some("b"), vec![Some(2.0), Some(4.0), Some(3.0), None]),
                batch(Some("a"), vec![Some(1.0), None, Some(f64::NAN), Some(-0.0)]),
                batch(Some("a"), vec![None, Some(5.0), None, None]),
            ],
            vec![
                batch(None, vec![Some(2.0), None, Some(4.0), Some(-1.0)]),
                batch(None, vec![Some(3.0), Some(2.0), None, None]),
            ],
        ]
    }

    #[test]
    fn arrow_join_sum_matches_materialized_semantics_and_rejects_invalid_grids() {
        let query = query();
        let program = JoinSum::recognize(&query).unwrap();
        let batches = inputs();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let memory = MemoryConsumer::new("join-test").register(&pool);
        let cancelled = AtomicBool::new(false);
        let actual = program
            .evaluate(&query, &batches, &memory, &cancelled)
            .unwrap();
        let mut vectors = Vec::new();
        let mut native_inputs = Vec::new();
        for (index, input) in batches.iter().enumerate() {
            let mut rows = BTreeMap::new();
            for batch in input {
                collect_packed(
                    &query.fragments.as_ref().unwrap()[index],
                    batch,
                    &query.grid,
                    &cancelled,
                    &memory,
                    &mut rows,
                )
                .unwrap();
            }
            native_inputs.push(rows.clone());
            vectors.push(
                rows.into_iter()
                    .map(|(labels, values)| SeriesVector { labels, values })
                    .collect::<Vec<_>>(),
            );
        }
        let joined = vector::binary(
            &vectors[0],
            &vectors[1],
            Some(&["pod".into()]),
            Some(&[]),
            Arithmetic::Multiply,
            4,
        )
        .unwrap();
        let expected =
            vector::aggregate(joined, &["pod".into()], false, Aggregate::Sum, 4).unwrap();
        let native = program
            .evaluate_series(&query, native_inputs.clone(), &memory, &cancelled)
            .unwrap();
        assert_eq!(native.len(), expected.len());
        for (actual, expected) in native.iter().zip(&expected) {
            assert_eq!(actual.labels, expected.labels);
            for (actual, expected) in actual.values.iter().zip(&expected.values) {
                assert!(
                    actual == expected
                        || actual.is_some_and(f64::is_nan) && expected.is_some_and(f64::is_nan)
                );
            }
        }
        assert_eq!(native[0].values[3].unwrap().to_bits(), 0.0f64.to_bits());
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].labels, expected[0].labels);
        for (a, b) in actual[0].values.iter().zip(&expected[0].values) {
            assert!(a == b || a.is_some_and(f64::is_nan) && b.is_some_and(f64::is_nan));
        }
        assert_eq!(actual[0].values[..2], [Some(9.0), Some(18.0)]);
        assert_eq!(actual[0].values[3].unwrap().to_bits(), 0.0f64.to_bits());
        let duplicate = [vec![batches[0][0].clone(), batches[0][0].clone()], vec![]];
        assert!(
            program
                .evaluate(&query, &duplicate, &memory, &cancelled)
                .unwrap_err()
                .to_string()
                .contains("Duplicate packed")
        );
        let ragged = [vec![batch(Some("a"), vec![Some(1.0)])], vec![]];
        assert!(
            program
                .evaluate(&query, &ragged, &memory, &cancelled)
                .unwrap_err()
                .to_string()
                .contains("grid length")
        );
        cancelled.store(true, Ordering::Relaxed);
        assert!(
            program
                .evaluate_series(&query, native_inputs.clone(), &memory, &cancelled)
                .is_err()
        );
        assert!(
            program
                .evaluate(&query, &batches, &memory, &cancelled)
                .is_err()
        );
        let tiny_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let tiny = MemoryConsumer::new("join-test-limit").register(&tiny_pool);
        assert!(
            program
                .evaluate_series(&query, native_inputs, &tiny, &AtomicBool::new(false))
                .is_err()
        );
        assert!(
            program
                .evaluate(&query, &batches, &tiny, &AtomicBool::new(false))
                .is_err()
        );
    }

    #[tokio::test]
    async fn arrow_join_sum_executes_partitioned_inputs_and_preserves_sum_order() {
        let query = Arc::new(query());
        let program = JoinSum::recognize(&query).unwrap();
        let data = [
            vec![
                batch(Some("c"), vec![Some(1.0); 4]),
                batch(Some("b"), vec![Some(-1e308); 4]),
                batch(Some("a"), vec![Some(1e308); 4]),
            ],
            vec![batch(None, vec![Some(1.0); 4])],
        ];
        let children = data
            .iter()
            .map(|batches| {
                let partitions = batches
                    .iter()
                    .cloned()
                    .map(|batch| vec![batch])
                    .collect::<Vec<_>>();
                Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(&partitions, batches[0].schema(), None).unwrap(),
                ))) as Arc<dyn ExecutionPlan>
            })
            .collect();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build_arc()
            .unwrap();
        let context = SessionContext::new_with_config_rt(Default::default(), runtime);
        {
            let memory = MemoryConsumer::new("native-join-order").register(&pool);
            let cancelled = AtomicBool::new(false);
            let mut inputs = Vec::new();
            for (index, batches) in data.iter().enumerate() {
                let mut rows = BTreeMap::new();
                for batch in batches {
                    collect_packed(
                        &query.fragments.as_ref().unwrap()[index],
                        batch,
                        &query.grid,
                        &cancelled,
                        &memory,
                        &mut rows,
                    )
                    .unwrap();
                }
                inputs.push(rows);
            }
            let output = program
                .evaluate_series(&query, inputs, &memory, &cancelled)
                .unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].values, vec![Some(1.0); 4]);
        }
        let output = program
            .execute(query, children, context.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        for batch in output {
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.values().as_ref(), &[1.0; 4]);
        }
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn arrow_join_sum_requires_proven_right_uniqueness() {
        let mut query = query();
        assert!(JoinSum::recognize(&query).is_some());
        for expression in [
            "sum by(pod)(left * on(pod) group_left() topk by(pod)(2, right))",
            "sum by(pod)(left * on(pod) group_left(replica) right)",
            "sum by(pod)(left * on(replica) group_left() right)",
            "sum by(pod)(left / on(pod) group_left() right)",
        ] {
            query.expr = parse(expression).unwrap();
            assert!(JoinSum::recognize(&query).is_none(), "{expression}");
        }
        query.expr = parse("sum(left * on(pod) group_left() right)").unwrap();
        Arc::make_mut(query.fragments.as_mut().unwrap())[1].partial_max = false;
        assert!(JoinSum::recognize(&query).is_none());
    }
}
