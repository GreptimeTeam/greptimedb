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

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::execution::memory_pool::MemoryReservation;
use datafusion_common::{DataFusionError, Result};
use promql::extension_plan::{HistogramFoldOperation, HistogramFoldStream};
use promql_parser::parser::{Expr, LabelModifier, Offset, VectorMatchCardinality, VectorSelector};

use crate::research_series_grid::block_eval::{BlockEvaluator, Mode};
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::range;
use crate::research_series_grid::source::{Series, SeriesReader, SnapshotIndex};
use crate::research_series_grid::vector::{self, Aggregate, Arithmetic, Labels, SeriesVector};

pub(crate) struct VectorResult {
    pub series: Vec<SeriesVector>,
    pub tag_order: Vec<String>,
    pub reservation: Option<MemoryReservation>,
}

impl VectorResult {
    fn memory_size(&self) -> usize {
        self.series
            .iter()
            .fold(
                self.series
                    .capacity()
                    .saturating_mul(size_of::<SeriesVector>()),
                |bytes, series| bytes.saturating_add(series.memory_size()),
            )
            .saturating_add(
                self.tag_order.iter().fold(
                    self.tag_order
                        .capacity()
                        .saturating_mul(size_of::<String>()),
                    |bytes, tag| bytes.saturating_add(tag.capacity()),
                ),
            )
    }
}

pub(crate) enum Value {
    Scalar(f64),
    Vector(VectorResult),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SelectorKey {
    pub selector: String,
    pub function: Option<(String, i64)>,
}

impl SelectorKey {
    pub(crate) fn new(selector: &VectorSelector, function: Option<(&str, i64)>) -> Self {
        Self {
            selector: selector.to_string(),
            function: function.map(|(name, window)| (name.to_owned(), window)),
        }
    }
}

pub(crate) enum SelectorInputs<'a> {
    Snapshot {
        source: &'a SnapshotIndex,
        path: &'a Path,
    },
    /// Query-local evaluation points produced by the storage input plans.
    Prepared(&'a BTreeMap<SelectorKey, VectorResult>),
}

pub(crate) struct Evaluator<'a> {
    pub inputs: SelectorInputs<'a>,
    pub workers: usize,
    pub block_evaluation: bool,
    pub grid: &'a TimeGrid,
    pub lookback: i64,
    pub tag_orders: &'a BTreeMap<String, Vec<String>>,
    pub cancelled: &'a AtomicBool,
    pub memory: Option<&'a MemoryReservation>,
}

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

impl Evaluator<'_> {
    fn check_cancelled(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Research series query cancelled"));
        }
        Ok(())
    }

    pub fn evaluate(&mut self, expr: &Expr) -> Result<Value> {
        self.check_cancelled()?;
        let mut value = self.evaluate_inner(expr)?;
        if let (Some(memory), Value::Vector(result)) = (self.memory, &mut value) {
            let bytes = result.memory_size();
            result
                .reservation
                .get_or_insert_with(|| memory.new_empty())
                .try_resize(bytes)?;
        }
        Ok(value)
    }

    fn reserve(&self, bytes: usize) -> Result<Option<MemoryReservation>> {
        if bytes == usize::MAX {
            return Err(invalid("Online evaluation memory size overflow"));
        }
        self.memory
            .map(|memory| {
                let reservation = memory.new_empty();
                reservation.try_grow(bytes)?;
                Ok(reservation)
            })
            .transpose()
    }

    fn evaluate_inner(&mut self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::NumberLiteral(number) => Ok(Value::Scalar(number.val)),
            Expr::Paren(paren) => self.evaluate(&paren.expr),
            Expr::VectorSelector(selector) => Ok(Value::Vector(self.select(selector, None)?)),
            Expr::Aggregate(aggregate) => {
                let Value::Vector(input) = self.evaluate(&aggregate.expr)? else {
                    return Err(invalid("Aggregation requires vector input"));
                };
                let (names, without) = match &aggregate.modifier {
                    Some(LabelModifier::Include(labels)) => (labels.labels.clone(), false),
                    Some(LabelModifier::Exclude(labels)) => (labels.labels.clone(), true),
                    None => (Vec::new(), false),
                };
                let op = aggregate.op.to_string();
                let _scratch = self.reserve(input.memory_size().saturating_mul(4))?;
                let series = match op.as_str() {
                    "sum" | "max" | "count" => vector::aggregate(
                        input.series,
                        &names,
                        without,
                        if op == "sum" {
                            Aggregate::Sum
                        } else if op == "count" {
                            Aggregate::Count
                        } else {
                            Aggregate::Max
                        },
                        self.grid.instants.len(),
                    )
                    .map_err(invalid)?,
                    "topk" => {
                        let Some(param) = &aggregate.param else {
                            return Err(invalid("Missing topk parameter"));
                        };
                        let Value::Scalar(k) = self.evaluate(param)? else {
                            return Err(invalid("Non-scalar topk parameter"));
                        };
                        if !k.is_finite() || k > usize::MAX as f64 {
                            return Err(invalid("Unsupported topk size"));
                        }
                        vector::topk(
                            &input.series,
                            &names,
                            without,
                            k.max(0.0) as usize,
                            self.grid.instants.len(),
                            &input.tag_order,
                        )
                        .map_err(invalid)?
                    }
                    _ => return Err(invalid(format!("Unsupported research aggregation: {op}"))),
                };
                let tag_order = if op == "topk" {
                    input.tag_order
                } else if without {
                    let mut retained: Vec<_> = input
                        .tag_order
                        .into_iter()
                        .filter(|name| name != "__name__" && !names.contains(name))
                        .collect();
                    retained.sort();
                    retained
                } else {
                    names
                        .into_iter()
                        .filter(|name| input.tag_order.contains(name))
                        .collect()
                };
                Ok(Value::Vector(VectorResult {
                    series,
                    tag_order,
                    reservation: None,
                }))
            }
            Expr::Binary(binary) => {
                let operation = match binary.op.to_string().as_str() {
                    "*" => Arithmetic::Multiply,
                    "/" => Arithmetic::Divide,
                    _ => return Err(invalid("Unsupported research binary operation")),
                };
                let left = self.evaluate(&binary.lhs)?;
                let right = self.evaluate(&binary.rhs)?;
                match (left, right) {
                    (Value::Scalar(a), Value::Scalar(b)) => {
                        Ok(Value::Scalar(operation.apply(a, b)))
                    }
                    (Value::Scalar(scalar), Value::Vector(mut input)) => {
                        for series in &mut input.series {
                            series.labels.remove("__name__");
                            for value in series.values.iter_mut().flatten() {
                                *value = operation.apply(scalar, *value);
                            }
                        }
                        Ok(Value::Vector(input))
                    }
                    (Value::Vector(mut input), Value::Scalar(scalar)) => {
                        for series in &mut input.series {
                            series.labels.remove("__name__");
                            for value in series.values.iter_mut().flatten() {
                                *value = operation.apply(*value, scalar);
                            }
                        }
                        Ok(Value::Vector(input))
                    }
                    (Value::Vector(left), Value::Vector(right)) => {
                        let mut scratch = left
                            .memory_size()
                            .saturating_add(right.memory_size())
                            .saturating_mul(4);
                        if binary.modifier.as_ref().is_some_and(|modifier| {
                            matches!(&modifier.card, VectorMatchCardinality::ManyToOne(labels) if !labels.labels.is_empty())
                        }) {
                            // A RHS label may change over time, producing several output series per LHS.
                            let largest = |input: &VectorResult| input.series.iter().map(SeriesVector::memory_size).max().unwrap_or(0);
                            scratch = scratch.saturating_add(left.series.len().saturating_mul(right.series.len())
                                .saturating_mul(largest(&left).saturating_add(largest(&right))).saturating_mul(2));
                        }
                        let _scratch = self.reserve(scratch)?;
                        let mut on = None;
                        let mut group_left = None;
                        if let Some(modifier) = &binary.modifier {
                            if modifier.return_bool || modifier.fill_values != Default::default() {
                                return Err(invalid("Unsupported binary modifier"));
                            }
                            on = match &modifier.matching {
                                Some(LabelModifier::Include(labels)) => {
                                    Some(labels.labels.as_slice())
                                }
                                None => None,
                                _ => {
                                    return Err(invalid(
                                        "Ignoring matching is not supported by this prototype",
                                    ));
                                }
                            };
                            group_left = match &modifier.card {
                                VectorMatchCardinality::OneToOne => None,
                                VectorMatchCardinality::ManyToOne(labels) => {
                                    Some(labels.labels.as_slice())
                                }
                                _ => return Err(invalid("Unsupported vector cardinality")),
                            };
                        }
                        // GT broadcasts a tag-free vector by timestamp instead of matching labels.
                        if left.tag_order.is_empty() || right.tag_order.is_empty() {
                            let labels_from_left = right.tag_order.is_empty();
                            let series = vector::broadcast(
                                &left.series,
                                &right.series,
                                labels_from_left,
                                operation,
                                self.grid.instants.len(),
                            )
                            .map_err(invalid)?;
                            let tag_order = if labels_from_left {
                                left.tag_order
                            } else {
                                right.tag_order
                            };
                            return Ok(Value::Vector(VectorResult {
                                series,
                                tag_order,
                                reservation: None,
                            }));
                        }
                        let matching_tags = |tags: &[String]| -> BTreeSet<String> {
                            tags.iter()
                                .filter(|name| on.is_none_or(|names| names.contains(name)))
                                .cloned()
                                .collect()
                        };
                        let same_schema =
                            matching_tags(&left.tag_order) == matching_tags(&right.tag_order);
                        let series = if same_schema {
                            vector::binary(
                                &left.series,
                                &right.series,
                                on,
                                group_left,
                                operation,
                                self.grid.instants.len(),
                            )
                            .map_err(invalid)?
                        } else {
                            Vec::new()
                        };
                        let mut tag_order = if let Some(names) = group_left {
                            let mut tags: Vec<_> = left
                                .tag_order
                                .into_iter()
                                .filter(|name| !names.contains(name))
                                .collect();
                            let included: BTreeSet<_> = names
                                .iter()
                                .filter(|name| right.tag_order.contains(name))
                                .cloned()
                                .collect();
                            tags.extend(included);
                            tags
                        } else {
                            left.tag_order
                                .into_iter()
                                .filter(|name| on.is_none_or(|names| names.contains(name)))
                                .collect()
                        };
                        tag_order.retain(|name| name != "__name__");
                        Ok(Value::Vector(VectorResult {
                            series,
                            tag_order,
                            reservation: None,
                        }))
                    }
                }
            }
            Expr::Call(call)
                if matches!(
                    call.func.name,
                    "rate" | "increase" | "irate" | "max_over_time"
                ) =>
            {
                let [argument] = call.args.args.as_slice() else {
                    return Err(invalid("Invalid range function arguments"));
                };
                let Expr::MatrixSelector(matrix) = argument.as_ref() else {
                    return Err(invalid("Range function requires matrix selector"));
                };
                let window = i64::try_from(matrix.range.as_millis())
                    .map_err(|_| invalid("Range duration overflow"))?;
                Ok(Value::Vector(
                    self.select(&matrix.vs, Some((call.func.name, window)))?,
                ))
            }
            Expr::Call(call) if call.func.name == "histogram_quantile" => {
                let [quantile, argument] = call.args.args.as_slice() else {
                    return Err(invalid("Invalid histogram arguments"));
                };
                let Value::Scalar(quantile) = self.evaluate(quantile)? else {
                    return Err(invalid("Non-scalar histogram quantile"));
                };
                let Value::Vector(input) = self.evaluate(argument)? else {
                    return Err(invalid("Non-vector histogram input"));
                };
                self.histogram(input, quantile).map(Value::Vector)
            }
            _ => Err(invalid(format!("Unsupported research expression: {expr}"))),
        }
    }

    fn select(
        &mut self,
        selector: &VectorSelector,
        function: Option<(&str, i64)>,
    ) -> Result<VectorResult> {
        if selector.at.is_some() || !selector.matchers.or_matchers.is_empty() {
            return Err(invalid(
                "Research selector does not support @ or OR matchers",
            ));
        }
        let (source, path) = match &self.inputs {
            SelectorInputs::Prepared(inputs) => {
                let key = SelectorKey::new(selector, function);
                let input = inputs
                    .get(&key)
                    .ok_or_else(|| invalid(format!("Missing prepared selector: {key:?}")))?;
                if input
                    .series
                    .iter()
                    .any(|series| series.values.len() != self.grid.instants.len())
                {
                    return Err(invalid("Prepared selector does not match time grid"));
                }
                let reservation = self.reserve(input.memory_size())?;
                return Ok(VectorResult {
                    series: input.series.clone(),
                    tag_order: input.tag_order.clone(),
                    reservation,
                });
            }
            SelectorInputs::Snapshot { source, path } => (*source, *path),
        };
        let metric = selector
            .name
            .as_ref()
            .ok_or_else(|| invalid("Research selector requires metric name"))?;
        let tag_order = self
            .tag_orders
            .get(metric)
            .ok_or_else(|| invalid("Metric schema was not resolved"))?
            .clone();
        let offset = match &selector.offset {
            Some(Offset::Pos(duration)) => {
                i64::try_from(duration.as_millis()).map_err(|_| invalid("Offset overflow"))?
            }
            Some(Offset::Neg(duration)) => {
                -i64::try_from(duration.as_millis()).map_err(|_| invalid("Offset overflow"))?
            }
            None => 0,
        };
        let window = function.map_or(self.lookback, |(_, window)| window);
        let start = i128::from(
            *self
                .grid
                .instants
                .first()
                .ok_or_else(|| invalid("Empty evaluation grid"))?,
        ) - i128::from(offset)
            - i128::from(window);
        let end = i128::from(
            *self
                .grid
                .instants
                .last()
                .ok_or_else(|| invalid("Empty evaluation grid"))?,
        ) - i128::from(offset);
        let bound = |value: i128| value.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        let mut selected = Vec::new();
        for series in &source.series {
            self.check_cancelled()?;
            let label_value = |name: &str| {
                series
                    .labels
                    .binary_search_by(|label| label.name.as_str().cmp(name))
                    .ok()
                    .map_or("", |index| series.labels[index].value.as_str())
            };
            if label_value("__name__") != metric
                || !selector
                    .matchers
                    .matchers
                    .iter()
                    .all(|matcher| matcher.is_match(label_value(&matcher.name)))
            {
                continue;
            }
            selected.push(series);
        }
        let result = ordered_chunks(&selected, self.workers, |chunk| {
            let mut reader = SeriesReader::for_snapshot(path, source)?;
            self.evaluate_series(
                chunk,
                &mut reader,
                bound(start),
                bound(end),
                offset,
                function,
            )
        })?;
        Ok(VectorResult {
            series: result,
            tag_order,
            reservation: None,
        })
    }

    fn evaluate_series(
        &self,
        selected: &[&Series],
        reader: &mut SeriesReader,
        start: i64,
        end: i64,
        offset: i64,
        function: Option<(&str, i64)>,
    ) -> Result<Vec<SeriesVector>> {
        let block_mode = if self.block_evaluation {
            match function {
                None => Some((Mode::Instant, self.lookback)),
                Some(("max_over_time", window))
                    if self.grid.instants.windows(2).all(|pair| {
                        i128::from(pair[1]) - i128::from(pair[0]) >= i128::from(window)
                    }) =>
                {
                    Some((Mode::DisjointMax, window))
                }
                _ => None,
            }
        } else {
            None
        };
        let mut result = Vec::with_capacity(selected.len());
        for series in selected {
            self.check_cancelled()?;
            let values = if let Some((mode, window)) = block_mode {
                let mut evaluator = BlockEvaluator::new(&self.grid.instants, window, offset, mode)
                    .map_err(invalid)?;
                reader.visit_samples(series, start, end, |timestamp, value| {
                    evaluator.push(timestamp, value).map_err(invalid)
                })?;
                evaluator.finish()
            } else {
                let (timestamps, samples) = reader.read(series, start, end)?;
                if let Some((name, window)) = function {
                    range::evaluate(name, self.grid, timestamps, samples, window, offset)?
                } else {
                    self.grid
                        .instant_indices(&timestamps, &samples, self.lookback, offset)
                        .map_err(invalid)?
                        .into_iter()
                        .map(|index| index.map(|index| samples[index]))
                        .collect()
                }
            };
            let mut labels: Labels = series
                .labels
                .iter()
                .map(|label| (label.name.clone(), label.value.clone()))
                .collect();
            if function.is_some() {
                labels.remove("__name__");
            }
            if values.iter().any(Option::is_some) {
                result.push(SeriesVector { labels, values });
            }
        }
        Ok(result)
    }

    fn histogram(&self, input: VectorResult, quantile: f64) -> Result<VectorResult> {
        let _scratch = self.reserve(input.memory_size().saturating_mul(4))?;
        let mut groups: BTreeMap<Labels, Vec<(f64, SeriesVector)>> = BTreeMap::new();
        for mut series in input.series {
            let boundary = series
                .labels
                .remove("le")
                .ok_or_else(|| invalid("Missing histogram boundary"))?
                .parse::<f64>()
                .map_err(|_| invalid("Invalid histogram boundary"))?;
            series.labels.remove("__name__");
            groups
                .entry(series.labels.clone())
                .or_default()
                .push((boundary, series));
        }
        let mut result = Vec::new();
        for (labels, mut buckets) in groups {
            self.check_cancelled()?;
            buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
            let mut values = Vec::with_capacity(self.grid.instants.len());
            let (mut bounds, mut counts) = (Vec::new(), Vec::new());
            for point in 0..self.grid.instants.len() {
                bounds.clear();
                counts.clear();
                for (boundary, series) in &buckets {
                    if let Some(value) = series.values[point] {
                        bounds.push(*boundary);
                        counts.push(value);
                    }
                }
                values.push(if bounds.is_empty() {
                    None
                } else {
                    Some(HistogramFoldStream::evaluate_row(
                        HistogramFoldOperation::Quantile(quantile.into()),
                        &bounds,
                        &counts,
                    )?)
                });
            }
            result.push(SeriesVector { labels, values });
        }
        Ok(VectorResult {
            series: result,
            reservation: None,
            tag_order: input
                .tag_order
                .into_iter()
                .filter(|name| name != "le" && name != "__name__")
                .collect(),
        })
    }
}

/// Joins contiguous chunks in input order so downstream floating-point reductions stay ordered.
fn ordered_chunks<T: Sync, R: Send>(
    input: &[T],
    workers: usize,
    evaluate: impl Fn(&[T]) -> Result<Vec<R>> + Sync,
) -> Result<Vec<R>> {
    if workers == 0 {
        return Err(invalid("Research worker count must be positive"));
    }
    if input.is_empty() {
        return Ok(Vec::new());
    }
    if workers == 1 || input.len() == 1 {
        return evaluate(input);
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = input
            .chunks(input.len().div_ceil(workers))
            .map(|chunk| {
                let evaluate = &evaluate;
                std::thread::Builder::new()
                    .name("research-series".into())
                    .spawn_scoped(scope, move || evaluate(chunk))
                    .map_err(|error| invalid(error.to_string()))
            })
            .collect();
        // Join every worker, including after an error, before releasing borrowed query state.
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| {
                handle?
                    .join()
                    .map_err(|_| invalid("Research series worker panicked"))?
            })
            .collect();
        let mut output = Vec::new();
        for result in results {
            output.extend(result?);
        }
        Ok(output)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};

    use super::*;

    #[test]
    fn prepared_inputs_remain_available_to_repeated_expressions() {
        let Expr::VectorSelector(selector) = promql_parser::parser::parse("up").unwrap() else {
            panic!("Expected selector");
        };
        let grid = TimeGrid::new(0, 1, 1).unwrap();
        let input = BTreeMap::from([(
            SelectorKey::new(&selector, None),
            VectorResult {
                series: vec![SeriesVector {
                    labels: BTreeMap::from([("__name__".into(), "up".into())]),
                    values: vec![Some(2.0), Some(3.0)],
                }],
                tag_order: Vec::new(),
                reservation: None,
            },
        )]);
        let cancelled = AtomicBool::new(false);
        let tags = BTreeMap::new();
        let mut evaluator = Evaluator {
            inputs: SelectorInputs::Prepared(&input),
            workers: 1,
            block_evaluation: false,
            grid: &grid,
            lookback: 300_000,
            tag_orders: &tags,
            cancelled: &cancelled,
            memory: None,
        };
        let Value::Vector(result) = evaluator
            .evaluate(&promql_parser::parser::parse("sum(up) * sum(up)").unwrap())
            .unwrap()
        else {
            panic!("Expected vector");
        };
        assert_eq!(result.series.len(), 1);
        assert_eq!(result.series[0].values, [Some(4.0), Some(9.0)]);
        drop(result);
        for limit in [1, 1 << 20] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
            let memory = MemoryConsumer::new("test-evaluation").register(&pool);
            let mut measured = Evaluator {
                inputs: SelectorInputs::Prepared(&input),
                workers: 1,
                block_evaluation: false,
                grid: &grid,
                lookback: 300_000,
                tag_orders: &tags,
                cancelled: &cancelled,
                memory: Some(&memory),
            };
            let result =
                measured.evaluate(&promql_parser::parser::parse("sum(up) * sum(up)").unwrap());
            if limit == 1 {
                assert!(matches!(
                    result,
                    Err(DataFusionError::ResourcesExhausted(_))
                ));
            } else {
                let Value::Vector(result) = result.unwrap() else {
                    panic!("Expected vector")
                };
                assert_eq!(result.series[0].values, [Some(4.0), Some(9.0)]);
                assert!(pool.reserved() > 0);
                drop(result);
            }
            assert_eq!(pool.reserved(), 0);
        }
        assert!(evaluator.select(&selector, Some(("rate", 60_000))).is_err());
        cancelled.store(true, Ordering::Relaxed);
        assert!(evaluator.evaluate(&Expr::VectorSelector(selector)).is_err());
    }

    #[test]
    fn parallel_chunks_preserve_order_and_join_on_error() {
        let input: Vec<_> = (0..19).collect();
        for workers in [1, 2, 8, 32] {
            let output = ordered_chunks(&input, workers, |chunk| {
                Ok(chunk.iter().filter(|n| **n % 3 != 0).copied().collect())
            })
            .unwrap();
            assert_eq!(
                output,
                input
                    .iter()
                    .filter(|n| **n % 3 != 0)
                    .copied()
                    .collect::<Vec<_>>()
            );
        }
        let completed = AtomicUsize::new(0);
        let result: Result<Vec<usize>> = ordered_chunks(&input, 8, |chunk| {
            completed.fetch_add(chunk.len(), Ordering::Relaxed);
            Err(invalid(format!("chunk {}", chunk[0])))
        });
        assert!(result.unwrap_err().to_string().contains("chunk 0"));
        assert_eq!(completed.load(Ordering::Relaxed), input.len());
    }
}
