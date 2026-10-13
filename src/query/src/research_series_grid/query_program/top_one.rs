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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use common_error::ext::BoxedError;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::expressions::Column;
use datatypes::value::Value;
use promql_parser::parser::{Expr, LabelModifier};
use store_api::region_engine::{SeriesBlockProcessor, VisibleSeriesBlock};

use super::{ProgramSource, SourceOperation};
use crate::research_series_grid::evaluate::SelectorKey;
use crate::research_series_grid::online_execution::OnlineQuery;
use crate::research_series_grid::range;
use crate::research_series_grid::scan_range::{RangeFunction, RangeOutput, RangeSpec};
use crate::research_series_grid::vector::{self, Aggregate, Labels, SeriesVector};

#[derive(Debug, Clone)]
pub(super) struct TopOne {
    labels: Vec<(String, String)>,
    metric: Option<String>,
    groups: Vec<String>,
    without: bool,
}

fn bare(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(paren) => bare(&paren.expr),
        expr => expr,
    }
}

impl TopOne {
    pub(super) fn recognize(query: &OnlineQuery, sources: &[ProgramSource]) -> Option<Self> {
        let [source] = sources else { return None };
        let [fragment] = query.fragments.as_ref()?.as_slice() else {
            return None;
        };
        let SourceOperation::Range(spec) = &source.operation else {
            return None;
        };
        if spec.function != RangeFunction::Instant
            || spec.aggregate.is_some()
            || fragment.partial_max
            || fragment.dense == Some(true)
        {
            return None;
        }
        let packed = spec.packed.as_ref()?;
        if packed.expression.downcast_ref::<Column>()?.index() != 0 {
            return None;
        }
        let Expr::Aggregate(sum) = bare(&query.expr) else {
            return None;
        };
        let Expr::Aggregate(top) = bare(&sum.expr) else {
            return None;
        };
        let Expr::NumberLiteral(k) = bare(top.param.as_ref()?) else {
            return None;
        };
        let Expr::VectorSelector(selector) = bare(&top.expr) else {
            return None;
        };
        if sum.op.to_string() != "sum"
            || top.op.to_string() != "topk"
            || k.val != 1.0
            || top.modifier.is_some()
            || fragment.key != SelectorKey::new(selector, None)
        {
            return None;
        }
        let input_labels: Vec<_> = spec
            .output
            .iter()
            .filter_map(|output| {
                if let RangeOutput::Tag(index) = output {
                    Some(*index)
                } else {
                    None
                }
            })
            .collect();
        if packed.labels.len() != fragment.tags.len() {
            return None;
        }
        let mut labels = Vec::new();
        for (name, &index) in fragment.tags.iter().zip(&packed.labels) {
            let field = spec.input_schema.fields().get(*input_labels.get(index)?)?;
            let datatype = match field.data_type() {
                DataType::Dictionary(_, value) => value.as_ref(),
                datatype => datatype,
            };
            if !matches!(
                datatype,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return None;
            }
            labels.push((name.clone(), field.name().clone()));
        }
        let (groups, without) = match &sum.modifier {
            Some(LabelModifier::Include(labels)) => (labels.labels.clone(), false),
            Some(LabelModifier::Exclude(labels)) => (labels.labels.clone(), true),
            None => (Vec::new(), false),
        };
        Some(Self {
            labels,
            metric: fragment.metric.clone(),
            groups,
            without,
        })
    }
}

pub(super) struct Processor {
    top: TopOne,
    spec: Arc<RangeSpec>,
    memory: MemoryReservation,
    cancelled: Arc<AtomicBool>,
    labels: Vec<Labels>,
    seen: BTreeMap<Labels, (usize, Vec<bool>)>,
    winners: Vec<Option<(usize, f64)>>,
}

impl Processor {
    pub(super) fn new(
        top: TopOne,
        spec: Arc<RangeSpec>,
        memory: MemoryReservation,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self> {
        let points = spec.grid.instants.len();
        memory.try_grow(
            points
                .checked_mul(64)
                .ok_or_else(|| invalid("Topk grid size overflow"))?,
        )?;
        Ok(Self {
            top,
            spec,
            memory,
            cancelled,
            labels: Vec::new(),
            seen: BTreeMap::new(),
            winners: vec![None; points],
        })
    }

    fn consume(
        &mut self,
        tags: &[(String, Value)],
        blocks: &mut dyn Iterator<Item = std::result::Result<VisibleSeriesBlock, BoxedError>>,
    ) -> Result<()> {
        let values = range::evaluate_selection_blocks(
            RangeFunction::Instant,
            &self.spec.grid,
            self.spec.window,
            self.spec.offset,
            blocks,
            &self.cancelled,
        )?;
        if values.iter().all(Option::is_none) {
            return Ok(());
        }
        let bytes = tags
            .iter()
            .try_fold(512usize, |bytes, (name, value)| {
                bytes
                    .checked_add(name.len())?
                    .checked_add(value.as_value_ref().data_size())?
                    .checked_add(256)
            })
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or_else(|| invalid("Topk label size overflow"))?;
        self.memory.try_grow(
            bytes
                .checked_add(values.len())
                .ok_or_else(|| invalid("Topk label size overflow"))?,
        )?;
        let mut labels = Labels::new();
        for (name, input) in &self.top.labels {
            let value = tags
                .iter()
                .find(|(name, _)| name == input)
                .map(|(_, value)| value)
                .ok_or_else(|| invalid("Missing topk source label"))?;
            match value {
                Value::String(value) => {
                    labels.insert(name.clone(), value.as_utf8().to_owned());
                }
                Value::Null => {}
                _ => return Err(invalid("Invalid topk source label")),
            }
        }
        if let Some(metric) = &self.top.metric {
            labels.insert("__name__".into(), metric.clone());
        }
        // Disjoint fragments of a label set are valid; overlapping points are not,
        // including those belonging to series that never win.
        let (index, present) = self
            .seen
            .entry(labels.clone())
            .or_insert_with(|| (self.labels.len(), vec![false; values.len()]));
        let index = *index;
        for (present, value) in present.iter_mut().zip(&values) {
            if value.is_some() && std::mem::replace(present, true) {
                return Err(invalid("Duplicate native topk series timestamp"));
            }
        }
        if index == self.labels.len() {
            self.labels.push(labels.clone());
        }
        for (point, value) in values.into_iter().enumerate() {
            if point % 1024 == 0 && self.cancelled.load(AtomicOrdering::Relaxed) {
                return Err(invalid("Topk query cancelled"));
            }
            let Some(value) = value else { continue };
            let replace = self.winners[point].is_none_or(|(other, current)| {
                let order = value.total_cmp(&current).then_with(|| {
                    for (name, _) in &self.top.labels {
                        let order = match (labels.get(name), self.labels[other].get(name)) {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => Ordering::Greater,
                            (Some(_), None) => Ordering::Less,
                            (Some(a), Some(b)) => a.cmp(b),
                        };
                        if order != Ordering::Equal {
                            return order;
                        }
                    }
                    Ordering::Equal
                });
                order == Ordering::Greater
            });
            if replace {
                self.winners[point] = Some((index, value));
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<(Vec<SeriesVector>, MemoryReservation)> {
        if self.cancelled.load(AtomicOrdering::Relaxed) {
            return Err(invalid("Topk query cancelled"));
        }
        let points = self.spec.grid.instants.len();
        let mut rows = BTreeMap::new();
        for (point, winner) in self.winners.into_iter().enumerate() {
            if let Some((index, value)) = winner {
                if !rows.contains_key(&index) {
                    self.memory.try_grow(
                        points
                            .checked_mul(32)
                            .ok_or_else(|| invalid("Topk output size overflow"))?,
                    )?;
                }
                rows.entry(index).or_insert_with(|| vec![None; points])[point] = Some(value);
            }
        }
        let series = rows
            .into_iter()
            .map(|(index, values)| SeriesVector {
                labels: self.labels[index].clone(),
                values,
            })
            .collect();
        let output = vector::aggregate(
            series,
            &self.top.groups,
            self.top.without,
            Aggregate::Sum,
            points,
        )
        .map_err(invalid)?;
        Ok((output, self.memory))
    }
}

impl SeriesBlockProcessor for Processor {
    fn tag_names(&self) -> Option<Vec<String>> {
        Some(
            self.top
                .labels
                .iter()
                .map(|(_, input)| input.clone())
                .collect(),
        )
    }

    fn process_series(
        &mut self,
        tags: &[(String, Value)],
        blocks: &mut dyn Iterator<Item = std::result::Result<VisibleSeriesBlock, BoxedError>>,
    ) -> std::result::Result<(), BoxedError> {
        self.consume(tags, blocks)
            .map_err(|error| BoxedError::new(crate::error::Error::from(error)))
    }
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use promql_parser::parser::parse;

    use super::*;
    use crate::research_series_grid::grid::TimeGrid;
    use crate::research_series_grid::online_frontier::FragmentInput;
    use crate::research_series_grid::scan_range::PackedRangeSpec;

    fn fixture() -> (OnlineQuery, Vec<ProgramSource>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pod", DataType::Utf8, true),
            Field::new("node", DataType::Utf8, true),
        ]));
        let grid = Arc::new(TimeGrid::new(0, 40, 10).unwrap());
        let Expr::VectorSelector(selector) = parse("m").unwrap() else {
            unreachable!()
        };
        let spec = Arc::new(RangeSpec {
            aggregate: None,
            packed: Some(Arc::new(PackedRangeSpec {
                batch_output: true,
                schema: Arc::new(datatypes::schema::Schema::try_from(schema.clone()).unwrap()),
                labels: vec![0, 1],
                expression: Arc::new(Column::new("value", 0)),
            })),
            function: RangeFunction::Instant,
            schema: Arc::new(datatypes::schema::Schema::try_from(schema.clone()).unwrap()),
            input_schema: schema.clone(),
            output: vec![RangeOutput::Tag(0), RangeOutput::Tag(1)],
            timestamp: 0,
            value: 0,
            tsid: 0,
            grid: grid.clone(),
            window: 10,
            offset: 0,
        });
        let query = OnlineQuery {
            fragments: Some(Arc::new(vec![FragmentInput {
                key: SelectorKey::new(&selector, None),
                time: "time".into(),
                value: "value".into(),
                tags: vec!["pod".into(), "node".into()],
                metric: Some("m".into()),
                packed: true,
                dense: Some(false),
                partial_max: false,
            }])),
            expr: parse("sum without(node) (topk(1, m))").unwrap(),
            grid,
            specs: Arc::new(Vec::new()),
            tag_orders: Arc::new(BTreeMap::new()),
            schema: schema.clone(),
            lookback: 10,
            workers: 8,
        };
        (
            query,
            vec![ProgramSource {
                name: "m".into(),
                schema,
                projection: vec![0, 1],
                filters: Vec::new(),
                ordering: Vec::new(),
                operation: SourceOperation::Range(spec),
            }],
        )
    }

    fn feed(
        processor: &mut Processor,
        pod: Option<&str>,
        node: &str,
        values: Vec<f64>,
    ) -> Result<()> {
        let tags = vec![
            (
                "pod".into(),
                pod.map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Null),
            ),
            ("node".into(), Value::String(node.into())),
        ];
        processor.consume(
            &tags,
            &mut [Ok(VisibleSeriesBlock {
                first_timestamp: 0,
                timestamp_step: 10,
                values: Float64Array::from(values).into(),
            })]
            .into_iter(),
        )
    }

    #[test]
    fn fused_top_one_matches_selector_topk_and_sum_with_ties_and_stale() {
        let (query, sources) = fixture();
        let top = TopOne::recognize(&query, &sources).unwrap();
        let SourceOperation::Range(spec) = &sources[0].operation else {
            unreachable!()
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let memory = MemoryConsumer::new("top-one-test").register(&pool);
        let mut processor =
            Processor::new(top, spec.clone(), memory, Arc::new(AtomicBool::new(false))).unwrap();
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        let inputs = [
            (None, "a", vec![1.0, -0.0, f64::NAN, stale, 4.0]),
            (Some("z"), "b", vec![1.0, 0.0, 2.0, 3.0, stale]),
            (Some(""), "c", vec![0.0, 0.0, 0.0, 2.0, 5.0]),
        ];
        let mut expected = Vec::new();
        for (pod, node, values) in inputs {
            let mut labels = Labels::from([
                ("__name__".into(), "m".into()),
                ("node".into(), node.into()),
            ]);
            if let Some(pod) = pod {
                labels.insert("pod".into(), pod.into());
            }
            expected.push(SeriesVector {
                labels,
                values: range::evaluate(
                    "instant",
                    &query.grid,
                    vec![0, 10, 20, 30, 40],
                    values.clone(),
                    10,
                    0,
                )
                .unwrap(),
            });
            feed(&mut processor, pod, node, values).unwrap();
        }
        let expected =
            vector::topk(&expected, &[], false, 1, 5, &["pod".into(), "node".into()]).unwrap();
        let expected =
            vector::aggregate(expected, &["node".into()], true, Aggregate::Sum, 5).unwrap();
        let (actual, memory) = processor.finish().unwrap();
        let bits = |series: Vec<SeriesVector>| {
            series
                .into_iter()
                .map(|series| {
                    (
                        series.labels,
                        series
                            .values
                            .into_iter()
                            .map(|value| value.map(f64::to_bits))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(bits(actual), bits(expected));
        assert!(pool.reserved() > 0);
        drop(memory);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn fusion_rejects_grouped_or_nonidentity_topk() {
        let (mut query, sources) = fixture();
        for expression in [
            "sum(topk(2, m))",
            "sum(topk by(pod)(1, m))",
            "sum(topk(1, m * 2))",
            "sum(topk(1, other))",
            "max(topk(1, m))",
        ] {
            query.expr = parse(expression).unwrap();
            assert!(TopOne::recognize(&query, &sources).is_none());
        }
    }

    #[test]
    fn duplicate_losers_cancellation_and_memory_failure_are_not_hidden() {
        let (query, sources) = fixture();
        let top = TopOne::recognize(&query, &sources).unwrap();
        let SourceOperation::Range(spec) = &sources[0].operation else {
            unreachable!()
        };
        let small: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        assert!(
            Processor::new(
                top.clone(),
                spec.clone(),
                MemoryConsumer::new("small").register(&small),
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        assert_eq!(small.reserved(), 0);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut processor = Processor::new(
            top,
            spec.clone(),
            MemoryConsumer::new("duplicates").register(&pool),
            cancelled.clone(),
        )
        .unwrap();
        feed(&mut processor, Some("winner"), "a", vec![100.0; 5]).unwrap();
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        feed(
            &mut processor,
            Some("split"),
            "c",
            vec![1.0, stale, 1.0, stale, 1.0],
        )
        .unwrap();
        feed(
            &mut processor,
            Some("split"),
            "c",
            vec![stale, 1.0, stale, 1.0, stale],
        )
        .unwrap();
        feed(&mut processor, Some("loser"), "b", vec![1.0; 5]).unwrap();
        assert!(
            feed(&mut processor, Some("loser"), "b", vec![1.0; 5])
                .unwrap_err()
                .to_string()
                .contains("Duplicate")
        );
        cancelled.store(true, AtomicOrdering::Relaxed);
        assert!(processor.finish().is_err());
        assert_eq!(pool.reserved(), 0);
    }
}
