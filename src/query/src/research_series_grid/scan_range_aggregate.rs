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

use std::collections::HashMap;
use std::sync::Arc;

use common_recordbatch::RecordBatch;
use datafusion::arrow::array::{ArrayRef, Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_common::{DataFusionError, Result, ScalarValue};

use super::grid::TimeGrid;

#[derive(Debug)]
pub(crate) struct RangeAggregateSpec {
    pub schema: datatypes::schema::SchemaRef,
    // None is the shared evaluation timestamp; Some indexes the series label tuple.
    pub groups: Vec<Option<usize>>,
    pub expression: Arc<dyn PhysicalExpr>,
    pub function: Arc<AggregateFunctionExpr>,
}

pub(crate) struct RangeAggregate {
    spec: Arc<RangeAggregateSpec>,
    grid: Arc<TimeGrid>,
    labels: HashMap<Vec<ScalarValue>, usize>,
    slots: Vec<Vec<usize>>,
    keys: Vec<(usize, usize)>,
    accumulator: Box<dyn GroupsAccumulator>,
    scratch: MemoryReservation,
    memory: MemoryReservation,
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

impl RangeAggregate {
    pub(crate) fn new(
        spec: Arc<RangeAggregateSpec>,
        grid: Arc<TimeGrid>,
        memory: MemoryReservation,
    ) -> Result<Self> {
        Ok(Self {
            accumulator: spec.function.create_groups_accumulator()?,
            spec,
            grid,
            labels: HashMap::new(),
            slots: Vec::new(),
            keys: Vec::new(),
            scratch: memory.new_empty(),
            memory,
        })
    }

    pub(crate) fn update(
        &mut self,
        labels: &[ScalarValue],
        values: Vec<Option<f64>>,
    ) -> Result<()> {
        if values.len() != self.grid.instants.len() {
            return Err(invalid("Aggregate grid length mismatch"));
        }
        if values.iter().all(Option::is_none) {
            return Ok(());
        }
        let scratch_bytes = values
            .len()
            .checked_mul(128)
            .and_then(|n| {
                labels
                    .iter()
                    .try_fold(n, |n, v| n.checked_add(v.size().checked_mul(4)?))
            })
            .ok_or_else(|| invalid("Aggregate scratch overflow"))?;
        if scratch_bytes > self.scratch.size() {
            self.scratch.try_grow(scratch_bytes - self.scratch.size())?;
        }
        let key = self
            .spec
            .groups
            .iter()
            .flatten()
            .map(|&index| {
                labels
                    .get(index)
                    .cloned()
                    .ok_or_else(|| invalid("Missing aggregate label"))
            })
            .collect::<Result<Vec<_>>>()?;
        let label = match self.labels.get(&key) {
            Some(&label) => label,
            None => {
                // Reserve grid slots, group keys, accumulator growth and retained labels
                // before allocating. Only Float64 SUM is admitted by the plan matcher.
                let bytes = self
                    .grid
                    .instants
                    .len()
                    .checked_mul(128)
                    .and_then(|n| n.checked_add(256))
                    .and_then(|n| {
                        key.iter()
                            .try_fold(n, |n, v| n.checked_add(v.size().checked_mul(4)?))
                    })
                    .ok_or_else(|| invalid("Aggregate state size overflow"))?;
                self.memory.try_grow(bytes)?;
                let label = self.labels.len();
                self.labels.insert(key, label);
                self.slots.push(vec![usize::MAX; self.grid.instants.len()]);
                label
            }
        };
        let mut ids = Vec::new();
        let mut input = Vec::new();
        for (slot, value) in values.into_iter().enumerate() {
            let Some(value) = value else {
                continue;
            };
            let id = &mut self.slots[label][slot];
            if *id == usize::MAX {
                *id = self.keys.len();
                self.keys.push((label, slot));
            }
            ids.push(*id);
            input.push(value);
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "rate",
            DataType::Float64,
            false,
        )]));
        let input = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(input))],
        )?;
        let values = self
            .spec
            .expression
            .evaluate(&input)?
            .into_array(input.num_rows())?;
        self.accumulator
            .update_batch(&[values], &ids, None, self.keys.len())
    }

    pub(crate) fn finish_series(&mut self) -> Result<Vec<super::scan_range::SeriesGrid>> {
        use datafusion::arrow::array::Array;

        if self.keys.is_empty() {
            return Ok(Vec::new());
        }
        let width = self.grid.instants.len();
        let bytes = self
            .labels
            .keys()
            .try_fold(
                self.labels
                    .len()
                    .checked_mul(width)
                    .and_then(|count| count.checked_mul(32))
                    .ok_or_else(|| invalid("Series aggregate output size overflow"))?,
                |size, key| {
                    key.iter().try_fold(size.checked_add(256)?, |size, value| {
                        size.checked_add(value.size().checked_mul(4)?)
                    })
                },
            )
            .ok_or_else(|| invalid("Series aggregate output size overflow"))?;
        self.memory.try_grow(bytes)?;
        let values = self.accumulator.evaluate(EmitTo::All)?;
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("Series aggregate requires Float64 sum"))?;
        let mut output = (0..self.labels.len())
            .map(|_| super::scan_range::SeriesGrid {
                labels: Vec::new(),
                values: vec![None; width],
            })
            .collect::<Vec<_>>();
        for (key, &id) in &self.labels {
            output[id].labels = key.clone();
        }
        for (index, &(label, point)) in self.keys.iter().enumerate() {
            if values.is_valid(index) {
                output[label].values[point] = Some(values.value(index));
            }
        }
        self.keys.clear();
        Ok(output)
    }

    pub(crate) fn finish_grid(
        &mut self,
        schema: datatypes::schema::SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        use datafusion::arrow::array::{Array, ListArray};
        use datafusion::arrow::buffer::OffsetBuffer;

        if self.keys.is_empty() {
            return Ok(Vec::new());
        }
        let width = self.grid.instants.len();
        let count = self
            .labels
            .len()
            .checked_mul(width)
            .ok_or_else(|| invalid("Aggregate grid size overflow"))?;
        let end = i32::try_from(count).map_err(|_| invalid("Aggregate grid exceeds list limit"))?;
        let label_bytes = self
            .labels
            .keys()
            .try_fold(0usize, |n, key| {
                key.iter()
                    .try_fold(n, |n, value| n.checked_add(value.size().checked_mul(4)?))
            })
            .ok_or_else(|| invalid("Aggregate label size overflow"))?;
        self.memory.try_grow(
            count
                .checked_mul(32)
                .and_then(|n| n.checked_add(label_bytes))
                .ok_or_else(|| invalid("Aggregate grid size overflow"))?,
        )?;
        let values = self.accumulator.evaluate(EmitTo::All)?;
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("Aggregate grid requires Float64 sum"))?;
        let mut dense = vec![None; count];
        for (index, &(label, point)) in self.keys.iter().enumerate() {
            if values.is_valid(index) {
                dense[label * width + point] = Some(values.value(index));
            }
        }
        let mut labels = vec![Vec::new(); self.labels.len()];
        for (key, &id) in &self.labels {
            labels[id] = key.clone();
        }
        let mut columns = (0..self.spec.groups.iter().flatten().count())
            .map(|column| ScalarValue::iter_to_array(labels.iter().map(|key| key[column].clone())))
            .collect::<Result<Vec<_>>>()?;
        let DataType::List(item) = schema.arrow_schema().field(columns.len()).data_type() else {
            return Err(invalid("Aggregate grid requires list output"));
        };
        let offsets = (0..self.labels.len())
            .map(|label| (label * width) as i32)
            .chain(std::iter::once(end))
            .collect::<Vec<_>>();
        columns.push(Arc::new(ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(Float64Array::from(dense)),
            None,
        )?));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.arrow_schema().clone(),
            columns,
        )?;
        self.keys.clear();
        Ok(vec![RecordBatch::from_df_record_batch(schema, batch)])
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        if self.keys.is_empty() {
            return Ok(Vec::new());
        }
        let row_bytes = self
            .labels
            .keys()
            .try_fold(128usize, |largest, key| {
                key.iter()
                    .try_fold(128usize, |n, v| n.checked_add(v.size().checked_mul(4)?))
                    .map(|n| largest.max(n))
            })
            .ok_or_else(|| invalid("Aggregate output row size overflow"))?;
        self.memory.try_grow(
            self.keys
                .len()
                .checked_mul(row_bytes)
                .ok_or_else(|| invalid("Aggregate output size overflow"))?,
        )?;
        let states = self.accumulator.state(EmitTo::All)?;
        let mut labels = vec![Vec::new(); self.labels.len()];
        for (key, &id) in &self.labels {
            labels[id] = key.clone();
        }
        let mut output = Vec::new();
        for begin in (0..self.keys.len()).step_by(4096) {
            let count = (self.keys.len() - begin).min(4096);
            let keys = &self.keys[begin..begin + count];
            let mut arrays: Vec<ArrayRef> = Vec::new();
            let mut label_column = 0;
            for group in &self.spec.groups {
                arrays.push(if group.is_none() {
                    Arc::new(TimestampMillisecondArray::from_iter_values(
                        keys.iter().map(|&(_, slot)| self.grid.instants[slot]),
                    ))
                } else {
                    let column = label_column;
                    label_column += 1;
                    ScalarValue::iter_to_array(
                        keys.iter().map(|&(label, _)| labels[label][column].clone()),
                    )?
                });
            }
            arrays.extend(states.iter().map(|array| array.slice(begin, count)));
            let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
                self.spec.schema.arrow_schema().clone(),
                arrays,
            )?;
            output.push(RecordBatch::from_df_record_batch(
                self.spec.schema.clone(),
                batch,
            ));
        }
        self.keys.clear();
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};

    fn create(pool: &Arc<GreedyMemoryPool>) -> RangeAggregate {
        let memory_pool: Arc<dyn MemoryPool> = pool.clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "rate",
            DataType::Float64,
            false,
        )]));
        let function = Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("rate", 0))])
                .schema(schema)
                .alias("sum")
                .build()
                .unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("sum", DataType::Float64, true),
        ]));
        RangeAggregate::new(
            Arc::new(RangeAggregateSpec {
                schema: Arc::new(datatypes::schema::Schema::try_from(schema).unwrap()),
                groups: vec![Some(0), None],
                expression: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("rate", 0)),
                    Operator::Multiply,
                    Arc::new(Literal::new(ScalarValue::Float64(Some(8.0)))),
                )),
                function,
            }),
            Arc::new(TimeGrid::new(0, 20, 10).unwrap()),
            MemoryConsumer::new("aggregate-test").register(&memory_pool),
        )
        .unwrap()
    }

    #[test]
    fn aggregation_preserves_missing_windows_nan_and_series_order() {
        let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut aggregate = create(&pool);
        let a = [ScalarValue::Utf8(Some("a".into()))];
        let b = [ScalarValue::Utf8(Some("b".into()))];
        let mut direct = create(&pool);
        for (labels, values) in [
            (&a, vec![Some(1e30), None, Some(f64::NAN)]),
            (&a, vec![Some(-1e30), Some(4.0), Some(5.0)]),
            (&a, vec![Some(1.0), None, None]),
            (&b, vec![None, Some(2.0), None]),
        ] {
            aggregate.update(labels, values.clone()).unwrap();
            direct.update(labels, values).unwrap();
        }
        let retained = pool.reserved();
        for _ in 0..3 {
            aggregate.update(&a, vec![Some(0.0); 3]).unwrap();
            direct.update(&a, vec![Some(0.0); 3]).unwrap();
            assert_eq!(pool.reserved(), retained);
        }
        let series = direct.finish_series().unwrap();
        assert_eq!(series.len(), 2);
        let first = series.iter().find(|series| series.labels == a).unwrap();
        assert_eq!(first.values[..2], [Some(8.0), Some(32.0)]);
        assert!(first.values[2].unwrap().is_nan());
        let second = series.iter().find(|series| series.labels == b).unwrap();
        assert_eq!(second.values, vec![None, Some(16.0), None]);
        let output = aggregate.finish().unwrap();
        let mut actual = Vec::new();
        for batch in &output {
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.null_count(), 0);
            for row in 0..batch.num_rows() {
                let host = ScalarValue::try_from_array(batch.column(0), row).unwrap();
                let timestamp = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(row);
                actual.push((host, timestamp, values.value(row)));
            }
        }
        assert_eq!(actual.len(), 4);
        assert!(
            actual
                .iter()
                .any(|(host, time, value)| host == &a[0] && *time == 0 && *value == 8.0)
        );
        assert!(
            actual
                .iter()
                .any(|(host, time, value)| host == &a[0] && *time == 10 && *value == 32.0)
        );
        assert!(
            actual
                .iter()
                .any(|(host, time, value)| host == &a[0] && *time == 20 && value.is_nan())
        );
        assert!(
            actual
                .iter()
                .any(|(host, time, value)| host == &b[0] && *time == 10 && *value == 16.0)
        );
        drop(output);
        drop(aggregate);
        drop(series);
        drop(direct);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn aggregation_rejects_budget_before_retaining_groups() {
        let pool = Arc::new(GreedyMemoryPool::new(1));
        let mut aggregate = create(&pool);
        assert!(matches!(
            aggregate.update(&[ScalarValue::Utf8(None)], vec![Some(1.0), None, None]),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        drop(aggregate);
        assert_eq!(pool.reserved(), 0);
    }
}
