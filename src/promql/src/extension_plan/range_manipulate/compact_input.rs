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

use datafusion::arrow::array::{BooleanArray, Float64Array};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{TryStreamExt, stream};

use super::*;
use crate::extension_plan::series_divide::TagIdentifier;
use crate::extension_plan::{SeriesDivideExec, SeriesNormalizeExec};

pub(super) fn eligible(plan: &RangeManipulateExec) -> Option<&SeriesDivideExec> {
    let normalize = plan.input.downcast_ref::<SeriesNormalizeExec>()?;
    if !normalize.research_filters_stale()
        || plan.field_columns.len() != 1
        || plan.interval <= 0
        || plan.start > plan.end
    {
        return None;
    }
    let divide = normalize.children()[0].downcast_ref::<SeriesDivideExec>()?;
    let schema = divide.schema();
    let time = schema.column_with_name(&plan.time_index_column)?.1;
    let value = schema.column_with_name(&plan.field_columns[0])?.1;
    if time.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
        || time.is_nullable()
        || value.data_type() != &DataType::Float64
        || schema.fields().iter().any(|field| {
            field.name() != value.name()
                && !matches!(
                    field.data_type(),
                    DataType::Timestamp(_, _) | DataType::UInt64
                )
                && !field.data_type().is_string()
                && !matches!(field.data_type(), DataType::Dictionary(_, value) if value.is_string())
        })
    {
        return None;
    }
    Some(divide)
}

/// Executes the already partitioned and ordered scan beneath SeriesDivide. The logical
/// tree is unchanged so distributed pushdown and column pruning keep their normal proof.
pub(super) fn execute(
    plan: &RangeManipulateExec,
    partition: usize,
    context: Arc<TaskContext>,
) -> DataFusionResult<Option<SendableRecordBatchStream>> {
    let Some(divide) = eligible(plan) else {
        return Ok(None);
    };
    let schema = divide.schema();
    let time = schema.index_of(&plan.time_index_column)?;
    let value = schema.index_of(&plan.field_columns[0])?;
    let tags = divide
        .research_tags()
        .iter()
        .map(|tag| schema.index_of(tag))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let numeric_schema = Arc::new(Schema::new(vec![
        schema.field(time).clone(),
        schema.field(value).clone(),
    ]));
    let metric = BaselineMetrics::new(&plan.metric, partition);
    let memory = MemoryConsumer::new("ResearchCompactRange").register(context.memory_pool());
    let points = usize::try_from(
        (i128::from(plan.end) - i128::from(plan.start)) / i128::from(plan.interval) + 1,
    )
    .map_err(|_| DataFusionError::Execution("Compact range grid size overflow".into()))?;
    memory.try_grow(points.checked_mul(8).ok_or_else(|| {
        DataFusionError::Execution("Compact range grid byte size overflow".into())
    })?)?;
    let range = RangeManipulateStream {
        offset: plan.offset,
        start: plan.start,
        end: plan.end,
        interval: plan.interval,
        range: plan.range,
        time_index: 0,
        time_unit: TimeUnit::Millisecond,
        field_columns: vec![1],
        aligned_ts_array: RangeManipulateStream::build_aligned_ts_array(
            plan.start,
            plan.end,
            plan.interval,
        ),
        output_schema: plan.output_schema.clone(),
        input: Box::pin(EmptyRecordBatchStream::new(numeric_schema.clone())),
        metric: metric.clone(),
        num_series: Count::new(),
    };
    let input = divide.children()[0].execute(partition, context.clone())?;
    let state = CompactInput {
        input,
        pending: None,
        key: None,
        first: None,
        times: Vec::new(),
        values: Vec::new(),
        tags,
        time,
        value,
        numeric_schema,
        range,
        metric,
        memory,
    };
    let output = stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(batch) = state.next_series().await? {
                state.metric.output_rows().add(batch.num_rows());
                return Ok(Some((batch, state)));
            }
            if state.pending.is_none() && state.key.is_none() {
                return Ok(None);
            }
        }
    });
    Ok(Some(Box::pin(RecordBatchStreamAdapter::new(
        plan.output_schema.clone(),
        output,
    ))))
}

struct CompactInput {
    input: SendableRecordBatchStream,
    pending: Option<RecordBatch>,
    key: Option<RecordBatch>,
    first: Option<Vec<ScalarValue>>,
    times: Vec<ArrayRef>,
    values: Vec<ArrayRef>,
    tags: Vec<usize>,
    time: usize,
    value: usize,
    numeric_schema: SchemaRef,
    range: RangeManipulateStream,
    metric: BaselineMetrics,
    memory: MemoryReservation,
}

impl CompactInput {
    fn retained_bytes(&self) -> usize {
        self.times
            .iter()
            .chain(&self.values)
            .map(|array| array.get_array_memory_size())
            .sum::<usize>()
            .saturating_add(self.range.aligned_ts_array.get_array_memory_size())
            .saturating_add(
                self.pending
                    .as_ref()
                    .map_or(0, RecordBatch::get_array_memory_size),
            )
            .saturating_add(
                self.key
                    .as_ref()
                    .map_or(0, RecordBatch::get_array_memory_size),
            )
            .saturating_add(
                self.first
                    .as_ref()
                    .map_or(0, |values| values.iter().map(ScalarValue::size).sum()),
            )
    }

    async fn next_series(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        self.memory.try_resize(self.retained_bytes())?;
        loop {
            let batch = match self.pending.take() {
                Some(batch) => batch,
                None => match self.input.try_next().await? {
                    Some(batch) if batch.num_rows() == 0 => continue,
                    Some(batch) => batch,
                    None => return self.finish(),
                },
            };
            self.memory.try_grow(batch.get_array_memory_size())?;
            let timer = self.metric.elapsed_compute().timer();
            let tags = TagIdentifier::try_new(&batch, &self.tags)?;
            if let Some(key) = &self.key {
                let previous = TagIdentifier::try_new(key, &self.tags)?;
                if !previous.equal_at(0, &tags, 0)? {
                    drop(timer);
                    self.pending = Some(batch);
                    return self.finish();
                }
            } else {
                let columns = batch
                    .columns()
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, 0)?.to_array_of_size(1))
                    .collect::<DataFusionResult<Vec<_>>>()?;
                self.key = Some(RecordBatch::try_new(batch.schema(), columns)?);
            }
            let mut end = batch.num_rows();
            if !tags.equal_at(0, &tags, end - 1)? {
                end = 1;
                while end < batch.num_rows() && tags.equal_at(0, &tags, end)? {
                    end += 1;
                }
            }
            let prefix = batch.slice(0, end);
            if end < batch.num_rows() {
                self.pending = Some(batch.slice(end, batch.num_rows() - end));
            }
            drop(timer);
            self.append(&prefix)?;
            if self.pending.is_some() {
                return self.finish();
            }
        }
    }

    fn append(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        let timer = self.metric.elapsed_compute().timer();
        let values = batch
            .column(self.value)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("Compact range requires Float64 values".into())
            })?;
        let stale =
            |row| values.is_valid(row) && values.value(row).to_bits() == 0x7ff0_0000_0000_0002;
        let first = (0..batch.num_rows()).find(|&row| !stale(row));
        if let Some(row) = first {
            if self.first.is_none() {
                self.first = Some(
                    batch
                        .columns()
                        .iter()
                        .map(|array| ScalarValue::try_from_array(array, row))
                        .collect::<DataFusionResult<Vec<_>>>()?,
                );
            }
            if (0..batch.num_rows()).any(stale) {
                self.memory.try_grow(batch.num_rows().saturating_mul(24))?;
                let mask =
                    BooleanArray::from_iter((0..batch.num_rows()).map(|row| Some(!stale(row))));
                self.times
                    .push(compute::filter(batch.column(self.time).as_ref(), &mask)?);
                self.values.push(compute::filter(values, &mask)?);
            } else {
                self.times.push(batch.column(self.time).clone());
                self.values.push(batch.column(self.value).clone());
            }
        }
        drop(timer);
        Ok(())
    }

    fn finish(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        self.key = None;
        let Some(first) = self.first.take() else {
            return Ok(None);
        };
        let timer = self.metric.elapsed_compute().timer();
        let rows = self.times.iter().map(|array| array.len()).sum::<usize>();
        if rows > u32::MAX as usize {
            return Err(DataFusionError::Execution(
                "Compact range exceeds range-index capacity".into(),
            ));
        }
        let points = self.range.aligned_ts_array.len();
        self.memory
            .try_grow(rows.saturating_mul(24).saturating_add(
                points.saturating_mul(32 + first.iter().map(ScalarValue::size).sum::<usize>()),
            ))?;
        let concat = |chunks: &[ArrayRef]| -> DataFusionResult<ArrayRef> {
            if chunks.len() == 1 {
                return Ok(chunks[0].clone());
            }
            Ok(compute::concat(
                &chunks.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
            )?)
        };
        let times = concat(&self.times)?;
        let values = concat(&self.values)?;
        self.times.clear();
        self.values.clear();
        let numeric = RecordBatch::try_new(
            self.numeric_schema.clone(),
            vec![times.clone(), values.clone()],
        )?;
        let (ranges, (start, end)) = self.range.calculate_range(&numeric)?;
        if ranges.iter().all(|(_, len)| *len == 0) {
            return Ok(None);
        }
        let times: ArrayRef = if self.range.offset == 0 {
            times
        } else {
            let timestamps = times
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Compact range requires millisecond timestamps".into(),
                    )
                })?;
            let shifted = timestamps
                .values()
                .iter()
                .map(|&time| {
                    time.checked_add(self.range.offset).ok_or_else(|| {
                        DataFusionError::Execution(
                            "RangeManipulate timestamp payload overflow".into(),
                        )
                    })
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            Arc::new(TimestampMillisecondArray::from(shifted))
        };
        let aligned = if ranges.len() == self.range.aligned_ts_array.len() {
            self.range.aligned_ts_array.clone()
        } else {
            RangeManipulateStream::build_aligned_ts_array(start, end, self.range.interval)
        };
        let mut columns = first
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if index == self.time {
                    Ok(aligned.clone())
                } else if index == self.value {
                    Ok(Arc::new(
                        RangeArray::from_ranges(values.clone(), ranges.clone())?.into_dict(),
                    ) as ArrayRef)
                } else {
                    value.to_array_of_size(ranges.len())
                }
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        columns.push(Arc::new(
            RangeArray::from_ranges(times, ranges)?.into_dict(),
        ));
        let result = RecordBatch::try_new(self.range.output_schema.clone(), columns)?;
        drop(timer);
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extension_plan::{SeriesDivide, SeriesNormalize};
    use datafusion::arrow::array::{DictionaryArray, StringArray, UInt64Array};
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::context::SessionConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    fn fixture(
        chunk: usize,
        offset: i64,
        step: i64,
        window: i64,
        dictionary: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let label_type = if dictionary {
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
        } else {
            DataType::Utf8
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("host", label_type.clone(), true),
            Field::new("__tsid", DataType::UInt64, false),
        ]));
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    0, 1, 0, 2, 3, 7, 1, 8,
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(stale),
                    Some(stale),
                    Some(stale),
                    None,
                    Some(1.0),
                    Some(f64::NAN),
                    Some(-0.0),
                    Some(f64::INFINITY),
                ])),
                compute::cast(
                    &StringArray::from(vec![
                        None,
                        None,
                        Some(""),
                        Some(""),
                        Some(""),
                        Some(""),
                        Some("b"),
                        Some("b"),
                    ]),
                    &label_type,
                )
                .unwrap(),
                Arc::new(UInt64Array::from(vec![0, 0, 1, 1, 1, 1, 2, 2])),
            ],
        )
        .unwrap();
        let mut batches = vec![RecordBatch::new_empty(schema.clone())];
        batches.extend((0..batch.num_rows()).step_by(chunk).map(|start| {
            let slice = batch.slice(start, chunk.min(batch.num_rows() - start));
            let mut columns = slice.columns().to_vec();
            let labels = compute::cast(columns[2].as_ref(), &DataType::Utf8).unwrap();
            columns[2] = compute::cast(labels.as_ref(), &label_type).unwrap();
            RecordBatch::try_new(schema.clone(), columns).unwrap()
        }));
        let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[batches], schema.clone(), None).unwrap(),
        )));
        let logical = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: schema.to_dfschema_ref().unwrap(),
        });
        let divide = SeriesDivide::new(vec!["__tsid".into()], "time".into(), logical.clone());
        let normalize =
            SeriesNormalize::new(offset, "time", true, vec!["__tsid".into()], logical.clone());
        RangeManipulate::new(
            -10,
            30,
            step,
            offset,
            window,
            "time".into(),
            vec!["value".into()],
            logical,
        )
        .unwrap()
        .to_execution_plan(normalize.to_execution_plan(divide.to_execution_plan(source)))
    }

    fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            if let Some(dict) =
                                column.as_any().downcast_ref::<DictionaryArray<Int64Type>>()
                            {
                                let range = RangeArray::try_new(dict.clone()).unwrap();
                                let values = range.get(row).unwrap();
                                if let Some(values) = values.as_any().downcast_ref::<Float64Array>()
                                {
                                    format!(
                                        "{:?}",
                                        values
                                            .iter()
                                            .map(|value| value.map(f64::to_bits))
                                            .collect::<Vec<_>>()
                                    )
                                } else {
                                    format!(
                                        "{:?}",
                                        values
                                            .as_any()
                                            .downcast_ref::<TimestampMillisecondArray>()
                                            .unwrap()
                                            .iter()
                                            .collect::<Vec<_>>()
                                    )
                                }
                            } else {
                                format!("{:?}", ScalarValue::try_from_array(column, row).unwrap())
                            }
                        })
                        .collect()
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn compact_input_matches_normalized_ranges_across_batches() {
        let context = SessionContext::new().task_ctx();
        for dictionary in [false, true] {
            for chunk in [1, 2, 4, 8] {
                for offset in [-5, 0, 7] {
                    for step in [1, 7] {
                        for window in [1, 8] {
                            let plan = fixture(chunk, offset, step, window, dictionary);
                            let range = plan.downcast_ref::<RangeManipulateExec>().unwrap();
                            let expected = plan
                                .execute(0, context.clone())
                                .unwrap()
                                .try_collect::<Vec<_>>()
                                .await
                                .unwrap();
                            let actual = execute(range, 0, context.clone())
                                .unwrap()
                                .unwrap()
                                .try_collect::<Vec<_>>()
                                .await
                                .unwrap();
                            assert_eq!(
                                rows(&expected),
                                rows(&actual),
                                "chunk={chunk} offset={offset} step={step} window={window}"
                            );
                            assert_eq!(context.memory_pool().reserved(), 0);
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn compact_input_respects_memory_limit() {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(64)))
            .build_arc()
            .unwrap();
        let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
        let plan = fixture(2, 0, 1, 8, false);
        let range = plan.downcast_ref::<RangeManipulateExec>().unwrap();
        assert!(execute(range, 0, context.clone()).is_err());
        assert_eq!(context.memory_pool().reserved(), 0);
    }
}
