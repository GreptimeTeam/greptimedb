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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::ArrayRef;
use arrow::compute::{concat, take_record_batch};
use arrow_schema::SchemaRef;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion::common::arrow::compute::sort_to_indices;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_err, DataFusionError};
use datafusion_physical_expr::PhysicalSortExpr;
use futures::Stream;
use itertools::Itertools;
use snafu::location;
use store_api::region_engine::PartitionRange;

use crate::downcast_ts_array;

/// Sort input within given PartitionRange
///
/// Input is assumed to be segmented by empty RecordBatch, which indicates a new `PartitionRange` is starting
///
/// and this operator will sort each partition independently within the partition.
#[derive(Debug, Clone)]
pub struct PartSortExec {
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    partition_ranges: Vec<Vec<PartitionRange>>,
    properties: PlanProperties,
}

impl PartSortExec {
    pub fn new(
        expression: PhysicalSortExpr,
        partition_ranges: Vec<Vec<PartitionRange>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let properties = PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.execution_mode(),
        );

        Self {
            expression,
            input,
            metrics,
            partition_ranges,
            properties,
        }
    }

    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let input_stream: DfSendableRecordBatchStream =
            self.input.execute(partition, context.clone())?;

        if partition >= self.partition_ranges.len() {
            internal_err!(
                "Partition index out of range: {} >= {}",
                partition,
                self.partition_ranges.len()
            )?;
        }

        let df_stream = Box::pin(PartSortStream::new(
            context,
            self,
            input_stream,
            self.partition_ranges[partition].clone(),
            partition,
        )) as _;

        Ok(df_stream)
    }
}

impl DisplayAs for PartSortExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartSortExec {}", self.expression)
    }
}

impl ExecutionPlan for PartSortExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let new_input = if let Some(first) = children.first() {
            first
        } else {
            internal_err!("No children found")?
        };
        Ok(Arc::new(Self::new(
            self.expression.clone(),
            self.partition_ranges.clone(),
            new_input.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        self.to_stream(context, partition)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// # Explain
    ///
    /// This plan needs to be executed on each partition independently,
    /// and is expected to run directly on storage engine's output
    /// distribution / partition.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
}

struct PartSortStream {
    /// Memory pool for this stream
    reservation: MemoryReservation,
    buffer: Vec<DfRecordBatch>,
    expression: PhysicalSortExpr,
    produced: usize,
    input: DfSendableRecordBatchStream,
    input_complete: bool,
    schema: SchemaRef,
    partition_ranges: Vec<PartitionRange>,
    #[allow(dead_code)] // this is used under #[debug_assertions]
    partition: usize,
    cur_part_idx: usize,
    metrics: BaselineMetrics,
}

impl PartSortStream {
    fn new(
        context: Arc<TaskContext>,
        sort: &PartSortExec,
        input: DfSendableRecordBatchStream,
        partition_ranges: Vec<PartitionRange>,
        partition: usize,
    ) -> Self {
        Self {
            reservation: MemoryConsumer::new("PartSortStream".to_string())
                .register(&context.runtime_env().memory_pool),
            buffer: Vec::new(),
            expression: sort.expression.clone(),
            produced: 0,
            input,
            input_complete: false,
            schema: sort.input.schema(),
            partition_ranges,
            partition,
            cur_part_idx: 0,
            metrics: BaselineMetrics::new(&sort.metrics, partition),
        }
    }
}

macro_rules! array_check_helper {
    ($t:ty, $unit:expr, $arr:expr, $cur_range:expr, $min_max_idx:expr) => {{
            if $cur_range.start.unit().as_arrow_time_unit() != $unit
            || $cur_range.end.unit().as_arrow_time_unit() != $unit
        {
            internal_err!(
                "PartitionRange unit mismatch, expect {:?}, found {:?}",
                $cur_range.start.unit(),
                $unit
            )?;
        }
        let arr = $arr
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<$t>>()
            .unwrap();

        let min = arr.value($min_max_idx.0);
        let max = arr.value($min_max_idx.1);
        let (min, max) = if min < max{
            (min, max)
        } else {
            (max, min)
        };
        let cur_min = $cur_range.start.value();
        let cur_max = $cur_range.end.value();
        // note that PartitionRange is left inclusive and right exclusive
        if !(min >= cur_min && max < cur_max) {
            internal_err!(
                "Sort column min/max value out of partition range: sort_column.min_max=[{:?}, {:?}] not in PartitionRange=[{:?}, {:?}]",
                min,
                max,
                cur_min,
                cur_max
            )?;
        }
    }};
}

impl PartSortStream {
    /// check whether the sort column's min/max value is within the partition range
    fn check_in_range(
        &self,
        sort_column: &ArrayRef,
        min_max_idx: (usize, usize),
    ) -> datafusion_common::Result<()> {
        if self.cur_part_idx >= self.partition_ranges.len() {
            internal_err!(
                "Partition index out of range: {} >= {}",
                self.cur_part_idx,
                self.partition_ranges.len()
            )?;
        }
        let cur_range = self.partition_ranges[self.cur_part_idx];

        downcast_ts_array!(
            sort_column.data_type() => (array_check_helper, sort_column, cur_range, min_max_idx),
            _ => internal_err!(
                "Unsupported data type for sort column: {:?}",
                sort_column.data_type()
            )?,
        );

        Ok(())
    }

    /// Sort and clear the buffer and return the sorted record batch
    ///
    /// this function should return a empty record batch if the buffer is empty
    fn sort_buffer(&mut self) -> datafusion_common::Result<DfRecordBatch> {
        if self.buffer.is_empty() {
            return Ok(DfRecordBatch::new_empty(self.schema.clone()));
        }
        let mut sort_columns = Vec::with_capacity(self.buffer.len());
        let mut opt = None;
        for batch in self.buffer.iter() {
            let sort_column = self.expression.evaluate_to_sort_column(batch)?;
            opt = opt.or(sort_column.options);
            sort_columns.push(sort_column.values);
        }

        let sort_column =
            concat(&sort_columns.iter().map(|a| a.as_ref()).collect_vec()).map_err(|e| {
                DataFusionError::ArrowError(
                    e,
                    Some(format!("Fail to concat sort columns at {}", location!())),
                )
            })?;

        let indices = sort_to_indices(&sort_column, opt, None).map_err(|e| {
            DataFusionError::ArrowError(
                e,
                Some(format!("Fail to sort to indices at {}", location!())),
            )
        })?;

        self.check_in_range(
            &sort_column,
            (
                indices.value(0) as usize,
                indices.value(indices.len() - 1) as usize,
            ),
        )
        .inspect_err(|_e| {
            #[cfg(debug_assertions)]
            common_telemetry::error!(
                "Fail to check sort column in range at {}, current_idx: {}, num_rows: {}, err: {}",
                self.partition,
                self.cur_part_idx,
                sort_column.len(),
                _e
            );
        })?;

        // reserve memory for the concat input and sorted output
        let total_mem: usize = self.buffer.iter().map(|r| r.get_array_memory_size()).sum();
        self.reservation.try_grow(total_mem * 2)?;

        let full_input = concat_batches(
            &self.schema,
            &self.buffer,
            self.buffer.iter().map(|r| r.num_rows()).sum(),
        )
        .map_err(|e| {
            DataFusionError::ArrowError(
                e,
                Some(format!(
                    "Fail to concat input batches when sorting at {}",
                    location!()
                )),
            )
        })?;

        let sorted = take_record_batch(&full_input, &indices).map_err(|e| {
            DataFusionError::ArrowError(
                e,
                Some(format!(
                    "Fail to take result record batch when sorting at {}",
                    location!()
                )),
            )
        })?;

        // only clear after sorted for better debugging
        self.buffer.clear();
        self.produced += sorted.num_rows();
        drop(full_input);
        // here remove both buffer and full_input memory
        self.reservation.shrink(2 * total_mem);
        Ok(sorted)
    }

    pub fn poll_next_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        loop {
            if self.input_complete {
                if self.buffer.is_empty() {
                    return Poll::Ready(None);
                } else {
                    return Poll::Ready(Some(self.sort_buffer()));
                }
            }
            let res = self.input.as_mut().poll_next(cx);
            match res {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() == 0 {
                        // mark end of current PartitionRange
                        let sorted_batch = self.sort_buffer()?;
                        self.cur_part_idx += 1;
                        if sorted_batch.num_rows() == 0 {
                            // Current part is empty, continue polling next part.
                            continue;
                        }
                        return Poll::Ready(Some(Ok(sorted_batch)));
                    }
                    self.buffer.push(batch);
                    // keep polling until boundary(a empty RecordBatch) is reached
                    continue;
                }
                // input stream end, sort the buffer and return
                Poll::Ready(None) => {
                    self.input_complete = true;
                    continue;
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Stream for PartSortStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        let result = self.as_mut().poll_next_inner(cx);
        self.metrics.record_poll(result)
    }
}

impl RecordBatchStream for PartSortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::json::ArrayWriter;
    use arrow_schema::{DataType, Field, Schema, SortOptions, TimeUnit};
    use common_time::Timestamp;
    use datafusion_physical_expr::expressions::Column;
    use futures::StreamExt;
    use store_api::region_engine::PartitionRange;

    use super::*;
    use crate::test_util::{new_ts_array, MockInputExec};

    #[tokio::test]
    async fn fuzzy_test() {
        let test_cnt = 100;
        let part_cnt_bound = 100;
        let range_size_bound = 100;
        let range_offset_bound = 100;
        let batch_cnt_bound = 20;
        let batch_size_bound = 100;

        let mut rng = fastrand::Rng::new();
        rng.seed(1337);

        let mut test_cases = Vec::new();

        for case_id in 0..test_cnt {
            let mut bound_val: Option<i64> = None;
            let descending = rng.bool();
            let nulls_first = rng.bool();
            let opt = SortOptions {
                descending,
                nulls_first,
            };
            let unit = match rng.u8(0..3) {
                0 => TimeUnit::Second,
                1 => TimeUnit::Millisecond,
                2 => TimeUnit::Microsecond,
                _ => TimeUnit::Nanosecond,
            };

            let schema = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(unit.clone(), None),
                false,
            )]);
            let schema = Arc::new(schema);

            let mut input_ranged_data = vec![];
            let mut output_data = vec![];
            // generate each input `PartitionRange`
            for part_id in 0..rng.usize(0..part_cnt_bound) {
                // generate each `PartitionRange`'s timestamp range
                let (start, end) = if descending {
                    let end = bound_val
                        .map(|i| i.checked_sub(rng.i64(0..range_offset_bound)).expect("Bad luck, fuzzy test generate data that will overflow, change seed and try again"))
                        .unwrap_or_else(|| rng.i64(..));
                    bound_val = Some(end);
                    let start = end - rng.i64(1..range_size_bound);
                    let start = Timestamp::new(start, unit.clone().into());
                    let end = Timestamp::new(end, unit.clone().into());
                    (start, end)
                } else {
                    let start = bound_val
                        .map(|i| i + rng.i64(0..range_offset_bound))
                        .unwrap_or_else(|| rng.i64(..));
                    bound_val = Some(start);
                    let end = start + rng.i64(1..range_size_bound);
                    let start = Timestamp::new(start, unit.clone().into());
                    let end = Timestamp::new(end, unit.clone().into());
                    (start, end)
                };
                assert!(start < end);

                let mut per_part_sort_data = vec![];
                let mut batches = vec![];
                for _batch_idx in 0..rng.usize(1..batch_cnt_bound) {
                    let cnt = rng.usize(0..batch_size_bound) + 1;
                    let iter = 0..rng.usize(0..cnt);
                    let data_gen = iter
                        .map(|_| rng.i64(start.value()..end.value()))
                        .collect_vec();
                    if data_gen.is_empty() {
                        // current batch is empty, skip
                        continue;
                    }
                    per_part_sort_data.extend(data_gen.clone());
                    let arr = new_ts_array(unit.clone(), data_gen.clone());
                    let batch = DfRecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
                    batches.push(batch);
                }

                let range = PartitionRange {
                    start,
                    end,
                    num_rows: batches.iter().map(|b| b.num_rows()).sum(),
                    identifier: part_id,
                };
                input_ranged_data.push((range, batches));

                if descending {
                    per_part_sort_data.sort_by(|a, b| b.cmp(a));
                } else {
                    per_part_sort_data.sort();
                }
                if per_part_sort_data.is_empty() {
                    continue;
                }
                output_data.push(per_part_sort_data);
            }

            let expected_output = output_data
                .into_iter()
                .map(|a| {
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit.clone(), a)])
                        .unwrap()
                })
                .collect_vec();
            test_cases.push((
                case_id,
                unit,
                input_ranged_data,
                schema,
                opt,
                expected_output,
            ));
        }

        for (case_id, _unit, input_ranged_data, schema, opt, expected_output) in test_cases {
            run_test(case_id, input_ranged_data, schema, opt, expected_output).await;
        }
    }

    #[tokio::test]
    async fn simple_case() {
        let testcases = vec![
            (
                TimeUnit::Millisecond,
                vec![
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]),
                    ((5, 10), vec![vec![5, 6], vec![7, 8]]),
                ],
                false,
                vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9], vec![5, 6, 7, 8]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((5, 10), vec![vec![5, 6], vec![7, 8, 9]]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                true,
                vec![vec![9, 8, 7, 6, 5], vec![8, 7, 6, 5, 4, 3, 2, 1]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((5, 10), vec![]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                true,
                vec![vec![8, 7, 6, 5, 4, 3, 2, 1]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((15, 20), vec![vec![17, 18, 19]]),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                true,
                vec![vec![19, 18, 17], vec![8, 7, 6, 5, 4, 3, 2, 1]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((15, 20), vec![]),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![]),
                ],
                true,
                vec![],
            ),
        ];

        for (identifier, (unit, input_ranged_data, descending, expected_output)) in
            testcases.into_iter().enumerate()
        {
            let schema = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(unit.clone(), None),
                false,
            )]);
            let schema = Arc::new(schema);
            let opt = SortOptions {
                descending,
                ..Default::default()
            };
            let input_ranged_data = input_ranged_data
                .into_iter()
                .map(|(range, data)| {
                    let part = PartitionRange {
                        start: Timestamp::new(range.0, unit.clone().into()),
                        end: Timestamp::new(range.1, unit.clone().into()),
                        num_rows: data.iter().map(|b| b.len()).sum(),
                        identifier,
                    };

                    let batches = data
                        .into_iter()
                        .map(|b| {
                            let arr = new_ts_array(unit.clone(), b);
                            DfRecordBatch::try_new(schema.clone(), vec![arr]).unwrap()
                        })
                        .collect_vec();
                    (part, batches)
                })
                .collect_vec();

            let expected_output = expected_output
                .into_iter()
                .map(|a| {
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit.clone(), a)])
                        .unwrap()
                })
                .collect_vec();

            run_test(0, input_ranged_data, schema.clone(), opt, expected_output).await;
        }
    }

    async fn run_test(
        case_id: usize,
        input_ranged_data: Vec<(PartitionRange, Vec<DfRecordBatch>)>,
        schema: SchemaRef,
        opt: SortOptions,
        expected_output: Vec<DfRecordBatch>,
    ) {
        let (ranges, batches): (Vec<_>, Vec<_>) = input_ranged_data.clone().into_iter().unzip();

        let batches = batches
            .into_iter()
            .flat_map(|mut cols| {
                cols.push(DfRecordBatch::new_empty(schema.clone()));
                cols
            })
            .collect_vec();
        let mock_input = MockInputExec::new(batches, schema.clone());

        let exec = PartSortExec::new(
            PhysicalSortExpr {
                expr: Arc::new(Column::new("ts", 0)),
                options: opt,
            },
            vec![ranges],
            Arc::new(mock_input),
        );

        let exec_stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

        let real_output = exec_stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

        // a makeshift solution for compare large data
        if real_output != expected_output {
            let mut full_msg = String::new();
            {
                let mut buf = Vec::with_capacity(10 * real_output.len());
                for batch in &real_output {
                    let mut rb_json: Vec<u8> = Vec::new();
                    let mut writer = ArrayWriter::new(&mut rb_json);
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                    buf.append(&mut rb_json);
                    buf.push(b',');
                }
                // TODO(discord9): better ways to print buf
                let _buf = String::from_utf8_lossy(&buf);
                full_msg += &format!("case_id:{case_id}, real_output");
            }
            {
                let mut buf = Vec::with_capacity(10 * real_output.len());
                for batch in &expected_output {
                    let mut rb_json: Vec<u8> = Vec::new();
                    let mut writer = ArrayWriter::new(&mut rb_json);
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                    buf.append(&mut rb_json);
                    buf.push(b',');
                }
                let _buf = String::from_utf8_lossy(&buf);
                full_msg += &format!("case_id:{case_id}, expected_output");
            }
            panic!(
                "case_{} failed, opt: {:?}, full msg: {}",
                case_id, opt, full_msg
            );
        }
    }
}
