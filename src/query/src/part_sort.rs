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

use arrow::array::Array;
use arrow::compute::{concat, take_record_batch};
use arrow_schema::SchemaRef;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion::common::arrow::compute::sort_to_indices;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_err, DataFusionError};
use datafusion_physical_expr::PhysicalSortExpr;
use futures::Stream;
use itertools::Itertools;
use snafu::location;
use store_api::region_engine::PartitionRange;

use crate::error::Result;

/// Sort input within given PartitionRange
///
/// Input is assumed to be segmented by empty RecordBatch, which indicates a new `PartitionRange` is starting
///
/// and this operator will sort each partition independently within the partition.
#[derive(Debug, Clone)]
pub struct PartSortExec {
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
    /// The input ranges indicate input stream will be composed of those ranges in given order
    ranges: Vec<PartitionRange>,
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl PartSortExec {
    pub fn try_new(
        expression: PhysicalSortExpr,
        fetch: Option<usize>,
        ranges: Vec<PartitionRange>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let metrics = ExecutionPlanMetricsSet::new();
        let properties = PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.execution_mode(),
        );

        Ok(Self {
            expression,
            fetch,
            ranges,
            input,
            metrics,
            properties,
        })
    }

    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let input_stream: DfSendableRecordBatchStream =
            self.input.execute(partition, context.clone())?;

        let df_stream = Box::pin(PartSortStream::new(context, self, input_stream)) as _;

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
        Ok(Arc::new(Self::try_new(
            self.expression.clone(),
            self.fetch,
            self.ranges.clone(),
            new_input.clone(),
        )?))
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
}

struct PartSortStream {
    /// Memory pool for this stream
    reservation: MemoryReservation,
    buffer: Vec<DfRecordBatch>,
    expression: PhysicalSortExpr,
    fetch: Option<usize>,
    produced: usize,
    input: DfSendableRecordBatchStream,
    input_complete: bool,
    schema: SchemaRef,
    /// The current PartitionRange's index
    range_idx: usize,
}

impl PartSortStream {
    fn new(
        context: Arc<TaskContext>,
        sort: &PartSortExec,
        input: DfSendableRecordBatchStream,
    ) -> Self {
        Self {
            reservation: MemoryConsumer::new("PartSortStream".to_string())
                .register(&context.runtime_env().memory_pool),
            buffer: Vec::new(),
            expression: sort.expression.clone(),
            fetch: sort.fetch,
            produced: 0,
            input,
            input_complete: false,
            schema: sort.input.schema(),
            range_idx: 0,
        }
    }
}

impl PartSortStream {
    fn remaining_fetch(&self) -> Option<usize> {
        self.fetch.map(|fetch| fetch - self.produced)
    }

    /// Sort and clear the buffer and return the sorted record batch
    ///
    /// this function should return None if RecordBatch is empty
    fn sort_buffer(&mut self) -> datafusion_common::Result<Option<DfRecordBatch>> {
        if self.buffer.is_empty() || self.buffer.iter().map(|r| r.num_rows()).sum::<usize>() == 0 {
            return Ok(None);
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

        let limit = self.remaining_fetch().and_then(|f| {
            let len = sort_column.len();
            if f < len {
                Some(f)
            } else {
                None
            }
        });

        if let Some(0) = limit {
            return Ok(None);
        }

        let indices = sort_to_indices(&sort_column, opt, limit).map_err(|e| {
            DataFusionError::ArrowError(
                e,
                Some(format!("Fail to sort to indices at {}", location!())),
            )
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
        self.reservation.shrink(total_mem);
        Ok(Some(sorted))
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
                    return Poll::Ready(self.sort_buffer().transpose());
                }
            }
            let res = self.input.as_mut().poll_next(cx);
            match res {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() == 0 {
                        // mark end of current PartitionRange
                        self.range_idx += 1;
                        return Poll::Ready(self.sort_buffer().transpose());
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
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        self.poll_next_inner(cx)
    }
}

impl RecordBatchStream for PartSortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::sync::Arc;

    use arrow::json::ArrayWriter;
    use arrow_schema::{DataType, Field, Schema, SortOptions, TimeUnit};
    use common_time::Timestamp;
    use datafusion_physical_expr::expressions::Column;
    use futures::StreamExt;

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
        let fetch_bound = 100;

        let mut rng = fastrand::Rng::new();
        rng.seed(1337);

        for case_id in 0..test_cnt {
            let mut bound_val = None;
            let mut produced = 0;
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
            let fetch = if rng.bool() {
                Some(rng.usize(0..fetch_bound))
            } else {
                None
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
                        .map(|i| i - rng.i64(0..range_offset_bound))
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

                let mut sort_data = vec![];
                let mut batches = vec![];
                for _batch_idx in 0..rng.usize(1..batch_cnt_bound) {
                    let cnt = rng.usize(0..batch_size_bound) + 2;
                    let iter = 0..rng.usize(1..cnt);
                    let data_gen = iter
                        .map(|_| rng.i64(start.value()..end.value()))
                        .collect_vec();
                    sort_data.extend(data_gen.clone());
                    let arr = new_ts_array(unit.clone(), data_gen.clone());

                    let batch = DfRecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
                    batches.push(batch);
                }
                assert!(batches.iter().all(|i| i.num_rows() >= 1));

                let range = PartitionRange {
                    start,
                    end,
                    num_rows: batches.iter().map(|b| b.num_rows()).sum(),
                    identifier: part_id,
                };
                input_ranged_data.push((range, batches));

                if descending {
                    sort_data.sort_by(|a, b| b.cmp(a));
                } else {
                    sort_data.sort();
                }
                if let Some(remain) = fetch.map(|f| f.saturating_sub(produced)) {
                    if remain != 0 {
                        if remain < sort_data.len() {
                            produced += remain;
                            output_data.push(sort_data[0..remain].to_vec());
                        } else {
                            produced += sort_data.len();
                            output_data.push(sort_data);
                        }
                    }
                } else {
                    produced += sort_data.len();
                    output_data.push(sort_data);
                }
            }
            let (ranges, batches): (Vec<_>, Vec<_>) = input_ranged_data.clone().into_iter().unzip();

            let batches = batches
                .into_iter()
                .flat_map(|mut cols| {
                    cols.push(DfRecordBatch::new_empty(schema.clone()));
                    cols
                })
                .collect_vec();

            let expected_output = output_data
                .into_iter()
                .map(|a| {
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit.clone(), a)])
                        .unwrap()
                })
                .collect_vec();

            assert!(!expected_output.is_empty());
            run_test(
                case_id,
                batches,
                schema,
                opt,
                fetch,
                ranges,
                expected_output,
            )
            .await;
        }
    }

    async fn run_test(
        case_id: usize,
        batches: Vec<DfRecordBatch>,
        schema: SchemaRef,
        opt: SortOptions,
        fetch: Option<usize>,
        ranges: Vec<PartitionRange>,
        expected_output: Vec<DfRecordBatch>,
    ) {
        let mock_input = MockInputExec::new(batches, schema.clone());

        let exec = PartSortExec::try_new(
            PhysicalSortExpr {
                expr: Arc::new(Column::new("ts", 0)),
                options: opt,
            },
            fetch,
            ranges,
            Arc::new(mock_input),
        )
        .unwrap();

        let exec_stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

        let real_output = exec_stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;

        // a makeshift solution for compare large data
        if real_output != expected_output {
            {
                let mut f_res =
                    std::fs::File::create(format!("case_{}_real_output.json", case_id)).unwrap();
                let mut buf = String::new();
                for batch in &real_output {
                    let mut rb_json: Vec<u8> = Vec::new();
                    let mut writer = ArrayWriter::new(&mut rb_json);
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                    buf += &String::from_utf8_lossy(&rb_json);
                    buf += ",";
                }
                f_res.write_all(format!("[{buf}]").as_bytes()).unwrap();
            }
            {
                let mut f_res =
                    std::fs::File::create(format!("case_{}_expected_output.json", case_id))
                        .unwrap();
                let mut buf = String::new();
                for batch in &expected_output {
                    let mut rb_json: Vec<u8> = Vec::new();
                    let mut writer = ArrayWriter::new(&mut rb_json);
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                    buf += &String::from_utf8_lossy(&rb_json);
                    buf += ",";
                }
                f_res.write_all(format!("[{buf}]").as_bytes()).unwrap();
            }
            panic!(
                "case_{} failed, opt: {:?}, fetch: {:?}",
                case_id, opt, fetch
            );
        }
    }
}
