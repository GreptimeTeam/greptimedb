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

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::PhysicalSortRequirement;
use datafusion::physical_plan::expressions::Column as ColumnExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::arrow::compute;
use futures::{ready, Stream, StreamExt};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::metrics::PROMQL_SERIES_COUNT;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SeriesDivide {
    tag_columns: Vec<String>,
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesDivide {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PromSeriesDivide: tags={:?}", self.tag_columns)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "SeriesDivide must have at least one input".to_string(),
            ));
        }

        Ok(Self {
            tag_columns: self.tag_columns.clone(),
            input: inputs[0].clone(),
        })
    }
}

impl SeriesDivide {
    pub fn new(tag_columns: Vec<String>, input: LogicalPlan) -> Self {
        Self { tag_columns, input }
    }

    pub const fn name() -> &'static str {
        "SeriesDivide"
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SeriesDivideExec {
            tag_columns: self.tag_columns.clone(),
            input: exec_input,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        pb::SeriesDivide {
            tag_columns: self.tag_columns.clone(),
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_series_divide = pb::SeriesDivide::decode(bytes).context(DeserializeSnafu)?;
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        Ok(Self {
            tag_columns: pb_series_divide.tag_columns,
            input: placeholder_plan,
        })
    }
}

#[derive(Debug)]
pub struct SeriesDivideExec {
    tag_columns: Vec<String>,
    input: Arc<dyn ExecutionPlan>,
    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for SeriesDivideExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        let input_schema = self.input.schema();
        let exprs: Vec<PhysicalSortRequirement> = self
            .tag_columns
            .iter()
            .map(|tag| PhysicalSortRequirement {
                // Safety: the tag column names is verified in the planning phase
                expr: Arc::new(ColumnExpr::new_with_schema(tag, &input_schema).unwrap()),
                options: None,
            })
            .collect();
        if !exprs.is_empty() {
            vec![Some(exprs)]
        } else {
            vec![None]
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        Ok(Arc::new(Self {
            tag_columns: self.tag_columns.clone(),
            input: children[0].clone(),
            metric: self.metric.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);

        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let tag_indices = self
            .tag_columns
            .iter()
            .map(|tag| {
                schema
                    .column_with_name(tag)
                    .unwrap_or_else(|| panic!("tag column not found {tag}"))
                    .0
            })
            .collect();
        Ok(Box::pin(SeriesDivideStream {
            tag_indices,
            buffer: vec![],
            schema,
            input,
            metric: baseline_metric,
            num_series: 0,
            inspect_start: 0,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

impl DisplayAs for SeriesDivideExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PromSeriesDivideExec: tags={:?}", self.tag_columns)
            }
        }
    }
}

/// Assume the input stream is ordered on the tag columns.
pub struct SeriesDivideStream {
    tag_indices: Vec<usize>,
    buffer: Vec<RecordBatch>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    num_series: usize,
    /// Index of buffered batches to start inspect next time.
    inspect_start: usize,
}

impl RecordBatchStream for SeriesDivideStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesDivideStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let timer = std::time::Instant::now();
        loop {
            // if let Some(batch) = self.buffer.as_ref() {
            if !self.buffer.is_empty() {
                // let same_length = self.find_first_diff_row(batch) + 1;
                let cut_at = self.find_first_diff_row();
                // println!("cut at: {:?}", cut_at);
                // if same_length >= batch.num_rows() {
                if let Some((batch_index, row_index)) = cut_at {
                    // slice out the first time series and return it.
                    let half_batch_of_first_series =
                        self.buffer[batch_index].slice(0, row_index + 1);
                    let half_batch_of_second_series = self.buffer[batch_index].slice(
                        row_index + 1,
                        self.buffer[batch_index].num_rows() - row_index - 1,
                    );
                    let result_batches = self
                        .buffer
                        .drain(0..batch_index)
                        .chain([half_batch_of_first_series])
                        .collect::<Vec<_>>();
                    // self.buffer.insert(0, half_batch_of_second_series);
                    self.buffer[0] = half_batch_of_second_series;
                    let result_batch = compute::concat_batches(&self.schema, &result_batches)?;

                    // let result_batch = batch.slice(0, same_length);
                    // let remaining_batch = batch.slice(same_length, batch.num_rows() - same_length);
                    // self.buffer = Some(remaining_batch);
                    self.inspect_start = 0;
                    self.num_series += 1;
                    self.metric.elapsed_compute().add_elapsed(timer);
                    return Poll::Ready(Some(Ok(result_batch)));
                } else {
                    // continue to fetch next batch as the current buffer only contains one time series.
                    let next_batch = ready!(self.as_mut().fetch_next_batch(cx)).transpose()?;
                    // SAFETY: if-let guards the buffer is not None;
                    //   and we cannot change the buffer at this point.
                    // let batch = self.buffer.take().expect("this batch must exist");
                    if let Some(next_batch) = next_batch {
                        // self.buffer = Some(compute::concat_batches(
                        //     &batch.schema(),
                        //     &[batch, next_batch],
                        // )?);
                        self.buffer.push(next_batch);
                        // println!("buffer: {:?}", self.buffer.len());
                        continue;
                    } else {
                        // input stream is ended
                        let result = compute::concat_batches(&self.schema, &self.buffer)?;
                        self.buffer.clear();
                        self.inspect_start = 0;
                        self.num_series += 1;
                        self.metric.elapsed_compute().add_elapsed(timer);
                        return Poll::Ready(Some(Ok(result)));
                    }
                }
            } else {
                let batch = match ready!(self.as_mut().fetch_next_batch(cx)) {
                    Some(Ok(batch)) => batch,
                    None => {
                        PROMQL_SERIES_COUNT.observe(self.num_series as f64);
                        return Poll::Ready(None);
                    }
                    error => return Poll::Ready(error),
                };
                self.buffer.push(batch);
                continue;
            }
        }
    }
}

impl SeriesDivideStream {
    fn fetch_next_batch(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DataFusionResult<RecordBatch>>> {
        let poll = match self.input.poll_next_unpin(cx) {
            Poll::Ready(batch) => Poll::Ready(batch),
            Poll::Pending => Poll::Pending,
        };
        self.metric.record_poll(poll)
    }

    /// Return the position to cut buffer.
    /// None for don't cut, which implies the current buffer only contains one
    /// time series.
    fn find_first_diff_row(&mut self) -> Option<(usize, usize)> {
        // fast path: no tag columns means all data belongs to the same series.
        if self.tag_indices.is_empty() {
            // return batch.num_rows();
            return None;
        }

        let mut resumed_batch_index = self.inspect_start;

        for batch in &self.buffer[resumed_batch_index..] {
            let num_rows = batch.num_rows();
            let mut result_index = num_rows;

            // check if the first row is the same with last batch's last row
            if resumed_batch_index > self.inspect_start {
                let last_batch = &self.buffer[resumed_batch_index - 1];
                let last_row = last_batch.num_rows() - 1;
                for index in &self.tag_indices {
                    let current_array = batch.column(*index);
                    let last_array = last_batch.column(*index);
                    let current_value = current_array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0);
                    let last_value = last_array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(last_row);
                    if current_value != last_value {
                        return Some((resumed_batch_index, 0));
                    }
                }
            }

            // check column by column
            for index in &self.tag_indices {
                let array = batch.column(*index);
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                // the first row number that not equal to the next row.
                let mut same_until = 0;
                while same_until < num_rows - 1 {
                    if string_array.value(same_until) != string_array.value(same_until + 1) {
                        break;
                    }
                    same_until += 1;
                }
                result_index = result_index.min(same_until);
            }

            // println!(
            //     "same until: {:?}, number of rows: {:?}",
            //     result_index, num_rows
            // );

            if result_index + 1 >= num_rows {
                // all rows are the same, inspect next batch
                resumed_batch_index += 1;
            } else {
                return Some((resumed_batch_index, result_index));
            }
        }

        self.inspect_start = resumed_batch_index;
        None
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn prepare_test_data() -> MemoryExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
        ]));

        let path_column_1 = Arc::new(StringArray::from(vec![
            "foo", "foo", "foo", "bar", "bar", "bar", "bar", "bar", "bar", "bla", "bla", "bla",
        ])) as _;
        let host_column_1 = Arc::new(StringArray::from(vec![
            "000", "000", "001", "002", "002", "002", "002", "002", "003", "005", "005", "005",
        ])) as _;

        let path_column_2 = Arc::new(StringArray::from(vec!["bla", "bla", "bla"])) as _;
        let host_column_2 = Arc::new(StringArray::from(vec!["005", "005", "005"])) as _;

        let path_column_3 = Arc::new(StringArray::from(vec![
            "bla", "🥺", "🥺", "🥺", "🥺", "🥺", "🫠", "🫠",
        ])) as _;
        let host_column_3 = Arc::new(StringArray::from(vec![
            "005", "001", "001", "001", "001", "001", "001", "001",
        ])) as _;

        let data_1 =
            RecordBatch::try_new(schema.clone(), vec![path_column_1, host_column_1]).unwrap();
        let data_2 =
            RecordBatch::try_new(schema.clone(), vec![path_column_2, host_column_2]).unwrap();
        let data_3 =
            RecordBatch::try_new(schema.clone(), vec![path_column_3, host_column_3]).unwrap();

        MemoryExec::try_new(&[vec![data_1, data_2, data_3]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn overall_data() {
        let memory_exec = Arc::new(prepare_test_data());
        let divide_exec = Arc::new(SeriesDivideExec {
            tag_columns: vec!["host".to_string(), "path".to_string()],
            input: memory_exec,
            metric: ExecutionPlanMetricsSet::new(),
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(divide_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        let expected = String::from(
            "+------+------+\
            \n| host | path |\
            \n+------+------+\
            \n| foo  | 000  |\
            \n| foo  | 000  |\
            \n| foo  | 001  |\
            \n| bar  | 002  |\
            \n| bar  | 002  |\
            \n| bar  | 002  |\
            \n| bar  | 002  |\
            \n| bar  | 002  |\
            \n| bar  | 003  |\
            \n| bla  | 005  |\
            \n| bla  | 005  |\
            \n| bla  | 005  |\
            \n| bla  | 005  |\
            \n| bla  | 005  |\
            \n| bla  | 005  |\
            \n| bla  | 005  |\
            \n| 🥺   | 001  |\
            \n| 🥺   | 001  |\
            \n| 🥺   | 001  |\
            \n| 🥺   | 001  |\
            \n| 🥺   | 001  |\
            \n| 🫠   | 001  |\
            \n| 🫠   | 001  |\
            \n+------+------+",
        );
        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn per_batch_data() {
        let memory_exec = Arc::new(prepare_test_data());
        let divide_exec = Arc::new(SeriesDivideExec {
            tag_columns: vec!["host".to_string(), "path".to_string()],
            input: memory_exec,
            metric: ExecutionPlanMetricsSet::new(),
        });
        let mut divide_stream = divide_exec
            .execute(0, SessionContext::default().task_ctx())
            .unwrap();

        let mut expectations = vec![
            String::from(
                "+------+------+\
                \n| host | path |\
                \n+------+------+\
                \n| foo  | 000  |\
                \n| foo  | 000  |\
                \n+------+------+",
            ),
            String::from(
                "+------+------+\
                \n| host | path |\
                \n+------+------+\
                \n| foo  | 001  |\
                \n+------+------+",
            ),
            String::from(
                "+------+------+\
                \n| host | path |\
                \n+------+------+\
                \n| bar  | 002  |\
                \n| bar  | 002  |\
                \n| bar  | 002  |\
                \n| bar  | 002  |\
                \n| bar  | 002  |\
                \n+------+------+",
            ),
            String::from(
                "+------+------+\
                \n| host | path |\
                \n+------+------+\
                \n| bar  | 003  |\
                \n+------+------+",
            ),
            String::from(
                "+------+------+\
                \n| host | path |\
                \n+------+------+\
                \n| bla  | 005  |\
                \n| bla  | 005  |\
                \n| bla  | 005  |\
                \n| bla  | 005  |\
                \n| bla  | 005  |\
                \n| bla  | 005  |\
                \n| bla  | 005  |\
                \n+------+------+",
            ),
            String::from(
                "+------+------+\
                \n| host | path |\
                \n+------+------+\
                \n| 🥺   | 001  |\
                \n| 🥺   | 001  |\
                \n| 🥺   | 001  |\
                \n| 🥺   | 001  |\
                \n| 🥺   | 001  |\
                \n+------+------+",
            ),
            String::from(
                "+------+------+\
                \n| host | path |\
                \n+------+------+\
                \n| 🫠   | 001  |\
                \n| 🫠   | 001  |\
                \n+------+------+",
            ),
        ];
        expectations.reverse();

        while let Some(batch) = divide_stream.next().await {
            let formatted =
                datatypes::arrow::util::pretty::pretty_format_batches(&[batch.unwrap()])
                    .unwrap()
                    .to_string();
            let expected = expectations.pop().unwrap();
            assert_eq!(formatted, expected);
        }
    }
}
