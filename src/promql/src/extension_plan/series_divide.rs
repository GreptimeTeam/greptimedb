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
use datafusion::physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion::physical_plan::expressions::Column as ColumnExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
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

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert!(!inputs.is_empty());

        Self {
            tag_columns: self.tag_columns.clone(),
            input: inputs[0].clone(),
        }
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

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
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

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
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
            buffer: None,
            schema,
            input,
            metric: baseline_metric,
            num_series: 0,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            // TODO(ruihang): support this column statistics
            column_statistics: None,
            is_exact: false,
        }
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
    buffer: Option<RecordBatch>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    num_series: usize,
}

impl RecordBatchStream for SeriesDivideStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesDivideStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.buffer.clone() {
                let same_length = self.find_first_diff_row(&batch) + 1;
                if same_length >= batch.num_rows() {
                    let next_batch = match ready!(self.as_mut().fetch_next_batch(cx)) {
                        Some(Ok(batch)) => batch,
                        None => {
                            self.buffer = None;
                            self.num_series += 1;
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        error => return Poll::Ready(error),
                    };
                    let new_batch =
                        compute::concat_batches(&batch.schema(), &[batch.clone(), next_batch])?;
                    self.buffer = Some(new_batch);
                    continue;
                } else {
                    let result_batch = batch.slice(0, same_length);
                    let remaining_batch = batch.slice(same_length, batch.num_rows() - same_length);
                    self.buffer = Some(remaining_batch);
                    self.num_series += 1;
                    return Poll::Ready(Some(Ok(result_batch)));
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
                self.buffer = Some(batch);
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
            Poll::Ready(batch) => {
                let _timer = self.metric.elapsed_compute().timer();
                Poll::Ready(batch)
            }
            Poll::Pending => Poll::Pending,
        };
        self.metric.record_poll(poll)
    }

    fn find_first_diff_row(&self, batch: &RecordBatch) -> usize {
        // fast path: no tag columns means all data belongs to the same series.
        if self.tag_indices.is_empty() {
            return batch.num_rows();
        }

        let num_rows = batch.num_rows();
        let mut result = num_rows;

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
            result = result.min(same_until);
        }

        result
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
