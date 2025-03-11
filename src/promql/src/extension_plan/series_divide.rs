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
use datafusion::physical_expr::{LexRequirement, PhysicalSortRequirement};
use datafusion::physical_plan::expressions::Column as ColumnExpr;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::arrow::compute;
use datatypes::compute::SortOptions;
use futures::{ready, Stream, StreamExt};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::METRIC_NUM_SERIES;
use crate::metrics::PROMQL_SERIES_COUNT;

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
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
        let schema = self.input.schema();
        vec![Distribution::HashPartitioned(
            self.tag_columns
                .iter()
                // Safety: the tag column names is verified in the planning phase
                .map(|tag| Arc::new(ColumnExpr::new_with_schema(tag, &schema).unwrap()) as _)
                .collect(),
        )]
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        let input_schema = self.input.schema();
        if self.tag_columns.is_empty() {
            return vec![None];
        }

        let exprs: Vec<PhysicalSortRequirement> = self
            .tag_columns
            .iter()
            .filter_map(|tag| {
                ColumnExpr::new_with_schema(tag, &input_schema)
                    .ok()
                    .map(|expr| PhysicalSortRequirement {
                        expr: Arc::new(expr),
                        options: Some(SortOptions {
                            descending: false,
                            nulls_first: true,
                        }),
                    })
            })
            .collect();

        if exprs.is_empty() {
            vec![None]
        } else {
            vec![Some(LexRequirement::new(exprs))]
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
        let metrics_builder = MetricBuilder::new(&self.metric);
        let num_series = Count::new();
        metrics_builder
            .with_partition(partition)
            .build(MetricValue::Count {
                name: METRIC_NUM_SERIES.into(),
                count: num_series.clone(),
            });

        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let tag_indices = self
            .tag_columns
            .iter()
            .filter_map(|tag| schema.column_with_name(tag).map(|(idx, _)| idx))
            .collect();

        Ok(Box::pin(SeriesDivideStream::new(
            tag_indices,
            schema,
            input,
            baseline_metric,
            num_series,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "SeriesDivideExec"
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

/// A helper struct to cache array accesses and avoid repeated type casting
struct TagArrayCache<'a> {
    string_arrays: Vec<&'a StringArray>,
}

impl<'a> TagArrayCache<'a> {
    fn new(batch: &'a RecordBatch, tag_indices: &[usize]) -> DataFusionResult<Self> {
        let string_arrays = tag_indices
            .iter()
            .map(|&idx| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "Failed to downcast tag column at index {} to StringArray",
                            idx
                        ))
                    })
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(Self { string_arrays })
    }

    fn are_tag_values_equal(&self, row1: usize, row2: usize) -> bool {
        self.string_arrays
            .iter()
            .all(|array| array.value(row1) == array.value(row2))
    }

    fn are_tag_values_equal_between_batches(
        &self,
        row1: usize,
        other_cache: &TagArrayCache,
        row2: usize,
    ) -> bool {
        self.string_arrays
            .iter()
            .zip(other_cache.string_arrays.iter())
            .all(|(array1, array2)| array1.value(row1) == array2.value(row2))
    }
}

/// Assume the input stream is ordered on the tag columns.
pub struct SeriesDivideStream {
    tag_indices: Vec<usize>,
    buffer: Vec<RecordBatch>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    /// Index of buffered batches to start inspect next time.
    inspect_start: usize,
    /// Number of series processed.
    num_series: Count,
}

impl SeriesDivideStream {
    fn new(
        tag_indices: Vec<usize>,
        schema: SchemaRef,
        input: SendableRecordBatchStream,
        metric: BaselineMetrics,
        num_series: Count,
    ) -> Self {
        Self {
            tag_indices,
            buffer: Vec::new(),
            schema,
            input,
            metric,
            inspect_start: 0,
            num_series,
        }
    }
}

impl RecordBatchStream for SeriesDivideStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesDivideStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Fast path: if no tag columns to divide by, just pass through the input stream
        if self.tag_indices.is_empty() {
            let poll = self.input.poll_next_unpin(cx);
            if let Poll::Ready(Some(Ok(_batch))) = &poll {
                self.num_series.add(1);
            } else if let Poll::Ready(None) = &poll {
                PROMQL_SERIES_COUNT.observe(self.num_series.value() as f64);
            }
            return self.metric.record_poll(poll);
        }

        loop {
            if !self.buffer.is_empty() {
                let timer = std::time::Instant::now();
                let cut_at = match self.find_first_diff_row() {
                    Ok(cut_at) => cut_at,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };

                if let Some((batch_index, row_index)) = cut_at {
                    // Extract a complete series from the buffer
                    let mut result_batches = Vec::with_capacity(batch_index + 1);

                    // Take complete batches that belong to this series
                    if batch_index > 0 {
                        result_batches.extend(self.buffer.drain(0..batch_index));
                    }

                    // Handle the boundary batch
                    let boundary_batch = &self.buffer[0];
                    let first_part = boundary_batch.slice(0, row_index + 1);
                    let second_part = boundary_batch
                        .slice(row_index + 1, boundary_batch.num_rows() - row_index - 1);

                    // Add first part to result
                    result_batches.push(first_part);

                    // Replace the first buffer batch with second part
                    self.buffer[0] = second_part;

                    // Concatenate and return the complete series
                    let result_batch = compute::concat_batches(&self.schema, &result_batches)?;
                    self.inspect_start = 0;
                    self.num_series.add(1);
                    self.metric.elapsed_compute().add_elapsed(timer);

                    return Poll::Ready(Some(Ok(result_batch)));
                } else {
                    // Current buffer contains a single series, fetch more data
                    match ready!(self.poll_fetch_next_batch(cx)) {
                        Some(Ok(batch)) => {
                            self.buffer.push(batch);
                            continue;
                        }
                        None => {
                            // Input stream is exhausted, return whatever is in the buffer
                            if !self.buffer.is_empty() {
                                let result = compute::concat_batches(&self.schema, &self.buffer)?;
                                self.buffer.clear();
                                self.num_series.add(1);
                                self.metric.elapsed_compute().add_elapsed(timer);
                                return Poll::Ready(Some(Ok(result)));
                            }
                            PROMQL_SERIES_COUNT.observe(self.num_series.value() as f64);
                            return Poll::Ready(None);
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    }
                }
            } else {
                // Buffer is empty, fetch more data
                match ready!(self.poll_fetch_next_batch(cx)) {
                    Some(Ok(batch)) => {
                        self.buffer.push(batch);
                        continue;
                    }
                    None => {
                        PROMQL_SERIES_COUNT.observe(self.num_series.value() as f64);
                        return Poll::Ready(None);
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                }
            }
        }
    }
}

impl SeriesDivideStream {
    fn poll_fetch_next_batch(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DataFusionResult<RecordBatch>>> {
        self.metric.record_poll(self.input.poll_next_unpin(cx))
    }

    /// Return the position to cut buffer.
    /// None implies the current buffer only contains one time series.
    fn find_first_diff_row(&mut self) -> DataFusionResult<Option<(usize, usize)>> {
        // Fast path: no tag columns means all data belongs to the same series.
        if self.tag_indices.is_empty() {
            return Ok(None);
        }

        let start_batch_idx = self.inspect_start;

        // Create array caches for batches to avoid repeated downcasting
        let batch_caches: Vec<TagArrayCache> = self
            .buffer
            .iter()
            .map(|batch| TagArrayCache::new(batch, &self.tag_indices))
            .collect::<DataFusionResult<_>>()?;

        // First check for transition between batches
        for batch_idx in start_batch_idx + 1..self.buffer.len() {
            let prev_batch = &batch_caches[batch_idx - 1];
            let curr_batch = &batch_caches[batch_idx];
            let prev_batch_last_row = self.buffer[batch_idx - 1].num_rows() - 1;

            // Check if first row of current batch differs from last row of previous batch
            if !curr_batch.are_tag_values_equal_between_batches(0, prev_batch, prev_batch_last_row)
            {
                return Ok(Some((batch_idx, 0)));
            }
        }

        // Then check for transitions within each batch
        for (batch_idx, cache) in batch_caches
            .iter()
            .enumerate()
            .take(self.buffer.len())
            .skip(start_batch_idx)
        {
            let batch = &self.buffer[batch_idx];
            let num_rows = batch.num_rows();
            if num_rows <= 1 {
                continue;
            }

            // Find the first row where tag values differ from the previous row
            for row_idx in 0..num_rows - 1 {
                if !cache.are_tag_values_equal(row_idx, row_idx + 1) {
                    return Ok(Some((batch_idx, row_idx)));
                }
            }
        }

        // All examined rows have the same tag values
        self.inspect_start = self.buffer.len();
        Ok(None)
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
