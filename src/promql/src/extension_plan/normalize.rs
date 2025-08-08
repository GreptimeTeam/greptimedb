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

use datafusion::arrow::array::{BooleanArray, Float64Array};
use datafusion::arrow::compute;
use datafusion::common::{DFSchema, DFSchemaRef, Result as DataFusionResult, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::expressions::Column as ColumnExpr;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::arrow::array::TimestampMillisecondArray;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::{Millisecond, METRIC_NUM_SERIES};
use crate::metrics::PROMQL_SERIES_COUNT;

/// Normalize the input record batch. Notice that for simplicity, this method assumes
/// the input batch only contains sample points from one time series.
///
/// Roughly speaking, this method does these things:
/// - bias sample's timestamp by offset
/// - sort the record batch based on timestamp column
/// - remove NaN values (optional)
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesNormalize {
    offset: Millisecond,
    time_index_column_name: String,
    need_filter_out_nan: bool,
    tag_columns: Vec<String>,

    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesNormalize {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromSeriesNormalize: offset=[{}], time index=[{}], filter NaN: [{}]",
            self.offset, self.time_index_column_name, self.need_filter_out_nan
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.is_empty() {
            return Err(DataFusionError::Internal(
                "SeriesNormalize should have at least one input".to_string(),
            ));
        }

        Ok(Self {
            offset: self.offset,
            time_index_column_name: self.time_index_column_name.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: inputs.into_iter().next().unwrap(),
            tag_columns: self.tag_columns.clone(),
        })
    }
}

impl SeriesNormalize {
    pub fn new<N: AsRef<str>>(
        offset: Millisecond,
        time_index_column_name: N,
        need_filter_out_nan: bool,
        tag_columns: Vec<String>,
        input: LogicalPlan,
    ) -> Self {
        Self {
            offset,
            time_index_column_name: time_index_column_name.as_ref().to_string(),
            need_filter_out_nan,
            tag_columns,
            input,
        }
    }

    pub const fn name() -> &'static str {
        "SeriesNormalize"
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SeriesNormalizeExec {
            offset: self.offset,
            time_index_column_name: self.time_index_column_name.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: exec_input,
            tag_columns: self.tag_columns.clone(),
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        pb::SeriesNormalize {
            offset: self.offset,
            time_index: self.time_index_column_name.clone(),
            filter_nan: self.need_filter_out_nan,
            tag_columns: self.tag_columns.clone(),
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_normalize = pb::SeriesNormalize::decode(bytes).context(DeserializeSnafu)?;
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        Ok(Self::new(
            pb_normalize.offset,
            pb_normalize.time_index,
            pb_normalize.filter_nan,
            pb_normalize.tag_columns,
            placeholder_plan,
        ))
    }
}

#[derive(Debug)]
pub struct SeriesNormalizeExec {
    offset: Millisecond,
    time_index_column_name: String,
    need_filter_out_nan: bool,
    tag_columns: Vec<String>,

    input: Arc<dyn ExecutionPlan>,
    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for SeriesNormalizeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
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

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
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
            offset: self.offset,
            time_index_column_name: self.time_index_column_name.clone(),
            need_filter_out_nan: self.need_filter_out_nan,
            input: children[0].clone(),
            tag_columns: self.tag_columns.clone(),
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
        let time_index = schema
            .column_with_name(&self.time_index_column_name)
            .expect("time index column not found")
            .0;
        Ok(Box::pin(SeriesNormalizeStream {
            offset: self.offset,
            time_index,
            need_filter_out_nan: self.need_filter_out_nan,
            schema,
            input,
            metric: baseline_metric,
            num_series,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn name(&self) -> &str {
        "SeriesNormalizeExec"
    }
}

impl DisplayAs for SeriesNormalizeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PromSeriesNormalizeExec: offset=[{}], time index=[{}], filter NaN: [{}]",
                    self.offset, self.time_index_column_name, self.need_filter_out_nan
                )
            }
        }
    }
}

pub struct SeriesNormalizeStream {
    offset: Millisecond,
    // Column index of TIME INDEX column's position in schema
    time_index: usize,
    need_filter_out_nan: bool,

    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    /// Number of series processed.
    num_series: Count,
}

impl SeriesNormalizeStream {
    pub fn normalize(&self, input: RecordBatch) -> DataFusionResult<RecordBatch> {
        let ts_column = input
            .column(self.time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Time index Column downcast to TimestampMillisecondArray failed".into(),
                )
            })?;

        // bias the timestamp column by offset
        let ts_column_biased = if self.offset == 0 {
            Arc::new(ts_column.clone()) as _
        } else {
            Arc::new(TimestampMillisecondArray::from_iter(
                ts_column.iter().map(|ts| ts.map(|ts| ts + self.offset)),
            ))
        };
        let mut columns = input.columns().to_vec();
        columns[self.time_index] = ts_column_biased;

        let result_batch = RecordBatch::try_new(input.schema(), columns)?;
        if !self.need_filter_out_nan {
            return Ok(result_batch);
        }

        // TODO(ruihang): consider the "special NaN"
        // filter out NaN
        let mut filter = vec![true; input.num_rows()];
        for column in result_batch.columns() {
            if let Some(float_column) = column.as_any().downcast_ref::<Float64Array>() {
                for (i, flag) in filter.iter_mut().enumerate() {
                    if float_column.value(i).is_nan() {
                        *flag = false;
                    }
                }
            }
        }

        let result = compute::filter_record_batch(&result_batch, &BooleanArray::from(filter))
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(result)
    }
}

impl RecordBatchStream for SeriesNormalizeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesNormalizeStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                self.num_series.add(1);
                let timer = std::time::Instant::now();
                let result = Ok(batch).and_then(|batch| self.normalize(batch));
                self.metric.elapsed_compute().add_elapsed(timer);
                Poll::Ready(Some(result))
            }
            None => {
                PROMQL_SERIES_COUNT.observe(self.num_series.value() as f64);
                Poll::Ready(None)
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
        };
        self.metric.record_poll(poll)
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::{
        ArrowPrimitiveType, DataType, Field, Schema, TimestampMillisecondType,
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::TimestampMillisecondArray;
    use datatypes::arrow_array::StringArray;

    use super::*;

    const TIME_INDEX_COLUMN: &str = "timestamp";

    fn prepare_test_data() -> DataSourceExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIME_INDEX_COLUMN, TimestampMillisecondType::DATA_TYPE, true),
            Field::new("value", DataType::Float64, true),
            Field::new("path", DataType::Utf8, true),
        ]));
        let timestamp_column = Arc::new(TimestampMillisecondArray::from(vec![
            60_000, 120_000, 0, 30_000, 90_000,
        ])) as _;
        let field_column = Arc::new(Float64Array::from(vec![0.0, 1.0, 10.0, 100.0, 1000.0])) as _;
        let path_column = Arc::new(StringArray::from(vec!["foo", "foo", "foo", "foo", "foo"])) as _;
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![timestamp_column, field_column, path_column],
        )
        .unwrap();

        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![data]], schema, None).unwrap(),
        ))
    }

    #[tokio::test]
    async fn test_sort_record_batch() {
        let memory_exec = Arc::new(prepare_test_data());
        let normalize_exec = Arc::new(SeriesNormalizeExec {
            offset: 0,
            time_index_column_name: TIME_INDEX_COLUMN.to_string(),
            need_filter_out_nan: true,
            input: memory_exec,
            tag_columns: vec!["path".to_string()],
            metric: ExecutionPlanMetricsSet::new(),
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        let expected = String::from(
            "+---------------------+--------+------+\
            \n| timestamp           | value  | path |\
            \n+---------------------+--------+------+\
            \n| 1970-01-01T00:01:00 | 0.0    | foo  |\
            \n| 1970-01-01T00:02:00 | 1.0    | foo  |\
            \n| 1970-01-01T00:00:00 | 10.0   | foo  |\
            \n| 1970-01-01T00:00:30 | 100.0  | foo  |\
            \n| 1970-01-01T00:01:30 | 1000.0 | foo  |\
            \n+---------------------+--------+------+",
        );

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn test_offset_record_batch() {
        let memory_exec = Arc::new(prepare_test_data());
        let normalize_exec = Arc::new(SeriesNormalizeExec {
            offset: 1_000,
            time_index_column_name: TIME_INDEX_COLUMN.to_string(),
            need_filter_out_nan: true,
            input: memory_exec,
            metric: ExecutionPlanMetricsSet::new(),
            tag_columns: vec!["path".to_string()],
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(normalize_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        let expected = String::from(
            "+---------------------+--------+------+\
            \n| timestamp           | value  | path |\
            \n+---------------------+--------+------+\
            \n| 1970-01-01T00:01:01 | 0.0    | foo  |\
            \n| 1970-01-01T00:02:01 | 1.0    | foo  |\
            \n| 1970-01-01T00:00:01 | 10.0   | foo  |\
            \n| 1970-01-01T00:00:31 | 100.0  | foo  |\
            \n| 1970-01-01T00:01:31 | 1000.0 | foo  |\
            \n+---------------------+--------+------+",
        );

        assert_eq!(result_literal, expected);
    }
}
