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
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DFField, DFSchema, DFSchemaRef, Result as DataFusionResult, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use datatypes::arrow::array::TimestampMillisecondArray;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::Stream;

use crate::extension_plan::Millisecond;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmptyMetric {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    schema: DFSchemaRef,
}

impl EmptyMetric {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        time_index_column_name: String,
        value_column_name: String,
    ) -> DataFusionResult<Self> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![
                DFField::new(
                    None::<TableReference>,
                    &time_index_column_name,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                DFField::new(
                    None::<TableReference>,
                    &value_column_name,
                    DataType::Float64,
                    true,
                ),
            ],
            HashMap::new(),
        )?);

        Ok(Self {
            start,
            end,
            interval,
            schema,
        })
    }

    pub fn to_execution_plan(&self) -> Arc<dyn ExecutionPlan> {
        // let schema =  self.schema.to
        Arc::new(EmptyMetricExec {
            start: self.start,
            end: self.end,
            interval: self.interval,
            schema: Arc::new(self.schema.as_ref().into()),
            metric: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl UserDefinedLogicalNodeCore for EmptyMetric {
    fn name(&self) -> &str {
        "EmptyMetric"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "EmptyMetric: range=[{}..{}], interval=[{}]",
            self.start, self.end, self.interval,
        )
    }

    fn from_template(&self, _expr: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}

#[derive(Debug, Clone)]
pub struct EmptyMetricExec {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    schema: SchemaRef,

    metric: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for EmptyMetricExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self.as_ref().clone()))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
        Ok(Box::pin(EmptyMetricStream {
            start: self.start,
            end: self.end,
            interval: self.interval,
            is_first_poll: true,
            schema: self.schema.clone(),
            metric: baseline_metric,
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(
                f,
                "EmptyMetric: range=[{}..{}], interval=[{}]",
                self.start, self.end, self.interval,
            ),
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let estimated_row_num = (self.end - self.start) as f64 / self.interval as f64;
        let total_byte_size = estimated_row_num * std::mem::size_of::<Millisecond>() as f64;

        Statistics {
            num_rows: Some(estimated_row_num.floor() as _),
            total_byte_size: Some(total_byte_size.floor() as _),
            column_statistics: None,
            is_exact: true,
        }
    }
}

pub struct EmptyMetricStream {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    // only generate one record batch at the first poll
    is_first_poll: bool,
    schema: SchemaRef,
    metric: BaselineMetrics,
}

impl RecordBatchStream for EmptyMetricStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyMetricStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = if self.is_first_poll {
            self.is_first_poll = false;
            let _timer = self.metric.elapsed_compute().timer();
            let result_array = (self.start..=self.end)
                .step_by(self.interval as _)
                .collect::<Vec<_>>();
            let float_array =
                Float64Array::from_iter(result_array.iter().map(|v| *v as f64 / 1000.0));
            let millisecond_array = TimestampMillisecondArray::from(result_array);
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(millisecond_array), Arc::new(float_array)],
            )
            .map_err(DataFusionError::ArrowError);
            Poll::Ready(Some(batch))
        } else {
            Poll::Ready(None)
        };
        self.metric.record_poll(result)
    }
}

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn do_empty_metric_test(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        time_column_name: String,
        value_column_name: String,
        expected: String,
    ) {
        let empty_metric =
            EmptyMetric::new(start, end, interval, time_column_name, value_column_name).unwrap();
        let empty_metric_exec = empty_metric.to_execution_plan();

        let session_context = SessionContext::default();
        let result =
            datafusion::physical_plan::collect(empty_metric_exec, session_context.task_ctx())
                .await
                .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn normal_empty_metric_test() {
        do_empty_metric_test(
            0,
            100,
            10,
            "time".to_string(),
            "value".to_string(),
            String::from(
                "+-------------------------+-------+\
                \n| time                    | value |\
                \n+-------------------------+-------+\
                \n| 1970-01-01T00:00:00     | 0.0   |\
                \n| 1970-01-01T00:00:00.010 | 0.01  |\
                \n| 1970-01-01T00:00:00.020 | 0.02  |\
                \n| 1970-01-01T00:00:00.030 | 0.03  |\
                \n| 1970-01-01T00:00:00.040 | 0.04  |\
                \n| 1970-01-01T00:00:00.050 | 0.05  |\
                \n| 1970-01-01T00:00:00.060 | 0.06  |\
                \n| 1970-01-01T00:00:00.070 | 0.07  |\
                \n| 1970-01-01T00:00:00.080 | 0.08  |\
                \n| 1970-01-01T00:00:00.090 | 0.09  |\
                \n| 1970-01-01T00:00:00.100 | 0.1   |\
                \n+-------------------------+-------+",
            ),
        )
        .await
    }

    #[tokio::test]
    async fn unaligned_empty_metric_test() {
        do_empty_metric_test(
            0,
            100,
            11,
            "time".to_string(),
            "value".to_string(),
            String::from(
                "+-------------------------+-------+\
                \n| time                    | value |\
                \n+-------------------------+-------+\
                \n| 1970-01-01T00:00:00     | 0.0   |\
                \n| 1970-01-01T00:00:00.011 | 0.011 |\
                \n| 1970-01-01T00:00:00.022 | 0.022 |\
                \n| 1970-01-01T00:00:00.033 | 0.033 |\
                \n| 1970-01-01T00:00:00.044 | 0.044 |\
                \n| 1970-01-01T00:00:00.055 | 0.055 |\
                \n| 1970-01-01T00:00:00.066 | 0.066 |\
                \n| 1970-01-01T00:00:00.077 | 0.077 |\
                \n| 1970-01-01T00:00:00.088 | 0.088 |\
                \n| 1970-01-01T00:00:00.099 | 0.099 |\
                \n+-------------------------+-------+",
            ),
        )
        .await
    }

    #[tokio::test]
    async fn one_row_empty_metric_test() {
        do_empty_metric_test(
            0,
            100,
            1000,
            "time".to_string(),
            "value".to_string(),
            String::from(
                "+---------------------+-------+\
                \n| time                | value |\
                \n+---------------------+-------+\
                \n| 1970-01-01T00:00:00 | 0.0   |\
                \n+---------------------+-------+",
            ),
        )
        .await
    }

    #[tokio::test]
    async fn negative_range_empty_metric_test() {
        do_empty_metric_test(
            1000,
            -1000,
            10,
            "time".to_string(),
            "value".to_string(),
            String::from(
                "+------+-------+\
                \n| time | value |\
                \n+------+-------+\
                \n+------+-------+",
            ),
        )
        .await
    }
}
