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

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DFField, DFSchema, DFSchemaRef, Result as DataFusionResult, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use datatypes::arrow::array::TimestampMillisecondArray;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::error::Result as ArrowResult;
use datatypes::arrow::record_batch::RecordBatch;
use futures::Stream;

use crate::extension_plan::Millisecond;

#[derive(Debug, Clone)]
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
    ) -> DataFusionResult<Self> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new(
                None,
                &time_index_column_name,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )],
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

impl UserDefinedLogicalNode for EmptyMetric {
    fn as_any(&self) -> &dyn Any {
        self as _
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

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(self.clone())
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

    fn maintains_input_order(&self) -> bool {
        true
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
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = if self.is_first_poll {
            self.is_first_poll = false;
            let _timer = self.metric.elapsed_compute().timer();
            let result_array = (self.start..self.end)
                .step_by(self.interval as _)
                .collect::<Vec<_>>();
            let millisecond_array = TimestampMillisecondArray::from(result_array);
            let batch =
                RecordBatch::try_new(self.schema.clone(), vec![Arc::new(millisecond_array)]);
            Poll::Ready(Some(batch))
        } else {
            Poll::Ready(None)
        };
        self.metric.record_poll(result)
    }
}
