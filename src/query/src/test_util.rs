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

use arrow::array::{
    ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_schema::{SchemaRef, TimeUnit};
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::Stream;

pub fn new_ts_array(unit: TimeUnit, arr: Vec<i64>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from_iter_values(arr)) as ArrayRef,
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from_iter_values(arr)) as ArrayRef
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from_iter_values(arr)) as ArrayRef
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from_iter_values(arr)) as ArrayRef
        }
    }
}

#[derive(Debug)]
pub struct MockInputExec {
    input: Vec<Vec<DfRecordBatch>>,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl MockInputExec {
    pub fn new(input: Vec<Vec<DfRecordBatch>>, schema: SchemaRef) -> Self {
        Self {
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            input,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for MockInputExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        unimplemented!()
    }
}

impl ExecutionPlan for MockInputExec {
    fn name(&self) -> &str {
        "MockInputExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let stream = MockStream {
            stream: self.input.clone().into_iter().flatten().collect(),
            schema: self.schema.clone(),
            idx: 0,
            metrics: BaselineMetrics::new(&self.metrics, partition),
        };
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct MockStream {
    stream: Vec<DfRecordBatch>,
    schema: SchemaRef,
    idx: usize,
    metrics: BaselineMetrics,
}

impl Stream for MockStream {
    type Item = datafusion_common::Result<DfRecordBatch>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        if self.idx < self.stream.len() {
            let ret = self.stream[self.idx].clone();
            self.idx += 1;
            self.metrics.record_poll(Poll::Ready(Some(Ok(ret))))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for MockStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
