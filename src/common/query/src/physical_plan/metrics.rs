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
use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::Partitioning;
use datatypes::schema::SchemaRef;
use futures::StreamExt;
use meter_macros::read_meter;
use session::context::QueryContextRef;

use super::visitor::{accept, PhysicalPlanVisitor};
use super::{PhysicalPlan, PhysicalPlanRef};
use crate::error::Result;
use crate::physical_plan::stream::RecordBatchReceiverStream;

#[derive(Debug, Clone)]
pub struct MetricsReporter {
    input: PhysicalPlanRef,
    query_ctx: QueryContextRef,
}

impl MetricsReporter {
    pub fn new(input: Arc<dyn PhysicalPlan>, query_ctx: QueryContextRef) -> Self {
        Self { input, query_ctx }
    }
}

impl PhysicalPlan for MetricsReporter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<PhysicalPlanRef> {
        vec![self.input.clone()]
    }

    fn with_new_children(&self, mut children: Vec<PhysicalPlanRef>) -> Result<PhysicalPlanRef> {
        assert!(!children.is_empty());

        Ok(Arc::new(Self::new(
            children.pop().unwrap(),
            self.query_ctx.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let captured_input = self.input.clone();
        let mut input_stream = captured_input.execute(0, context)?;

        let captured_ctx = self.query_ctx.clone();

        let join_handle = tokio::spawn(async move {
            while let Some(b) = input_stream.next().await {
                if tx.send(b).await.is_err() {
                    return;
                }
            }

            let wrapper = Wrapper::new(captured_input.as_ref());
            let cpu_time = wrapper.cpu_time_ns();
            let catalog = captured_ctx.current_catalog();
            let schema = captured_ctx.current_schema();

            read_meter!(catalog, schema, cpu_time: cpu_time);
        });

        Ok(RecordBatchReceiverStream::create(
            &self.schema(),
            rx,
            join_handle,
        ))
    }
}

pub struct Wrapper<'a> {
    inner: &'a dyn PhysicalPlan,
}

impl<'a> Wrapper<'a> {
    pub fn new(inner: &'a dyn PhysicalPlan) -> Self {
        Self { inner }
    }

    pub fn cpu_time_ns(&self) -> u64 {
        let mut vistor = MetricsVisitor::new();

        // Safety: pre_visit and post_visit mthod in MetricsVisitor will not return Err.
        accept(self.inner, &mut vistor).unwrap();

        vistor.cpu_time
    }
}

struct MetricsVisitor {
    cpu_time: u64,
}

impl MetricsVisitor {
    pub fn new() -> Self {
        Self { cpu_time: 0 }
    }
}

impl PhysicalPlanVisitor for MetricsVisitor {
    type Error = String;

    fn pre_visit(&mut self, plan: &dyn PhysicalPlan) -> std::result::Result<bool, Self::Error> {
        let metrics = plan.metrics();

        if let Some(m) = metrics {
            if let Some(cpu_time) = m.elapsed_compute() {
                self.cpu_time += cpu_time as u64;
            }
        }

        Ok(true)
    }
}
