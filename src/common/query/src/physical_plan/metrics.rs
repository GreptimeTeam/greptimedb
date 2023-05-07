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

use super::visitor::{visit_physical_plan, PhysicalPlanVisitor};
use super::{PhysicalPlan, PhysicalPlanRef};
use crate::error::Result;
use crate::physical_plan::stream::RecordBatchReceiverStream;

/// Metrics physical plan operator. This operation will collect some metrics(ex: sql_cpu_time,
/// table_scan_size, etc) after the input operator is executed.
#[derive(Debug, Clone)]
pub struct MetricsPhysicalPlan {
    input: PhysicalPlanRef,
    query_ctx: QueryContextRef,
}

impl MetricsPhysicalPlan {
    pub fn new(input: Arc<dyn PhysicalPlan>, query_ctx: QueryContextRef) -> Self {
        Self { input, query_ctx }
    }
}

impl PhysicalPlan for MetricsPhysicalPlan {
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

            let mut collector = MetricsCollector::new(captured_input.as_ref());
            collector.collect();

            let cpu_time = collector.cpu_time_ns();
            let table_scan_bytes = collector.table_scan_bytes();

            let catalog = captured_ctx.current_catalog();
            let schema = captured_ctx.current_schema();

            if let Some(cpu_time) = cpu_time {
                read_meter!(&catalog, &schema, cpu_time: cpu_time);
            }

            if let Some(table_scan) = table_scan_bytes {
                read_meter!(&catalog, &schema, table_scan: table_scan);
            }
        });

        Ok(RecordBatchReceiverStream::create(
            &self.schema(),
            rx,
            join_handle,
        ))
    }
}

pub struct MetricsCollector<'a> {
    inner: &'a dyn PhysicalPlan,
    vistor: Option<MetricsVisitor>,
}

impl<'a> MetricsCollector<'a> {
    pub fn new(inner: &'a dyn PhysicalPlan) -> Self {
        Self {
            inner,
            vistor: None,
        }
    }

    pub fn collect(&mut self) {
        let mut vistor = MetricsVisitor::new();

        // Safety: pre_visit and post_visit method in MetricsVisitor not return Err.
        visit_physical_plan(self.inner, &mut vistor).unwrap();

        self.vistor = Some(vistor);
    }

    pub fn cpu_time_ns(&self) -> Option<u64> {
        self.vistor.as_ref().map(|v| v.cpu_time_ns)
    }

    pub fn table_scan_bytes(&self) -> Option<u64> {
        self.vistor.as_ref().map(|v| v.table_scan_bytes)
    }
}

struct MetricsVisitor {
    cpu_time_ns: u64,
    table_scan_bytes: u64,
}

impl MetricsVisitor {
    pub fn new() -> Self {
        Self {
            cpu_time_ns: 0,
            table_scan_bytes: 0,
        }
    }
}

impl PhysicalPlanVisitor for MetricsVisitor {
    type Error = String;

    fn pre_visit(&mut self, plan: &dyn PhysicalPlan) -> std::result::Result<bool, Self::Error> {
        let metrics = plan.metrics();

        if let Some(m) = metrics {
            if let Some(val) = m.elapsed_compute() {
                self.cpu_time_ns += val as u64;
            }

            if let Some(val) = m.sum_by_name("table_scan_bytes") {
                self.table_scan_bytes += val.as_usize() as u64;
            }
        }

        Ok(true)
    }
}
