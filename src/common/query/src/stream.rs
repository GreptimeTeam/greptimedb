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
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::execution::context::TaskContext;
use datafusion::execution::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::schema::SchemaRef;

/// Adapts greptime's [SendableRecordBatchStream] to DataFusion's [ExecutionPlan].
pub struct StreamScanAdapter {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    properties: PlanProperties,
    output_ordering: Option<Vec<PhysicalSortExpr>>,
}

impl Debug for StreamScanAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamScanAdapter")
            .field("stream", &"<SendableRecordBatchStream>")
            .field("schema", &self.schema)
            .finish()
    }
}

impl StreamScanAdapter {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        let arrow_schema = schema.arrow_schema().clone();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            stream: Mutex::new(Some(stream)),
            schema,
            arrow_schema,
            properties,
            output_ordering: None,
        }
    }

    pub fn with_output_ordering(mut self, output_ordering: Option<Vec<PhysicalSortExpr>>) -> Self {
        self.output_ordering = output_ordering;
        self
    }
}

impl DisplayAs for StreamScanAdapter {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "StreamScanAdapter: [<SendableRecordBatchStream>], schema: ["
        )?;
        write!(f, "{:?}", &self.arrow_schema)?;
        write!(f, "]")
    }
}

impl ExecutionPlan for StreamScanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    // DataFusion will swap children unconditionally.
    // But since this node is leaf node, it's safe to just return self.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let mut stream = self.stream.lock().unwrap();
        let stream = stream
            .take()
            .ok_or_else(|| DataFusionError::Execution("Stream already exhausted".to_string()))?;
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(stream)))
    }

    fn name(&self) -> &str {
        "StreamScanAdapter"
    }

    // bypass DataFusionError:
    // Context("EnforceDistribution", Internal("YieldStreamExec requires exactly one child"))
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
}

#[cfg(test)]
mod test {
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datafusion::prelude::SessionContext;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn test_simple_table_scan() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));

        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1, 2])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([3, 4, 5])) as _],
        )
        .unwrap();

        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();
        let stream = recordbatches.as_stream();

        let scan = StreamScanAdapter::new(stream);

        assert_eq!(scan.schema(), schema.arrow_schema().clone());

        let stream = scan.execute(0, ctx.task_ctx()).unwrap();
        let recordbatches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(recordbatches[0], batch1.into_df_record_batch());
        assert_eq!(recordbatches[1], batch2.into_df_record_batch());

        let result = scan.execute(0, ctx.task_ctx());
        assert!(result.is_err());
        match result {
            Err(e) => assert!(e.to_string().contains("Stream already exhausted")),
            _ => unreachable!(),
        }
    }
}
