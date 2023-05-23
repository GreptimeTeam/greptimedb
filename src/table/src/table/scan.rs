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

use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use common_query::physical_plan::{Partitioning, PhysicalPlan, PhysicalPlanRef};
use common_recordbatch::SendableRecordBatchStream;
use datafusion::execution::context::TaskContext;
use datafusion_physical_expr::PhysicalSortExpr;
use datatypes::schema::SchemaRef;
use snafu::OptionExt;

pub struct SimpleTableScan {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
    output_ordering: Option<Vec<PhysicalSortExpr>>,
}

impl Debug for SimpleTableScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleTableScan")
            .field("stream", &"<SendableRecordBatchStream>")
            .field("schema", &self.schema)
            .finish()
    }
}

impl SimpleTableScan {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();

        Self {
            stream: Mutex::new(Some(stream)),
            schema,
            output_ordering: None,
        }
    }

    pub fn with_output_ordering(mut self, output_ordering: Vec<PhysicalSortExpr>) -> Self {
        self.output_ordering = Some(output_ordering);
        self
    }
}

impl PhysicalPlan for SimpleTableScan {
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
        self.output_ordering.as_deref()
    }

    fn children(&self) -> Vec<PhysicalPlanRef> {
        vec![]
    }

    fn with_new_children(&self, _children: Vec<PhysicalPlanRef>) -> QueryResult<PhysicalPlanRef> {
        unimplemented!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> QueryResult<SendableRecordBatchStream> {
        let mut stream = self.stream.lock().unwrap();
        stream.take().context(query_error::ExecuteRepeatedlySnafu)
    }
}

#[cfg(test)]
mod test {
    use common_recordbatch::{util, RecordBatch, RecordBatches};
    use datafusion::prelude::SessionContext;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;

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

        let scan = SimpleTableScan::new(stream);

        assert_eq!(scan.schema(), schema);

        let stream = scan.execute(0, ctx.task_ctx()).unwrap();
        let recordbatches = util::collect(stream).await.unwrap();
        assert_eq!(recordbatches[0], batch1);
        assert_eq!(recordbatches[1], batch2);

        let result = scan.execute(0, ctx.task_ctx());
        assert!(result.is_err());
        match result {
            Err(e) => assert!(e
                .to_string()
                .contains("Not expected to run ExecutionPlan more than once")),
            _ => unreachable!(),
        }
    }
}
