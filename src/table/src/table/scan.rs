use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::Statistics;
use datatypes::schema::SchemaRef;
use snafu::OptionExt;

use crate::table::ExecutionPlan;

pub struct SimpleTableScan {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
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
        }
    }
}

#[async_trait]
impl ExecutionPlan for SimpleTableScan {
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> QueryResult<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Option<Arc<RuntimeEnv>>,
    ) -> QueryResult<SendableRecordBatchStream> {
        let mut stream = self.stream.lock().unwrap();
        Ok(stream.take().context(query_error::ExecuteRepeatedlySnafu)?)
    }

    fn statistics(&self) -> Statistics {
        // TODO(LFC): implement statistics
        Statistics::default()
    }
}
