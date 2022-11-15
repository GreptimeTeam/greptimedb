use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use common_query::physical_plan::{Partitioning, PhysicalPlan, PhysicalPlanRef, RuntimeEnv};
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use snafu::OptionExt;

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

    fn children(&self) -> Vec<PhysicalPlanRef> {
        vec![]
    }

    fn with_new_children(&self, _children: Vec<PhysicalPlanRef>) -> QueryResult<PhysicalPlanRef> {
        unimplemented!()
    }

    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> QueryResult<SendableRecordBatchStream> {
        let mut stream = self.stream.lock().unwrap();
        Ok(stream.take().context(query_error::ExecuteRepeatedlySnafu)?)
    }
}

#[cfg(test)]
mod test {
    use common_recordbatch::{util, RecordBatch, RecordBatches};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;

    use super::*;

    #[tokio::test]
    async fn test_simple_table_scan() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));

        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice(&[1, 2])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice(&[3, 4, 5])) as _],
        )
        .unwrap();

        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();
        let stream = recordbatches.as_stream();

        let scan = SimpleTableScan::new(stream);

        assert_eq!(scan.schema(), schema);

        let runtime = Arc::new(RuntimeEnv::default());
        let stream = scan.execute(0, runtime.clone()).await.unwrap();
        let recordbatches = util::collect(stream).await.unwrap();
        assert_eq!(recordbatches[0], batch1);
        assert_eq!(recordbatches[1], batch2);

        let result = scan.execute(0, runtime).await;
        assert!(result.is_err());
        match result {
            Err(e) => assert!(e
                .to_string()
                .contains("Not expected to run ExecutionPlan more than once")),
            _ => unreachable!(),
        }
    }
}
