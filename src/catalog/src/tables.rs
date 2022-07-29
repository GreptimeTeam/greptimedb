// The `tables` table in system catalog keeps a record of all tables created by user.

use std::any::Any;

use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use table::Table;

/// Tables holds all tables created by user.
pub struct Tables {
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl Table for Tables {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> table::error::Result<SendableRecordBatchStream> {
        todo!("Tables' scan should be delegated to CatalogManager")
    }
}
