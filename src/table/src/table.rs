pub mod adapter;
pub mod numbers;

use std::any::Any;
use std::sync::Arc;

use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;

use crate::error::Result;
use crate::metadata::{FilterPushDownType, TableInfoRef, TableType};
use crate::requests::{AlterTableRequest, InsertRequest};

/// Table abstraction.
#[async_trait::async_trait]
pub trait Table: Send + Sync {
    /// Returns the table as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Get a reference to the table info.
    fn table_info(&self) -> TableInfoRef;

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Insert values into table.
    async fn insert(&self, _request: InsertRequest) -> Result<usize> {
        unimplemented!();
    }

    /// Scan the table and returns a SendableRecordBatchStream.
    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream>;

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<FilterPushDownType> {
        Ok(FilterPushDownType::Unsupported)
    }

    async fn alter(&self, _request: AlterTableRequest) -> Result<()> {
        unimplemented!()
    }
}

pub type TableRef = Arc<dyn Table>;
