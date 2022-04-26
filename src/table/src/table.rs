use crate::error::Result;
use chrono::DateTime;
use chrono::Utc;
use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::{Schema as TableSchema, SchemaRef as TableSchemaRef};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub mod adapter;
pub mod memory;

pub type TableId = u64;
pub type TableVersion = u64;

/// Indicates whether and how a filter expression can be handled by a
/// Table for table scans.
#[derive(Debug, Clone, PartialEq)]
pub enum TableProviderFilterPushDown {
    /// The expression cannot be used by the provider.
    Unsupported,
    /// The expression can be used to help minimise the data retrieved,
    /// but the provider cannot guarantee that all returned tuples
    /// satisfy the filter. The Filter plan node containing this expression
    /// will be preserved.
    Inexact,
    /// The provider guarantees that all returned data satisfies this
    /// filter expression. The Filter plan node containing this expression
    /// will be removed.
    Exact,
}

/// Indicates the type of this table for metadata/catalog purposes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableType {
    /// An ordinary physical table.
    Base,
    /// A non-materialised table that itself uses a query internally to provide data.
    View,
    /// A transient table.
    Temporary,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableIdent {
    pub table_id: TableId,
    pub version: TableVersion,
}

#[derive(Debug)]
pub struct TableInfo {
    pub ident: TableIdent,
    pub name: String,
    pub desc: Option<String>,
    pub meta: TableMeta,
}

#[derive(Clone, Debug)]
pub struct TableMeta {
    pub schema: Arc<TableSchema>,
    pub engine: String,
    pub engine_options: HashMap<String, String>,
    pub options: HashMap<String, String>,
    pub created_on: DateTime<Utc>,
}

/// Table abstraction.
#[async_trait::async_trait]
pub trait Table: Send + Sync {
    /// Returns the table as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> TableSchemaRef;

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible of grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
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
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}

pub type TableRef = Arc<dyn Table>;
