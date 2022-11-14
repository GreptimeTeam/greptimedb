use std::fmt::{self, Display};
use std::sync::Arc;

use crate::error::Result;
use crate::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use crate::TableRef;

/// Represents a resolved path to a table of the form “catalog.schema.table”
pub struct TableReference<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
}

impl<'a> Display for TableReference<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Table engine abstraction.
#[async_trait::async_trait]
pub trait TableEngine: Send + Sync {
    /// Return engine name
    fn name(&self) -> &str;

    /// Create a table by given request.
    ///
    /// Return the created table.
    async fn create_table(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef>;

    /// Open an existing table by given `request`, returns the opened table. If the table does not
    /// exist, returns an `Ok(None)`.
    async fn open_table(
        &self,
        ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> Result<Option<TableRef>>;

    /// Alter table schema, options etc. by given request,
    ///
    /// Returns the table after altered.
    async fn alter_table(
        &self,
        ctx: &EngineContext,
        request: AlterTableRequest,
    ) -> Result<TableRef>;

    /// Returns the table by it's name.
    fn get_table<'a>(
        &self,
        ctx: &EngineContext,
        table_ref: &'a TableReference,
    ) -> Result<Option<TableRef>>;

    /// Returns true when the given table is exists.
    /// TODO(hl): support catalog and schema
    fn table_exists<'a>(&self, ctx: &EngineContext, table_ref: &'a TableReference) -> bool;

    /// Drops the given table.
    async fn drop_table(&self, ctx: &EngineContext, request: DropTableRequest) -> Result<()>;
}

pub type TableEngineRef = Arc<dyn TableEngine>;

/// Storage engine context.
#[derive(Debug, Clone, Default)]
pub struct EngineContext {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_reference() {
        let table_ref = TableReference {
            catalog: "greptime",
            schema: "public",
            table: "test",
        };

        assert_eq!("greptime.public.test", table_ref.to_string());
    }
}
