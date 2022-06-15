use common_error::ext::ErrorExt;

use crate::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest};
use crate::TableRef;

/// Table engine abstraction.
#[async_trait::async_trait]
pub trait TableEngine: Send + Sync + Clone {
    type Error: ErrorExt + Send + Sync;

    /// Create a table by given request.
    ///
    /// Return the created table.
    async fn create_table(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef, Self::Error>;

    /// Alter table schema, options etc. by given request,
    ///
    /// Returns the table after altered.
    async fn alter_table(
        &self,
        ctx: &EngineContext,
        request: AlterTableRequest,
    ) -> Result<TableRef, Self::Error>;

    /// Returns the table by it's name.
    fn get_table(&self, ctx: &EngineContext, name: &str) -> Result<Option<TableRef>, Self::Error>;

    /// Returns true when the given table is exists.
    fn table_exists(&self, ctx: &EngineContext, name: &str) -> bool;

    /// Drops the given table.
    async fn drop_table(
        &self,
        ctx: &EngineContext,
        request: DropTableRequest,
    ) -> Result<(), Self::Error>;
}

/// Storage engine context.
#[derive(Debug, Clone, Default)]
pub struct EngineContext {}
