use std::any::Any;
use std::sync::Arc;

use table::Table;

use crate::error::Result;

/// Represents a schema, comprising a number of named tables.
pub trait SchemaProvider: Sync + Send {
    /// Returns the schema provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String>;

    /// Retrieves a specific table from the schema by name, provided it exists.
    fn table(&self, name: &str) -> Option<Arc<dyn Table>>;

    /// If supported by the implementation, adds a new table to this schema.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn Table>,
    ) -> Result<Option<Arc<dyn Table>>> {
        todo!();
    }

    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn Table>>> {
        todo!();
    }

    /// If supported by the implementation, checks the table exist in the schema provider or not.
    /// If no matched table in the schema provider, return false.
    /// Otherwise, return true.
    fn table_exist(&self, name: &str) -> bool;
}
