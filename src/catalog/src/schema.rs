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
use std::sync::Arc;

use async_trait::async_trait;
use table::TableRef;

use crate::error::{NotSupportedSnafu, Result};

/// Represents a schema, comprising a number of named tables.
#[async_trait]
pub trait SchemaProvider: Sync + Send {
    /// Returns the schema provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available table names in this schema.
    async fn table_names(&self) -> Result<Vec<String>>;

    /// Retrieves a specific table from the schema by name, provided it exists.
    async fn table(&self, name: &str) -> Result<Option<TableRef>>;

    /// If supported by the implementation, adds a new table to this schema.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    async fn register_table(&self, name: String, _table: TableRef) -> Result<Option<TableRef>> {
        NotSupportedSnafu {
            op: format!("register_table({name}, <table>)"),
        }
        .fail()
    }

    /// If supported by the implementation, renames an existing table from this schema and returns it.
    /// If no table of that name exists, returns "Table not found" error.
    async fn rename_table(&self, name: &str, new_name: String) -> Result<TableRef> {
        NotSupportedSnafu {
            op: format!("rename_table({name}, {new_name})"),
        }
        .fail()
    }

    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    async fn deregister_table(&self, name: &str) -> Result<Option<TableRef>> {
        NotSupportedSnafu {
            op: format!("deregister_table({name})"),
        }
        .fail()
    }

    /// If supported by the implementation, checks the table exist in the schema provider or not.
    /// If no matched table in the schema provider, return false.
    /// Otherwise, return true.
    async fn table_exist(&self, name: &str) -> Result<bool>;
}

pub type SchemaProviderRef = Arc<dyn SchemaProvider>;
