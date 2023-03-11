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

use std::fmt::{self, Display};
use std::sync::Arc;

use common_procedure::BoxedProcedure;

use crate::error::Result;
use crate::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use crate::TableRef;

/// Represents a resolved path to a table of the form “catalog.schema.table”
#[derive(Debug, PartialEq)]
pub struct TableReference<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
}

// TODO(LFC): Find a better place for `TableReference`,
// so that we can reuse the default catalog and schema consts.
// Could be done together with issue #559.
impl<'a> TableReference<'a> {
    pub fn bare(table: &'a str) -> Self {
        TableReference {
            catalog: "greptime",
            schema: "public",
            table,
        }
    }

    pub fn full(catalog: &'a str, schema: &'a str, table: &'a str) -> Self {
        TableReference {
            catalog,
            schema,
            table,
        }
    }
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
    fn get_table(
        &self,
        ctx: &EngineContext,
        table_ref: &TableReference,
    ) -> Result<Option<TableRef>>;

    /// Returns true when the given table is exists.
    fn table_exists(&self, ctx: &EngineContext, table_ref: &TableReference) -> bool;

    /// Drops the given table. Return true if the table is dropped, or false if the table doesn't exist.
    async fn drop_table(&self, ctx: &EngineContext, request: DropTableRequest) -> Result<bool>;

    /// Close the table.
    async fn close(&self) -> Result<()>;
}

pub type TableEngineRef = Arc<dyn TableEngine>;

/// Table engine context.
#[derive(Debug, Clone, Default)]
pub struct EngineContext {}

/// Procedures for table engine.
pub trait TableEngineProcedure: Send + Sync {
    /// Returns a procedure that creates table by specific `request`.
    fn create_table_procedure(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<BoxedProcedure>;
}

pub type TableEngineProcedureRef = Arc<dyn TableEngineProcedure>;

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
