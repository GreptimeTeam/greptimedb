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

#![feature(assert_matches)]
#![feature(try_blocks)]

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use futures::future::BoxFuture;
use table::metadata::TableId;
use table::requests::CreateTableRequest;
use table::TableRef;

use crate::error::Result;

pub mod error;
pub mod information_schema;
pub mod kvbackend;
pub mod memory;
mod metrics;
pub mod table_source;

#[async_trait::async_trait]
pub trait CatalogManager: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn catalog_names(&self) -> Result<Vec<String>>;

    async fn schema_names(&self, catalog: &str) -> Result<Vec<String>>;

    async fn table_names(&self, catalog: &str, schema: &str) -> Result<Vec<String>>;

    async fn catalog_exists(&self, catalog: &str) -> Result<bool>;

    async fn schema_exists(&self, catalog: &str, schema: &str) -> Result<bool>;

    async fn table_exists(&self, catalog: &str, schema: &str, table: &str) -> Result<bool>;

    /// Returns the table by catalog, schema and table name.
    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>>;
}

pub type CatalogManagerRef = Arc<dyn CatalogManager>;

/// Hook called after system table opening.
pub type OpenSystemTableHook =
    Box<dyn Fn(TableRef) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Register system table request:
/// - When system table is already created and registered, the hook will be called
///     with table ref after opening the system table
/// - When system table is not exists, create and register the table by create_table_request and calls open_hook with the created table.
pub struct RegisterSystemTableRequest {
    pub create_table_request: CreateTableRequest,
    pub open_hook: Option<OpenSystemTableHook>,
}

#[derive(Clone)]
pub struct RegisterTableRequest {
    pub catalog: String,
    pub schema: String,
    pub table_name: String,
    pub table_id: TableId,
    pub table: TableRef,
}

impl Debug for RegisterTableRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisterTableRequest")
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("table_name", &self.table_name)
            .field("table_id", &self.table_id)
            .field("table", &self.table.table_info())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct RenameTableRequest {
    pub catalog: String,
    pub schema: String,
    pub table_name: String,
    pub new_table_name: String,
    pub table_id: TableId,
}

#[derive(Debug, Clone)]
pub struct DeregisterTableRequest {
    pub catalog: String,
    pub schema: String,
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub struct DeregisterSchemaRequest {
    pub catalog: String,
    pub schema: String,
}

#[derive(Debug, Clone)]
pub struct RegisterSchemaRequest {
    pub catalog: String,
    pub schema: String,
}
