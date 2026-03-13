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

use api::v1::CreateTableExpr;
use common_catalog::consts::{INFORMATION_SCHEMA_NAME, PG_CATALOG_NAME};
use futures::future::BoxFuture;
use futures_util::stream::BoxStream;
use session::context::QueryContext;
use table::TableRef;
use table::metadata::{TableId, TableInfoRef};

use crate::error::Result;

pub mod error;
pub mod information_extension;
pub mod kvbackend;
#[cfg(any(test, feature = "testing"))]
pub mod memory;
mod metrics;
pub mod system_schema;
pub mod information_schema {
    // TODO(j0hn50n133): re-export to make it compatible with the legacy code, migrate to the new path later
    pub use crate::system_schema::information_schema::*;
}

pub mod process_manager;
pub mod table_source;

#[async_trait::async_trait]
pub trait CatalogManager: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn catalog_names(&self) -> Result<Vec<String>>;

    async fn schema_names(
        &self,
        catalog: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Vec<String>>;

    async fn table_names(
        &self,
        catalog: &str,
        schema: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Vec<String>>;

    async fn catalog_exists(&self, catalog: &str) -> Result<bool>;

    async fn schema_exists(
        &self,
        catalog: &str,
        schema: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<bool>;

    async fn table_exists(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<bool>;

    /// Returns the table by catalog, schema and table name.
    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Option<TableRef>>;

    /// Returns the table id of provided table ident.
    async fn table_id(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Option<TableId>> {
        Ok(self
            .table(catalog, schema, table_name, query_ctx)
            .await?
            .map(|t| t.table_info().ident.table_id))
    }

    /// Returns the table of provided id.
    async fn table_info_by_id(&self, table_id: TableId) -> Result<Option<TableInfoRef>>;

    /// Returns the tables by table ids.
    async fn tables_by_ids(
        &self,
        catalog: &str,
        schema: &str,
        table_ids: &[TableId],
    ) -> Result<Vec<TableRef>>;

    /// Returns all tables with a stream by catalog and schema.
    fn tables<'a>(
        &'a self,
        catalog: &'a str,
        schema: &'a str,
        query_ctx: Option<&'a QueryContext>,
    ) -> BoxStream<'a, Result<TableRef>>;

    /// Check if `schema` is a reserved schema name
    fn is_reserved_schema_name(&self, schema: &str) -> bool {
        // We have to check whether a schema name is reserved before create schema.
        // We need this rather than use schema_exists directly because `pg_catalog` is
        // only visible via postgres protocol. So if we don't check, a mysql client may
        // create a schema named `pg_catalog` which is somehow malformed.
        schema == INFORMATION_SCHEMA_NAME || schema == PG_CATALOG_NAME
    }
}

pub type CatalogManagerRef = Arc<dyn CatalogManager>;

/// Hook called after system table opening.
pub type OpenSystemTableHook =
    Box<dyn Fn(TableRef) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Register system table request:
/// - When system table is already created and registered, the hook will be called
///     with table ref after opening the system table
/// - When system table is not exists, create and register the table by `create_table_expr` and calls `open_hook` with the created table.
pub struct RegisterSystemTableRequest {
    pub create_table_expr: CreateTableExpr,
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
