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

use futures::stream::BoxStream;
use session::context::QueryContext;
use table::metadata::{TableId, TableInfoRef};
use table::TableRef;

use crate::error::Result;
use crate::{CatalogManager, CatalogManagerRef};

/// A dummy catalog manager that always returns empty results.
///
/// Used to fill the arg of `QueryEngineFactory::new_with_plugins` in datanode.
pub struct DummyCatalogManager;

impl DummyCatalogManager {
    /// Returns a new `CatalogManagerRef` instance.
    pub fn arc() -> CatalogManagerRef {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl CatalogManager for DummyCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn schema_names(
        &self,
        _catalog: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn table_names(
        &self,
        _catalog: &str,
        _schema: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn catalog_exists(&self, _catalog: &str) -> Result<bool> {
        Ok(false)
    }

    async fn schema_exists(
        &self,
        _catalog: &str,
        _schema: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn table_exists(
        &self,
        _catalog: &str,
        _schema: &str,
        _table: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn table(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_name: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> Result<Option<TableRef>> {
        Ok(None)
    }

    async fn table_id(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_name: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> Result<Option<TableId>> {
        Ok(None)
    }

    async fn table_info_by_id(&self, _table_id: TableId) -> Result<Option<TableInfoRef>> {
        Ok(None)
    }

    async fn tables_by_ids(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_ids: &[TableId],
    ) -> Result<Vec<TableRef>> {
        Ok(vec![])
    }

    fn tables<'a>(
        &'a self,
        _catalog: &'a str,
        _schema: &'a str,
        _query_ctx: Option<&'a QueryContext>,
    ) -> BoxStream<'a, Result<TableRef>> {
        Box::pin(futures::stream::empty())
    }
}
