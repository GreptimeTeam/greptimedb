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

//! Dummy catalog for region server.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::format_full_table_name;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableFunction};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use session::context::QueryContextRef;
use snafu::OptionExt;
use table::table::adapter::DfTableProviderAdapter;

use crate::CatalogManagerRef;
use crate::error::TableNotExistSnafu;

/// Delegate the resolving requests to the `[CatalogManager]` unconditionally.
#[derive(Clone)]
pub struct DummyCatalogList {
    catalog_manager: CatalogManagerRef,
    query_ctx: Option<QueryContextRef>,
    table_function: Option<Arc<TableFunction>>,
}

impl DummyCatalogList {
    /// Creates a new catalog list with the given catalog manager (no query context).
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self {
            catalog_manager,
            query_ctx: None,
            table_function: None,
        }
    }

    /// Creates a new catalog list with the given catalog manager and query context.
    pub fn new_with_query_ctx(
        catalog_manager: CatalogManagerRef,
        query_ctx: QueryContextRef,
    ) -> Self {
        Self {
            catalog_manager,
            query_ctx: Some(query_ctx),
            table_function: None,
        }
    }

    /// Adds the exact table function used to reconstruct legacy persisted-view markers.
    pub(super) fn with_persisted_view_table_function(
        mut self,
        table_function: Option<Arc<TableFunction>>,
    ) -> Self {
        self.table_function = table_function;
        self
    }
}

impl fmt::Debug for DummyCatalogList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DummyCatalogList").finish()
    }
}

impl CatalogProviderList for DummyCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![]
    }

    fn catalog(&self, catalog_name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(DummyCatalogProvider {
            catalog_name: catalog_name.to_string(),
            catalog_manager: self.catalog_manager.clone(),
            query_ctx: self.query_ctx.clone(),
            table_function: self.table_function.clone(),
        }))
    }
}

/// A dummy catalog provider for [DummyCatalogList].
#[derive(Clone)]
struct DummyCatalogProvider {
    catalog_name: String,
    catalog_manager: CatalogManagerRef,
    query_ctx: Option<QueryContextRef>,
    table_function: Option<Arc<TableFunction>>,
}

impl CatalogProvider for DummyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, schema_name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(DummySchemaProvider {
            catalog_name: self.catalog_name.clone(),
            schema_name: schema_name.to_string(),
            catalog_manager: self.catalog_manager.clone(),
            query_ctx: self.query_ctx.clone(),
            table_function: self.table_function.clone(),
        }))
    }
}

impl fmt::Debug for DummyCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DummyCatalogProvider")
            .field("catalog_name", &self.catalog_name)
            .finish()
    }
}

/// A dummy schema provider for [DummyCatalogList].
#[derive(Clone)]
struct DummySchemaProvider {
    catalog_name: String,
    schema_name: String,
    catalog_manager: CatalogManagerRef,
    query_ctx: Option<QueryContextRef>,
    table_function: Option<Arc<TableFunction>>,
}

#[async_trait]
impl SchemaProvider for DummySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        if name == "pg_get_keywords()" {
            let table_function = self.table_function.as_ref().ok_or_else(|| {
                DataFusionError::Plan(
                    "persisted view table function 'pg_get_keywords' is not available".to_string(),
                )
            })?;
            if table_function.name() != "pg_get_keywords" {
                return Err(DataFusionError::Plan(format!(
                    "persisted view table function 'pg_get_keywords' was injected as '{}'",
                    table_function.name()
                )));
            }
            return Ok(Some(table_function.create_table_provider(&[])?));
        }

        let table = self
            .catalog_manager
            .table(
                &self.catalog_name,
                &self.schema_name,
                name,
                self.query_ctx.as_deref(),
            )
            .await?
            .with_context(|| TableNotExistSnafu {
                table: format_full_table_name(&self.catalog_name, &self.schema_name, name),
            })?;

        let table_provider: Arc<dyn TableProvider> = Arc::new(DfTableProviderAdapter::new(table));

        Ok(Some(table_provider))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}

impl fmt::Debug for DummySchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DummySchemaProvider")
            .field("catalog_name", &self.catalog_name)
            .field("schema_name", &self.schema_name)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datafusion::catalog::TableFunctionImpl;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::Expr;
    use datatypes::schema::Schema as GreptimeSchema;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable as GreptimeEmptyTable;

    use super::*;
    use crate::RegisterTableRequest;
    use crate::memory::MemoryCatalogManager;

    #[derive(Debug)]
    struct TestTableFunction {
        fail: bool,
    }

    impl TableFunctionImpl for TestTableFunction {
        fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
            assert!(args.is_empty());
            if self.fail {
                return Err(DataFusionError::Plan(
                    "test table function failure".to_string(),
                ));
            }
            Ok(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))))
        }
    }

    fn schema_provider(table_function: Option<Arc<TableFunction>>) -> DummySchemaProvider {
        DummySchemaProvider {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            catalog_manager: MemoryCatalogManager::with_default_setup(),
            query_ctx: None,
            table_function,
        }
    }

    fn catalog_manager_with_sentinel() -> CatalogManagerRef {
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        let table_info = TableInfoBuilder::new(
            "pg_get_keywords()",
            TableMetaBuilder::empty()
                .schema(Arc::new(GreptimeSchema::new(vec![])))
                .primary_key_indices(vec![])
                .value_indices(vec![])
                .next_column_id(1024)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "pg_get_keywords()".to_string(),
                table_id: 1024,
                table: GreptimeEmptyTable::from_table_info(&table_info),
            })
            .unwrap();
        catalog_manager
    }

    #[tokio::test]
    async fn test_resolve_persisted_view_table_function() {
        let table_function = Arc::new(TableFunction::new(
            "pg_get_keywords".to_string(),
            Arc::new(TestTableFunction { fail: false }),
        ));
        assert!(
            schema_provider(Some(table_function))
                .table("pg_get_keywords()")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_persisted_view_table_function_errors_do_not_fallback() {
        // The generic dummy catalog constructor deliberately has no injected table function.
        let generic_catalog = DummyCatalogList::new(catalog_manager_with_sentinel());
        let missing = generic_catalog
            .catalog("greptime")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("pg_get_keywords()")
            .await
            .unwrap_err();
        assert!(missing.to_string().contains("not available"));

        let unknown = generic_catalog
            .catalog("greptime")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("other()")
            .await
            .unwrap_err();
        assert!(unknown.to_string().contains("greptime.public.other()"));
        assert!(!unknown.to_string().contains("not available"));

        let mismatched = Arc::new(TableFunction::new(
            "another_function".to_string(),
            Arc::new(TestTableFunction { fail: false }),
        ));
        let mismatched = schema_provider(Some(mismatched))
            .table("pg_get_keywords()")
            .await
            .unwrap_err();
        assert!(mismatched.to_string().contains("was injected as"));

        let failing = Arc::new(TableFunction::new(
            "pg_get_keywords".to_string(),
            Arc::new(TestTableFunction { fail: true }),
        ));
        let failing = schema_provider(Some(failing))
            .table("pg_get_keywords()")
            .await
            .unwrap_err();
        assert_eq!(
            failing.to_string(),
            "Error during planning: test table function failure"
        );

        assert!(
            schema_provider(None)
                .table("ordinary_unknown_table")
                .await
                .is_err()
        );
    }
}
