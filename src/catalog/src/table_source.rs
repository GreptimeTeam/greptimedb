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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::consts::INFORMATION_SCHEMA_NAME;
use common_catalog::format_full_table_name;
use datafusion::common::{ResolvedTableReference, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;
use session::context::QueryContext;
use snafu::{ensure, OptionExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    CatalogNotFoundSnafu, QueryAccessDeniedSnafu, Result, SchemaNotFoundSnafu, TableNotExistSnafu,
};
use crate::information_schema::InformationSchemaProvider;
use crate::{CatalogListRef, CatalogManagerRef};

pub struct DfTableSourceProvider {
    catalog_manager: CatalogManagerRef,
    resolved_tables: HashMap<String, Arc<dyn TableSource>>,
    disallow_cross_schema_query: bool,
    default_catalog: String,
    default_schema: String,
}

impl DfTableSourceProvider {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        disallow_cross_schema_query: bool,
        query_ctx: &QueryContext,
    ) -> Self {
        Self {
            catalog_manager,
            disallow_cross_schema_query,
            resolved_tables: HashMap::new(),
            default_catalog: query_ctx.current_catalog(),
            default_schema: query_ctx.current_schema(),
        }
    }

    pub fn resolve_table_ref<'a>(
        &'a self,
        table_ref: TableReference<'a>,
    ) -> Result<ResolvedTableReference<'a>> {
        if self.disallow_cross_schema_query {
            match &table_ref {
                TableReference::Bare { .. } => (),
                TableReference::Partial { schema, .. } => {
                    ensure!(
                        schema.as_ref() == self.default_schema,
                        QueryAccessDeniedSnafu {
                            catalog: &self.default_catalog,
                            schema: schema.as_ref(),
                        }
                    );
                }
                TableReference::Full {
                    catalog, schema, ..
                } => {
                    ensure!(
                        catalog.as_ref() == self.default_catalog
                            && schema.as_ref() == self.default_schema,
                        QueryAccessDeniedSnafu {
                            catalog: catalog.as_ref(),
                            schema: schema.as_ref()
                        }
                    );
                }
            };
        }

        Ok(table_ref.resolve(&self.default_catalog, &self.default_schema))
    }

    pub async fn resolve_table(
        &mut self,
        table_ref: TableReference<'_>,
    ) -> Result<Arc<dyn TableSource>> {
        let table_ref = self.resolve_table_ref(table_ref)?;

        let resolved_name = table_ref.to_string();
        if let Some(table) = self.resolved_tables.get(&resolved_name) {
            return Ok(table.clone());
        }

        let catalog_name = table_ref.catalog.as_ref();
        let schema_name = table_ref.schema.as_ref();
        let table_name = table_ref.table.as_ref();

        let schema = if schema_name != INFORMATION_SCHEMA_NAME {
            let catalog = self
                .catalog_manager
                .catalog(catalog_name)?
                .context(CatalogNotFoundSnafu { catalog_name })?;
            catalog.schema(schema_name)?.context(SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            })?
        } else {
            let catalog_provider = self
                .catalog_manager
                .catalog(catalog_name)?
                .context(CatalogNotFoundSnafu { catalog_name })?;
            Arc::new(InformationSchemaProvider::new(
                catalog_name.to_string(),
                catalog_provider,
            ))
        };
        let table = schema
            .table(table_name)
            .await?
            .with_context(|| TableNotExistSnafu {
                table: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let table = DfTableProviderAdapter::new(table);
        let table = provider_as_source(Arc::new(table));
        self.resolved_tables.insert(resolved_name, table.clone());
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use session::context::QueryContext;

    use super::*;
    use crate::local::MemoryCatalogManager;

    #[test]
    fn test_validate_table_ref() {
        let query_ctx = &QueryContext::with("greptime", "public");

        let table_provider =
            DfTableSourceProvider::new(Arc::new(MemoryCatalogManager::default()), true, query_ctx);

        let table_ref = TableReference::Bare {
            table: Cow::Borrowed("table_name"),
        };
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::Partial {
            schema: Cow::Borrowed("public"),
            table: Cow::Borrowed("table_name"),
        };
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::Partial {
            schema: Cow::Borrowed("wrong_schema"),
            table: Cow::Borrowed("table_name"),
        };
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_err());

        let table_ref = TableReference::Full {
            catalog: Cow::Borrowed("greptime"),
            schema: Cow::Borrowed("public"),
            table: Cow::Borrowed("table_name"),
        };
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::Full {
            catalog: Cow::Borrowed("wrong_catalog"),
            schema: Cow::Borrowed("public"),
            table: Cow::Borrowed("table_name"),
        };
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_err());
    }
}
