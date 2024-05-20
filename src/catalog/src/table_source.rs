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

use bytes::Bytes;
use common_catalog::format_full_table_name;
use common_query::logical_plan::SubstraitPlanDecoderRef;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::common::{ResolvedTableReference, TableReference};
use datafusion::datasource::view::ViewTable;
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::logical_expr::TableSource;
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
mod memory_catalog;
use datafusion::catalog::CatalogProvider;
use memory_catalog::MemoryCatalogProviderList;

use crate::error::{
    CastManagerSnafu, DatafusionSnafu, DecodePlanSnafu, GetTableCacheSnafu, QueryAccessDeniedSnafu,
    Result, TableNotExistSnafu, ViewInfoNotFoundSnafu,
};
use crate::CatalogManagerRef;

pub struct DfTableSourceProvider {
    catalog_manager: CatalogManagerRef,
    resolved_tables: HashMap<String, Arc<dyn TableSource>>,
    disallow_cross_catalog_query: bool,
    default_catalog: String,
    default_schema: String,
    plan_decoder: SubstraitPlanDecoderRef,
}

impl DfTableSourceProvider {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        disallow_cross_catalog_query: bool,
        query_ctx: &QueryContext,
        plan_decoder: SubstraitPlanDecoderRef,
    ) -> Self {
        Self {
            catalog_manager,
            disallow_cross_catalog_query,
            resolved_tables: HashMap::new(),
            default_catalog: query_ctx.current_catalog().to_owned(),
            default_schema: query_ctx.current_schema().to_owned(),
            plan_decoder,
        }
    }

    pub fn resolve_table_ref(&self, table_ref: TableReference) -> Result<ResolvedTableReference> {
        if self.disallow_cross_catalog_query {
            match &table_ref {
                TableReference::Bare { .. } => (),
                TableReference::Partial { .. } => {}
                TableReference::Full {
                    catalog, schema, ..
                } => {
                    ensure!(
                        catalog.as_ref() == self.default_catalog,
                        QueryAccessDeniedSnafu {
                            catalog: catalog.as_ref(),
                            schema: schema.as_ref(),
                        }
                    );
                }
            };
        }

        Ok(table_ref.resolve(&self.default_catalog, &self.default_schema))
    }

    pub async fn resolve_table(
        &mut self,
        table_ref: TableReference,
    ) -> Result<Arc<dyn TableSource>> {
        let table_ref = self.resolve_table_ref(table_ref)?;

        let resolved_name = table_ref.to_string();
        if let Some(table) = self.resolved_tables.get(&resolved_name) {
            return Ok(table.clone());
        }

        let catalog_name = table_ref.catalog.as_ref();
        let schema_name = table_ref.schema.as_ref();
        let table_name = table_ref.table.as_ref();

        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await?
            .with_context(|| TableNotExistSnafu {
                table: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let provider: Arc<dyn TableProvider> = if table.table_info().table_type == TableType::View {
            use crate::kvbackend::KvBackendCatalogManager;
            let catalog_manager = self
                .catalog_manager
                .as_any()
                .downcast_ref::<KvBackendCatalogManager>()
                .context(CastManagerSnafu)?;

            let view_info = catalog_manager
                .view_info_cache()?
                .get(table.table_info().ident.table_id)
                .await
                .context(GetTableCacheSnafu)?
                .context(ViewInfoNotFoundSnafu {
                    name: &table.table_info().name,
                })?;

            // Build the catalog list provider for deserialization.
            let catalog_provider = Arc::new(MemoryCatalogProvider::new());
            let catalog_list = Arc::new(MemoryCatalogProviderList::with_catalog_provider(
                catalog_provider.clone(),
            ));

            for table_name in &view_info.table_names {
                // We don't have a cross-catalog view.
                debug_assert_eq!(catalog_name, &table_name.catalog_name);

                let catalog_name = &table_name.catalog_name;
                let schema_name = &table_name.schema_name;
                let table_name = &table_name.table_name;

                let table = self
                    .catalog_manager
                    .table(catalog_name, schema_name, table_name)
                    .await?
                    .with_context(|| TableNotExistSnafu {
                        table: format_full_table_name(catalog_name, schema_name, table_name),
                    })?;

                let schema_provider =
                    if let Some(schema_provider) = catalog_provider.schema(schema_name) {
                        schema_provider
                    } else {
                        let schema_provider = Arc::new(MemorySchemaProvider::new());
                        let _ = catalog_provider
                            .register_schema(schema_name, schema_provider.clone())
                            .context(DatafusionSnafu)?;
                        schema_provider
                    };

                let table_provider: Arc<dyn TableProvider> =
                    Arc::new(DfTableProviderAdapter::new(table));

                let _ = schema_provider
                    .register_table(table_name.to_string(), table_provider)
                    .context(DatafusionSnafu)?;
            }

            let logical_plan = self
                .plan_decoder
                .decode(Bytes::from(view_info.view_info.clone()), catalog_list)
                .await
                .context(DecodePlanSnafu {
                    name: &table.table_info().name,
                })?;

            Arc::new(ViewTable::try_new(logical_plan, None).context(DatafusionSnafu)?)
        } else {
            Arc::new(DfTableProviderAdapter::new(table))
        };

        let source = provider_as_source(provider);

        let _ = self.resolved_tables.insert(resolved_name, source.clone());
        Ok(source)
    }
}

#[cfg(test)]
mod tests {
    use common_query::test_util::DummyDecoder;
    use session::context::QueryContext;

    use super::*;
    use crate::memory::MemoryCatalogManager;

    #[test]
    fn test_validate_table_ref() {
        let query_ctx = &QueryContext::with("greptime", "public");

        let table_provider = DfTableSourceProvider::new(
            MemoryCatalogManager::with_default_setup(),
            true,
            query_ctx,
            DummyDecoder::arc(),
        );

        let table_ref = TableReference::bare("table_name");
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::partial("public", "table_name");
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::partial("wrong_schema", "table_name");
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::full("greptime", "public", "table_name");
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::full("wrong_catalog", "public", "table_name");
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_err());

        let table_ref = TableReference::partial("information_schema", "columns");
        let result = table_provider.resolve_table_ref(table_ref);
        assert!(result.is_ok());

        let table_ref = TableReference::full("greptime", "information_schema", "columns");
        assert!(table_provider.resolve_table_ref(table_ref).is_ok());

        let table_ref = TableReference::full("dummy", "information_schema", "columns");
        assert!(table_provider.resolve_table_ref(table_ref).is_err());

        let table_ref = TableReference::full("greptime", "greptime_private", "columns");
        assert!(table_provider.resolve_table_ref(table_ref).is_ok());
    }
}
