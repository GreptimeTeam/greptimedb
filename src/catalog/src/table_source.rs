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
use common_query::logical_plan::{rename_logical_plan_columns, SubstraitPlanDecoderRef};
use datafusion::common::{ResolvedTableReference, TableReference};
use datafusion::datasource::view::ViewTable;
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::logical_expr::TableSource;
use itertools::Itertools;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
pub mod dummy_catalog;
use dummy_catalog::DummyCatalogList;
use table::TableRef;

use crate::error::{
    CastManagerSnafu, DecodePlanSnafu, GetViewCacheSnafu, ProjectViewColumnsSnafu,
    QueryAccessDeniedSnafu, Result, TableNotExistSnafu, ViewInfoNotFoundSnafu,
    ViewPlanColumnsChangedSnafu,
};
use crate::kvbackend::KvBackendCatalogManager;
use crate::CatalogManagerRef;

pub struct DfTableSourceProvider {
    catalog_manager: CatalogManagerRef,
    resolved_tables: HashMap<String, Arc<dyn TableSource>>,
    disallow_cross_catalog_query: bool,
    default_catalog: String,
    default_schema: String,
    query_ctx: QueryContextRef,
    plan_decoder: SubstraitPlanDecoderRef,
    enable_ident_normalization: bool,
}

impl DfTableSourceProvider {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        disallow_cross_catalog_query: bool,
        query_ctx: QueryContextRef,
        plan_decoder: SubstraitPlanDecoderRef,
        enable_ident_normalization: bool,
    ) -> Self {
        Self {
            catalog_manager,
            disallow_cross_catalog_query,
            resolved_tables: HashMap::new(),
            default_catalog: query_ctx.current_catalog().to_owned(),
            default_schema: query_ctx.current_schema(),
            query_ctx,
            plan_decoder,
            enable_ident_normalization,
        }
    }

    /// Returns the query context.
    pub fn query_ctx(&self) -> &QueryContextRef {
        &self.query_ctx
    }

    pub fn resolve_table_ref(&self, table_ref: TableReference) -> Result<ResolvedTableReference> {
        if self.disallow_cross_catalog_query {
            match &table_ref {
                TableReference::Bare { .. } | TableReference::Partial { .. } => {}
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
            .table(catalog_name, schema_name, table_name, Some(&self.query_ctx))
            .await?
            .with_context(|| TableNotExistSnafu {
                table: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let provider: Arc<dyn TableProvider> = if table.table_info().table_type == TableType::View {
            self.create_view_provider(&table).await?
        } else {
            Arc::new(DfTableProviderAdapter::new(table))
        };

        let source = provider_as_source(provider);

        let _ = self.resolved_tables.insert(resolved_name, source.clone());
        Ok(source)
    }

    async fn create_view_provider(&self, table: &TableRef) -> Result<Arc<dyn TableProvider>> {
        let catalog_manager = self
            .catalog_manager
            .as_any()
            .downcast_ref::<KvBackendCatalogManager>()
            .context(CastManagerSnafu)?;

        let view_info = catalog_manager
            .view_info_cache()?
            .get(table.table_info().ident.table_id)
            .await
            .context(GetViewCacheSnafu)?
            .context(ViewInfoNotFoundSnafu {
                name: &table.table_info().name,
            })?;

        // Build the catalog list provider for deserialization.
        let catalog_list = Arc::new(DummyCatalogList::new(self.catalog_manager.clone()));
        let logical_plan = self
            .plan_decoder
            .decode(Bytes::from(view_info.view_info.clone()), catalog_list, true)
            .await
            .context(DecodePlanSnafu {
                name: &table.table_info().name,
            })?;

        let columns: Vec<_> = view_info.columns.iter().map(|c| c.as_str()).collect();

        let original_plan_columns: Vec<_> =
            view_info.plan_columns.iter().map(|c| c.as_str()).collect();

        let plan_columns: Vec<_> = logical_plan
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name)
            .collect();

        // Only check columns number, because substrait doesn't include aliases currently.
        // See https://github.com/apache/datafusion/issues/10815#issuecomment-2158666881
        // and https://github.com/apache/datafusion/issues/6489
        // TODO(dennis): check column names
        ensure!(
            original_plan_columns.len() == plan_columns.len(),
            ViewPlanColumnsChangedSnafu {
                origin_names: original_plan_columns.iter().join(","),
                actual_names: plan_columns.iter().join(","),
            }
        );

        // We have to do `columns` projection here, because
        // substrait doesn't include aliases neither for tables nor for columns:
        // https://github.com/apache/datafusion/issues/10815#issuecomment-2158666881
        let logical_plan = if !columns.is_empty() {
            rename_logical_plan_columns(
                self.enable_ident_normalization,
                logical_plan,
                plan_columns
                    .iter()
                    .map(|c| c.as_str())
                    .zip(columns.into_iter())
                    .collect(),
            )
            .context(ProjectViewColumnsSnafu)?
        } else {
            logical_plan
        };

        Ok(Arc::new(ViewTable::new(
            logical_plan,
            Some(view_info.definition.to_string()),
        )))
    }
}

#[cfg(test)]
mod tests {
    use common_query::test_util::DummyDecoder;
    use session::context::QueryContext;

    use super::*;
    use crate::kvbackend::KvBackendCatalogManagerBuilder;
    use crate::memory::MemoryCatalogManager;

    #[test]
    fn test_validate_table_ref() {
        let query_ctx = Arc::new(QueryContext::with("greptime", "public"));

        let table_provider = DfTableSourceProvider::new(
            MemoryCatalogManager::with_default_setup(),
            true,
            query_ctx.clone(),
            DummyDecoder::arc(),
            true,
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

    use std::collections::HashSet;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
    use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_query::error::Result as QueryResult;
    use common_query::logical_plan::SubstraitPlanDecoder;
    use datafusion::catalog::CatalogProviderList;
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};

    use crate::information_schema::NoopInformationExtension;

    struct MockDecoder;
    impl MockDecoder {
        pub fn arc() -> Arc<Self> {
            Arc::new(MockDecoder)
        }
    }

    #[async_trait::async_trait]
    impl SubstraitPlanDecoder for MockDecoder {
        async fn decode(
            &self,
            _message: bytes::Bytes,
            _catalog_list: Arc<dyn CatalogProviderList>,
            _optimize: bool,
        ) -> QueryResult<LogicalPlan> {
            Ok(mock_plan())
        }
    }

    fn mock_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let table_source = LogicalTableSource::new(SchemaRef::new(schema));

        let projection = None;

        let builder =
            LogicalPlanBuilder::scan("person", Arc::new(table_source), projection).unwrap();

        builder
            .filter(col("id").gt(lit(500)))
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_resolve_view() {
        let query_ctx = Arc::new(QueryContext::with("greptime", "public"));
        let backend = Arc::new(MemoryKvBackend::default());
        let layered_cache_builder = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(CacheRegistryBuilder::default().build());
        let fundamental_cache_registry = build_fundamental_cache_registry(backend.clone());
        let layered_cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        let catalog_manager = KvBackendCatalogManagerBuilder::new(
            Arc::new(NoopInformationExtension),
            backend.clone(),
            layered_cache_registry,
        )
        .build();

        let table_metadata_manager = TableMetadataManager::new(backend);
        let mut view_info = common_meta::key::test_utils::new_test_table_info(1024, vec![]);
        view_info.table_type = TableType::View;
        let logical_plan = vec![1, 2, 3];
        // Create view metadata
        table_metadata_manager
            .create_view_metadata(
                view_info.clone().into(),
                logical_plan,
                HashSet::new(),
                vec!["a".to_string(), "b".to_string()],
                vec!["id".to_string(), "name".to_string()],
                "definition".to_string(),
            )
            .await
            .unwrap();

        let mut table_provider = DfTableSourceProvider::new(
            catalog_manager,
            true,
            query_ctx.clone(),
            MockDecoder::arc(),
            true,
        );

        // View not found
        let table_ref = TableReference::bare("not_exists_view");
        assert!(table_provider.resolve_table(table_ref).await.is_err());

        let table_ref = TableReference::bare(view_info.name);
        let source = table_provider.resolve_table(table_ref).await.unwrap();
        assert_eq!(
            r#"
Projection: person.id AS a, person.name AS b
  Filter: person.id > Int32(500)
    TableScan: person"#,
            format!("\n{}", source.get_logical_plan().unwrap())
        );
    }
}
