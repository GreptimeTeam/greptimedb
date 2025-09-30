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

use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemTable, SchemaProvider, TableProvider,
};
use datafusion::datasource::provider_as_source;
use datafusion::error as df_error;
use datafusion::error::Result as DfResult;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_expr::{LogicalPlan, TableSource};
use futures::TryStreamExt;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::sst_entry::{ManifestSstEntry, PuffinIndexMetaEntry, StorageSstEntry};
use store_api::storage::RegionId;

use crate::error::{DataFusionSnafu, ListStorageSstsSnafu, Result, UnexpectedSnafu};
use crate::region_server::RegionServer;

/// Reserved internal table kinds used.
/// These are recognized by reserved table names and mapped to providers.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
enum InternalTableKind {
    InspectSstManifest,
    InspectSstStorage,
    InspectSstIndexMeta,
}

impl InternalTableKind {
    /// Determine if the name is a reserved internal table (case-insensitive).
    pub fn from_table_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case(ManifestSstEntry::reserved_table_name_for_inspection()) {
            return Some(Self::InspectSstManifest);
        }
        if name.eq_ignore_ascii_case(StorageSstEntry::reserved_table_name_for_inspection()) {
            return Some(Self::InspectSstStorage);
        }
        if name.eq_ignore_ascii_case(PuffinIndexMetaEntry::reserved_table_name_for_inspection()) {
            return Some(Self::InspectSstIndexMeta);
        }
        None
    }

    /// Return the `TableProvider` for the internal table.
    pub async fn table_provider(&self, server: &RegionServer) -> Result<Arc<dyn TableProvider>> {
        match self {
            Self::InspectSstManifest => server.inspect_sst_manifest_provider().await,
            Self::InspectSstStorage => server.inspect_sst_storage_provider().await,
            Self::InspectSstIndexMeta => server.inspect_sst_index_meta_provider().await,
        }
    }
}

impl RegionServer {
    /// Expose SSTs listed in Manifest as an in-memory table for inspection.
    pub async fn inspect_sst_manifest_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let mito = {
            let guard = self.inner.mito_engine.read().unwrap();
            guard.as_ref().cloned().context(UnexpectedSnafu {
                violated: "mito engine not available",
            })?
        };

        let entries = mito.all_ssts_from_manifest().await;
        let schema = ManifestSstEntry::schema().arrow_schema().clone();
        let batch = ManifestSstEntry::to_record_batch(&entries)
            .map_err(DataFusionError::from)
            .context(DataFusionSnafu)?;

        let table = MemTable::try_new(schema, vec![vec![batch]]).context(DataFusionSnafu)?;
        Ok(Arc::new(table))
    }

    /// Expose SSTs found in storage as an in-memory table for inspection.
    pub async fn inspect_sst_storage_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let mito = {
            let guard = self.inner.mito_engine.read().unwrap();
            guard.as_ref().cloned().context(UnexpectedSnafu {
                violated: "mito engine not available",
            })?
        };
        let entries = mito
            .all_ssts_from_storage()
            .try_collect::<Vec<_>>()
            .await
            .context(ListStorageSstsSnafu)?;
        let schema = StorageSstEntry::schema().arrow_schema().clone();
        let batch = StorageSstEntry::to_record_batch(&entries)
            .map_err(DataFusionError::from)
            .context(DataFusionSnafu)?;

        let table = MemTable::try_new(schema, vec![vec![batch]]).context(DataFusionSnafu)?;
        Ok(Arc::new(table))
    }

    /// Expose index metadata across the engine as an in-memory table.
    pub async fn inspect_sst_index_meta_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let mito = {
            let guard = self.inner.mito_engine.read().unwrap();
            guard.as_ref().cloned().context(UnexpectedSnafu {
                violated: "mito engine not available",
            })?
        };

        let entries = mito.all_index_metas().await;
        let schema = PuffinIndexMetaEntry::schema().arrow_schema().clone();
        let batch = PuffinIndexMetaEntry::to_record_batch(&entries)
            .map_err(DataFusionError::from)
            .context(DataFusionSnafu)?;

        let table = MemTable::try_new(schema, vec![vec![batch]]).context(DataFusionSnafu)?;
        Ok(Arc::new(table))
    }
}

/// A catalog list that resolves `TableProvider` by table name:
/// - For reserved internal names, return inspection providers;
/// - Otherwise, fall back to the Region provider.
#[derive(Clone, Debug)]
pub(crate) struct NameAwareCatalogList {
    catalog: NameAwareCatalogProvider,
}

impl NameAwareCatalogList {
    /// Creates the catalog list.
    pub fn new(server: RegionServer, region_id: RegionId, query_ctx: QueryContextRef) -> Self {
        let schema_provider = NameAwareSchemaProvider {
            server,
            region_id,
            query_ctx,
        };
        let catalog = NameAwareCatalogProvider {
            schema: schema_provider,
        };
        Self { catalog }
    }
}

impl CatalogProviderList for NameAwareCatalogList {
    fn as_any(&self) -> &dyn std::any::Any {
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
    fn catalog(&self, _name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(self.catalog.clone()))
    }
}

#[derive(Clone, Debug)]
struct NameAwareCatalogProvider {
    schema: NameAwareSchemaProvider,
}

impl CatalogProvider for NameAwareCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema_names(&self) -> Vec<String> {
        vec![]
    }
    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(self.schema.clone()))
    }
}

#[derive(Clone)]
struct NameAwareSchemaProvider {
    server: RegionServer,
    region_id: RegionId,
    query_ctx: QueryContextRef,
}

impl std::fmt::Debug for NameAwareSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NameAwareSchemaProvider")
    }
}

#[async_trait::async_trait]
impl SchemaProvider for NameAwareSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        // Resolve inspect providers by reserved names.
        if let Some(kind) = InternalTableKind::from_table_name(name) {
            return kind
                .table_provider(&self.server)
                .await
                .map(Some)
                .map_err(|e| df_error::DataFusionError::External(Box::new(e)));
        }

        // Fallback to region provider for any other table name.
        let provider = self
            .server
            .table_provider(self.region_id, Some(self.query_ctx.clone()))
            .await
            .map_err(|e| df_error::DataFusionError::External(Box::new(e)))?;
        Ok(Some(provider))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}
/// Builds a `NameAwareDataSourceInjector` from a logical plan.
///
/// It scans the plan to determine:
/// - whether a Region `TableSource` is required, and
/// - which internal inspection sources are referenced.
pub(crate) struct NameAwareDataSourceInjectorBuilder {
    /// Whether the plan requires a Region `TableSource`.
    need_region_provider: bool,
    /// Internal table kinds referenced by the plan.
    reserved_table_needed: Vec<InternalTableKind>,
}

impl NameAwareDataSourceInjectorBuilder {
    /// Walk the `LogicalPlan` to determine whether a Region source is needed,
    /// and collect the kinds of internal sources required.
    pub fn from_plan(plan: &LogicalPlan) -> DfResult<Self> {
        let mut need_region_provider = false;
        let mut reserved_table_needed = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::TableScan(ts) = node {
                let name = ts.table_name.to_string();
                if let Some(kind) = InternalTableKind::from_table_name(&name) {
                    if !reserved_table_needed.contains(&kind) {
                        reserved_table_needed.push(kind);
                    }
                } else {
                    // Any normal table scan implies a Region source is needed.
                    need_region_provider = true;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(Self {
            need_region_provider,
            reserved_table_needed,
        })
    }

    pub async fn build(
        self,
        server: &RegionServer,
        region_id: RegionId,
        query_ctx: QueryContextRef,
    ) -> Result<NameAwareDataSourceInjector> {
        let region = if self.need_region_provider {
            let provider = server.table_provider(region_id, Some(query_ctx)).await?;
            Some(provider_as_source(provider))
        } else {
            None
        };

        let mut reserved_sources = HashMap::new();
        for kind in &self.reserved_table_needed {
            let provider = kind.table_provider(server).await?;
            reserved_sources.insert(*kind, provider_as_source(provider));
        }

        Ok(NameAwareDataSourceInjector {
            reserved_sources,
            region_source: region,
        })
    }
}

/// Rewrites `LogicalPlan` to inject proper data sources for `TableScan`.
/// Uses internal sources for reserved tables; otherwise uses the Region source.
pub(crate) struct NameAwareDataSourceInjector {
    /// Sources for reserved internal tables, keyed by kind.
    reserved_sources: HashMap<InternalTableKind, Arc<dyn TableSource>>,
    /// Optional Region-level source used for normal tables.
    region_source: Option<Arc<dyn TableSource>>,
}

impl TreeNodeRewriter for NameAwareDataSourceInjector {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        Ok(match node {
            LogicalPlan::TableScan(mut scan) => {
                let name = scan.table_name.to_string();
                if let Some(kind) = InternalTableKind::from_table_name(&name)
                    && let Some(source) = self.reserved_sources.get(&kind)
                {
                    // Matched a reserved internal table: rewrite to its dedicated source.
                    scan.source = source.clone();
                } else {
                    let Some(region) = &self.region_source else {
                        // Region source required but not constructed; this is unexpected.
                        return Err(datafusion::error::DataFusionError::Plan(
                            "region provider not available".to_string(),
                        ));
                    };
                    // Normal table: rewrite to the Region source.
                    scan.source = region.clone();
                }
                Transformed::yes(LogicalPlan::TableScan(scan))
            }
            _ => Transformed::no(node),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::catalog::MemTable as DfMemTable;
    use datafusion_common::tree_node::TreeNode;
    use datafusion_expr::{LogicalPlanBuilder, table_scan};
    use datatypes::arrow::array::Int32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*; // bring rewrite() into scope

    fn test_schema() -> Schema {
        Schema::new(vec![Field::new("a", DataType::Int32, true)])
    }

    fn empty_mem_table() -> Arc<DfMemTable> {
        let schema = Arc::new(test_schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )
        .unwrap();
        Arc::new(DfMemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    #[test]
    fn test_injector_builder_from_plan_flags() {
        let schema = test_schema();
        let reserved = ManifestSstEntry::reserved_table_name_for_inspection();
        // plan1: reserved table scan only
        let plan1 = table_scan(Some(reserved), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let b1 = NameAwareDataSourceInjectorBuilder::from_plan(&plan1).unwrap();
        assert!(!b1.need_region_provider);
        assert_eq!(
            b1.reserved_table_needed,
            vec![InternalTableKind::InspectSstManifest]
        );

        // plan2: normal table scan only
        let plan2 = table_scan(Some("normal_table"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let b2 = NameAwareDataSourceInjectorBuilder::from_plan(&plan2).unwrap();
        assert!(b2.need_region_provider);
        assert!(b2.reserved_table_needed.is_empty());

        // plan3: both reserved and normal (via UNION)
        let p_res = table_scan(Some(reserved), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let p_norm = table_scan(Some("normal_table"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let plan3 = LogicalPlanBuilder::from(p_res)
            .union(LogicalPlanBuilder::from(p_norm).build().unwrap())
            .unwrap()
            .build()
            .unwrap();
        let b3 = NameAwareDataSourceInjectorBuilder::from_plan(&plan3).unwrap();
        assert!(b3.need_region_provider);
        assert_eq!(
            b3.reserved_table_needed,
            vec![InternalTableKind::InspectSstManifest]
        );
    }

    #[test]
    fn test_rewriter_replaces_with_reserved_source() {
        let schema = test_schema();
        let table_name = ManifestSstEntry::reserved_table_name_for_inspection();
        let plan = table_scan(Some(table_name), &schema, None)
            .unwrap()
            .build()
            .unwrap();

        let provider = empty_mem_table();
        let source = provider_as_source(provider);

        let mut injector = NameAwareDataSourceInjector {
            reserved_sources: {
                let mut m = HashMap::new();
                m.insert(InternalTableKind::InspectSstManifest, source.clone());
                m
            },
            region_source: None,
        };

        let transformed = plan.rewrite(&mut injector).unwrap();
        let new_plan = transformed.data;

        if let LogicalPlan::TableScan(scan) = new_plan {
            // Compare the underlying Arc ptrs to ensure replacement happened
            let src_ptr = Arc::as_ptr(&scan.source);
            let want_ptr = Arc::as_ptr(&source);
            assert!(std::ptr::eq(src_ptr, want_ptr));
        } else {
            panic!("expected TableScan after rewrite");
        }
    }

    #[test]
    fn test_rewriter_replaces_with_region_source_for_normal() {
        let schema = test_schema();
        let plan = table_scan(Some("normal_table"), &schema, None)
            .unwrap()
            .build()
            .unwrap();

        let provider = empty_mem_table();
        let region_source = provider_as_source(provider);

        let mut injector = NameAwareDataSourceInjector {
            reserved_sources: HashMap::new(),
            region_source: Some(region_source.clone()),
        };

        let transformed = plan.rewrite(&mut injector).unwrap();
        let new_plan = transformed.data;

        if let LogicalPlan::TableScan(scan) = new_plan {
            let src_ptr = Arc::as_ptr(&scan.source);
            let want_ptr = Arc::as_ptr(&region_source);
            assert!(std::ptr::eq(src_ptr, want_ptr));
        } else {
            panic!("expected TableScan after rewrite");
        }
    }
}
