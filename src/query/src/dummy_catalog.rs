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
use std::sync::{Arc, Mutex};

use api::v1::SemanticType;
use async_trait::async_trait;
use catalog::error::Result as CatalogResult;
use catalog::{CatalogManager, CatalogManagerRef};
use common_recordbatch::OrderOption;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, Session};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datatypes::arrow::datatypes::SchemaRef;
use futures::stream::BoxStream;
use session::context::{QueryContext, QueryContextRef};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngineRef;
use store_api::storage::{
    RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector, VectorSearchRequest,
};
use table::TableRef;
use table::metadata::{TableId, TableInfoRef};
use table::table::scan::RegionScanExec;

use crate::error::{GetRegionMetadataSnafu, Result};

/// Resolve to the given region (specified by [RegionId]) unconditionally.
#[derive(Clone, Debug)]
pub struct DummyCatalogList {
    catalog: DummyCatalogProvider,
}

impl DummyCatalogList {
    /// Creates a new catalog list with the given table provider.
    pub fn with_table_provider(table_provider: Arc<dyn TableProvider>) -> Self {
        let schema_provider = DummySchemaProvider {
            table: table_provider,
        };
        let catalog_provider = DummyCatalogProvider {
            schema: schema_provider,
        };
        Self {
            catalog: catalog_provider,
        }
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

    fn catalog(&self, _name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(self.catalog.clone()))
    }
}

/// A dummy catalog provider for [DummyCatalogList].
#[derive(Clone, Debug)]
struct DummyCatalogProvider {
    schema: DummySchemaProvider,
}

impl CatalogProvider for DummyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(self.schema.clone()))
    }
}

/// A dummy schema provider for [DummyCatalogList].
#[derive(Clone, Debug)]
struct DummySchemaProvider {
    table: Arc<dyn TableProvider>,
}

#[async_trait]
impl SchemaProvider for DummySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Ok(Some(self.table.clone()))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}

/// For [TableProvider] and [DummyCatalogList]
#[derive(Clone)]
pub struct DummyTableProvider {
    region_id: RegionId,
    engine: RegionEngineRef,
    metadata: RegionMetadataRef,
    /// Keeping a mutable request makes it possible to change in the optimize phase.
    scan_request: Arc<Mutex<ScanRequest>>,
    query_ctx: Option<QueryContextRef>,
}

impl fmt::Debug for DummyTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DummyTableProvider")
            .field("region_id", &self.region_id)
            .field("metadata", &self.metadata)
            .field("scan_request", &self.scan_request)
            .finish()
    }
}

#[async_trait]
impl TableProvider for DummyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.metadata.schema.arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut request = self.scan_request.lock().unwrap().clone();
        request.projection = projection.cloned();
        request.filters = filters.to_vec();
        request.limit = limit;

        let scanner = self
            .engine
            .handle_query(self.region_id, request.clone())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let query_memory_permit = self.engine.register_query_memory_permit();
        let mut scan_exec = RegionScanExec::new(scanner, request, query_memory_permit)?;
        if let Some(query_ctx) = &self.query_ctx {
            scan_exec.set_explain_verbose(query_ctx.explain_verbose());
        }
        Ok(Arc::new(scan_exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        let supported = filters
            .iter()
            .map(|e| {
                // Simple filter on primary key columns are precisely evaluated.
                if let Some(simple_filter) = SimpleFilterEvaluator::try_new(e) {
                    if self
                        .metadata
                        .column_by_name(simple_filter.column_name())
                        .and_then(|c| {
                            (c.semantic_type == SemanticType::Tag
                                || c.semantic_type == SemanticType::Timestamp)
                                .then_some(())
                        })
                        .is_some()
                    {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Inexact
                    }
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect();
        Ok(supported)
    }
}

impl DummyTableProvider {
    /// Creates a new provider.
    pub fn new(region_id: RegionId, engine: RegionEngineRef, metadata: RegionMetadataRef) -> Self {
        Self {
            region_id,
            engine,
            metadata,
            scan_request: Default::default(),
            query_ctx: None,
        }
    }

    pub fn region_metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    /// Sets the ordering hint of the query to the provider.
    pub fn with_ordering_hint(&self, order_opts: &[OrderOption]) {
        self.scan_request.lock().unwrap().output_ordering = Some(order_opts.to_vec());
    }

    /// Sets the distribution hint of the query to the provider.
    pub fn with_distribution(&self, distribution: TimeSeriesDistribution) {
        self.scan_request.lock().unwrap().distribution = Some(distribution);
    }

    /// Sets the time series selector hint of the query to the provider.
    pub fn with_time_series_selector_hint(&self, selector: TimeSeriesRowSelector) {
        self.scan_request.lock().unwrap().series_row_selector = Some(selector);
    }

    pub fn with_vector_search_hint(&self, hint: VectorSearchRequest) {
        self.scan_request.lock().unwrap().vector_search = Some(hint);
    }

    pub fn get_vector_search_hint(&self) -> Option<VectorSearchRequest> {
        self.scan_request.lock().unwrap().vector_search.clone()
    }

    pub fn with_sequence(&self, sequence: u64) {
        self.scan_request.lock().unwrap().memtable_max_sequence = Some(sequence);
    }

    /// Gets the scan request of the provider.
    #[cfg(test)]
    pub fn scan_request(&self) -> ScanRequest {
        self.scan_request.lock().unwrap().clone()
    }
}

pub struct DummyTableProviderFactory;

impl DummyTableProviderFactory {
    pub async fn create_table_provider(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
        query_ctx: Option<QueryContextRef>,
    ) -> Result<DummyTableProvider> {
        let metadata =
            engine
                .get_metadata(region_id)
                .await
                .with_context(|_| GetRegionMetadataSnafu {
                    engine: engine.name(),
                    region_id,
                })?;

        let scan_request = query_ctx
            .as_ref()
            .map(|ctx| ScanRequest {
                memtable_max_sequence: ctx.get_snapshot(region_id.as_u64()),
                sst_min_sequence: ctx.sst_min_sequence(region_id.as_u64()),
                ..Default::default()
            })
            .unwrap_or_default();

        Ok(DummyTableProvider {
            region_id,
            engine,
            metadata,
            scan_request: Arc::new(Mutex::new(scan_request)),
            query_ctx,
        })
    }
}

#[async_trait]
impl TableProviderFactory for DummyTableProviderFactory {
    async fn create(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
        ctx: Option<QueryContextRef>,
    ) -> Result<Arc<dyn TableProvider>> {
        let provider = self.create_table_provider(region_id, engine, ctx).await?;
        Ok(Arc::new(provider))
    }
}

#[async_trait]
pub trait TableProviderFactory: Send + Sync {
    async fn create(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
        ctx: Option<QueryContextRef>,
    ) -> Result<Arc<dyn TableProvider>>;
}

pub type TableProviderFactoryRef = Arc<dyn TableProviderFactory>;

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

    async fn catalog_names(&self) -> CatalogResult<Vec<String>> {
        Ok(vec![])
    }

    async fn schema_names(
        &self,
        _catalog: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Vec<String>> {
        Ok(vec![])
    }

    async fn table_names(
        &self,
        _catalog: &str,
        _schema: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Vec<String>> {
        Ok(vec![])
    }

    async fn catalog_exists(&self, _catalog: &str) -> CatalogResult<bool> {
        Ok(false)
    }

    async fn schema_exists(
        &self,
        _catalog: &str,
        _schema: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<bool> {
        Ok(false)
    }

    async fn table_exists(
        &self,
        _catalog: &str,
        _schema: &str,
        _table: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<bool> {
        Ok(false)
    }

    async fn table(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_name: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Option<TableRef>> {
        Ok(None)
    }

    async fn table_id(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_name: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Option<TableId>> {
        Ok(None)
    }

    async fn table_info_by_id(&self, _table_id: TableId) -> CatalogResult<Option<TableInfoRef>> {
        Ok(None)
    }

    async fn tables_by_ids(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_ids: &[TableId],
    ) -> CatalogResult<Vec<TableRef>> {
        Ok(vec![])
    }

    fn tables<'a>(
        &'a self,
        _catalog: &'a str,
        _schema: &'a str,
        _query_ctx: Option<&'a QueryContext>,
    ) -> BoxStream<'a, CatalogResult<TableRef>> {
        Box::pin(futures::stream::empty())
    }
}
