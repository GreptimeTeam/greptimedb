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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_recordbatch::OrderOption;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datatypes::arrow::datatypes::SchemaRef;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngineRef;
use store_api::storage::{RegionId, ScanRequest, TimeSeriesRowSelector};
use table::table::scan::RegionScanExec;

use crate::error::{GetRegionMetadataSnafu, Result};

/// Resolve to the given region (specified by [RegionId]) unconditionally.
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut request = self.scan_request.lock().unwrap().clone();
        request.projection = match projection {
            Some(x) if !x.is_empty() => Some(x.clone()),
            _ => None,
        };
        request.filters = filters.to_vec();
        request.limit = limit;

        let scanner = self
            .engine
            .handle_query(self.region_id, request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Arc::new(RegionScanExec::new(scanner)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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
        }
    }

    pub fn region_metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    /// Sets the ordering hint of the query to the provider.
    pub fn with_ordering_hint(&self, order_opts: &[OrderOption]) {
        self.scan_request.lock().unwrap().output_ordering = Some(order_opts.to_vec());
    }

    /// Sets the time series selector hint of the query to the provider.
    pub fn with_time_series_selector_hint(&self, selector: TimeSeriesRowSelector) {
        self.scan_request.lock().unwrap().series_row_selector = Some(selector);
    }

    /// Gets the scan request of the provider.
    #[cfg(test)]
    pub fn scan_request(&self) -> ScanRequest {
        self.scan_request.lock().unwrap().clone()
    }
}

pub struct DummyTableProviderFactory;

#[async_trait]
impl TableProviderFactory for DummyTableProviderFactory {
    async fn create(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
    ) -> Result<Arc<dyn TableProvider>> {
        let metadata =
            engine
                .get_metadata(region_id)
                .await
                .with_context(|_| GetRegionMetadataSnafu {
                    engine: engine.name(),
                    region_id,
                })?;
        Ok(Arc::new(DummyTableProvider {
            region_id,
            engine,
            metadata,
            scan_request: Default::default(),
        }))
    }
}

#[async_trait]
pub trait TableProviderFactory: Send + Sync {
    async fn create(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
    ) -> Result<Arc<dyn TableProvider>>;
}

pub type TableProviderFactoryRef = Arc<dyn TableProviderFactory>;
