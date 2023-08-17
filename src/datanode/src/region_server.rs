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
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::region::QueryRequest;
use async_trait::async_trait;
use common_base::bytes::Bytes;
use common_query::{DfPhysicalPlan, Output};
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::info;
use dashmap::DashMap;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogList, CatalogProvider};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion_expr::{Expr, TableType};
use datatypes::arrow::datatypes::SchemaRef;
use query::QueryEngineRef;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngineRef;
use store_api::region_request::RegionRequest;
use store_api::storage::RegionId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::error::{
    HandleRegionRequestSnafu, RegionEngineNotFoundSnafu, RegionNotFoundSnafu, Result,
    UnsupportedOutputSnafu,
};

#[derive(Default)]
pub struct RegionServer {
    engines: HashMap<String, RegionEngineRef>,
    region_map: DashMap<RegionId, RegionEngineRef>,
    query_engine: QueryEngineRef,
}

impl RegionServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_engine(&mut self, engine: RegionEngineRef) {
        let engine_name = engine.name();
        self.engines.insert(engine_name.to_string(), engine);
    }

    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<Output> {
        // TODO(ruihang): add some metrics

        let region_change = match &request {
            RegionRequest::Create(create) => RegionChange::Register(create.engine.clone()),
            RegionRequest::Open(open) => RegionChange::Register(open.engine.clone()),
            RegionRequest::Close(_) | RegionRequest::Drop(_) => RegionChange::Deregisters,
            RegionRequest::Write(_)
            | RegionRequest::Delete(_)
            | RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_) => RegionChange::None,
        };

        let engine = match &region_change {
            RegionChange::Register(engine_type) => self
                .engines
                .get(engine_type)
                .with_context(|| RegionEngineNotFoundSnafu { name: engine_type })?
                .clone(),
            RegionChange::None | RegionChange::Deregisters => self
                .region_map
                .get(&region_id)
                .with_context(|| RegionNotFoundSnafu { region_id })?
                .clone(),
        };
        let engine_type = engine.name();

        let result = engine
            .handle_request(region_id, request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })?;

        match region_change {
            RegionChange::None => {}
            RegionChange::Register(_) => {
                info!("Region {region_id} is registered to engine {engine_type}");
                self.region_map.insert(region_id, engine);
            }
            RegionChange::Deregisters => {
                info!("Region {region_id} is deregistered from engine {engine_type}");
                self.region_map.remove(&region_id);
            }
        }

        Ok(result)
    }

    pub fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        // TODO(ruihang): add metrics and set trace id

        let QueryRequest { region_id, plan } = request;

        // build dummy catalog list
        let engine = self
            .engines
            .get(&region_id)
            .with_context(|| RegionNotFoundSnafu { region_id })?
            .clone();
        let catalog_list = Arc::new(DummyCatalogList::new(region_id, engine).await?);

        // decode substrait plan to logical plan and execute it
        let logical_plan = DFLogicalSubstraitConvertor
            .decode(plan, catalog_list, "", "")
            .await
            .context(DecodeLogicalPlan)?;
        let result = self
            .query_engine
            .execute(logical_plan, QueryContext::arc())
            .await?;

        match result {
            Output::AffectedRows(_) | Output::RecordBatches(_) => {
                UnsupportedOutputSnafu { expected: "stream" }.fail()
            }
            Output::Stream(stream) => Ok(stream),
        }
    }
}

enum RegionChange {
    None,
    Register(String),
    Deregisters,
}

/// Resolve to the given region (specified by [RegionId]) unconditionally.
struct DummyCatalogList {
    catalog: Arc<DummyCatalogProvider>,
}

impl DummyCatalogList {
    pub async fn new(region_id: RegionId, engine: RegionEngineRef) -> Result<Self> {
        let metadata = engine.get_metadata(region_id).await?;
        let table_provider = DummyTableProvider {
            region_id,
            engine,
            metadata,
        };
        let schema_provider = DummySchemaProvider {
            table: Arc::new(table_provider),
        };
        let catalog_provider = DummyCatalogProvider {
            schema: Arc::new(schema_provider),
        };
        let catalog_list = Self {
            catalog: Arc::new(catalog_provider),
        };
        Ok(catalog_list)
    }
}

impl CatalogList for DummyCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(self.catalog.clone())
    }
}

/// For [DummyCatalogList].
struct DummyCatalogProvider {
    schema: Arc<DummySchemaProvider>,
}

impl CatalogProvider for DummyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(self.schema.clone())
    }
}

/// For [DummyCatalogList].
struct DummySchemaProvider {
    table: Arc<DummyTableProvider>,
}

impl SchemaProvider for DummySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    fn table(&self, _name: &str) -> Option<Arc<dyn TableProvider>> {
        Some(Arc::new(self.table.clone()))
    }
}

/// For [TableProvider](datafusion::datasource::TableProvider) and [DummyCatalogList]
struct DummyTableProvider {
    region_id: RegionId,
    engine: RegionEngineRef,
    metadata: RegionMetadataRef,
}

impl DummyTableProvider {
    pub fn new(region_id: RegionId, engine: RegionEngineRef, metadata: RegionMetadataRef) -> Self {
        Self {
            region_id,
            engine,
            metadata,
        }
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn DfPhysicalPlan>> {
        todo!()
    }
}
