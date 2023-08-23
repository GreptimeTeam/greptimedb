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
use std::sync::{Arc, Mutex, RwLock};

use api::v1::region::region_request::Request as RequestBody;
use api::v1::region::{QueryRequest, RegionResponse};
use arrow_flight::{FlightData, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use common_query::logical_plan::Expr;
use common_query::physical_plan::DfPhysicalPlanAdapter;
use common_query::{DfPhysicalPlan, Output};
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::info;
use dashmap::DashMap;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogList, CatalogProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr as DfExpr, TableType};
use datatypes::arrow::datatypes::SchemaRef;
use prost::Message;
use query::QueryEngineRef;
use servers::error as servers_error;
use servers::error::Result as ServerResult;
use servers::grpc::flight::{FlightCraft, FlightRecordBatchStream, TonicStream};
use servers::grpc::region_server::RegionServerHandler;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngineRef;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::scan::StreamScanAdapter;
use tonic::{Request, Response, Result as TonicResult};

use crate::error::{
    DecodeLogicalPlanSnafu, ExecuteLogicalPlanSnafu, GetRegionMetadataSnafu,
    HandleRegionRequestSnafu, RegionEngineNotFoundSnafu, RegionNotFoundSnafu, Result,
    UnsupportedOutputSnafu,
};

#[derive(Clone)]
pub struct RegionServer {
    inner: Arc<RegionServerInner>,
}

impl RegionServer {
    pub fn new(query_engine: QueryEngineRef) -> Self {
        Self {
            inner: Arc::new(RegionServerInner::new(query_engine)),
        }
    }

    pub fn register_engine(&mut self, engine: RegionEngineRef) {
        self.inner.register_engine(engine);
    }

    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<Output> {
        self.inner.handle_request(region_id, request).await
    }

    pub async fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        self.inner.handle_read(request).await
    }
}

#[async_trait]
impl RegionServerHandler for RegionServer {
    async fn handle(&self, _request: RequestBody) -> ServerResult<RegionResponse> {
        todo!()
    }
}

#[async_trait]
impl FlightCraft for RegionServer {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> TonicResult<Response<TonicStream<FlightData>>> {
        let ticket = request.into_inner().ticket;
        let request = QueryRequest::decode(ticket.as_ref())
            .context(servers_error::InvalidFlightTicketSnafu)?;

        let result = self.handle_read(request).await?;

        let stream = Box::pin(FlightRecordBatchStream::new(result));
        Ok(Response::new(stream))
    }
}

struct RegionServerInner {
    engines: RwLock<HashMap<String, RegionEngineRef>>,
    region_map: DashMap<RegionId, RegionEngineRef>,
    query_engine: QueryEngineRef,
}

impl RegionServerInner {
    pub fn new(query_engine: QueryEngineRef) -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
            region_map: DashMap::new(),
            query_engine,
        }
    }

    pub fn register_engine(&self, engine: RegionEngineRef) {
        let engine_name = engine.name();
        self.engines
            .write()
            .unwrap()
            .insert(engine_name.to_string(), engine);
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
            RegionRequest::Put(_)
            | RegionRequest::Delete(_)
            | RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_) => RegionChange::None,
        };

        let engine = match &region_change {
            RegionChange::Register(engine_type) => self
                .engines
                .read()
                .unwrap()
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

    pub async fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        // TODO(ruihang): add metrics and set trace id

        let QueryRequest { region_id, plan } = request;
        let region_id = RegionId::from_u64(region_id);

        // build dummy catalog list
        let engine = self
            .region_map
            .get(&region_id)
            .with_context(|| RegionNotFoundSnafu { region_id })?
            .clone();
        let catalog_list = Arc::new(DummyCatalogList::new(region_id, engine).await?);

        // decode substrait plan to logical plan and execute it
        let logical_plan = DFLogicalSubstraitConvertor
            .decode(Bytes::from(plan), catalog_list, "", "")
            .await
            .context(DecodeLogicalPlanSnafu)?;
        let result = self
            .query_engine
            .execute(logical_plan.into(), QueryContext::arc())
            .await
            .context(ExecuteLogicalPlanSnafu)?;

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
#[derive(Clone)]
struct DummyCatalogList {
    catalog: DummyCatalogProvider,
}

impl DummyCatalogList {
    pub async fn new(region_id: RegionId, engine: RegionEngineRef) -> Result<Self> {
        let metadata =
            engine
                .get_metadata(region_id)
                .await
                .with_context(|_| GetRegionMetadataSnafu {
                    engine: engine.name(),
                    region_id,
                })?;
        let table_provider = DummyTableProvider {
            region_id,
            engine,
            metadata,
            scan_request: Default::default(),
        };
        let schema_provider = DummySchemaProvider {
            table: table_provider,
        };
        let catalog_provider = DummyCatalogProvider {
            schema: schema_provider,
        };
        let catalog_list = Self {
            catalog: catalog_provider,
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

/// For [DummyCatalogList].
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

/// For [DummyCatalogList].
#[derive(Clone)]
struct DummySchemaProvider {
    table: DummyTableProvider,
}

#[async_trait]
impl SchemaProvider for DummySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(&self, _name: &str) -> Option<Arc<dyn TableProvider>> {
        Some(Arc::new(self.table.clone()))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}

/// For [TableProvider](datafusion::datasource::TableProvider) and [DummyCatalogList]
#[derive(Clone)]
struct DummyTableProvider {
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
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn DfPhysicalPlan>> {
        let mut request = self.scan_request.lock().unwrap().clone();
        request.projection = projection.cloned();
        request.filters = filters.iter().map(|e| Expr::from(e.clone())).collect();
        request.limit = limit;

        let stream = self
            .engine
            .handle_query(self.region_id, request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Arc::new(DfPhysicalPlanAdapter(Arc::new(
            StreamScanAdapter::new(stream),
        ))))
    }
}
