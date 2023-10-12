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

use api::v1::region::{region_request, QueryRequest, RegionResponse};
use api::v1::{ResponseHeader, Status};
use arrow_flight::{FlightData, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use common_error::ext::BoxedError;
use common_error::status_code::StatusCode;
use common_query::logical_plan::Expr;
use common_query::physical_plan::DfPhysicalPlanAdapter;
use common_query::{DfPhysicalPlan, Output};
use common_recordbatch::SendableRecordBatchStream;
use common_runtime::Runtime;
use common_telemetry::{info, timer, warn};
use dashmap::DashMap;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogList, CatalogProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr as DfExpr, TableProviderFilterPushDown, TableType};
use datatypes::arrow::datatypes::SchemaRef;
use futures_util::future::try_join_all;
use prost::Message;
use query::QueryEngineRef;
use servers::error::{self as servers_error, ExecuteGrpcRequestSnafu, Result as ServerResult};
use servers::grpc::flight::{FlightCraft, FlightRecordBatchStream, TonicStream};
use servers::grpc::region_server::RegionServerHandler;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngineRef;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::scan::StreamScanAdapter;
use tonic::{Request, Response, Result as TonicResult};

use crate::error::{
    BuildRegionRequestsSnafu, DecodeLogicalPlanSnafu, ExecuteLogicalPlanSnafu,
    GetRegionMetadataSnafu, HandleRegionRequestSnafu, RegionEngineNotFoundSnafu,
    RegionNotFoundSnafu, Result, StopRegionEngineSnafu, UnsupportedOutputSnafu,
};
use crate::event_listener::RegionServerEventListenerRef;

#[derive(Clone)]
pub struct RegionServer {
    inner: Arc<RegionServerInner>,
}

impl RegionServer {
    pub fn new(
        query_engine: QueryEngineRef,
        runtime: Arc<Runtime>,
        event_listener: RegionServerEventListenerRef,
    ) -> Self {
        Self {
            inner: Arc::new(RegionServerInner::new(
                query_engine,
                runtime,
                event_listener,
            )),
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

    pub fn opened_regions(&self) -> Vec<(RegionId, String)> {
        self.inner
            .region_map
            .iter()
            .map(|e| (*e.key(), e.value().name().to_string()))
            .collect()
    }

    pub fn set_writable(&self, region_id: RegionId, writable: bool) -> Result<()> {
        let engine = self
            .inner
            .region_map
            .get(&region_id)
            .with_context(|| RegionNotFoundSnafu { region_id })?;
        engine
            .set_writable(region_id, writable)
            .with_context(|_| HandleRegionRequestSnafu { region_id })
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        self.inner.runtime.clone()
    }

    /// Stop the region server.
    pub async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }
}

#[async_trait]
impl RegionServerHandler for RegionServer {
    async fn handle(&self, request: region_request::Body) -> ServerResult<RegionResponse> {
        let requests = RegionRequest::try_from_request_body(request)
            .context(BuildRegionRequestsSnafu)
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)?;
        let join_tasks = requests.into_iter().map(|(region_id, req)| {
            let self_to_move = self.clone();
            async move { self_to_move.handle_request(region_id, req).await }
        });

        let results = try_join_all(join_tasks)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)?;

        // merge results by simply sum up affected rows.
        // only insert/delete will have multiple results.
        let mut affected_rows = 0;
        for result in results {
            match result {
                Output::AffectedRows(rows) => affected_rows += rows,
                Output::Stream(_) | Output::RecordBatches(_) => {
                    // TODO: change the output type to only contains `affected_rows`
                    unreachable!()
                }
            }
        }

        Ok(RegionResponse {
            header: Some(ResponseHeader {
                status: Some(Status {
                    status_code: StatusCode::Success as _,
                    ..Default::default()
                }),
            }),
            affected_rows: affected_rows as _,
        })
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
        let trace_id = request
            .header
            .as_ref()
            .map(|h| h.trace_id)
            .unwrap_or_default();

        let result = self.handle_read(request).await?;

        let stream = Box::pin(FlightRecordBatchStream::new(result, trace_id));
        Ok(Response::new(stream))
    }
}

struct RegionServerInner {
    engines: RwLock<HashMap<String, RegionEngineRef>>,
    region_map: DashMap<RegionId, RegionEngineRef>,
    query_engine: QueryEngineRef,
    runtime: Arc<Runtime>,
    event_listener: RegionServerEventListenerRef,
}

impl RegionServerInner {
    pub fn new(
        query_engine: QueryEngineRef,
        runtime: Arc<Runtime>,
        event_listener: RegionServerEventListenerRef,
    ) -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
            region_map: DashMap::new(),
            query_engine,
            runtime,
            event_listener,
        }
    }

    pub fn register_engine(&self, engine: RegionEngineRef) {
        let engine_name = engine.name();
        info!("Region Engine {engine_name} is registered");
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
        let request_type = request.request_type();
        let _timer = timer!(
            crate::metrics::HANDLE_REGION_REQUEST_ELAPSED,
            &[(crate::metrics::REGION_REQUEST_TYPE, request_type),]
        );

        let region_change = match &request {
            RegionRequest::Create(create) => RegionChange::Register(create.engine.clone()),
            RegionRequest::Open(open) => RegionChange::Register(open.engine.clone()),
            RegionRequest::Close(_) | RegionRequest::Drop(_) => RegionChange::Deregisters,
            RegionRequest::Put(_)
            | RegionRequest::Delete(_)
            | RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_)
            | RegionRequest::Truncate(_) => RegionChange::None,
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
                self.event_listener.on_region_registered(region_id);
            }
            RegionChange::Deregisters => {
                info!("Region {region_id} is deregistered from engine {engine_type}");
                self.region_map
                    .remove(&region_id)
                    .map(|(id, engine)| engine.set_writable(id, false));
                self.event_listener.on_region_deregistered(region_id);
            }
        }

        Ok(result)
    }

    pub async fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        // TODO(ruihang): add metrics and set trace id

        let QueryRequest {
            header,
            region_id,
            plan,
        } = request;
        let region_id = RegionId::from_u64(region_id);

        let ctx: QueryContextRef = header
            .as_ref()
            .map(|h| Arc::new(h.into()))
            .unwrap_or_else(|| QueryContextBuilder::default().build());

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
            .execute(logical_plan.into(), ctx)
            .await
            .context(ExecuteLogicalPlanSnafu)?;

        match result {
            Output::AffectedRows(_) | Output::RecordBatches(_) => {
                UnsupportedOutputSnafu { expected: "stream" }.fail()
            }
            Output::Stream(stream) => Ok(stream),
        }
    }

    async fn stop(&self) -> Result<()> {
        for region in self.region_map.iter() {
            let region_id = *region.key();
            let engine = region.value();
            let closed = engine
                .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
                .await;
            match closed {
                Ok(_) => info!("Region {region_id} is closed"),
                Err(e) => warn!("Failed to close region {region_id}, err: {e}"),
            }
        }
        self.region_map.clear();

        let engines = self.engines.write().unwrap().drain().collect::<Vec<_>>();
        for (engine_name, engine) in engines {
            engine
                .stop()
                .await
                .context(StopRegionEngineSnafu { name: &engine_name })?;
            info!("Region engine {engine_name} is stopped");
        }

        Ok(())
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

/// For [TableProvider](TableProvider) and [DummyCatalogList]
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
