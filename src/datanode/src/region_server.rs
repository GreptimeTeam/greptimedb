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
use std::fmt::Debug;
use std::ops::Deref;
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
use common_telemetry::tracing::{self, info_span};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{info, warn};
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
use metric_engine::engine::MetricEngine;
use prost::Message;
use query::QueryEngineRef;
use servers::error::{self as servers_error, ExecuteGrpcRequestSnafu, Result as ServerResult};
use servers::grpc::flight::{FlightCraft, FlightRecordBatchStream, TonicStream};
use servers::grpc::region_server::RegionServerHandler;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::{METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY};
use store_api::region_engine::{RegionEngineRef, RegionRole, SetReadonlyResponse};
use store_api::region_request::{AffectedRows, RegionCloseRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::scan::StreamScanAdapter;
use tonic::{Request, Response, Result as TonicResult};

use crate::error::{
    self, BuildRegionRequestsSnafu, DecodeLogicalPlanSnafu, ExecuteLogicalPlanSnafu,
    FindLogicalRegionsSnafu, GetRegionMetadataSnafu, HandleRegionRequestSnafu,
    RegionEngineNotFoundSnafu, RegionNotFoundSnafu, Result, StopRegionEngineSnafu, UnexpectedSnafu,
    UnsupportedOutputSnafu,
};
use crate::event_listener::RegionServerEventListenerRef;

#[derive(Clone)]
pub struct RegionServer {
    inner: Arc<RegionServerInner>,
}

pub struct RegionStat {
    pub region_id: RegionId,
    pub engine: String,
    pub role: RegionRole,
}

impl RegionServer {
    pub fn new(
        query_engine: QueryEngineRef,
        runtime: Arc<Runtime>,
        event_listener: RegionServerEventListenerRef,
    ) -> Self {
        Self::with_table_provider(
            query_engine,
            runtime,
            event_listener,
            Arc::new(DummyTableProviderFactory),
        )
    }

    pub fn with_table_provider(
        query_engine: QueryEngineRef,
        runtime: Arc<Runtime>,
        event_listener: RegionServerEventListenerRef,
        table_provider_factory: TableProviderFactoryRef,
    ) -> Self {
        Self {
            inner: Arc::new(RegionServerInner::new(
                query_engine,
                runtime,
                event_listener,
                table_provider_factory,
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
    ) -> Result<AffectedRows> {
        self.inner.handle_request(region_id, request).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        self.inner.handle_read(request).await
    }

    /// Returns all opened and reportable regions.
    ///
    /// Notes: except all metrics regions.
    pub fn reportable_regions(&self) -> Vec<RegionStat> {
        self.inner
            .region_map
            .iter()
            .filter_map(|e| {
                let region_id = *e.key();
                // Filters out any regions whose role equals None.
                e.role(region_id).map(|role| RegionStat {
                    region_id,
                    engine: e.value().name().to_string(),
                    role,
                })
            })
            .collect()
    }

    pub fn is_writable(&self, region_id: RegionId) -> Option<bool> {
        // TODO(weny): Finds a better way.
        self.inner.region_map.get(&region_id).and_then(|engine| {
            engine.role(region_id).map(|role| match role {
                RegionRole::Follower => false,
                RegionRole::Leader => true,
            })
        })
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

    pub async fn set_readonly_gracefully(
        &self,
        region_id: RegionId,
    ) -> Result<SetReadonlyResponse> {
        match self.inner.region_map.get(&region_id) {
            Some(engine) => Ok(engine
                .set_readonly_gracefully(region_id)
                .await
                .with_context(|_| HandleRegionRequestSnafu { region_id })?),
            None => Ok(SetReadonlyResponse::NotFound),
        }
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        self.inner.runtime.clone()
    }

    pub async fn region_disk_usage(&self, region_id: RegionId) -> Option<i64> {
        match self.inner.region_map.get(&region_id) {
            Some(e) => e.region_disk_usage(region_id).await,
            None => None,
        }
    }

    /// Stop the region server.
    pub async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }

    #[cfg(test)]
    /// Registers a region for test purpose.
    pub(crate) fn register_test_region(&self, region_id: RegionId, engine: RegionEngineRef) {
        self.inner
            .region_map
            .insert(region_id, RegionEngineWithStatus::Ready(engine));
    }
}

#[async_trait]
impl RegionServerHandler for RegionServer {
    async fn handle(&self, request: region_request::Body) -> ServerResult<RegionResponse> {
        let requests = RegionRequest::try_from_request_body(request)
            .context(BuildRegionRequestsSnafu)
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)?;
        let tracing_context = TracingContext::from_current_span();
        let join_tasks = requests.into_iter().map(|(region_id, req)| {
            let self_to_move = self.clone();
            let span = tracing_context.attach(info_span!(
                "RegionServer::handle_region_request",
                region_id = region_id.to_string()
            ));
            async move {
                self_to_move
                    .handle_request(region_id, req)
                    .trace(span)
                    .await
            }
        });

        let results = try_join_all(join_tasks)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)?;

        // merge results by simply sum up affected rows.
        // only insert/delete will have multiple results.
        let mut affected_rows = 0;
        for result in results {
            affected_rows += result;
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
        let tracing_context = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default();

        let result = self
            .handle_read(request)
            .trace(tracing_context.attach(info_span!("RegionServer::handle_read")))
            .await?;

        let stream = Box::pin(FlightRecordBatchStream::new(result, tracing_context));
        Ok(Response::new(stream))
    }
}

#[derive(Clone)]
enum RegionEngineWithStatus {
    // An opening, or creating region.
    Registering(RegionEngineRef),
    // A closing, or dropping region.
    Deregistering(RegionEngineRef),
    // A ready region.
    Ready(RegionEngineRef),
}

impl RegionEngineWithStatus {
    /// Returns [RegionEngineRef].
    pub fn into_engine(self) -> RegionEngineRef {
        match self {
            RegionEngineWithStatus::Registering(engine) => engine,
            RegionEngineWithStatus::Deregistering(engine) => engine,
            RegionEngineWithStatus::Ready(engine) => engine,
        }
    }

    pub fn is_registering(&self) -> bool {
        matches!(self, Self::Registering(_))
    }
}

impl Deref for RegionEngineWithStatus {
    type Target = RegionEngineRef;

    fn deref(&self) -> &Self::Target {
        match self {
            RegionEngineWithStatus::Registering(engine) => engine,
            RegionEngineWithStatus::Deregistering(engine) => engine,
            RegionEngineWithStatus::Ready(engine) => engine,
        }
    }
}

struct RegionServerInner {
    engines: RwLock<HashMap<String, RegionEngineRef>>,
    region_map: DashMap<RegionId, RegionEngineWithStatus>,
    query_engine: QueryEngineRef,
    runtime: Arc<Runtime>,
    event_listener: RegionServerEventListenerRef,
    table_provider_factory: TableProviderFactoryRef,
}

enum CurrentEngine {
    Engine(RegionEngineRef),
    EarlyReturn(AffectedRows),
}

impl Debug for CurrentEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CurrentEngine::Engine(engine) => f
                .debug_struct("CurrentEngine")
                .field("engine", &engine.name())
                .finish(),
            CurrentEngine::EarlyReturn(rows) => f
                .debug_struct("CurrentEngine")
                .field("return", rows)
                .finish(),
        }
    }
}

impl RegionServerInner {
    pub fn new(
        query_engine: QueryEngineRef,
        runtime: Arc<Runtime>,
        event_listener: RegionServerEventListenerRef,
        table_provider_factory: TableProviderFactoryRef,
    ) -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
            region_map: DashMap::new(),
            query_engine,
            runtime,
            event_listener,
            table_provider_factory,
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

    fn get_engine(
        &self,
        region_id: RegionId,
        region_change: &RegionChange,
    ) -> Result<CurrentEngine> {
        let current_region_status = self.region_map.get(&region_id);

        let engine = match region_change {
            RegionChange::Register(ref engine_type, _) => match current_region_status {
                Some(status) => match status.clone() {
                    RegionEngineWithStatus::Registering(_) => {
                        return Ok(CurrentEngine::EarlyReturn(0))
                    }
                    RegionEngineWithStatus::Deregistering(_) => {
                        return error::RegionBusySnafu { region_id }.fail()
                    }
                    RegionEngineWithStatus::Ready(_) => status.clone().into_engine(),
                },
                _ => self
                    .engines
                    .read()
                    .unwrap()
                    .get(engine_type)
                    .with_context(|| RegionEngineNotFoundSnafu { name: engine_type })?
                    .clone(),
            },
            RegionChange::Deregisters => match current_region_status {
                Some(status) => match status.clone() {
                    RegionEngineWithStatus::Registering(_) => {
                        return error::RegionBusySnafu { region_id }.fail()
                    }
                    RegionEngineWithStatus::Deregistering(_) => {
                        return Ok(CurrentEngine::EarlyReturn(0))
                    }
                    RegionEngineWithStatus::Ready(_) => status.clone().into_engine(),
                },
                None => return Ok(CurrentEngine::EarlyReturn(0)),
            },
            RegionChange::None => match current_region_status {
                Some(status) => match status.clone() {
                    RegionEngineWithStatus::Registering(_) => {
                        return error::RegionNotReadySnafu { region_id }.fail()
                    }
                    RegionEngineWithStatus::Deregistering(_) => {
                        return error::RegionNotFoundSnafu { region_id }.fail()
                    }
                    RegionEngineWithStatus::Ready(engine) => engine,
                },
                None => return error::RegionNotFoundSnafu { region_id }.fail(),
            },
        };

        Ok(CurrentEngine::Engine(engine))
    }

    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<AffectedRows> {
        let request_type = request.request_type();
        let _timer = crate::metrics::HANDLE_REGION_REQUEST_ELAPSED
            .with_label_values(&[request_type])
            .start_timer();

        let region_change = match &request {
            RegionRequest::Create(create) => RegionChange::Register(create.engine.clone(), false),
            RegionRequest::Open(open) => {
                let is_opening_physical_region =
                    open.options.contains_key(PHYSICAL_TABLE_METADATA_KEY);
                RegionChange::Register(open.engine.clone(), is_opening_physical_region)
            }
            RegionRequest::Close(_) | RegionRequest::Drop(_) => RegionChange::Deregisters,
            RegionRequest::Put(_)
            | RegionRequest::Delete(_)
            | RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_)
            | RegionRequest::Truncate(_)
            | RegionRequest::Catchup(_) => RegionChange::None,
        };

        let engine = match self.get_engine(region_id, &region_change)? {
            CurrentEngine::Engine(engine) => engine,
            CurrentEngine::EarlyReturn(rows) => return Ok(rows),
        };

        let engine_type = engine.name();

        // Sets corresponding region status to registering/deregistering before the operation.
        self.set_region_status_not_ready(region_id, &engine, &region_change);

        match engine
            .handle_request(region_id, request)
            .trace(info_span!(
                "RegionEngine::handle_region_request",
                engine_type
            ))
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })
        {
            Ok(result) => {
                // Sets corresponding region status to ready.
                self.set_region_status_ready(region_id, engine, region_change)
                    .await?;
                Ok(result)
            }
            Err(err) => {
                // Removes the region status if the operation fails.
                self.unset_region_status(region_id, region_change);
                Err(err)
            }
        }
    }

    fn set_region_status_not_ready(
        &self,
        region_id: RegionId,
        engine: &RegionEngineRef,
        region_change: &RegionChange,
    ) {
        match region_change {
            RegionChange::Register(_, _) => {
                self.region_map.insert(
                    region_id,
                    RegionEngineWithStatus::Registering(engine.clone()),
                );
            }
            RegionChange::Deregisters => {
                self.region_map.insert(
                    region_id,
                    RegionEngineWithStatus::Deregistering(engine.clone()),
                );
            }
            _ => {}
        }
    }

    fn unset_region_status(&self, region_id: RegionId, region_change: RegionChange) {
        match region_change {
            RegionChange::None => {}
            RegionChange::Register(_, _) | RegionChange::Deregisters => {
                self.region_map
                    .remove(&region_id)
                    .map(|(id, engine)| engine.set_writable(id, false));
            }
        }
    }

    async fn set_region_status_ready(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
        region_change: RegionChange,
    ) -> Result<()> {
        let engine_type = engine.name();
        match region_change {
            RegionChange::None => {}
            RegionChange::Register(_, is_opening_physical_region) => {
                if is_opening_physical_region {
                    self.register_logical_regions(&engine, region_id).await?;
                }

                info!("Region {region_id} is registered to engine {engine_type}");
                self.region_map
                    .insert(region_id, RegionEngineWithStatus::Ready(engine));
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
        Ok(())
    }

    async fn register_logical_regions(
        &self,
        engine: &RegionEngineRef,
        physical_region_id: RegionId,
    ) -> Result<()> {
        let metric_engine =
            engine
                .as_any()
                .downcast_ref::<MetricEngine>()
                .context(UnexpectedSnafu {
                    violated: format!(
                        "expecting engine type '{}', actual '{}'",
                        METRIC_ENGINE_NAME,
                        engine.name(),
                    ),
                })?;

        let logical_regions = metric_engine
            .logical_regions(physical_region_id)
            .await
            .context(FindLogicalRegionsSnafu { physical_region_id })?;

        for region in logical_regions {
            self.region_map
                .insert(region, RegionEngineWithStatus::Ready(engine.clone()));
            info!("Logical region {} is registered!", region);
        }
        Ok(())
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
        let region_status = self
            .region_map
            .get(&region_id)
            .with_context(|| RegionNotFoundSnafu { region_id })?
            .clone();

        if region_status.is_registering() {
            return error::RegionNotReadySnafu { region_id }.fail();
        }

        let table_provider = self
            .table_provider_factory
            .create(region_id, region_status.into_engine())
            .await?;

        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(table_provider));

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
        // Calling async functions while iterating inside the Dashmap could easily cause the Rust
        // complains "higher-ranked lifetime error". Rust can't prove some future is legit.
        // Possible related issue: https://github.com/rust-lang/rust/issues/102211
        //
        // The walkaround is to put the async functions in the `common_runtime::spawn_bg`. Or like
        // it here, collect the values first then use later separately.

        let regions = self
            .region_map
            .iter()
            .map(|x| (*x.key(), x.value().clone()))
            .collect::<Vec<_>>();

        for (region_id, engine) in regions {
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
    Register(String, bool),
    Deregisters,
}

/// Resolve to the given region (specified by [RegionId]) unconditionally.
#[derive(Clone)]
struct DummyCatalogList {
    catalog: DummyCatalogProvider,
}

impl DummyCatalogList {
    fn with_table_provider(table_provider: Arc<dyn TableProvider>) -> Self {
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

    async fn table(&self, _name: &str) -> Option<Arc<dyn TableProvider>> {
        Some(self.table.clone())
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

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;

    use common_error::ext::ErrorExt;
    use mito2::test_util::CreateRequestBuilder;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{RegionDropRequest, RegionOpenRequest, RegionTruncateRequest};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Result;
    use crate::tests::{mock_region_server, MockRegionEngine};

    #[tokio::test]
    async fn test_region_registering() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let (engine, _receiver) = MockRegionEngine::new();
        let engine_name = engine.name();

        mock_region_server.register_engine(engine.clone());

        let region_id = RegionId::new(1, 1);
        let builder = CreateRequestBuilder::new();
        let create_req = builder.build();

        // Tries to create/open a registering region.
        mock_region_server.inner.region_map.insert(
            region_id,
            RegionEngineWithStatus::Registering(engine.clone()),
        );

        let affected_rows = mock_region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();
        assert_eq!(affected_rows, 0);

        let status = mock_region_server
            .inner
            .region_map
            .get(&region_id)
            .unwrap()
            .clone();

        assert!(matches!(status, RegionEngineWithStatus::Registering(_)));

        let affected_rows = mock_region_server
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: engine_name.to_string(),
                    region_dir: String::new(),
                    options: Default::default(),
                    skip_wal_replay: false,
                }),
            )
            .await
            .unwrap();
        assert_eq!(affected_rows, 0);

        let status = mock_region_server
            .inner
            .region_map
            .get(&region_id)
            .unwrap()
            .clone();
        assert!(matches!(status, RegionEngineWithStatus::Registering(_)));
    }

    #[tokio::test]
    async fn test_region_deregistering() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let (engine, _receiver) = MockRegionEngine::new();

        mock_region_server.register_engine(engine.clone());

        let region_id = RegionId::new(1, 1);

        // Tries to drop/close a registering region.
        mock_region_server.inner.region_map.insert(
            region_id,
            RegionEngineWithStatus::Deregistering(engine.clone()),
        );

        let affected_rows = mock_region_server
            .handle_request(region_id, RegionRequest::Drop(RegionDropRequest {}))
            .await
            .unwrap();
        assert_eq!(affected_rows, 0);

        let status = mock_region_server
            .inner
            .region_map
            .get(&region_id)
            .unwrap()
            .clone();
        assert!(matches!(status, RegionEngineWithStatus::Deregistering(_)));

        mock_region_server.inner.region_map.insert(
            region_id,
            RegionEngineWithStatus::Deregistering(engine.clone()),
        );

        let affected_rows = mock_region_server
            .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
            .await
            .unwrap();
        assert_eq!(affected_rows, 0);

        let status = mock_region_server
            .inner
            .region_map
            .get(&region_id)
            .unwrap()
            .clone();
        assert!(matches!(status, RegionEngineWithStatus::Deregistering(_)));
    }

    #[tokio::test]
    async fn test_region_not_ready() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let (engine, _receiver) = MockRegionEngine::new();

        mock_region_server.register_engine(engine.clone());

        let region_id = RegionId::new(1, 1);

        // Tries to drop/close a registering region.
        mock_region_server.inner.region_map.insert(
            region_id,
            RegionEngineWithStatus::Registering(engine.clone()),
        );

        let err = mock_region_server
            .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
            .await
            .unwrap_err();

        assert_eq!(err.status_code(), StatusCode::RegionNotReady);
    }

    #[tokio::test]
    async fn test_region_request_failed() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let (engine, _receiver) =
            MockRegionEngine::with_mock_fn(Box::new(|_region_id, _request| {
                error::UnexpectedSnafu {
                    violated: "test".to_string(),
                }
                .fail()
            }));

        mock_region_server.register_engine(engine.clone());

        let region_id = RegionId::new(1, 1);
        let builder = CreateRequestBuilder::new();
        let create_req = builder.build();
        mock_region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap_err();

        let status = mock_region_server.inner.region_map.get(&region_id);
        assert!(status.is_none());

        mock_region_server
            .inner
            .region_map
            .insert(region_id, RegionEngineWithStatus::Ready(engine.clone()));

        mock_region_server
            .handle_request(region_id, RegionRequest::Drop(RegionDropRequest {}))
            .await
            .unwrap_err();

        let status = mock_region_server.inner.region_map.get(&region_id);
        assert!(status.is_none());
    }

    struct CurrentEngineTest {
        region_id: RegionId,
        current_region_status: Option<RegionEngineWithStatus>,
        region_change: RegionChange,
        assert: Box<dyn FnOnce(Result<CurrentEngine>)>,
    }

    #[tokio::test]
    async fn test_current_engine() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let (engine, _) = MockRegionEngine::new();
        mock_region_server.register_engine(engine.clone());

        let region_id = RegionId::new(1024, 1);
        let tests = vec![
            // RegionChange::None
            CurrentEngineTest {
                region_id,
                current_region_status: None,
                region_change: RegionChange::None,
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionNotFound);
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Ready(engine.clone())),
                region_change: RegionChange::None,
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::Engine(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Registering(engine.clone())),
                region_change: RegionChange::None,
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionNotReady);
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Deregistering(engine.clone())),
                region_change: RegionChange::None,
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionNotFound);
                }),
            },
            // RegionChange::Register
            CurrentEngineTest {
                region_id,
                current_region_status: None,
                region_change: RegionChange::Register(engine.name().to_string(), false),
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::Engine(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Registering(engine.clone())),
                region_change: RegionChange::Register(engine.name().to_string(), false),
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::EarlyReturn(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Deregistering(engine.clone())),
                region_change: RegionChange::Register(engine.name().to_string(), false),
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionBusy);
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Ready(engine.clone())),
                region_change: RegionChange::Register(engine.name().to_string(), false),
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::Engine(_));
                }),
            },
            // RegionChange::Deregister
            CurrentEngineTest {
                region_id,
                current_region_status: None,
                region_change: RegionChange::Deregisters,
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::EarlyReturn(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Registering(engine.clone())),
                region_change: RegionChange::Deregisters,
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionBusy);
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Deregistering(engine.clone())),
                region_change: RegionChange::Deregisters,
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::EarlyReturn(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Ready(engine.clone())),
                region_change: RegionChange::Deregisters,
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::Engine(_));
                }),
            },
        ];

        for test in tests {
            let CurrentEngineTest {
                region_id,
                current_region_status,
                region_change,
                assert,
            } = test;

            // Sets up
            if let Some(status) = current_region_status {
                mock_region_server
                    .inner
                    .region_map
                    .insert(region_id, status);
            } else {
                mock_region_server.inner.region_map.remove(&region_id);
            }

            let result = mock_region_server
                .inner
                .get_engine(region_id, &region_change);

            assert(result);
        }
    }
}
