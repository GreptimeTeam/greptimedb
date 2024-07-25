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
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use api::region::RegionResponse;
use api::v1::region::{region_request, RegionResponse as RegionResponseV1};
use api::v1::{ResponseHeader, Status};
use arrow_flight::{FlightData, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use common_error::ext::BoxedError;
use common_error::status_code::StatusCode;
use common_query::request::QueryRequest;
use common_query::OutputData;
use common_recordbatch::SendableRecordBatchStream;
use common_runtime::Runtime;
use common_telemetry::tracing::{self, info_span};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{error, info, warn};
use dashmap::DashMap;
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::error::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::{LogicalPlan, TableSource};
use futures_util::future::try_join_all;
use metric_engine::engine::MetricEngine;
use mito2::engine::MITO_ENGINE_NAME;
use prost::Message;
pub use query::dummy_catalog::{
    DummyCatalogList, DummyTableProviderFactory, TableProviderFactoryRef,
};
use query::QueryEngineRef;
use servers::error::{self as servers_error, ExecuteGrpcRequestSnafu, Result as ServerResult};
use servers::grpc::flight::{FlightCraft, FlightRecordBatchStream, TonicStream};
use servers::grpc::region_server::RegionServerHandler;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metric_engine_consts::{
    FILE_ENGINE_NAME, LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME,
};
use store_api::region_engine::{RegionEngineRef, RegionRole, SetReadonlyResponse};
use store_api::region_request::{
    AffectedRows, RegionCloseRequest, RegionOpenRequest, RegionRequest,
};
use store_api::storage::RegionId;
use tonic::{Request, Response, Result as TonicResult};

use crate::error::{
    self, BuildRegionRequestsSnafu, DataFusionSnafu, DecodeLogicalPlanSnafu,
    ExecuteLogicalPlanSnafu, FindLogicalRegionsSnafu, HandleBatchOpenRequestSnafu,
    HandleRegionRequestSnafu, NewPlanDecoderSnafu, RegionEngineNotFoundSnafu, RegionNotFoundSnafu,
    RegionNotReadySnafu, Result, StopRegionEngineSnafu, UnexpectedSnafu, UnsupportedOutputSnafu,
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
        runtime: Runtime,
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
        runtime: Runtime,
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

    /// Finds the region's engine by its id. If the region is not ready, returns `None`.
    pub fn find_engine(&self, region_id: RegionId) -> Result<Option<RegionEngineRef>> {
        self.inner
            .get_engine(region_id, &RegionChange::None)
            .map(|x| match x {
                CurrentEngine::Engine(engine) => Some(engine),
                CurrentEngine::EarlyReturn(_) => None,
            })
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<Vec<RegionId>> {
        self.inner
            .handle_batch_open_requests(parallelism, requests)
            .await
    }

    #[tracing::instrument(skip_all, fields(request_type = request.request_type()))]
    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse> {
        self.inner.handle_request(region_id, request).await
    }

    async fn table_provider(&self, region_id: RegionId) -> Result<Arc<dyn TableProvider>> {
        let status = self
            .inner
            .region_map
            .get(&region_id)
            .context(RegionNotFoundSnafu { region_id })?
            .clone();
        ensure!(
            matches!(status, RegionEngineWithStatus::Ready(_)),
            RegionNotReadySnafu { region_id }
        );

        self.inner
            .table_provider_factory
            .create(region_id, status.into_engine())
            .await
            .context(ExecuteLogicalPlanSnafu)
    }

    /// Handle reads from remote. They're often query requests received by our Arrow Flight service.
    pub async fn handle_remote_read(
        &self,
        request: api::v1::region::QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        let region_id = RegionId::from_u64(request.region_id);
        let provider = self.table_provider(region_id).await?;
        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(provider));

        let query_ctx: QueryContextRef = request
            .header
            .as_ref()
            .map(|h| Arc::new(h.into()))
            .unwrap_or_else(|| Arc::new(QueryContextBuilder::default().build()));

        let decoder = self
            .inner
            .query_engine
            .engine_context(query_ctx)
            .new_plan_decoder()
            .context(NewPlanDecoderSnafu)?;

        let plan = decoder
            .decode(Bytes::from(request.plan), catalog_list, false)
            .await
            .context(DecodeLogicalPlanSnafu)?;

        self.inner
            .handle_read(QueryRequest {
                header: request.header,
                region_id,
                plan,
            })
            .await
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let provider = self.table_provider(request.region_id).await?;

        struct RegionDataSourceInjector {
            source: Arc<dyn TableSource>,
        }

        impl TreeNodeRewriter for RegionDataSourceInjector {
            type Node = LogicalPlan;

            fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
                Ok(match node {
                    LogicalPlan::TableScan(mut scan) => {
                        scan.source = self.source.clone();
                        Transformed::yes(LogicalPlan::TableScan(scan))
                    }
                    _ => Transformed::no(node),
                })
            }
        }

        let plan = request
            .plan
            .rewrite(&mut RegionDataSourceInjector {
                source: provider_as_source(provider),
            })
            .context(DataFusionSnafu)?
            .data;

        self.inner
            .handle_read(QueryRequest { plan, ..request })
            .await
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

    pub fn runtime(&self) -> Runtime {
        self.inner.runtime.clone()
    }

    pub fn region_disk_usage(&self, region_id: RegionId) -> Option<i64> {
        match self.inner.region_map.get(&region_id) {
            Some(e) => e.region_disk_usage(region_id),
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
    async fn handle(&self, request: region_request::Body) -> ServerResult<RegionResponseV1> {
        let is_parallel = matches!(
            request,
            region_request::Body::Inserts(_) | region_request::Body::Deletes(_)
        );
        let requests = RegionRequest::try_from_request_body(request)
            .context(BuildRegionRequestsSnafu)
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)?;

        let tracing_context = TracingContext::from_current_span();

        let results = if is_parallel {
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

            try_join_all(join_tasks)
                .await
                .map_err(BoxedError::new)
                .context(ExecuteGrpcRequestSnafu)?
        } else {
            let mut results = Vec::with_capacity(requests.len());
            // FIXME(jeremy, ruihang): Once the engine supports merged calls, we should immediately
            // modify this part to avoid inefficient serial loop calls.
            for (region_id, req) in requests {
                let span = tracing_context.attach(info_span!(
                    "RegionServer::handle_region_request",
                    region_id = region_id.to_string()
                ));
                let result = self
                    .handle_request(region_id, req)
                    .trace(span)
                    .await
                    .map_err(BoxedError::new)
                    .context(ExecuteGrpcRequestSnafu)?;
                results.push(result);
            }
            results
        };

        // merge results by sum up affected rows and merge extensions.
        let mut affected_rows = 0;
        let mut extension = HashMap::new();
        for result in results {
            affected_rows += result.affected_rows;
            extension.extend(result.extension);
        }

        Ok(RegionResponseV1 {
            header: Some(ResponseHeader {
                status: Some(Status {
                    status_code: StatusCode::Success as _,
                    ..Default::default()
                }),
            }),
            affected_rows: affected_rows as _,
            extension,
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
        let request = api::v1::region::QueryRequest::decode(ticket.as_ref())
            .context(servers_error::InvalidFlightTicketSnafu)?;
        let tracing_context = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default();

        let result = self
            .handle_remote_read(request)
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
    runtime: Runtime,
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
        runtime: Runtime,
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
            RegionChange::Register(attribute) => match current_region_status {
                Some(status) => match status.clone() {
                    RegionEngineWithStatus::Registering(engine) => engine,
                    RegionEngineWithStatus::Deregistering(_) => {
                        return error::RegionBusySnafu { region_id }.fail()
                    }
                    RegionEngineWithStatus::Ready(_) => status.clone().into_engine(),
                },
                _ => self
                    .engines
                    .read()
                    .unwrap()
                    .get(attribute.engine())
                    .with_context(|| RegionEngineNotFoundSnafu {
                        name: attribute.engine(),
                    })?
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
            RegionChange::None | RegionChange::Catchup => match current_region_status {
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

    async fn handle_batch_open_requests_inner(
        &self,
        engine: RegionEngineRef,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<Vec<RegionId>> {
        let region_changes = requests
            .iter()
            .map(|(region_id, open)| {
                let attribute = parse_region_attribute(&open.engine, &open.options)?;
                Ok((*region_id, RegionChange::Register(attribute)))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        for (&region_id, region_change) in &region_changes {
            self.set_region_status_not_ready(region_id, &engine, region_change)
        }

        let mut open_regions = Vec::with_capacity(requests.len());
        let mut errors = vec![];
        match engine
            .handle_batch_open_requests(parallelism, requests)
            .await
            .with_context(|_| HandleBatchOpenRequestSnafu)
        {
            Ok(results) => {
                for (region_id, result) in results {
                    let region_change = &region_changes[&region_id];
                    match result {
                        Ok(_) => {
                            if let Err(e) = self
                                .set_region_status_ready(region_id, engine.clone(), *region_change)
                                .await
                            {
                                error!(e; "Failed to set region to ready: {}", region_id);
                                errors.push(BoxedError::new(e));
                            } else {
                                open_regions.push(region_id)
                            }
                        }
                        Err(e) => {
                            self.unset_region_status(region_id, *region_change);
                            error!(e; "Failed to open region: {}", region_id);
                            errors.push(e);
                        }
                    }
                }
            }
            Err(e) => {
                for (&region_id, region_change) in &region_changes {
                    self.unset_region_status(region_id, *region_change);
                }
                error!(e; "Failed to open batch regions");
                errors.push(BoxedError::new(e));
            }
        }

        if !errors.is_empty() {
            return error::UnexpectedSnafu {
                // Returns the first error.
                violated: format!("Failed to open batch regions: {:?}", errors[0]),
            }
            .fail();
        }

        Ok(open_regions)
    }

    pub async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<Vec<RegionId>> {
        let mut engine_grouped_requests: HashMap<String, Vec<_>> =
            HashMap::with_capacity(requests.len());
        for (region_id, request) in requests {
            if let Some(requests) = engine_grouped_requests.get_mut(&request.engine) {
                requests.push((region_id, request));
            } else {
                engine_grouped_requests
                    .insert(request.engine.to_string(), vec![(region_id, request)]);
            }
        }

        let mut results = Vec::with_capacity(engine_grouped_requests.keys().len());
        for (engine, requests) in engine_grouped_requests {
            let engine = self
                .engines
                .read()
                .unwrap()
                .get(&engine)
                .with_context(|| RegionEngineNotFoundSnafu { name: &engine })?
                .clone();
            results.push(
                self.handle_batch_open_requests_inner(engine, parallelism, requests)
                    .await,
            )
        }

        Ok(results
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>())
    }

    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse> {
        let request_type = request.request_type();
        let _timer = crate::metrics::HANDLE_REGION_REQUEST_ELAPSED
            .with_label_values(&[request_type])
            .start_timer();

        let region_change = match &request {
            RegionRequest::Create(create) => {
                let attribute = parse_region_attribute(&create.engine, &create.options)?;
                RegionChange::Register(attribute)
            }
            RegionRequest::Open(open) => {
                let attribute = parse_region_attribute(&open.engine, &open.options)?;
                RegionChange::Register(attribute)
            }
            RegionRequest::Close(_) | RegionRequest::Drop(_) => RegionChange::Deregisters,
            RegionRequest::Put(_)
            | RegionRequest::Delete(_)
            | RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_)
            | RegionRequest::Truncate(_) => RegionChange::None,
            RegionRequest::Catchup(_) => RegionChange::Catchup,
        };

        let engine = match self.get_engine(region_id, &region_change)? {
            CurrentEngine::Engine(engine) => engine,
            CurrentEngine::EarlyReturn(rows) => return Ok(RegionResponse::new(rows)),
        };

        // Sets corresponding region status to registering/deregistering before the operation.
        self.set_region_status_not_ready(region_id, &engine, &region_change);

        match engine
            .handle_request(region_id, request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })
        {
            Ok(result) => {
                // Sets corresponding region status to ready.
                self.set_region_status_ready(region_id, engine, region_change)
                    .await?;
                Ok(RegionResponse {
                    affected_rows: result.affected_rows,
                    extension: result.extension,
                })
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
            RegionChange::Register(_) => {
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
            RegionChange::Register(_) | RegionChange::Deregisters => {
                self.region_map.remove(&region_id);
            }
            RegionChange::Catchup => {}
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
            RegionChange::Register(attribute) => {
                info!(
                    "Region {region_id} is registered to engine {}",
                    attribute.engine()
                );
                self.region_map
                    .insert(region_id, RegionEngineWithStatus::Ready(engine.clone()));

                match attribute {
                    RegionAttribute::Metric { physical } => {
                        if physical {
                            // Registers the logical regions belong to the physical region (`region_id`).
                            self.register_logical_regions(&engine, region_id).await?;
                            // We only send the `on_region_registered` event of the physical region.
                            self.event_listener.on_region_registered(region_id);
                        }
                    }
                    RegionAttribute::Mito => self.event_listener.on_region_registered(region_id),
                    RegionAttribute::File => {
                        // do nothing
                    }
                }
            }
            RegionChange::Deregisters => {
                info!("Region {region_id} is deregistered from engine {engine_type}");
                self.region_map
                    .remove(&region_id)
                    .map(|(id, engine)| engine.set_writable(id, false));
                self.event_listener.on_region_deregistered(region_id);
            }
            RegionChange::Catchup => {
                if is_metric_engine(engine.name()) {
                    // Registers the logical regions belong to the physical region (`region_id`).
                    self.register_logical_regions(&engine, region_id).await?;
                }
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

        // Build query context from gRPC header
        let query_ctx: QueryContextRef = request
            .header
            .as_ref()
            .map(|h| Arc::new(h.into()))
            .unwrap_or_else(|| QueryContextBuilder::default().build().into());

        let result = self
            .query_engine
            .execute(request.plan.into(), query_ctx)
            .await
            .context(ExecuteLogicalPlanSnafu)?;

        match result.data {
            OutputData::AffectedRows(_) | OutputData::RecordBatches(_) => {
                UnsupportedOutputSnafu { expected: "stream" }.fail()
            }
            OutputData::Stream(stream) => Ok(stream),
        }
    }

    async fn stop(&self) -> Result<()> {
        // Calling async functions while iterating inside the Dashmap could easily cause the Rust
        // complains "higher-ranked lifetime error". Rust can't prove some future is legit.
        // Possible related issue: https://github.com/rust-lang/rust/issues/102211
        //
        // The walkaround is to put the async functions in the `common_runtime::spawn_global`. Or like
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

#[derive(Debug, Clone, Copy)]
enum RegionChange {
    None,
    Register(RegionAttribute),
    Deregisters,
    Catchup,
}

fn is_metric_engine(engine: &str) -> bool {
    engine == METRIC_ENGINE_NAME
}

fn parse_region_attribute(
    engine: &str,
    options: &HashMap<String, String>,
) -> Result<RegionAttribute> {
    match engine {
        MITO_ENGINE_NAME => Ok(RegionAttribute::Mito),
        METRIC_ENGINE_NAME => {
            let physical = !options.contains_key(LOGICAL_TABLE_METADATA_KEY);

            Ok(RegionAttribute::Metric { physical })
        }
        FILE_ENGINE_NAME => Ok(RegionAttribute::File),
        _ => error::UnexpectedSnafu {
            violated: format!("Unknown engine: {}", engine),
        }
        .fail(),
    }
}

#[derive(Debug, Clone, Copy)]
enum RegionAttribute {
    Mito,
    Metric { physical: bool },
    File,
}

impl RegionAttribute {
    fn engine(&self) -> &'static str {
        match self {
            RegionAttribute::Mito => MITO_ENGINE_NAME,
            RegionAttribute::Metric { .. } => METRIC_ENGINE_NAME,
            RegionAttribute::File => FILE_ENGINE_NAME,
        }
    }
}

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
        let (engine, _receiver) = MockRegionEngine::new(MITO_ENGINE_NAME);
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
        let response = mock_region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 0);
        let status = mock_region_server
            .inner
            .region_map
            .get(&region_id)
            .unwrap()
            .clone();
        assert!(matches!(status, RegionEngineWithStatus::Ready(_)));

        mock_region_server.inner.region_map.insert(
            region_id,
            RegionEngineWithStatus::Registering(engine.clone()),
        );
        let response = mock_region_server
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
        assert_eq!(response.affected_rows, 0);
        let status = mock_region_server
            .inner
            .region_map
            .get(&region_id)
            .unwrap()
            .clone();
        assert!(matches!(status, RegionEngineWithStatus::Ready(_)));
    }

    #[tokio::test]
    async fn test_region_deregistering() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let (engine, _receiver) = MockRegionEngine::new(MITO_ENGINE_NAME);

        mock_region_server.register_engine(engine.clone());

        let region_id = RegionId::new(1, 1);

        // Tries to drop/close a registering region.
        mock_region_server.inner.region_map.insert(
            region_id,
            RegionEngineWithStatus::Deregistering(engine.clone()),
        );

        let response = mock_region_server
            .handle_request(region_id, RegionRequest::Drop(RegionDropRequest {}))
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 0);

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

        let response = mock_region_server
            .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 0);

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
        let (engine, _receiver) = MockRegionEngine::new(MITO_ENGINE_NAME);

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
        let (engine, _receiver) = MockRegionEngine::with_mock_fn(
            MITO_ENGINE_NAME,
            Box::new(|_region_id, _request| {
                error::UnexpectedSnafu {
                    violated: "test".to_string(),
                }
                .fail()
            }),
        );

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
        let (engine, _) = MockRegionEngine::new(MITO_ENGINE_NAME);
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
                region_change: RegionChange::Register(RegionAttribute::Mito),
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::Engine(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Registering(engine.clone())),
                region_change: RegionChange::Register(RegionAttribute::Mito),
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::Engine(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Deregistering(engine.clone())),
                region_change: RegionChange::Register(RegionAttribute::Mito),
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionBusy);
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Ready(engine.clone())),
                region_change: RegionChange::Register(RegionAttribute::Mito),
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
