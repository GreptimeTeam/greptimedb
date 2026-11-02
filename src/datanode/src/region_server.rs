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

mod catalog;
mod registrations;
mod remote_dyn_filter;

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use api::region::{BatchRegionResponse, RegionResponse};
use api::v1::meta::TopicStat;
use api::v1::region::sync_request::ManifestInfo;
use api::v1::region::{
    ListMetadataRequest, RegionRequest as RegionRequestV1, RegionRequestFailure,
    RegionResponse as RegionResponseV1, SyncRequest, region_request,
};
use api::v1::{ResponseHeader, Status};
use arrow_flight::{FlightData, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_meta::datanode::TopicStatsReporter;
use common_query::OutputData;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_runtime::{JoinHandle, Runtime};
use common_telemetry::tracing::{self, info_span};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, error, info, warn};
use dashmap::DashMap;
use datafusion::datasource::TableProvider;
use datafusion_common::tree_node::TreeNode;
use datatypes::schema::SchemaRef;
use either::Either;
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt};
use metric_engine::engine::MetricEngine;
use mito2::engine::{MITO_ENGINE_NAME, MitoEngine};
use prost::Message;
use query::QueryEngineRef;
pub use query::dummy_catalog::{
    DummyCatalogList, DummyTableProviderFactory, TableProviderFactoryRef,
};
use query::options::should_collect_region_watermark_from_extensions;
use serde_json;
use servers::error::{
    self as servers_error, ExecuteGrpcRequestSnafu, Result as ServerResult, SuspendedSnafu,
};
use servers::grpc::FlightCompression;
use servers::grpc::flight::{
    FlightCraft, FlightRecordBatchSource, FlightRecordBatchStream, FlightRecordBatchStreamInput,
    TonicStream,
};
use servers::grpc::region_server::RegionServerHandler;
use session::context::{
    FLIGHT_METRICS_HEARTBEAT_INTERVAL, QueryContext, QueryContextBuilder, QueryContextRef,
};
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    FILE_ENGINE_NAME, LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, RELATIONAL_ENGINE_NAME,
};
use store_api::region_engine::{
    RegionEngineRef, RegionManifestInfo, RegionRole, RegionStatistic, RemapManifestsRequest,
    RemapManifestsResponse, SetRegionRoleStateResponse, SettableRegionRoleState,
    SyncRegionFromRequest,
};
use store_api::region_request::{
    self as store_region_request, AffectedRows, BatchRegionDdlRequest, RegionCatchupRequest,
    RegionCloseRequest, RegionOpenRequest, RegionRequest,
};
use store_api::storage::RegionId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::{self, timeout};
use tonic::{Request, Response, Result as TonicResult};

use crate::datanode::record_shutdown_error;
use crate::error::{
    self, BuildRegionRequestsSnafu, ConcurrentQueryLimiterClosedSnafu,
    ConcurrentQueryLimiterTimeoutSnafu, DataFusionSnafu, DecodeLogicalPlanSnafu,
    ExecuteLogicalPlanSnafu, FindLogicalRegionsSnafu, GetRegionMetadataSnafu,
    HandleBatchDdlRequestSnafu, HandleBatchOpenRequestSnafu, HandleRegionRequestSnafu,
    NewPlanDecoderSnafu, RegionEngineNotFoundSnafu, RegionNotFoundSnafu, RegionNotReadySnafu,
    Result, RuntimeJoinSnafu, SerializeJsonSnafu, StopRegionEngineSnafu, UnexpectedSnafu,
    UnsupportedOutputSnafu,
};
use crate::event_listener::RegionServerEventListenerRef;
use crate::query_stream::QueryRuntimeStream;
use crate::region_server::catalog::{NameAwareCatalogList, NameAwareDataSourceInjectorBuilder};
use crate::region_server::registrations::RemoteDynFilterRegistry;
use crate::region_server::remote_dyn_filter::wrap_remote_dyn_filter_guarded_stream;

const QUERY_RUNTIME_STREAM_BUFFER_SIZE: usize = 8;

#[derive(Clone)]
pub struct RegionServer {
    inner: Arc<RegionServerInner>,
    flight_compression: FlightCompression,
    suspend: Arc<AtomicBool>,
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
        flight_compression: FlightCompression,
    ) -> Self {
        Self::with_table_provider(
            query_engine,
            runtime,
            event_listener,
            Arc::new(DummyTableProviderFactory),
            0,
            Duration::from_millis(0),
            flight_compression,
        )
    }

    pub fn with_table_provider(
        query_engine: QueryEngineRef,
        runtime: Runtime,
        event_listener: RegionServerEventListenerRef,
        table_provider_factory: TableProviderFactoryRef,
        max_concurrent_queries: usize,
        concurrent_query_limiter_timeout: Duration,
        flight_compression: FlightCompression,
    ) -> Self {
        Self {
            inner: Arc::new(RegionServerInner::new(
                query_engine,
                runtime,
                event_listener,
                table_provider_factory,
                RegionServerParallelism::from_opts(
                    max_concurrent_queries,
                    concurrent_query_limiter_timeout,
                ),
            )),
            flight_compression,
            suspend: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Registers an engine.
    pub fn register_engine(&mut self, engine: RegionEngineRef) {
        self.inner.register_engine(engine);
    }

    /// Sets the topic stats.
    pub fn set_topic_stats_reporter(&mut self, topic_stats_reporter: Box<dyn TopicStatsReporter>) {
        self.inner.set_topic_stats_reporter(topic_stats_reporter);
    }

    /// Finds the region's engine by its id. If the region is not ready, returns `None`.
    pub fn find_engine(&self, region_id: RegionId) -> Result<Option<RegionEngineRef>> {
        match self.inner.get_engine(region_id, &RegionChange::None) {
            Ok(CurrentEngine::Engine(engine)) => Ok(Some(engine)),
            Ok(CurrentEngine::EarlyReturn(_)) => Ok(None),
            Err(error::Error::RegionNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Gets the MitoEngine if it's registered.
    pub fn mito_engine(&self) -> Option<MitoEngine> {
        if let Some(mito) = self.inner.mito_engine.read().unwrap().clone() {
            Some(mito)
        } else {
            self.inner
                .engines
                .read()
                .unwrap()
                .get(MITO_ENGINE_NAME)
                .cloned()
                .and_then(|e| {
                    let mito = e.as_any().downcast_ref::<MitoEngine>().cloned();
                    if mito.is_none() {
                        warn!("Mito engine not found in region server engines");
                    }
                    mito
                })
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
        ignore_nonexistent_region: bool,
    ) -> Result<Vec<RegionId>> {
        self.inner
            .handle_batch_open_requests(parallelism, requests, ignore_nonexistent_region)
            .await
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_batch_catchup_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionCatchupRequest)>,
    ) -> Result<Vec<(RegionId, std::result::Result<(), BoxedError>)>> {
        self.inner
            .handle_batch_catchup_requests(parallelism, requests)
            .await
    }

    #[tracing::instrument(skip_all, fields(request_type = request.request_type()))]
    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse> {
        if RegionServerInner::is_ingest_request(&request) {
            let inner = self.inner.clone();
            let request_type = request.request_type();
            let task = common_runtime::spawn_ingest(async move {
                inner.handle_request(region_id, request).await
            });
            return join_ingest_task(task, request_type).await?;
        }

        self.inner.handle_request(region_id, request).await
    }

    /// Returns a table provider for the region. Will set snapshot sequence if available in the context.
    async fn table_provider(
        &self,
        region_id: RegionId,
        ctx: Option<QueryContextRef>,
    ) -> Result<Arc<dyn TableProvider>> {
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

        let provider = self
            .inner
            .table_provider_factory
            .create(region_id, status.into_engine(), ctx)
            .await
            .context(ExecuteLogicalPlanSnafu)?;

        Ok(provider)
    }

    /// Handle reads from remote. They're often query requests received by our Arrow Flight service.
    pub async fn handle_remote_read(
        &self,
        request: api::v1::region::QueryRequest,
        query_ctx: QueryContextRef,
    ) -> Result<SendableRecordBatchStream> {
        self.handle_remote_read_inner(request, query_ctx).await
    }

    async fn handle_remote_read_inner(
        &self,
        request: api::v1::region::QueryRequest,
        query_ctx: QueryContextRef,
    ) -> Result<SendableRecordBatchStream> {
        let permit = if let Some(p) = &self.inner.parallelism {
            Some(p.acquire().await?)
        } else {
            None
        };

        let region_id = RegionId::from_u64(request.region_id);
        let engine_ctx = self
            .inner
            .query_engine
            .engine_context(query_ctx)
            .map_err(|error| {
                crate::error::InvalidQueryContextSnafu {
                    reason: error.to_string(),
                }
                .build()
            })?;
        let query_ctx = engine_ctx.query_ctx();
        let catalog_list = Arc::new(NameAwareCatalogList::new(
            self.clone(),
            region_id,
            query_ctx.clone(),
        ));

        if query_ctx.explain_verbose() {
            common_telemetry::info!("Handle remote read for region: {}", region_id);
        }

        let decoder = engine_ctx.new_plan_decoder().context(NewPlanDecoderSnafu)?;

        let plan = decoder
            .decode(Bytes::from(request.plan), catalog_list, false)
            .await
            .context(DecodeLogicalPlanSnafu)?;

        let cleanup = self.register_initial_remote_dyn_filter_cleanup(&query_ctx, region_id);

        let stream = self
            .inner
            .handle_read(
                QueryRequest {
                    header: request.header,
                    region_id,
                    plan,
                },
                query_ctx.clone(),
            )
            .await?;

        let stream = wrap_flow_region_watermark_stream(stream, region_id, &query_ctx);
        let stream = if let Some(cleanup) = cleanup {
            wrap_remote_dyn_filter_guarded_stream(stream, cleanup)
        } else {
            stream
        };
        Ok(maybe_guard_stream(stream, permit))
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        self.handle_read_inner(request).await
    }

    async fn handle_read_inner(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let permit = if let Some(p) = &self.inner.parallelism {
            Some(p.acquire().await?)
        } else {
            None
        };

        let ctx = request
            .header
            .as_ref()
            .map(QueryContext::try_from)
            .transpose()
            .map_err(|err| {
                crate::error::InvalidQueryContextSnafu {
                    reason: err.to_string(),
                }
                .build()
            })?;
        let query_ctx = Arc::new(ctx.unwrap_or_else(|| QueryContextBuilder::default().build()));

        let region_id = request.region_id;
        let injector_builder = NameAwareDataSourceInjectorBuilder::from_plan(&request.plan)
            .context(DataFusionSnafu)?;
        let mut injector = injector_builder
            .build(self, request.region_id, query_ctx.clone())
            .await?;

        let plan = request
            .plan
            .rewrite(&mut injector)
            .context(DataFusionSnafu)?
            .data;

        let cleanup = self.register_initial_remote_dyn_filter_cleanup(&query_ctx, region_id);

        let stream = self
            .inner
            .handle_read(QueryRequest { plan, ..request }, query_ctx.clone())
            .await?;

        let stream = wrap_flow_region_watermark_stream(stream, region_id, &query_ctx);
        let stream = if let Some(cleanup) = cleanup {
            wrap_remote_dyn_filter_guarded_stream(stream, cleanup)
        } else {
            stream
        };
        Ok(maybe_guard_stream(stream, permit))
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

    /// Returns the reportable topics.
    pub fn topic_stats(&self) -> Vec<TopicStat> {
        let mut reporter = self.inner.topic_stats_reporter.write().unwrap();
        let Some(reporter) = reporter.as_mut() else {
            return vec![];
        };
        reporter
            .reportable_topics()
            .into_iter()
            .map(|stat| TopicStat {
                topic_name: stat.topic,
                record_size: stat.record_size,
                record_num: stat.record_num,
                latest_entry_id: stat.latest_entry_id,
            })
            .collect()
    }

    pub fn is_region_leader(&self, region_id: RegionId) -> Option<bool> {
        self.inner.region_map.get(&region_id).and_then(|engine| {
            engine.role(region_id).map(|role| match role {
                RegionRole::Follower => false,
                RegionRole::Leader => true,
                RegionRole::StagingLeader => true,
                RegionRole::DowngradingLeader => true,
            })
        })
    }

    pub fn set_region_role(&self, region_id: RegionId, role: RegionRole) -> Result<()> {
        let engine = self
            .inner
            .region_map
            .get(&region_id)
            .with_context(|| RegionNotFoundSnafu { region_id })?;
        engine
            .set_region_role(region_id, role)
            .with_context(|_| HandleRegionRequestSnafu { region_id })
    }

    /// Sets the role of a region from a lease the metasrv grants until `deadline`, in reply to a
    /// heartbeat sent at `sent_at`. Returns false if the engine ignores a Leader lease because the
    /// region was downgraded or (re)opened after `sent_at`.
    pub fn set_region_role_with_lease(
        &self,
        region_id: RegionId,
        role: RegionRole,
        sent_at: std::time::Instant,
        deadline: std::time::Instant,
    ) -> Result<bool> {
        let engine = self
            .inner
            .region_map
            .get(&region_id)
            .with_context(|| RegionNotFoundSnafu { region_id })?;
        engine
            .set_region_role_with_lease(region_id, role, sent_at, deadline)
            .with_context(|_| HandleRegionRequestSnafu { region_id })
    }

    /// Set region role state gracefully.
    ///
    /// For [SettableRegionRoleState::Follower]:
    /// After the call returns, the engine ensures that
    /// no **further** write or flush operations will succeed in this region.
    ///
    /// For [SettableRegionRoleState::DowngradingLeader]:
    /// After the call returns, the engine ensures that
    /// no **further** write operations will succeed in this region.
    pub async fn set_region_role_state_gracefully(
        &self,
        region_id: RegionId,
        state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse> {
        // Cloning releases the DashMap guard before awaiting the engine operation.
        let engine = self
            .inner
            .region_map
            .get(&region_id)
            .map(|entry| entry.value().clone());

        match engine {
            Some(engine) => Ok(engine
                .set_region_role_state_gracefully(region_id, state)
                .await
                .with_context(|_| HandleRegionRequestSnafu { region_id })?),
            None => Ok(SetRegionRoleStateResponse::NotFound),
        }
    }

    pub fn runtime(&self) -> Runtime {
        self.inner.runtime.clone()
    }

    pub fn region_statistic(&self, region_id: RegionId) -> Option<RegionStatistic> {
        match self.inner.region_map.get(&region_id) {
            Some(e) => e.region_statistic(region_id),
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
        {
            let mut engines = self.inner.engines.write().unwrap();
            if !engines.contains_key(engine.name()) {
                debug!("Registering test engine: {}", engine.name());
                engines.insert(engine.name().to_string(), engine.clone());
            }
        }

        self.inner
            .region_map
            .insert(region_id, RegionEngineWithStatus::Ready(engine));
    }

    async fn handle_batch_ddl_requests(
        &self,
        request: region_request::Body,
    ) -> Result<RegionResponse> {
        // Safety: we have already checked the request type in `RegionServer::handle()`.
        let batch_request = BatchRegionDdlRequest::try_from_request_body(request)
            .context(BuildRegionRequestsSnafu)?
            .unwrap();
        let tracing_context = TracingContext::from_current_span();

        let span = tracing_context.attach(info_span!("RegionServer::handle_batch_ddl_requests"));
        self.inner
            .handle_batch_ddl_requests(batch_request)
            .trace(span)
            .await
    }

    async fn handle_requests_in_parallel(
        &self,
        request: region_request::Body,
    ) -> Result<BatchRegionResponse<error::Error>> {
        let requests = prepare_batch_write_requests(request);
        let requests = match self.try_handle_mito_batch_puts(requests).await? {
            Either::Left(response) => return Ok(response),
            Either::Right(requests) => requests,
        };
        let requests = match self.try_handle_metric_batch_puts(requests).await? {
            Either::Left(response) => return Ok(response),
            Either::Right(requests) => requests,
        };
        let tracing_context = TracingContext::from_current_span();
        let results = futures::future::join_all(requests.into_iter().map(|request| {
            let span = tracing_context.attach(info_span!(
                "RegionServer::handle_region_request",
                region_id = request.region_id.to_string()
            ));
            async move {
                (
                    request.index,
                    self.handle_request(request.region_id, request.request)
                        .trace(span)
                        .await,
                )
            }
        }))
        .await;
        let mut response = BatchRegionResponse {
            affected_rows: 0,
            extensions: HashMap::new(),
            failures: Vec::new(),
        };
        for (index, result) in results {
            match result {
                Ok(result) => {
                    response.affected_rows += result.affected_rows;
                    response.extensions.extend(result.extensions);
                }
                Err(error) => {
                    log_batch_write_error(&error, index);
                    response.failures.push((index, error));
                }
            }
        }
        Ok(response)
    }

    async fn handle_requests_in_serial(
        &self,
        request: region_request::Body,
    ) -> Result<RegionResponse> {
        let requests =
            RegionRequest::try_from_request_body(request).context(BuildRegionRequestsSnafu)?;
        let tracing_context = TracingContext::from_current_span();

        let mut affected_rows = 0;
        let mut extensions = HashMap::new();
        for (region_id, req) in requests {
            let span = tracing_context.attach(info_span!(
                "RegionServer::handle_region_request",
                region_id = region_id.to_string()
            ));
            let result = self.handle_request(region_id, req).trace(span).await?;

            affected_rows += result.affected_rows;
            extensions.extend(result.extensions);
        }

        Ok(RegionResponse {
            affected_rows,
            extensions,
            metadata: Vec::new(),
        })
    }

    /// Attempts metric batch writes, returning the requests when ordinary execution is needed.
    async fn try_handle_metric_batch_puts(
        &self,
        requests: Vec<IndexedRegionRequest>,
    ) -> Result<Either<BatchRegionResponse<error::Error>, Vec<IndexedRegionRequest>>> {
        let Some(first) = requests.first() else {
            return Ok(Either::Right(requests));
        };
        if !matches!(first.request, RegionRequest::Put(_)) {
            return Ok(Either::Right(requests));
        }

        let first_region_id = first.region_id;
        let request_type = first.request.request_type();

        // Upstream batching keeps requests from the same engine together.
        // Ordinary execution reports engine lookup failures per entry.
        let Ok(CurrentEngine::Engine(engine)) =
            self.inner.get_engine(first_region_id, &RegionChange::None)
        else {
            return Ok(Either::Right(requests));
        };
        if engine.name() != METRIC_ENGINE_NAME
            || !requests
                .iter()
                .all(|request| matches!(request.request, RegionRequest::Put(_)))
        {
            return Ok(Either::Right(requests));
        }
        let metric_engine = engine
            .as_any()
            .downcast_ref::<MetricEngine>()
            .context(UnexpectedSnafu {
                violated: "Failed to downcast to MetricEngine",
            })?
            .clone();

        let indices: Vec<_> = requests
            .iter()
            .map(|request| (request.index, request.region_id))
            .collect();
        let put_requests = requests.into_iter().map(|request| match request.request {
            RegionRequest::Put(put) => (request.region_id, put),
            _ => unreachable!("Already checked all are Put"),
        });
        let tracing_context = TracingContext::from_current_span();
        let batch_size = put_requests.len();
        let span = tracing_context.attach(info_span!(
            "RegionServer::handle_metric_batch_puts",
            batch_size = batch_size
        ));
        let task = common_runtime::spawn_ingest(async move {
            metric_engine
                .put_regions_batch(put_requests)
                .trace(span)
                .await
        });
        let results = join_ingest_task(task, request_type).await?;
        let mut response = BatchRegionResponse {
            affected_rows: 0,
            extensions: HashMap::new(),
            failures: Vec::new(),
        };
        for group in results {
            let region_id = indices[group.request_indices[0]].1;
            let result = group
                .result
                .map_err(BoxedError::new)
                .context(HandleRegionRequestSnafu { region_id })
                .map(RegionResponse::new);
            record_ingest_result(request_type, group.request_indices.len(), &result);
            match result {
                Ok(result) => {
                    response.affected_rows += result.affected_rows;
                }
                Err(error) => {
                    log_batch_write_error(&error, indices[group.request_indices[0]].0);
                    let error = Arc::new(error);
                    response
                        .failures
                        .extend(group.request_indices.into_iter().map(|index| {
                            (
                                indices[index].0,
                                error::BatchWriteSnafu.into_error(Arc::clone(&error)),
                            )
                        }));
                }
            }
        }
        Ok(Either::Left(response))
    }

    /// Batches puts into ready mito regions and retains every entry's outcome.
    async fn try_handle_mito_batch_puts(
        &self,
        requests: Vec<IndexedRegionRequest>,
    ) -> Result<Either<BatchRegionResponse<error::Error>, Vec<IndexedRegionRequest>>> {
        let Some(engine) = self.mito_engine_for_batch_puts(&requests) else {
            return Ok(Either::Right(requests));
        };
        let request_type = requests[0].request.request_type();
        let mut indices: HashMap<RegionId, std::collections::VecDeque<usize>> = HashMap::new();
        let puts = requests
            .into_iter()
            .map(|request| {
                indices
                    .entry(request.region_id)
                    .or_default()
                    .push_back(request.index);
                match request.request {
                    RegionRequest::Put(put) => (request.region_id, put),
                    _ => unreachable!("Already checked all are Put"),
                }
            })
            .collect();
        let started = Instant::now();
        let tracing_context = TracingContext::from_current_span();
        let task = common_runtime::spawn_ingest(async move {
            let mut pending = engine
                .handle_batch_put_requests(puts)
                .into_iter()
                .map(|(region_ids, response)| {
                    let request_indices = region_ids
                        .iter()
                        .filter_map(|region_id| {
                            indices
                                .get_mut(region_id)
                                .and_then(|indices| indices.pop_front())
                        })
                        .collect::<Vec<_>>();
                    let span = tracing_context.attach(info_span!(
                        "RegionServer::handle_region_request",
                        region_id = region_ids[0].to_string()
                    ));
                    async move { (region_ids, request_indices, response.await) }.trace(span)
                })
                .collect::<FuturesUnordered<_>>();
            let histogram =
                crate::metrics::HANDLE_REGION_REQUEST_ELAPSED.with_label_values(&[request_type]);
            let mut response = BatchRegionResponse {
                affected_rows: 0,
                extensions: HashMap::new(),
                failures: Vec::new(),
            };
            while let Some((region_ids, request_indices, result)) = pending.next().await {
                let elapsed = started.elapsed().as_secs_f64();
                for _ in &region_ids {
                    histogram.observe(elapsed);
                }
                let result = result.context(HandleRegionRequestSnafu {
                    region_id: region_ids[0],
                });
                record_ingest_result(request_type, region_ids.len(), &result);
                match result {
                    Ok(result) => {
                        response.affected_rows += result.affected_rows;
                        response.extensions.extend(result.extensions);
                    }
                    Err(error) => {
                        log_batch_write_error(&error, request_indices[0]);
                        let error = Arc::new(error);
                        response
                            .failures
                            .extend(request_indices.into_iter().map(|index| {
                                (index, error::BatchWriteSnafu.into_error(Arc::clone(&error)))
                            }));
                    }
                }
            }
            response.failures.sort_unstable_by_key(|(index, _)| *index);
            response
        });
        join_ingest_task(task, request_type).await.map(Either::Left)
    }

    /// Returns the mito engine when there are at least two requests, every one a put into a ready
    /// mito region.
    fn mito_engine_for_batch_puts(
        &self,
        requests: &[IndexedRegionRequest],
    ) -> Option<RegionEngineRef> {
        if requests.len() < 2 {
            return None;
        }
        let first_region_id = requests[0].region_id;
        let all_puts_to_ready_mito_regions = requests.iter().all(|request| {
            matches!(request.request, RegionRequest::Put(_))
                && matches!(
                    self.inner.region_map.get(&request.region_id).as_deref(),
                    Some(RegionEngineWithStatus::Ready(engine)) if engine.name() == MITO_ENGINE_NAME
                )
        });
        if !all_puts_to_ready_mito_regions {
            return None;
        }
        match self.inner.region_map.get(&first_region_id).as_deref() {
            Some(RegionEngineWithStatus::Ready(engine)) => Some(engine.clone()),
            _ => None,
        }
    }

    async fn handle_sync_region_request(&self, request: &SyncRequest) -> Result<RegionResponse> {
        let region_id = RegionId::from_u64(request.region_id);
        let manifest_info = request
            .manifest_info
            .context(error::MissingRequiredFieldSnafu {
                name: "manifest_info",
            })?;

        let manifest_info = match manifest_info {
            ManifestInfo::MitoManifestInfo(info) => {
                RegionManifestInfo::mito(info.data_manifest_version, 0, 0)
            }
            ManifestInfo::MetricManifestInfo(info) => RegionManifestInfo::metric(
                info.data_manifest_version,
                0,
                info.metadata_manifest_version,
                0,
            ),
        };

        let tracing_context = TracingContext::from_current_span();
        let span = tracing_context.attach(info_span!("RegionServer::handle_sync_region_request"));

        self.sync_region(
            region_id,
            SyncRegionFromRequest::from_manifest(manifest_info),
        )
        .trace(span)
        .await
        .map(|_| RegionResponse::new(AffectedRows::default()))
    }

    /// Handles the ListMetadata request and retrieves metadata for specified regions.
    ///
    /// Returns the results as a JSON-serialized list in the [RegionResponse]. It serializes
    /// non-existing regions as `null`.
    #[tracing::instrument(skip_all)]
    async fn handle_list_metadata_request(
        &self,
        request: &ListMetadataRequest,
    ) -> Result<RegionResponse> {
        let mut region_metadatas = Vec::new();
        // Collect metadata for each region
        for region_id in &request.region_ids {
            let region_id = RegionId::from_u64(*region_id);
            // Get the engine.
            let Some(engine) = self.find_engine(region_id)? else {
                region_metadatas.push(None);
                continue;
            };

            match engine.get_metadata(region_id).await {
                Ok(metadata) => region_metadatas.push(Some(metadata)),
                Err(err) => {
                    if err.status_code() == StatusCode::RegionNotFound {
                        region_metadatas.push(None);
                    } else {
                        Err(err).with_context(|_| GetRegionMetadataSnafu {
                            engine: engine.name(),
                            region_id,
                        })?;
                    }
                }
            }
        }

        // Serialize metadata to JSON
        let json_result = serde_json::to_vec(&region_metadatas).context(SerializeJsonSnafu)?;

        let response = RegionResponse::from_metadata(json_result);

        Ok(response)
    }

    /// Sync region manifest and registers new opened logical regions.
    pub async fn sync_region(
        &self,
        region_id: RegionId,
        request: SyncRegionFromRequest,
    ) -> Result<()> {
        let engine = match self.inner.get_engine(region_id, &RegionChange::None)? {
            CurrentEngine::Engine(engine) => engine,
            _ => {
                return UnexpectedSnafu {
                    violated: "unexpected EarlyReturn engine status for a ready region",
                }
                .fail();
            }
        };

        self.inner
            .handle_sync_region(&engine, region_id, request)
            .await
    }

    /// Remaps manifests from old regions to new regions.
    pub async fn remap_manifests(
        &self,
        request: RemapManifestsRequest,
    ) -> Result<RemapManifestsResponse> {
        let region_id = request.region_id;
        let engine = match self.inner.get_engine(region_id, &RegionChange::None)? {
            CurrentEngine::Engine(engine) => engine,
            _ => {
                return UnexpectedSnafu {
                    violated: "unexpected EarlyReturn engine status for a ready region",
                }
                .fail();
            }
        };

        engine
            .remap_manifests(request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })
    }

    fn is_suspended(&self) -> bool {
        self.suspend.load(Ordering::Relaxed)
    }

    pub(crate) fn suspend_state(&self) -> Arc<AtomicBool> {
        self.suspend.clone()
    }
}

fn wrap_flow_region_watermark_stream(
    stream: SendableRecordBatchStream,
    region_id: RegionId,
    query_ctx: &QueryContextRef,
) -> SendableRecordBatchStream {
    if should_collect_region_watermark_from_extensions(&query_ctx.extensions())
        && let Some(seq) = query_ctx.get_snapshot(region_id.as_u64())
    {
        Box::pin(RegionWatermarkStream::new(stream, region_id, seq)) as SendableRecordBatchStream
    } else {
        stream
    }
}

/// Wraps a region read stream so terminal metrics can carry the scan-open watermark.
struct RegionWatermarkStream {
    stream: SendableRecordBatchStream,
    region_id: u64,
    snapshot_seq: u64,
    finished: bool,
}

impl RegionWatermarkStream {
    fn new(stream: SendableRecordBatchStream, region_id: RegionId, snapshot_seq: u64) -> Self {
        Self {
            stream,
            region_id: region_id.as_u64(),
            snapshot_seq,
            finished: false,
        }
    }

    fn merged_metrics(&self, mut metrics: RecordBatchMetrics) -> RecordBatchMetrics {
        if metrics
            .region_watermarks
            .iter()
            .any(|entry| entry.region_id == self.region_id)
        {
            return metrics;
        }

        metrics
            .region_watermarks
            .push(common_recordbatch::adapter::RegionWatermarkEntry {
                region_id: self.region_id,
                watermark: Some(self.snapshot_seq),
            });
        metrics
    }
}

impl RecordBatchStream for RegionWatermarkStream {
    fn name(&self) -> &str {
        self.stream.name()
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.stream.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.stream.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        let base = self.stream.metrics();
        if !self.finished {
            return base;
        }

        Some(self.merged_metrics(base.unwrap_or_default()))
    }
}

impl Stream for RegionWatermarkStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

#[async_trait]
impl RegionServerHandler for RegionServer {
    async fn handle_region_request(
        &self,
        request: RegionRequestV1,
    ) -> ServerResult<RegionResponseV1> {
        let accept_partial_result = request
            .header
            .as_ref()
            .and_then(|header| header.query_context.as_ref())
            .and_then(|context| context.extensions.get(api::region::ACCEPT_PARTIAL_RESULT))
            .is_some_and(|value| value == "true");
        let body = request.body.context(servers_error::InvalidQuerySnafu {
            reason: "Expecting non-empty region request body.",
        })?;
        self.handle_inner(body, accept_partial_result).await
    }

    async fn handle(&self, request: region_request::Body) -> ServerResult<RegionResponseV1> {
        self.handle_inner(request, false).await
    }
}

impl RegionServer {
    async fn handle_inner(
        &self,
        request: region_request::Body,
        accept_partial_result: bool,
    ) -> ServerResult<RegionResponseV1> {
        let request_count = match &request {
            region_request::Body::Inserts(requests) => Some(requests.requests.len()),
            region_request::Body::Deletes(requests) => Some(requests.requests.len()),
            _ => None,
        };
        if let Some(request_count) = request_count {
            return self
                .handle_write_request(request, request_count, accept_partial_result)
                .await;
        }
        let failed_requests_cnt = crate::metrics::REGION_SERVER_REQUEST_FAILURE_COUNT
            .with_label_values(&[request.as_ref()]);
        let response = match &request {
            region_request::Body::Creates(_)
            | region_request::Body::Drops(_)
            | region_request::Body::Alters(_) => self.handle_batch_ddl_requests(request).await,
            region_request::Body::Sync(sync_request) => {
                self.handle_sync_region_request(sync_request).await
            }
            region_request::Body::ListMetadata(list_metadata_request) => {
                self.handle_list_metadata_request(list_metadata_request)
                    .await
            }
            region_request::Body::RemoteDynFilter(remote_dyn_filter_request) => {
                self.handle_remote_dyn_filter_request(remote_dyn_filter_request)
                    .await
            }
            _ => self.handle_requests_in_serial(request).await,
        }
        .map_err(BoxedError::new)
        .inspect_err(|_| failed_requests_cnt.inc())
        .context(ExecuteGrpcRequestSnafu)?;
        Ok(RegionResponseV1 {
            header: Some(ResponseHeader {
                status: Some(Status {
                    status_code: StatusCode::Success as _,
                    ..Default::default()
                }),
            }),
            affected_rows: response.affected_rows as _,
            extensions: response.extensions,
            metadata: response.metadata,
            failures: Vec::new(),
        })
    }

    async fn handle_write_request(
        &self,
        request: region_request::Body,
        entry_count: usize,
        accept_partial_result: bool,
    ) -> ServerResult<RegionResponseV1> {
        let failed_requests_cnt = crate::metrics::REGION_SERVER_REQUEST_FAILURE_COUNT
            .with_label_values(&[request.as_ref()]);
        let mut result = self
            .handle_requests_in_parallel(request)
            .await
            .map_err(BoxedError::new)
            .inspect_err(|_| failed_requests_cnt.inc())
            .context(ExecuteGrpcRequestSnafu)?;
        if !result.failures.is_empty() {
            failed_requests_cnt.inc();
            // Old clients and single-entry writes retain tonic errors and retry metadata.
            if (!accept_partial_result || entry_count == 1)
                && let Some((_, error)) = result.failures.drain(..).min_by_key(|(index, _)| *index)
            {
                return Err(BoxedError::new(error)).context(ExecuteGrpcRequestSnafu);
            }
        }
        Ok(encode_batch_write_response(result))
    }
}

#[async_trait]
impl FlightCraft for RegionServer {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> TonicResult<Response<TonicStream<FlightData>>> {
        ensure!(!self.is_suspended(), SuspendedSnafu);

        let ticket = request.into_inner().ticket;
        let request = api::v1::region::QueryRequest::decode(ticket.as_ref())
            .context(servers_error::InvalidFlightTicketSnafu)?;
        let tracing_context = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default();
        let query_ctx = request
            .header
            .as_ref()
            .map(QueryContext::try_from)
            .transpose()
            .map_err(|err| {
                servers_error::InvalidParameterSnafu {
                    reason: err.to_string(),
                }
                .build()
            })?
            .map(Arc::new)
            .unwrap_or(QueryContext::arc());

        let region_server = self.clone();
        let initializer_query_ctx = query_ctx.clone();
        let initializer_tracing_context = tracing_context.clone();
        let initializer = async move {
            region_server
                .handle_remote_read(request, initializer_query_ctx)
                .trace(initializer_tracing_context.attach(info_span!("RegionServer::handle_read")))
                .await
                .map_err(Into::into)
        };

        let stream = Box::pin(FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::initializer(async move {
                initializer
                    .await
                    .map(FlightRecordBatchSource::RecordBatches)
            }),
            tracing_context,
            self.flight_compression,
            query_ctx,
        ));
        Ok(Response::new(stream))
    }
}

/// A decoded write retains its protobuf entry index even when preceding entries are empty.
struct IndexedRegionRequest {
    index: usize,
    region_id: RegionId,
    request: RegionRequest,
}

fn prepare_batch_write_requests(body: region_request::Body) -> Vec<IndexedRegionRequest> {
    match body {
        region_request::Body::Inserts(inserts) => inserts
            .requests
            .into_iter()
            .enumerate()
            .filter_map(|(index, request)| {
                store_region_request::make_region_put(request).map(|(region_id, request)| {
                    IndexedRegionRequest {
                        index,
                        region_id,
                        request: RegionRequest::Put(request),
                    }
                })
            })
            .collect(),
        region_request::Body::Deletes(deletes) => deletes
            .requests
            .into_iter()
            .enumerate()
            .filter_map(|(index, request)| {
                store_region_request::make_region_delete(request).map(|(region_id, request)| {
                    IndexedRegionRequest {
                        index,
                        region_id,
                        request: RegionRequest::Delete(request),
                    }
                })
            })
            .collect(),
        _ => unreachable!("Only inserts/deletes are prepared as batch writes"),
    }
}

fn encode_batch_write_response(result: BatchRegionResponse<error::Error>) -> RegionResponseV1 {
    RegionResponseV1 {
        header: Some(ResponseHeader {
            status: Some(Status {
                status_code: if result.failures.is_empty() {
                    StatusCode::Success
                } else {
                    StatusCode::PartialFailure
                } as u32,
                err_msg: if let Some((index, error)) = result.first_failure() {
                    format!(
                        "{} region requests failed; entry {index}: {}",
                        result.failures.len(),
                        error.output_msg()
                    )
                } else {
                    String::new()
                },
                ..Default::default()
            }),
        }),
        metadata: Vec::new(),
        affected_rows: result.affected_rows as _,
        extensions: result.extensions,
        failures: result
            .failures
            .into_iter()
            .map(|(index, error)| RegionRequestFailure {
                request_index: index as u32,
                status: Some(Status {
                    status_code: error.status_code() as u32,
                    err_msg: error.output_msg(),
                    retry_hint: error.retry_hint().as_str().to_string(),
                }),
            })
            .collect(),
    }
}

fn log_batch_write_error(error: &error::Error, index: usize) {
    if error.status_code().should_log_error() {
        error!(error; "Failed to handle batch entry, request_index: {}", index);
    } else {
        debug!(
            "Failed to handle batch entry, request_index: {}, err: {}",
            index, error
        );
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
    /// The number of queries allowed to be executed at the same time.
    /// Act as last line of defense on datanode to prevent query overloading.
    parallelism: Option<RegionServerParallelism>,
    /// The topic stats reporter.
    topic_stats_reporter: RwLock<Option<Box<dyn TopicStatsReporter>>>,
    /// HACK(zhongzc): Direct MitoEngine handle for diagnostics. This couples the
    /// server with a concrete engine; acceptable for now to fetch Mito-specific
    /// info (e.g., list SSTs). Consider a diagnostics trait later.
    mito_engine: RwLock<Option<MitoEngine>>,
    /// TODO(remote-dyn-filter): Reap this query-scoped placeholder registry on query finish/cancel
    /// and later fold it into the real remote dyn filter runtime state lifecycle.
    initial_remote_dyn_filter_registrations: RemoteDynFilterRegistry,
}

struct RegionServerParallelism {
    semaphore: Arc<Semaphore>,
    timeout: Duration,
}

impl RegionServerParallelism {
    pub fn from_opts(
        max_concurrent_queries: usize,
        concurrent_query_limiter_timeout: Duration,
    ) -> Option<Self> {
        if max_concurrent_queries == 0 {
            return None;
        }
        Some(RegionServerParallelism {
            semaphore: Arc::new(Semaphore::new(max_concurrent_queries)),
            timeout: concurrent_query_limiter_timeout,
        })
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        timeout(self.timeout, self.semaphore.clone().acquire_owned())
            .await
            .context(ConcurrentQueryLimiterTimeoutSnafu)?
            .context(ConcurrentQueryLimiterClosedSnafu)
    }
}

/// Wraps a record batch stream and holds a concurrency permit until the stream is
/// fully consumed (dropped), so `max_concurrent_queries` bounds the number of
/// in-flight read streams, not just query planning.
struct PermitGuardedStream {
    inner: SendableRecordBatchStream,
    _permit: OwnedSemaphorePermit,
}

impl RecordBatchStream for PermitGuardedStream {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.metrics()
    }
}

impl Stream for PermitGuardedStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Wraps `stream` so it holds `permit` until fully consumed. Returns `stream`
/// unchanged when no permit was acquired (limiter disabled).
fn maybe_guard_stream(
    stream: SendableRecordBatchStream,
    permit: Option<OwnedSemaphorePermit>,
) -> SendableRecordBatchStream {
    match permit {
        Some(permit) => Box::pin(PermitGuardedStream {
            inner: stream,
            _permit: permit,
        }),
        None => stream,
    }
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
        parallelism: Option<RegionServerParallelism>,
    ) -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
            region_map: DashMap::new(),
            query_engine,
            runtime,
            event_listener,
            table_provider_factory,
            parallelism,
            topic_stats_reporter: RwLock::new(None),
            mito_engine: RwLock::new(None),
            initial_remote_dyn_filter_registrations: RemoteDynFilterRegistry::new(),
        }
    }

    pub fn register_engine(&self, engine: RegionEngineRef) {
        let engine_name = engine.name();
        if engine_name == MITO_ENGINE_NAME
            && let Some(mito_engine) = engine.as_any().downcast_ref::<MitoEngine>()
        {
            *self.mito_engine.write().unwrap() = Some(mito_engine.clone());
        }

        info!("Region Engine {engine_name} is registered");
        self.engines
            .write()
            .unwrap()
            .insert(engine_name.to_string(), engine);
    }

    pub fn set_topic_stats_reporter(&self, topic_stats_reporter: Box<dyn TopicStatsReporter>) {
        info!("Set topic stats reporter");
        *self.topic_stats_reporter.write().unwrap() = Some(topic_stats_reporter);
    }

    fn is_ingest_request(request: &RegionRequest) -> bool {
        matches!(
            request,
            RegionRequest::Put(_) | RegionRequest::Delete(_) | RegionRequest::BulkInserts(_)
        )
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
                        return error::RegionBusySnafu { region_id }.fail();
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
            RegionChange::OfflineCleanup(attribute) => match current_region_status {
                Some(status) => match status.clone() {
                    RegionEngineWithStatus::Registering(_)
                    | RegionEngineWithStatus::Deregistering(_)
                    | RegionEngineWithStatus::Ready(_) => {
                        return error::RegionBusySnafu { region_id }.fail();
                    }
                },
                None => self
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
                        return error::RegionBusySnafu { region_id }.fail();
                    }
                    RegionEngineWithStatus::Deregistering(_) => {
                        return Ok(CurrentEngine::EarlyReturn(0));
                    }
                    RegionEngineWithStatus::Ready(_) => status.clone().into_engine(),
                },
                None => return Ok(CurrentEngine::EarlyReturn(0)),
            },
            RegionChange::None | RegionChange::Catchup | RegionChange::Ingest => {
                match current_region_status {
                    Some(status) => match status.clone() {
                        RegionEngineWithStatus::Registering(_) => {
                            return error::RegionNotReadySnafu { region_id }.fail();
                        }
                        RegionEngineWithStatus::Deregistering(_) => {
                            return error::RegionNotFoundSnafu { region_id }.fail();
                        }
                        RegionEngineWithStatus::Ready(engine) => engine,
                    },
                    None => return error::RegionNotFoundSnafu { region_id }.fail(),
                }
            }
        };

        Ok(CurrentEngine::Engine(engine))
    }

    async fn handle_batch_open_requests_inner(
        &self,
        engine: RegionEngineRef,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
        ignore_nonexistent_region: bool,
    ) -> Result<Vec<RegionId>> {
        let request_count = requests.len();
        let region_changes = requests
            .iter()
            .map(|(region_id, open)| {
                let attribute = parse_region_attribute(&open.engine, &open.options)?;
                Ok((*region_id, RegionChange::Register(attribute)))
            })
            .collect::<Result<HashMap<_, _>>>()
            .inspect_err(|err| {
                record_region_open_failures(engine.name(), err.status_code(), request_count);
            })?;

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
                                record_region_open_failures(engine.name(), e.status_code(), 1);
                                errors.push(BoxedError::new(e));
                            } else {
                                open_regions.push(region_id)
                            }
                        }
                        Err(e) => {
                            self.unset_region_status(region_id, &engine, *region_change);
                            if e.status_code() == StatusCode::RegionNotFound
                                && ignore_nonexistent_region
                            {
                                warn!("Region {} not found, ignore it, source: {:?}", region_id, e);
                            } else {
                                error!(e; "Failed to open region: {}", region_id);
                                record_region_open_failures(engine.name(), e.status_code(), 1);
                                errors.push(e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                for (&region_id, region_change) in &region_changes {
                    self.unset_region_status(region_id, &engine, *region_change);
                }
                error!(e; "Failed to open batch regions");
                record_region_open_failures(engine.name(), e.status_code(), request_count);
                errors.push(BoxedError::new(e));
            }
        }

        if !errors.is_empty() {
            // Preserve the first region error so callers can honor its status code and retry hint.
            return Err(errors.swap_remove(0)).context(HandleBatchOpenRequestSnafu);
        }

        Ok(open_regions)
    }

    pub async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
        ignore_nonexistent_region: bool,
    ) -> Result<Vec<RegionId>> {
        let mut engine_grouped_requests: HashMap<String, Vec<_>> =
            HashMap::with_capacity(requests.len());
        for (region_id, request) in requests {
            if let Some(requests) = engine_grouped_requests.get_mut(&request.engine) {
                requests.push((region_id, request));
            } else {
                engine_grouped_requests.insert(request.engine.clone(), vec![(region_id, request)]);
            }
        }

        let mut results = Vec::with_capacity(engine_grouped_requests.keys().len());
        for (engine, requests) in engine_grouped_requests {
            let engine = self
                .engines
                .read()
                .unwrap()
                .get(&engine)
                .with_context(|| RegionEngineNotFoundSnafu { name: &engine })
                .inspect_err(|err| {
                    record_region_open_failures(&engine, err.status_code(), requests.len());
                })?
                .clone();
            results.push(
                self.handle_batch_open_requests_inner(
                    engine,
                    parallelism,
                    requests,
                    ignore_nonexistent_region,
                )
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

    pub async fn handle_batch_catchup_requests_inner(
        &self,
        engine: RegionEngineRef,
        parallelism: usize,
        requests: Vec<(RegionId, RegionCatchupRequest)>,
    ) -> Result<Vec<(RegionId, std::result::Result<(), BoxedError>)>> {
        for (region_id, _) in &requests {
            self.set_region_status_not_ready(*region_id, &engine, &RegionChange::Catchup);
        }
        let region_ids = requests
            .iter()
            .map(|(region_id, _)| *region_id)
            .collect::<Vec<_>>();
        let mut responses = Vec::with_capacity(requests.len());
        match engine
            .handle_batch_catchup_requests(parallelism, requests)
            .await
        {
            Ok(results) => {
                for (region_id, result) in results {
                    match result {
                        Ok(_) => {
                            if let Err(e) = self
                                .set_region_status_ready(
                                    region_id,
                                    engine.clone(),
                                    RegionChange::Catchup,
                                )
                                .await
                            {
                                error!(e; "Failed to set region to ready: {}", region_id);
                                responses.push((region_id, Err(BoxedError::new(e))));
                            } else {
                                responses.push((region_id, Ok(())));
                            }
                        }
                        Err(e) => {
                            self.unset_region_status(region_id, &engine, RegionChange::Catchup);
                            error!(e; "Failed to catchup region: {}", region_id);
                            responses.push((region_id, Err(e)));
                        }
                    }
                }
            }
            Err(e) => {
                for region_id in region_ids {
                    self.unset_region_status(region_id, &engine, RegionChange::Catchup);
                }
                error!(e; "Failed to catchup batch regions");
                return error::UnexpectedSnafu {
                    violated: format!("Failed to catchup batch regions: {:?}", e),
                }
                .fail();
            }
        }

        Ok(responses)
    }

    pub async fn handle_batch_catchup_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionCatchupRequest)>,
    ) -> Result<Vec<(RegionId, std::result::Result<(), BoxedError>)>> {
        let mut engine_grouped_requests: HashMap<String, Vec<_>> = HashMap::new();

        let mut responses = Vec::with_capacity(requests.len());
        for (region_id, request) in requests {
            if let Ok(engine) = self.get_engine(region_id, &RegionChange::Catchup) {
                match engine {
                    CurrentEngine::Engine(engine) => {
                        engine_grouped_requests
                            .entry(engine.name().to_string())
                            .or_default()
                            .push((region_id, request));
                    }
                    CurrentEngine::EarlyReturn(_) => {
                        return error::UnexpectedSnafu {
                            violated: format!("Unexpected engine type for region {}", region_id),
                        }
                        .fail();
                    }
                }
            } else {
                responses.push((
                    region_id,
                    Err(BoxedError::new(
                        error::RegionNotFoundSnafu { region_id }.build(),
                    )),
                ));
            }
        }

        for (engine, requests) in engine_grouped_requests {
            let engine = self
                .engines
                .read()
                .unwrap()
                .get(&engine)
                .with_context(|| RegionEngineNotFoundSnafu { name: &engine })?
                .clone();
            responses.extend(
                self.handle_batch_catchup_requests_inner(engine, parallelism, requests)
                    .await?,
            );
        }

        Ok(responses)
    }

    // Handle requests in batch.
    //
    // limitation: all create requests must be in the same engine.
    pub async fn handle_batch_ddl_requests(
        &self,
        batch_request: BatchRegionDdlRequest,
    ) -> Result<RegionResponse> {
        let region_changes = match &batch_request {
            BatchRegionDdlRequest::Create(requests) => requests
                .iter()
                .map(|(region_id, create)| {
                    let attribute = parse_region_attribute(&create.engine, &create.options)?;
                    Ok((*region_id, RegionChange::Register(attribute)))
                })
                .collect::<Result<Vec<_>>>()?,
            BatchRegionDdlRequest::Drop(requests) => requests
                .iter()
                .map(|(region_id, _)| (*region_id, RegionChange::Deregisters))
                .collect::<Vec<_>>(),
            BatchRegionDdlRequest::Alter(requests) => requests
                .iter()
                .map(|(region_id, _)| (*region_id, RegionChange::None))
                .collect::<Vec<_>>(),
        };

        // The ddl procedure will ensure all requests are in the same engine.
        // Therefore, we can get the engine from the first request.
        let (first_region_id, first_region_change) = region_changes.first().unwrap();
        let engine = match self.get_engine(*first_region_id, first_region_change)? {
            CurrentEngine::Engine(engine) => engine,
            CurrentEngine::EarlyReturn(rows) => return Ok(RegionResponse::new(rows)),
        };

        for (region_id, region_change) in region_changes.iter() {
            self.set_region_status_not_ready(*region_id, &engine, region_change);
        }

        let ddl_type = batch_request.request_type();
        let result = engine
            .handle_batch_ddl_requests(batch_request)
            .await
            .context(HandleBatchDdlRequestSnafu { ddl_type });

        match result {
            Ok(result) => {
                for (region_id, region_change) in &region_changes {
                    self.set_region_status_ready(*region_id, engine.clone(), *region_change)
                        .await?;
                }

                Ok(RegionResponse {
                    affected_rows: result.affected_rows,
                    extensions: result.extensions,
                    metadata: Vec::new(),
                })
            }
            Err(err) => {
                for (region_id, region_change) in region_changes {
                    self.unset_region_status(region_id, &engine, region_change);
                }

                Err(err)
            }
        }
    }

    pub async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse> {
        let open_engine = match &request {
            RegionRequest::Open(open) => Some(open.engine.clone()),
            _ => None,
        };
        self.handle_request_inner(region_id, request)
            .await
            .inspect_err(|err| {
                if let Some(engine) = open_engine {
                    record_region_open_failures(&engine, err.status_code(), 1);
                }
            })
    }

    async fn handle_request_inner(
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
            RegionRequest::CleanUp(clean_up) => {
                let attribute = parse_region_attribute(&clean_up.engine, &clean_up.options)?;
                RegionChange::OfflineCleanup(attribute)
            }
            RegionRequest::Close(_) | RegionRequest::Drop(_) => RegionChange::Deregisters,
            RegionRequest::Put(_) | RegionRequest::Delete(_) | RegionRequest::BulkInserts(_) => {
                RegionChange::Ingest
            }
            RegionRequest::Alter(_)
            | RegionRequest::Flush(_)
            | RegionRequest::Compact(_)
            | RegionRequest::Truncate(_)
            | RegionRequest::BuildIndex(_)
            | RegionRequest::EnterStaging(_)
            | RegionRequest::ApplyStagingManifest(_) => RegionChange::None,
            RegionRequest::Catchup(_) => RegionChange::Catchup,
        };

        let engine = match self.get_engine(region_id, &region_change)? {
            CurrentEngine::Engine(engine) => engine,
            CurrentEngine::EarlyReturn(rows) => return Ok(RegionResponse::new(rows)),
        };

        // Sets corresponding region status to registering/deregistering before the operation.
        self.set_region_status_not_ready(region_id, &engine, &region_change);

        let result = engine
            .handle_request(region_id, request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id });
        if matches!(region_change, RegionChange::Ingest) {
            record_ingest_result(request_type, 1, &result);
        }
        match result {
            Ok(result) => {
                // Sets corresponding region status to ready.
                self.set_region_status_ready(region_id, engine.clone(), region_change)
                    .await?;

                Ok(RegionResponse {
                    affected_rows: result.affected_rows,
                    extensions: result.extensions,
                    metadata: Vec::new(),
                })
            }
            Err(err) => {
                // Removes the region status if the operation fails.
                self.unset_region_status(region_id, &engine, region_change);
                Err(err)
            }
        }
    }

    /// Handles the sync region request.
    pub async fn handle_sync_region(
        &self,
        engine: &RegionEngineRef,
        region_id: RegionId,
        request: SyncRegionFromRequest,
    ) -> Result<()> {
        let Some(new_opened_regions) = engine
            .sync_region(region_id, request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })?
            .new_opened_logical_region_ids()
        else {
            return Ok(());
        };

        for region in &new_opened_regions {
            self.region_map
                .insert(*region, RegionEngineWithStatus::Ready(engine.clone()));
        }
        if !new_opened_regions.is_empty() {
            info!(
                region_id = %region_id,
                logical_region_count = new_opened_regions.len(),
                logical_regions = ?new_opened_regions,
                "Logical regions are registered"
            );
        }

        Ok(())
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

    fn unset_region_status(
        &self,
        region_id: RegionId,
        engine: &RegionEngineRef,
        region_change: RegionChange,
    ) {
        match region_change {
            RegionChange::None | RegionChange::Ingest | RegionChange::OfflineCleanup(_) => {}
            RegionChange::Register(_) => {
                self.region_map.remove(&region_id);
            }
            RegionChange::Deregisters => {
                self.region_map
                    .insert(region_id, RegionEngineWithStatus::Ready(engine.clone()));
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
            RegionChange::None | RegionChange::Ingest | RegionChange::OfflineCleanup(_) => {}
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
                    RegionAttribute::Relational => {
                        // do nothing
                    }
                }
            }
            RegionChange::Deregisters => {
                info!("Region {region_id} is deregistered from engine {engine_type}");
                self.region_map
                    .remove(&region_id)
                    .map(|(id, engine)| engine.set_region_role(id, RegionRole::Follower));
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

        for region in &logical_regions {
            self.region_map
                .insert(*region, RegionEngineWithStatus::Ready(engine.clone()));
        }
        if !logical_regions.is_empty() {
            info!(
                physical_region_id = %physical_region_id,
                logical_region_count = logical_regions.len(),
                logical_regions = ?logical_regions,
                "Logical regions are registered"
            );
        }
        Ok(())
    }

    pub async fn handle_read(
        self: &Arc<Self>,
        request: QueryRequest,
        query_ctx: QueryContextRef,
    ) -> Result<SendableRecordBatchStream> {
        let live_analyze_metrics =
            query_ctx.explain_verbose() && query_ctx.live_analyze_metrics_enabled();
        let inner = self.clone();
        let mut stream = common_runtime::spawn_query(async move {
            inner.handle_read_inner(request, query_ctx).await
        })
        .await
        .context(RuntimeJoinSnafu {
            request_type: "read",
        })??;
        let schema = stream.schema();
        let output_ordering = stream.output_ordering().map(|ordering| ordering.to_vec());

        let (sender, receiver) = mpsc::channel(QUERY_RUNTIME_STREAM_BUFFER_SIZE);
        let metrics = QueryRuntimeStream::metrics_store();
        let producer_metrics = metrics.clone();

        let producer_handle = common_runtime::spawn_query(async move {
            if live_analyze_metrics {
                loop {
                    match time::timeout(FLIGHT_METRICS_HEARTBEAT_INTERVAL, stream.next()).await {
                        Ok(Some(batch)) => {
                            *producer_metrics.write().unwrap() = stream.metrics();
                            if sender.send(batch).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            *producer_metrics.write().unwrap() = stream.metrics();
                        }
                    }
                }
            } else {
                while let Some(batch) = stream.next().await {
                    *producer_metrics.write().unwrap() = stream.metrics();
                    if sender.send(batch).await.is_err() {
                        break;
                    }
                }
            }
            *producer_metrics.write().unwrap() = stream.metrics();
        });

        Ok(Box::pin(
            QueryRuntimeStream::new(schema, receiver)
                .with_output_ordering(output_ordering)
                .with_metrics_store(metrics)
                .with_producer_handle(producer_handle),
        ))
    }

    async fn handle_read_inner(
        &self,
        request: QueryRequest,
        query_ctx: QueryContextRef,
    ) -> Result<SendableRecordBatchStream> {
        // TODO(ruihang): add metrics and set trace id

        let result = self
            .query_engine
            .execute(request.plan, query_ctx)
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
        // The workaround is to put the async functions in the `common_runtime::spawn_global`. Or like
        // it here, collect the values first then use later separately.

        let regions = self
            .region_map
            .iter()
            .map(|x| (*x.key(), x.value().clone()))
            .collect::<Vec<_>>();
        let num_regions = regions.len();

        let mut first_error = None;
        for (region_id, engine) in regions {
            let closed = engine
                .handle_request(
                    region_id,
                    RegionRequest::Close(RegionCloseRequest::default()),
                )
                .await;
            match closed {
                Ok(_) => debug!("Region {region_id} is closed"),
                Err(e) => {
                    // A close that failed may not have flushed writes that
                    // skipped the WAL, which nothing replays.
                    error!(e; "Failed to close region {region_id}; writes that skipped the WAL may be lost");
                    record_shutdown_error(
                        &mut first_error,
                        Err(e).context(HandleRegionRequestSnafu { region_id }),
                    );
                }
            }
        }
        self.region_map.clear();
        info!("closed {num_regions} regions");

        drop(self.mito_engine.write().unwrap().take());
        let engines = self.engines.write().unwrap().drain().collect::<Vec<_>>();
        for (engine_name, engine) in engines {
            let result = engine
                .stop()
                .await
                .context(StopRegionEngineSnafu { name: &engine_name });
            if result.is_ok() {
                info!("Region engine {engine_name} is stopped");
            }
            record_shutdown_error(&mut first_error, result);
        }

        first_error.map_or(Ok(()), Err)
    }
}

#[derive(Debug, Clone, Copy)]
enum RegionChange {
    None,
    Register(RegionAttribute),
    OfflineCleanup(RegionAttribute),
    Deregisters,
    Catchup,
    Ingest,
}

fn is_metric_engine(engine: &str) -> bool {
    engine == METRIC_ENGINE_NAME
}

/// Records failed open attempts without using arbitrary engine names as metric labels.
fn record_region_open_failures(engine: &str, status_code: StatusCode, count: usize) {
    let engine = match engine {
        MITO_ENGINE_NAME | METRIC_ENGINE_NAME | FILE_ENGINE_NAME | RELATIONAL_ENGINE_NAME => engine,
        _ => "unknown",
    };
    crate::metrics::REGION_OPEN_FAILURES_TOTAL
        .with_label_values(&[engine, status_code.as_ref()])
        .inc_by(count as u64);
}

/// Awaits an ingest task without treating a join failure as an entry-level write result.
async fn join_ingest_task<T>(task: JoinHandle<T>, request_type: &'static str) -> Result<T> {
    task.await
        .inspect_err(|error| {
            let reason = if error.is_panic() {
                "panic"
            } else {
                "cancelled"
            };
            crate::metrics::INGEST_TASK_FAILURES_TOTAL
                .with_label_values(&[request_type, reason])
                .inc();
        })
        .context(RuntimeJoinSnafu { request_type })
}

/// Records the result of `count` ingest requests answered by one response: the rows they
/// changed, or their failures.
fn record_ingest_result(request_type: &str, count: usize, result: &Result<RegionResponse>) {
    match result {
        Ok(response) => crate::metrics::REGION_CHANGED_ROW_COUNT
            .with_label_values(&[request_type])
            .inc_by(response.affected_rows as u64),
        Err(err) => crate::metrics::REGION_SERVER_INSERT_FAIL_COUNT
            .with_label_values(&[request_type, err.status_code().as_ref()])
            .inc_by(count as u64),
    }
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
        RELATIONAL_ENGINE_NAME => Ok(RegionAttribute::Relational),
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
    Relational,
}

impl RegionAttribute {
    fn engine(&self) -> &'static str {
        match self {
            RegionAttribute::Mito => MITO_ENGINE_NAME,
            RegionAttribute::Metric { .. } => METRIC_ENGINE_NAME,
            RegionAttribute::File => FILE_ENGINE_NAME,
            RegionAttribute::Relational => RELATIONAL_ENGINE_NAME,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::region::{InsertRequest, InsertRequests};
    use api::v1::{PartitionExprVersion, Rows, SemanticType};
    use common_error::ext::{ErrorExt, RetryHint};
    use common_recordbatch::RecordBatches;
    use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
    use common_wal::config::object_store::{ObjectStoreWalConfig, STANDALONE_GENERATION};
    use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::StreamExt;
    use log_store::ObjectStoreLogStore;
    use mito2::config::MitoConfig;
    use mito2::test_util::{CreateRequestBuilder, TestEnv, build_rows, rows_schema};
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use query::options::FLOW_RETURN_REGION_SEQ;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{
        PathType, RegionCleanUpRequest, RegionCompactRequest, RegionDeleteRequest,
        RegionDropRequest, RegionOpenRequest, RegionPutRequest, RegionTruncateRequest,
    };
    use store_api::storage::RegionId;

    use super::*;
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[test]
    fn test_is_ingest_request() {
        let rows = || Rows {
            schema: Vec::new(),
            rows: Vec::new(),
        };

        assert!(RegionServerInner::is_ingest_request(&RegionRequest::Put(
            RegionPutRequest {
                skip_wal: false,
                rows: rows(),
                hint: None,
                partition_expr_version: None,
            },
        )));
        assert!(RegionServerInner::is_ingest_request(
            &RegionRequest::Delete(RegionDeleteRequest {
                rows: rows(),
                hint: None,
                partition_expr_version: None,
            },)
        ));
        assert!(!RegionServerInner::is_ingest_request(
            &RegionRequest::Compact(RegionCompactRequest::default()),
        ));
    }

    fn single_value_stream() -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let values: VectorRef = Arc::new(Int32Vector::from_slice([1]));
        let batch = RecordBatch::new(schema.clone(), vec![values]).unwrap();
        RecordBatches::try_new(schema, vec![batch])
            .unwrap()
            .as_stream()
    }

    #[tokio::test]
    async fn test_region_watermark_stream_only_sets_terminal_metrics() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let values: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let batch = RecordBatch::new(schema.clone(), vec![values]).unwrap();
        let stream = RecordBatches::try_new(schema, vec![batch])
            .unwrap()
            .as_stream();

        let region_id = RegionId::new(42, 7);
        let wrapped = RegionWatermarkStream::new(stream, region_id, 99);
        let mut pinned = Box::pin(wrapped);

        assert!(pinned.as_ref().get_ref().metrics().is_none());
        while pinned.next().await.is_some() {}

        let metrics = pinned.as_ref().get_ref().metrics().unwrap();
        assert_eq!(
            metrics.region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: region_id.as_u64(),
                watermark: Some(99),
            }]
        );
    }

    #[test]
    fn test_region_watermark_stream_preserves_unproved_watermark() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let values: VectorRef = Arc::new(Int32Vector::from_slice([1]));
        let batch = RecordBatch::new(schema.clone(), vec![values]).unwrap();
        let stream = RecordBatches::try_new(schema, vec![batch])
            .unwrap()
            .as_stream();

        let region_id = RegionId::new(42, 7);
        let wrapped = RegionWatermarkStream::new(stream, region_id, 99);
        let metrics = RecordBatchMetrics {
            region_watermarks: vec![RegionWatermarkEntry {
                region_id: region_id.as_u64(),
                watermark: None,
            }],
            ..Default::default()
        };

        let merged = wrapped.merged_metrics(metrics);
        assert_eq!(
            merged.region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: region_id.as_u64(),
                watermark: None,
            }]
        );
    }

    #[tokio::test]
    async fn test_wrap_flow_region_watermark_stream_adds_terminal_metrics() {
        let region_id = RegionId::new(42, 7);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .extensions(HashMap::from([(
                    FLOW_RETURN_REGION_SEQ.to_string(),
                    "true".to_string(),
                )]))
                .build(),
        );
        query_ctx.set_snapshot(region_id.as_u64(), 99);

        let wrapped =
            wrap_flow_region_watermark_stream(single_value_stream(), region_id, &query_ctx);
        let mut pinned = Box::pin(wrapped);
        while pinned.next().await.is_some() {}

        let metrics = pinned.as_ref().get_ref().metrics().unwrap();
        assert_eq!(
            metrics.region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: region_id.as_u64(),
                watermark: Some(99),
            }]
        );
    }

    #[tokio::test]
    async fn test_wrap_flow_region_watermark_stream_skips_without_extension() {
        let region_id = RegionId::new(42, 7);
        let query_ctx = Arc::new(QueryContextBuilder::default().build());
        query_ctx.set_snapshot(region_id.as_u64(), 99);

        let wrapped =
            wrap_flow_region_watermark_stream(single_value_stream(), region_id, &query_ctx);
        let mut pinned = Box::pin(wrapped);
        while pinned.next().await.is_some() {}

        assert!(pinned.as_ref().get_ref().metrics().is_none());
    }

    #[tokio::test]
    async fn test_wrap_flow_region_watermark_stream_skips_without_snapshot() {
        let region_id = RegionId::new(42, 7);
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .extensions(HashMap::from([(
                    FLOW_RETURN_REGION_SEQ.to_string(),
                    "true".to_string(),
                )]))
                .build(),
        );

        let wrapped =
            wrap_flow_region_watermark_stream(single_value_stream(), region_id, &query_ctx);
        let mut pinned = Box::pin(wrapped);
        while pinned.next().await.is_some() {}

        assert!(pinned.as_ref().get_ref().metrics().is_none());
    }

    #[tokio::test]
    async fn test_offline_cleanup_does_not_register_region() {
        let mut mock_region_server = mock_region_server();
        let (engine, mut receiver) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(engine);

        let region_id = RegionId::new(1, 1);
        let response = mock_region_server
            .handle_request(
                region_id,
                RegionRequest::CleanUp(RegionCleanUpRequest {
                    engine: MITO_ENGINE_NAME.to_string(),
                    table_dir: String::new(),
                    path_type: PathType::Bare,
                    options: HashMap::new(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(response.affected_rows, 0);
        let (handled_region_id, handled_request) = receiver.try_recv().unwrap();
        assert_eq!(handled_region_id, region_id);
        assert_matches!(handled_request, RegionRequest::CleanUp(_));
        assert!(
            mock_region_server
                .inner
                .region_map
                .get(&region_id)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_offline_cleanup_rejects_registered_region() {
        let mut mock_region_server = mock_region_server();
        let (engine, mut receiver) = MockRegionEngine::new(MITO_ENGINE_NAME);
        mock_region_server.register_engine(engine.clone());

        let region_id = RegionId::new(1, 1);
        mock_region_server
            .inner
            .region_map
            .insert(region_id, RegionEngineWithStatus::Ready(engine));

        let err = mock_region_server
            .handle_request(
                region_id,
                RegionRequest::CleanUp(RegionCleanUpRequest {
                    engine: MITO_ENGINE_NAME.to_string(),
                    table_dir: String::new(),
                    path_type: PathType::Bare,
                    options: HashMap::new(),
                }),
            )
            .await
            .unwrap_err();

        assert_eq!(err.status_code(), StatusCode::RegionBusy);
        assert!(receiver.try_recv().is_err());
        assert!(matches!(
            mock_region_server
                .inner
                .region_map
                .get(&region_id)
                .unwrap()
                .clone(),
            RegionEngineWithStatus::Ready(_)
        ));
    }

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
                    table_dir: String::new(),
                    path_type: PathType::Bare,
                    options: Default::default(),
                    skip_wal_replay: false,
                    checkpoint: None,
                    requirements: Default::default(),
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
            .handle_request(
                region_id,
                RegionRequest::Drop(RegionDropRequest {
                    fast_path: false,
                    force: false,
                    partial_drop: false,
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
        assert!(matches!(status, RegionEngineWithStatus::Deregistering(_)));

        mock_region_server.inner.region_map.insert(
            region_id,
            RegionEngineWithStatus::Deregistering(engine.clone()),
        );

        let response = mock_region_server
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
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
            .handle_request(
                region_id,
                RegionRequest::Truncate(RegionTruncateRequest::All),
            )
            .await
            .unwrap_err();

        assert_eq!(err.status_code(), StatusCode::RegionNotReady);
    }

    #[test]
    fn test_batch_write_summary_uses_first_input_failure() {
        let first = error::RegionNotFoundSnafu {
            region_id: RegionId::new(1, 0),
        }
        .build();
        let expected = format!("2 region requests failed; entry 0: {}", first.output_msg());
        let later = error::RegionBusySnafu {
            region_id: RegionId::new(1, 1),
        }
        .build();
        let response = encode_batch_write_response(BatchRegionResponse {
            affected_rows: 0,
            extensions: HashMap::new(),
            failures: vec![(1, later), (0, first)],
        });
        let status = response.header.unwrap().status.unwrap();
        assert_eq!(status.status_code, StatusCode::PartialFailure as u32);
        assert_eq!(status.err_msg, expected);
        assert_eq!(response.failures[0].request_index, 1);
        assert_eq!(response.failures[1].request_index, 0);
    }

    #[tokio::test]
    async fn test_write_failure_response_compatibility() {
        let server = mock_region_server();
        let missing = RegionId::new(1, 1);
        let registering = RegionId::new(1, 2);
        let (engine, _receiver) = MockRegionEngine::new(MITO_ENGINE_NAME);
        server
            .inner
            .region_map
            .insert(registering, RegionEngineWithStatus::Registering(engine));
        for (region_id, code) in [
            (missing, StatusCode::RegionNotFound),
            (registering, StatusCode::RegionNotReady),
        ] {
            for delete in [false, true] {
                for accept_partial in [false, true] {
                    // A preceding empty entry must still count toward the original envelope size.
                    for with_empty_entry in [false, true] {
                        let rows = Some(Rows {
                            schema: Vec::new(),
                            rows: Vec::new(),
                        });
                        let body = if delete {
                            let request = api::v1::region::DeleteRequest {
                                region_id: region_id.as_u64(),
                                rows,
                                ..Default::default()
                            };
                            region_request::Body::Deletes(api::v1::region::DeleteRequests {
                                requests: if with_empty_entry {
                                    vec![Default::default(), request]
                                } else {
                                    vec![request]
                                },
                            })
                        } else {
                            let request = InsertRequest {
                                region_id: region_id.as_u64(),
                                rows,
                                ..Default::default()
                            };
                            region_request::Body::Inserts(InsertRequests {
                                requests: if with_empty_entry {
                                    vec![Default::default(), request]
                                } else {
                                    vec![request]
                                },
                            })
                        };
                        let mut header = api::v1::region::RegionRequestHeader::default();
                        if accept_partial {
                            header
                                .query_context
                                .get_or_insert_with(Default::default)
                                .extensions
                                .insert(
                                    api::region::ACCEPT_PARTIAL_RESULT.to_string(),
                                    "true".to_string(),
                                );
                        }
                        let result = server
                            .handle_region_request(RegionRequestV1 {
                                header: Some(header),
                                body: Some(body),
                            })
                            .await;
                        if accept_partial && with_empty_entry {
                            let response = result.unwrap();
                            assert_eq!(
                                StatusCode::PartialFailure as u32,
                                response.header.unwrap().status.unwrap().status_code
                            );
                            assert_eq!(1, response.failures.len());
                            assert_eq!(1, response.failures[0].request_index);
                            let status = response.failures[0].status.as_ref().unwrap();
                            assert_eq!(code as u32, status.status_code);
                            assert_eq!(
                                RetryHint::RetryAfterRouteRefresh.as_str(),
                                status.retry_hint
                            );
                        } else {
                            let status: tonic::Status = result.unwrap_err().into();
                            assert_eq!(
                                common_error::status_code::status_to_tonic_code(code),
                                status.code()
                            );
                            assert_eq!(
                                (code as u32).to_string(),
                                status
                                    .metadata()
                                    .get(common_error::GREPTIME_DB_HEADER_ERROR_CODE)
                                    .unwrap()
                                    .to_str()
                                    .unwrap()
                            );
                            assert_eq!(
                                RetryHint::RetryAfterRouteRefresh.as_str(),
                                status
                                    .metadata()
                                    .get(common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT)
                                    .unwrap()
                                    .to_str()
                                    .unwrap()
                            );
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_legacy_batch_returns_first_input_error() {
        let server = mock_region_server();
        let first = RegionId::new(1, 1);
        let second = RegionId::new(1, 2);
        let (engine, _receiver) = MockRegionEngine::with_custom_apply_fn("mock", |engine| {
            engine.handle_request_delay = Some(Duration::from_millis(10));
            engine.handle_request_mock_fn = Some(Box::new(|region_id, _| {
                error::RegionBusySnafu { region_id }.fail()
            }));
        });
        server.register_test_region(first, engine);
        let requests = [first, second]
            .map(|region_id| InsertRequest {
                region_id: region_id.as_u64(),
                rows: Some(Rows::default()),
                ..Default::default()
            })
            .to_vec();
        let error = server
            .handle_region_request(RegionRequestV1 {
                header: Some(Default::default()),
                body: Some(region_request::Body::Inserts(InsertRequests { requests })),
            })
            .await
            .unwrap_err();
        assert_eq!(StatusCode::RegionBusy, error.status_code());
        assert_eq!(RetryHint::Retryable, error.retry_hint());
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

        assert!(!mock_region_server.inner.region_map.contains_key(&region_id));

        mock_region_server
            .inner
            .region_map
            .insert(region_id, RegionEngineWithStatus::Ready(engine.clone()));

        mock_region_server
            .handle_request(
                region_id,
                RegionRequest::Drop(RegionDropRequest {
                    fast_path: false,
                    force: false,
                    partial_drop: false,
                }),
            )
            .await
            .unwrap_err();

        assert!(mock_region_server.inner.region_map.contains_key(&region_id));

        let insert = api::v1::region::InsertRequest {
            region_id: region_id.as_u64(),
            rows: Some(Rows {
                schema: Vec::new(),
                rows: Vec::new(),
            }),
            ..Default::default()
        };
        let success_region = RegionId::new(1, 2);
        let (success_engine, _receiver) =
            MockRegionEngine::with_custom_apply_fn("mock", |engine| {
                engine.handle_request_delay = Some(std::time::Duration::from_millis(10));
                engine.handle_request_mock_fn = Some(Box::new(|_, _| Ok(5)));
            });
        mock_region_server.register_test_region(success_region, success_engine);
        let successful_insert = api::v1::region::InsertRequest {
            region_id: success_region.as_u64(),
            ..insert.clone()
        };
        let response = mock_region_server
            .handle_region_request(RegionRequestV1 {
                header: Some(api::v1::region::RegionRequestHeader {
                    query_context: Some(api::v1::QueryContext {
                        extensions: HashMap::from([(
                            api::region::ACCEPT_PARTIAL_RESULT.to_string(),
                            "true".to_string(),
                        )]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                body: Some(region_request::Body::Inserts(InsertRequests {
                    requests: vec![
                        Default::default(),
                        insert.clone(),
                        successful_insert,
                        insert,
                    ],
                })),
            })
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 5);
        assert_eq!(
            response
                .header
                .as_ref()
                .unwrap()
                .status
                .as_ref()
                .unwrap()
                .status_code,
            StatusCode::PartialFailure as u32
        );
        assert_eq!(
            response
                .failures
                .iter()
                .map(|error| error.request_index)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(
            response
                .failures
                .iter()
                .all(|error| error.status.as_ref().is_some_and(|status| {
                    status.status_code == StatusCode::Unexpected as u32
                        && status.retry_hint == RetryHint::NonRetryable.as_str()
                }))
        );
    }

    /// A region that fails to close is reported in the result of the stop,
    /// which still closes the other regions.
    #[tokio::test]
    async fn test_stop_reports_a_region_that_failed_to_close() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let failing = RegionId::new(1, 1);
        let closing = RegionId::new(1, 2);
        let closed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (engine, _receiver) = MockRegionEngine::with_mock_fn(MITO_ENGINE_NAME, {
            let closed = closed.clone();
            Box::new(move |region_id, request| {
                if matches!(request, RegionRequest::Close(_)) {
                    closed.lock().unwrap().push(region_id);
                    if region_id == failing {
                        return error::UnexpectedSnafu {
                            violated: "close failed".to_string(),
                        }
                        .fail();
                    }
                }
                Ok(0)
            })
        });
        mock_region_server.register_engine(engine.clone());
        for region_id in [failing, closing] {
            mock_region_server
                .inner
                .region_map
                .insert(region_id, RegionEngineWithStatus::Ready(engine.clone()));
        }

        let err = mock_region_server.stop().await.unwrap_err();

        assert_matches!(err, error::Error::HandleRegionRequest { region_id, .. } if region_id == failing);
        let mut closed = closed.lock().unwrap().clone();
        closed.sort();
        assert_eq!(vec![failing, closing], closed);
    }

    #[tokio::test]
    async fn test_batch_open_region_ignore_nonexistent_regions() {
        common_telemetry::init_default_ut_logging();
        let mut mock_region_server = mock_region_server();
        let (engine, _receiver) = MockRegionEngine::with_mock_fn(
            MITO_ENGINE_NAME,
            Box::new(|region_id, _request| {
                if region_id == RegionId::new(1, 1) {
                    error::RegionNotFoundSnafu { region_id }.fail()
                } else {
                    Ok(0)
                }
            }),
        );
        mock_region_server.register_engine(engine.clone());

        let region_ids = mock_region_server
            .handle_batch_open_requests(
                8,
                vec![
                    (
                        RegionId::new(1, 1),
                        RegionOpenRequest {
                            engine: MITO_ENGINE_NAME.to_string(),
                            table_dir: String::new(),
                            path_type: PathType::Bare,
                            options: Default::default(),
                            skip_wal_replay: false,
                            checkpoint: None,
                            requirements: Default::default(),
                        },
                    ),
                    (
                        RegionId::new(1, 2),
                        RegionOpenRequest {
                            engine: MITO_ENGINE_NAME.to_string(),
                            table_dir: String::new(),
                            path_type: PathType::Bare,
                            options: Default::default(),
                            skip_wal_replay: false,
                            checkpoint: None,
                            requirements: Default::default(),
                        },
                    ),
                ],
                true,
            )
            .await
            .unwrap();
        assert_eq!(region_ids, vec![RegionId::new(1, 2)]);

        let err = mock_region_server
            .handle_batch_open_requests(
                8,
                vec![
                    (
                        RegionId::new(1, 1),
                        RegionOpenRequest {
                            engine: MITO_ENGINE_NAME.to_string(),
                            table_dir: String::new(),
                            path_type: PathType::Bare,
                            options: Default::default(),
                            skip_wal_replay: false,
                            checkpoint: None,
                            requirements: Default::default(),
                        },
                    ),
                    (
                        RegionId::new(1, 2),
                        RegionOpenRequest {
                            engine: MITO_ENGINE_NAME.to_string(),
                            table_dir: String::new(),
                            path_type: PathType::Bare,
                            options: Default::default(),
                            skip_wal_replay: false,
                            checkpoint: None,
                            requirements: Default::default(),
                        },
                    ),
                ],
                false,
            )
            .await
            .unwrap_err();
        assert_matches!(&err, error::Error::HandleBatchOpenRequest { .. });
        assert_eq!(err.status_code(), StatusCode::RegionNotFound);
        assert_eq!(err.retry_hint(), RetryHint::RetryAfterRouteRefresh);
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
            // RegionChange::OfflineCleanup
            CurrentEngineTest {
                region_id,
                current_region_status: None,
                region_change: RegionChange::OfflineCleanup(RegionAttribute::Mito),
                assert: Box::new(|result| {
                    let current_engine = result.unwrap();
                    assert_matches!(current_engine, CurrentEngine::Engine(_));
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Registering(engine.clone())),
                region_change: RegionChange::OfflineCleanup(RegionAttribute::Mito),
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionBusy);
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Deregistering(engine.clone())),
                region_change: RegionChange::OfflineCleanup(RegionAttribute::Mito),
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionBusy);
                }),
            },
            CurrentEngineTest {
                region_id,
                current_region_status: Some(RegionEngineWithStatus::Ready(engine.clone())),
                region_change: RegionChange::OfflineCleanup(RegionAttribute::Mito),
                assert: Box::new(|result| {
                    let err = result.unwrap_err();
                    assert_eq!(err.status_code(), StatusCode::RegionBusy);
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

    #[tokio::test]
    async fn test_region_server_parallelism() {
        let p = RegionServerParallelism::from_opts(2, Duration::from_millis(1)).unwrap();
        let first_query = p.acquire().await;
        assert!(first_query.is_ok());
        let second_query = p.acquire().await;
        assert!(second_query.is_ok());
        let third_query = p.acquire().await;
        assert!(third_query.is_err());
        let err = third_query.unwrap_err();
        assert_eq!(
            err.output_msg(),
            "Failed to acquire permit under timeouts: deadline has elapsed".to_string()
        );
        drop(first_query);
        let forth_query = p.acquire().await;
        assert!(forth_query.is_ok());
    }

    fn mock_region_metadata(region_id: RegionId) -> RegionMetadata {
        let mut metadata_builder = RegionMetadataBuilder::new(region_id);
        metadata_builder.push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_nanosecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 0,
        });
        metadata_builder.push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "file",
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        });
        metadata_builder.push_column_metadata(ColumnMetadata {
            column_schema: datatypes::schema::ColumnSchema::new(
                "message",
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Field,
            column_id: 2,
        });
        metadata_builder.primary_key(vec![1]);
        metadata_builder.build().unwrap()
    }

    #[tokio::test]
    async fn test_handle_list_metadata_request() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let region_id_1 = RegionId::new(1, 0);
        let region_id_2 = RegionId::new(2, 0);

        let metadata_1 = mock_region_metadata(region_id_1);
        let metadata_2 = mock_region_metadata(region_id_2);
        let metadatas = vec![Some(metadata_1.clone()), Some(metadata_2.clone())];

        let metadata_1 = Arc::new(metadata_1);
        let metadata_2 = Arc::new(metadata_2);
        let (engine, _) = MockRegionEngine::with_metadata_mock_fn(
            MITO_ENGINE_NAME,
            Box::new(move |region_id| {
                if region_id == region_id_1 {
                    Ok(metadata_1.clone())
                } else if region_id == region_id_2 {
                    Ok(metadata_2.clone())
                } else {
                    error::RegionNotFoundSnafu { region_id }.fail()
                }
            }),
        );

        mock_region_server.register_engine(engine.clone());
        mock_region_server
            .inner
            .region_map
            .insert(region_id_1, RegionEngineWithStatus::Ready(engine.clone()));
        mock_region_server
            .inner
            .region_map
            .insert(region_id_2, RegionEngineWithStatus::Ready(engine.clone()));

        // All regions exist.
        let list_metadata_request = ListMetadataRequest {
            region_ids: vec![region_id_1.as_u64(), region_id_2.as_u64()],
        };
        let response = mock_region_server
            .handle_list_metadata_request(&list_metadata_request)
            .await
            .unwrap();
        let decoded_metadata: Vec<Option<RegionMetadata>> =
            serde_json::from_slice(&response.metadata).unwrap();
        assert_eq!(metadatas, decoded_metadata);
    }

    #[tokio::test]
    async fn test_handle_list_metadata_not_found() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let region_id_1 = RegionId::new(1, 0);
        let region_id_2 = RegionId::new(2, 0);

        let metadata_1 = mock_region_metadata(region_id_1);
        let metadatas = vec![Some(metadata_1.clone()), None];

        let metadata_1 = Arc::new(metadata_1);
        let (engine, _) = MockRegionEngine::with_metadata_mock_fn(
            MITO_ENGINE_NAME,
            Box::new(move |region_id| {
                if region_id == region_id_1 {
                    Ok(metadata_1.clone())
                } else {
                    error::RegionNotFoundSnafu { region_id }.fail()
                }
            }),
        );

        mock_region_server.register_engine(engine.clone());
        mock_region_server
            .inner
            .region_map
            .insert(region_id_1, RegionEngineWithStatus::Ready(engine.clone()));

        // Not in region map.
        let list_metadata_request = ListMetadataRequest {
            region_ids: vec![region_id_1.as_u64(), region_id_2.as_u64()],
        };
        let response = mock_region_server
            .handle_list_metadata_request(&list_metadata_request)
            .await
            .unwrap();
        let decoded_metadata: Vec<Option<RegionMetadata>> =
            serde_json::from_slice(&response.metadata).unwrap();
        assert_eq!(metadatas, decoded_metadata);

        // Not in region engine.
        mock_region_server
            .inner
            .region_map
            .insert(region_id_2, RegionEngineWithStatus::Ready(engine.clone()));
        let response = mock_region_server
            .handle_list_metadata_request(&list_metadata_request)
            .await
            .unwrap();
        let decoded_metadata: Vec<Option<RegionMetadata>> =
            serde_json::from_slice(&response.metadata).unwrap();
        assert_eq!(metadatas, decoded_metadata);
    }

    #[tokio::test]
    async fn test_handle_list_metadata_failed() {
        common_telemetry::init_default_ut_logging();

        let mut mock_region_server = mock_region_server();
        let region_id_1 = RegionId::new(1, 0);

        let (engine, _) = MockRegionEngine::with_metadata_mock_fn(
            MITO_ENGINE_NAME,
            Box::new(move |region_id| {
                error::UnexpectedSnafu {
                    violated: format!("Failed to get region {region_id}"),
                }
                .fail()
            }),
        );

        mock_region_server.register_engine(engine.clone());
        mock_region_server
            .inner
            .region_map
            .insert(region_id_1, RegionEngineWithStatus::Ready(engine.clone()));

        // Failed to get.
        let list_metadata_request = ListMetadataRequest {
            region_ids: vec![region_id_1.as_u64()],
        };
        mock_region_server
            .handle_list_metadata_request(&list_metadata_request)
            .await
            .unwrap_err();
    }

    /// Returns a region server with a Mito engine on a durable object store WAL, the WAL, and the
    /// WAL options of a region on it.
    async fn region_server_on_object_store_wal(
        env: &mut TestEnv,
        config: MitoConfig,
    ) -> (RegionServer, Arc<ObjectStoreLogStore>, String) {
        let wal_config = ObjectStoreWalConfig::default();
        let log_store = ObjectStoreLogStore::try_new(
            ObjectStore::new(Memory::default()).unwrap(),
            &wal_config,
            0,
            STANDALONE_GENERATION,
        )
        .await
        .unwrap();
        let engine = env
            .create_engine_with_log_store(config, log_store.clone())
            .await;
        let mut region_server = mock_region_server();
        region_server.register_engine(Arc::new(engine));
        let wal_options = serde_json::to_string(&WalOptions::ObjectStore(
            ObjectStoreWalOptions::new(wal_config.node_prefix(0, STANDALONE_GENERATION)),
        ))
        .unwrap();
        (region_server, log_store, wal_options)
    }

    /// Creates a region with `wal_options` and returns an insert of three rows into it.
    async fn create_region(
        region_server: &RegionServer,
        region_id: RegionId,
        wal_options: &str,
    ) -> InsertRequest {
        let request = CreateRequestBuilder::new()
            .insert_option(WAL_OPTIONS_KEY, wal_options)
            .build();
        let rows = Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        };
        region_server
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        InsertRequest {
            region_id: region_id.as_u64(),
            rows: Some(rows),
            partition_expr_version: None,
            skip_wal: false,
        }
    }

    #[tokio::test]
    async fn test_metric_batch_failure_indices() {
        let mut env = TestEnv::with_prefix("region-server-metric-results").await;
        let mito = env.create_engine(MitoConfig::default()).await;
        let metric = Arc::new(
            MetricEngine::try_new(mito, metric_engine::config::EngineConfig::default()).unwrap(),
        );
        let mut server = mock_region_server();
        server.register_engine(metric.clone());
        let mut inserts = Vec::new();
        for table in [10, 20] {
            let physical = RegionId::new(table, 0);
            let logical = RegionId::new(table + 1, 0);
            let mut create = CreateRequestBuilder::new().build();
            create.engine = METRIC_ENGINE_NAME.to_string();
            create.column_metadatas = vec![
                store_api::metadata::ColumnMetadata {
                    column_id: 0,
                    semantic_type: SemanticType::Timestamp,
                    column_schema: ColumnSchema::new(
                        "greptime_timestamp",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                },
                store_api::metadata::ColumnMetadata {
                    column_id: 1,
                    semantic_type: SemanticType::Field,
                    column_schema: ColumnSchema::new(
                        "greptime_value",
                        ConcreteDataType::float64_datatype(),
                        false,
                    ),
                },
            ];
            create.primary_key.clear();
            create.table_dir = format!("metric-results/{table}/");
            create.options = HashMap::from([(
                store_api::metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY.to_string(),
                String::new(),
            )]);
            server
                .handle_request(physical, RegionRequest::Create(create.clone()))
                .await
                .unwrap();
            create.table_dir = format!("metric-results/{}/", table + 1);
            create.options = HashMap::from([(
                store_api::metric_engine_consts::LOGICAL_TABLE_METADATA_KEY.to_string(),
                physical.as_u64().to_string(),
            )]);
            server
                .handle_request(logical, RegionRequest::Create(create.clone()))
                .await
                .unwrap();
            inserts.push(InsertRequest {
                region_id: logical.as_u64(),
                rows: Some(Rows {
                    schema: rows_schema(&create),
                    rows: vec![api::v1::Row {
                        values: vec![
                            api::v1::Value {
                                value_data: Some(
                                    api::v1::value::ValueData::TimestampMillisecondValue(1),
                                ),
                            },
                            api::v1::Value {
                                value_data: Some(api::v1::value::ValueData::F64Value(1.0)),
                            },
                        ],
                    }],
                }),
                ..Default::default()
            });
        }
        // Both entries of the second physical group fail at write_data.
        metric
            .set_region_role(RegionId::new(20, 0), RegionRole::Follower)
            .unwrap();
        let response = server
            .handle_requests_in_parallel(region_request::Body::Inserts(InsertRequests {
                requests: vec![
                    Default::default(),
                    inserts[0].clone(),
                    inserts[1].clone(),
                    inserts[1].clone(),
                ],
            }))
            .await
            .unwrap();
        assert_eq!(1, response.affected_rows);
        let mut failures = response.failures;
        failures.sort_unstable_by_key(|(index, _)| *index);
        assert_eq!(
            vec![2, 3],
            failures.iter().map(|(index, _)| *index).collect::<Vec<_>>()
        );
        assert!(
            failures
                .iter()
                .all(|(_, error)| error.status_code() == StatusCode::RegionNotReady)
        );
        assert!(
            failures
                .iter()
                .all(|(_, error)| error.retry_hint() == RetryHint::RetryAfterRouteRefresh)
        );
    }

    #[tokio::test]
    async fn test_mito_puts_of_one_request_share_a_wal_append() {
        // One worker that takes one message at a time, so that only puts that
        // arrive in one message share a WAL append.
        let mut env = TestEnv::with_prefix("region-server-mito-batch-puts").await;
        let config = MitoConfig {
            num_workers: 1,
            worker_request_batch_size: 1,
            ..Default::default()
        };
        let (region_server, log_store, wal_options) =
            region_server_on_object_store_wal(&mut env, config).await;
        let mut requests = Vec::new();
        for region_number in 1..=2 {
            let region_id = RegionId::new(1, region_number);
            requests.push(create_region(&region_server, region_id, &wal_options).await);
        }

        let response = region_server
            .handle_requests_in_parallel(region_request::Body::Inserts(InsertRequests { requests }))
            .await
            .unwrap();

        assert_eq!(6, response.affected_rows);
        // The worker got both puts at once and appended them together.
        log_store.wait_for_admitted_appends(1).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::ZERO, log_store.wait_for_admitted_appends(2))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_mito_puts_to_one_region_share_a_wal_append() {
        // As above: only puts that arrive in one message share a WAL append.
        let mut env = TestEnv::with_prefix("region-server-mito-batch-puts-one-region").await;
        let config = MitoConfig {
            num_workers: 1,
            worker_request_batch_size: 1,
            ..Default::default()
        };
        let (region_server, log_store, wal_options) =
            region_server_on_object_store_wal(&mut env, config).await;
        let insert = create_region(&region_server, RegionId::new(1, 1), &wal_options).await;
        let requests = vec![insert.clone(), insert];

        let response = region_server
            .handle_requests_in_parallel(region_request::Body::Inserts(InsertRequests { requests }))
            .await
            .unwrap();

        assert_eq!(6, response.affected_rows);
        log_store.wait_for_admitted_appends(1).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::ZERO, log_store.wait_for_admitted_appends(2))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_mito_batch_failure_indices_for_same_region() {
        for failed_index in [0, 1] {
            let mut env = TestEnv::with_prefix("region-server-mito-same-region-failure").await;
            let (server, _log_store, wal_options) =
                region_server_on_object_store_wal(&mut env, MitoConfig::default()).await;
            let region_id = RegionId::new(1, 1);
            let insert = create_region(&server, region_id, &wal_options).await;
            let mut requests = vec![insert.clone(), insert];
            requests[failed_index].partition_expr_version = Some(PartitionExprVersion { value: 1 });
            let response = server
                .handle_requests_in_parallel(region_request::Body::Inserts(InsertRequests {
                    requests,
                }))
                .await
                .unwrap();
            assert_eq!(response.affected_rows, 3);
            assert_eq!(response.failures.len(), 1);
            assert_eq!(response.failures[0].0, failed_index);
            assert_eq!(
                response.failures[0].1.status_code(),
                StatusCode::InvalidArguments
            );
            let engine = server
                .inner
                .region_map
                .get(&region_id)
                .unwrap()
                .clone()
                .into_engine();
            let mito = engine.as_any().downcast_ref::<MitoEngine>().unwrap();
            let stream = mito
                .scan_to_stream(region_id, Default::default())
                .await
                .unwrap();
            let batches = RecordBatches::try_collect(stream).await.unwrap();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                3
            );
        }
    }

    #[tokio::test]
    async fn test_mito_batch_puts_retain_successful_regions() {
        for accept_partial_result in [false, true] {
            let mut env = TestEnv::with_prefix("region-server-mito-batch-puts-error").await;
            let config = MitoConfig {
                num_workers: 2,
                ..Default::default()
            };
            let (region_server, log_store, wal_options) =
                region_server_on_object_store_wal(&mut env, config).await;
            // Regions 1 and 2 of table 1 belong to different workers. The put to region 1 waits for a
            // WAL object whose create is held; Mito does not know region 2, so its put fails at once.
            let held = RegionId::new(1, 1);
            let unknown = RegionId::new(1, 2);
            let held_insert = create_region(&region_server, held, &wal_options).await;
            let unknown_insert = InsertRequest {
                region_id: unknown.as_u64(),
                ..held_insert.clone()
            };
            let mito = region_server
                .inner
                .region_map
                .get(&held)
                .unwrap()
                .clone()
                .into_engine();
            region_server
                .inner
                .region_map
                .insert(unknown, RegionEngineWithStatus::Ready(mito.clone()));
            log_store.hold_creates();

            let requests = vec![Default::default(), held_insert, unknown_insert];
            let mut header = api::v1::region::RegionRequestHeader::default();
            if accept_partial_result {
                api::region::set_accept_partial_result(&mut header);
            }
            let write = region_server.handle_region_request(RegionRequestV1 {
                header: Some(header),
                body: Some(region_request::Body::Inserts(InsertRequests { requests })),
            });
            tokio::pin!(write);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut write)
                    .await
                    .is_err()
            );
            log_store.release_creates();
            let result = tokio::time::timeout(Duration::from_secs(10), write)
                .await
                .unwrap();
            if accept_partial_result {
                let response = result.unwrap();
                assert_eq!(3, response.affected_rows);
                assert_eq!(
                    StatusCode::PartialFailure as u32,
                    response.header.unwrap().status.unwrap().status_code
                );
                assert_eq!(1, response.failures.len());
                assert_eq!(2, response.failures[0].request_index);
                assert_eq!(
                    StatusCode::RegionNotFound as u32,
                    response.failures[0].status.as_ref().unwrap().status_code
                );
            } else {
                assert_eq!(
                    StatusCode::RegionNotFound,
                    result.unwrap_err().status_code()
                );
            }
            let mito = mito.as_any().downcast_ref::<MitoEngine>().unwrap();
            let stream = mito.scan_to_stream(held, Default::default()).await.unwrap();
            let batches = RecordBatches::try_collect(stream).await.unwrap();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                3
            );
        }
    }

    #[tokio::test]
    async fn test_mito_batch_puts_to_a_stopped_worker_fail_as_a_single_put_does() {
        let mut env = TestEnv::with_prefix("region-server-mito-batch-puts-stopped").await;
        let (region_server, _log_store, wal_options) =
            region_server_on_object_store_wal(&mut env, MitoConfig::default()).await;
        let region_id = RegionId::new(1, 1);
        let insert = create_region(&region_server, region_id, &wal_options).await;
        let mito = region_server
            .inner
            .region_map
            .get(&region_id)
            .unwrap()
            .clone()
            .into_engine();
        mito.stop().await.unwrap();

        // One put takes the per-region path and two take the batch path.
        for requests in [vec![insert.clone()], vec![insert.clone(), insert]] {
            let count = requests.len();
            let response = region_server
                .handle_requests_in_parallel(region_request::Body::Inserts(InsertRequests {
                    requests,
                }))
                .await
                .unwrap();
            assert_eq!(0, response.affected_rows);
            assert_eq!(count, response.failures.len());
            for (index, (request_index, error)) in response.failures.iter().enumerate() {
                assert_eq!(index, *request_index);
                assert!(format!("{error:?}").contains("is stopped"), "{error:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_default_batch_puts_handle_each_put_as_a_request() {
        let failing = RegionId::new(1, 2);
        let (engine, _receiver) = MockRegionEngine::with_mock_fn(
            "mock",
            Box::new(move |region_id, request| {
                assert!(matches!(request, RegionRequest::Put(_)));
                if region_id == failing {
                    return error::UnexpectedSnafu {
                        violated: "test".to_string(),
                    }
                    .fail();
                }
                Ok(region_id.region_number() as usize)
            }),
        );
        let region_ids = [1, 2, 3].map(|n| RegionId::new(1, n));
        let puts = region_ids
            .iter()
            .map(|region_id| {
                let request = RegionPutRequest {
                    rows: Rows {
                        schema: Vec::new(),
                        rows: Vec::new(),
                    },
                    hint: None,
                    skip_wal: false,
                    partition_expr_version: None,
                };
                (*region_id, request)
            })
            .collect();

        let (answered, responses): (Vec<_>, Vec<_>) =
            engine.handle_batch_put_requests(puts).into_iter().unzip();
        let responses = futures_util::future::join_all(responses).await;

        let answered_regions = region_ids.map(|region_id| vec![region_id]);
        assert_eq!(answered_regions.to_vec(), answered);
        assert_eq!(1, responses[0].as_ref().unwrap().affected_rows);
        assert!(responses[1].is_err());
        assert_eq!(3, responses[2].as_ref().unwrap().affected_rows);
    }

    #[tokio::test]
    async fn test_only_several_puts_into_ready_mito_regions_are_batched() {
        let mut env = TestEnv::with_prefix("region-server-mito-batch-puts-guard").await;
        let mito: RegionEngineRef = Arc::new(env.create_engine(MitoConfig::default()).await);
        let (other, _receiver) = MockRegionEngine::new("mock");
        let region_server = mock_region_server();
        let [mito_a, mito_b, registering, other_region] = [1, 2, 3, 4].map(|n| RegionId::new(1, n));
        let region_map = &region_server.inner.region_map;
        region_map.insert(mito_a, RegionEngineWithStatus::Ready(mito.clone()));
        region_map.insert(mito_b, RegionEngineWithStatus::Ready(mito.clone()));
        region_map.insert(registering, RegionEngineWithStatus::Registering(mito));
        region_map.insert(other_region, RegionEngineWithStatus::Ready(other));
        let rows = || Rows {
            schema: Vec::new(),
            rows: Vec::new(),
        };
        let put = |region_id| {
            let request = RegionPutRequest {
                rows: rows(),
                hint: None,
                skip_wal: false,
                partition_expr_version: None,
            };
            IndexedRegionRequest {
                index: 0,
                region_id,
                request: RegionRequest::Put(request),
            }
        };
        let delete = |region_id| {
            let request = RegionDeleteRequest {
                rows: rows(),
                hint: None,
                partition_expr_version: None,
            };
            IndexedRegionRequest {
                index: 0,
                region_id,
                request: RegionRequest::Delete(request),
            }
        };

        for requests in [
            vec![put(mito_a), put(mito_b)],
            vec![put(mito_a), put(mito_a)],
        ] {
            assert!(
                region_server
                    .mito_engine_for_batch_puts(&requests)
                    .is_some()
            );
        }
        for requests in [
            vec![put(mito_a)],
            vec![put(mito_a), delete(mito_b)],
            vec![put(mito_a), put(registering)],
            vec![put(mito_a), put(other_region)],
            vec![put(other_region), put(mito_a)],
        ] {
            assert!(
                region_server
                    .mito_engine_for_batch_puts(&requests)
                    .is_none()
            );
        }
    }
}
