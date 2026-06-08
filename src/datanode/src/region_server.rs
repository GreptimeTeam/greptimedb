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

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use api::region::RegionResponse;
use api::v1::meta::TopicStat;
use api::v1::region::remote_dyn_filter_request::Action;
use api::v1::region::sync_request::ManifestInfo;
use api::v1::region::{
    ListMetadataRequest, RegionResponse as RegionResponseV1, RemoteDynFilterRequest, SyncRequest,
    region_request,
};
use api::v1::{ResponseHeader, Status};
use arrow_flight::{FlightData, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use common_base::Plugins;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_meta::datanode::TopicStatsReporter;
use common_query::OutputData;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_runtime::Runtime;
use common_telemetry::tracing::{self, info_span};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, error, info, warn};
use dashmap::DashMap;
use datafusion::config::ConfigOptions;
use datafusion::datasource::TableProvider;
use datafusion::physical_expr::utils::conjunction;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion_common::tree_node::TreeNode;
use datatypes::schema::SchemaRef;
use either::Either;
use futures_util::Stream;
use futures_util::future::try_join_all;
use metric_engine::engine::MetricEngine;
use mito2::engine::{MITO_ENGINE_NAME, MitoEngine};
use prost::Message;
use query::QueryEngineRef;
pub use query::dummy_catalog::{
    DummyCatalogList, DummyTableProviderFactory, TableProviderFactoryRef,
};
use query::options::should_collect_region_watermark_from_extensions;
use query::physical_wrapper::{PhysicalPlanWrapper, PhysicalPlanWrappers, PhysicalPlanWrappersRef};
use serde_json;
use servers::error::{
    self as servers_error, ExecuteGrpcRequestSnafu, Result as ServerResult, SuspendedSnafu,
};
use servers::grpc::FlightCompression;
use servers::grpc::flight::{FlightCraft, FlightRecordBatchStream, TonicStream};
use servers::grpc::region_server::RegionServerHandler;
use session::context::{QueryContext, QueryContextBuilder, QueryContextRef};
use session::query_id::QueryId;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    FILE_ENGINE_NAME, LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME,
};
use store_api::region_engine::{
    RegionEngineRef, RegionManifestInfo, RegionRole, RegionStatistic, RemapManifestsRequest,
    RemapManifestsResponse, SetRegionRoleStateResponse, SettableRegionRoleState,
    SyncRegionFromRequest,
};
use store_api::region_request::{
    AffectedRows, BatchRegionDdlRequest, RegionCatchupRequest, RegionCloseRequest,
    RegionOpenRequest, RegionRequest,
};
use store_api::storage::RegionId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tonic::{Request, Response, Result as TonicResult};

use crate::error::{
    self, BuildRegionRequestsSnafu, ConcurrentQueryLimiterClosedSnafu,
    ConcurrentQueryLimiterTimeoutSnafu, DataFusionSnafu, DecodeLogicalPlanSnafu,
    ExecuteLogicalPlanSnafu, FindLogicalRegionsSnafu, GetRegionMetadataSnafu,
    HandleBatchDdlRequestSnafu, HandleBatchOpenRequestSnafu, HandleRegionRequestSnafu,
    NewPlanDecoderSnafu, RegionEngineNotFoundSnafu, RegionNotFoundSnafu, RegionNotReadySnafu,
    Result, SerializeJsonSnafu, StopRegionEngineSnafu, UnexpectedSnafu, UnsupportedOutputSnafu,
};
use crate::event_listener::RegionServerEventListenerRef;
use crate::region_server::catalog::{NameAwareCatalogList, NameAwareDataSourceInjectorBuilder};
use crate::region_server::registrations::{
    RemoteDynFilterId, RemoteDynFilterRegistry, RemoteDynFilterUpdateOutcome,
    apply_remote_dyn_filter_update, initial_dyn_filter_regs_from_query_ctx,
    register_initial_dyn_filter_regs, remote_dyn_filter_exprs_for_initial_regs,
    remove_initial_dyn_filter_regs, unregister_remote_dyn_filter,
};

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

struct RemoteDynFilterPhysicalPlanWrapper {
    server: Weak<RegionServerInner>,
}

impl PhysicalPlanWrapper for RemoteDynFilterPhysicalPlanWrapper {
    fn wrap(
        &self,
        origin: Arc<dyn ExecutionPlan>,
        ctx: QueryContextRef,
        config: &ConfigOptions,
    ) -> Arc<dyn ExecutionPlan> {
        let Some(query_id) = ctx.remote_query_id_value() else {
            return origin;
        };

        let Some(initial_regs) = initial_dyn_filter_regs_from_query_ctx(&ctx) else {
            return origin;
        };

        let Some(server) = self.server.upgrade() else {
            return origin;
        };

        let input_schema = origin.schema();
        let dyn_filters = remote_dyn_filter_exprs_for_initial_regs(
            &server.initial_remote_dyn_filter_registrations,
            &query_id,
            &initial_regs,
            input_schema.as_ref(),
        );

        if dyn_filters.is_empty() {
            return origin;
        }

        let predicate = conjunction(dyn_filters);
        let filter_plan = match FilterExec::try_new(predicate, Arc::clone(&origin)) {
            Ok(plan) => Arc::new(plan) as Arc<dyn ExecutionPlan>,
            Err(error) => {
                warn!(error; "Failed to build remote dynamic filter FilterExec");
                return origin;
            }
        };

        match FilterPushdown::new().optimize(Arc::clone(&filter_plan), config) {
            Ok(plan) => plan,
            Err(error) => {
                warn!(error; "Failed to push down remote dynamic filters; keeping FilterExec fallback");
                filter_plan
            }
        }
    }
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

    pub fn install_remote_dyn_filter_physical_plan_wrapper(&self, plugins: &Plugins) {
        let wrappers = plugins.get_or_insert::<PhysicalPlanWrappersRef, _>(|| {
            Arc::new(PhysicalPlanWrappers::default())
        });
        wrappers.push(Arc::new(RemoteDynFilterPhysicalPlanWrapper {
            server: Arc::downgrade(&self.inner),
        }));
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
        let permit = if let Some(p) = &self.inner.parallelism {
            Some(p.acquire().await?)
        } else {
            None
        };

        let region_id = RegionId::from_u64(request.region_id);
        let catalog_list = Arc::new(NameAwareCatalogList::new(
            self.clone(),
            region_id,
            query_ctx.clone(),
        ));

        if query_ctx.explain_verbose() {
            common_telemetry::info!("Handle remote read for region: {}", region_id);
        }

        let decoder = self
            .inner
            .query_engine
            .engine_context(query_ctx.clone())
            .new_plan_decoder()
            .context(NewPlanDecoderSnafu)?;

        let plan = decoder
            .decode(Bytes::from(request.plan), catalog_list, false)
            .await
            .context(DecodeLogicalPlanSnafu)?;

        let initial_dyn_filter_regs = initial_dyn_filter_regs_from_query_ctx(&query_ctx);
        let query_id = query_ctx.remote_query_id_value();
        let registered_filter_ids = if let (Some(query_id), Some(regs)) =
            (query_id.as_ref(), initial_dyn_filter_regs.as_ref())
        {
            register_initial_dyn_filter_regs(
                &self.inner.initial_remote_dyn_filter_registrations,
                query_id,
                region_id,
                regs,
            )
        } else {
            Vec::new()
        };
        let cleanup = match (query_id, registered_filter_ids.is_empty()) {
            (Some(query_id), false) => Some(RemoteDynFilterCleanup::new(
                self.clone(),
                query_id,
                region_id,
                registered_filter_ids,
            )),
            _ => None,
        };

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
            wrap_remote_dyn_filter_cleanup_stream(stream, cleanup)
        } else {
            stream
        };
        Ok(maybe_guard_stream(stream, permit))
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_read(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let permit = if let Some(p) = &self.inner.parallelism {
            Some(p.acquire().await?)
        } else {
            None
        };

        let ctx = request.header.as_ref().map(|h| h.into());
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

        let stream = self
            .inner
            .handle_read(QueryRequest { plan, ..request }, query_ctx.clone())
            .await?;

        let stream = wrap_flow_region_watermark_stream(stream, region_id, &query_ctx);
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
        match self.inner.region_map.get(&region_id) {
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
            .handle_batch_request(batch_request)
            .trace(span)
            .await
    }

    async fn handle_requests_in_parallel(
        &self,
        request: region_request::Body,
    ) -> Result<RegionResponse> {
        let requests =
            RegionRequest::try_from_request_body(request).context(BuildRegionRequestsSnafu)?;

        // Try to optimize batch Put requests for metric engine
        // Returns either Some(response) or None(requests_back)
        match self.try_handle_metric_batch_puts(requests).await? {
            Either::Left(response) => Ok(response),
            Either::Right(requests) => {
                // Fallback: original parallel processing
                let tracing_context = TracingContext::from_current_span();
                let join_tasks =
                    requests
                        .into_iter()
                        .map(|(region_id, req): (RegionId, RegionRequest)| {
                            let self_to_move = self;
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

                let results = try_join_all(join_tasks).await?;
                let mut affected_rows = 0;
                let mut extensions = HashMap::new();
                for result in results {
                    affected_rows += result.affected_rows;
                    extensions.extend(result.extensions);
                }

                Ok(RegionResponse {
                    affected_rows,
                    extensions,
                    metadata: Vec::new(),
                })
            }
        }
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

    /// Attempts to optimize batch Put requests for metric engine.
    ///
    /// Returns Either::Left(response) if optimization succeeded,
    /// or Either::Right(original_requests) to fall back to parallel processing.
    ///
    /// This avoids cloning requests when optimization cannot be applied.
    async fn try_handle_metric_batch_puts(
        &self,
        requests: Vec<(RegionId, RegionRequest)>,
    ) -> Result<Either<RegionResponse, Vec<(RegionId, RegionRequest)>>> {
        if requests.is_empty() {
            return Ok(Either::Right(requests));
        }

        // Quick check: verify first request is Put and is metric engine
        if !matches!(requests[0].1, RegionRequest::Put(_)) {
            return Ok(Either::Right(requests));
        }
        let first_region_id = requests[0].0;
        let request_type = requests[0].1.request_type();

        // SAFETY: If the first request belongs to metric engine, then ALL requests
        // in this batch are guaranteed to belong to metric engine. This invariant
        // is maintained by the request batching logic upstream.
        let engine = match self
            .inner
            .get_engine(first_region_id, &RegionChange::None)?
        {
            CurrentEngine::Engine(e) => e,
            _ => return Ok(Either::Right(requests)),
        };

        if engine.name() != METRIC_ENGINE_NAME {
            return Ok(Either::Right(requests));
        }

        // Check if ALL requests are Put (now we know it's worth checking)
        let mut all_puts = true;
        for (_, req) in &requests {
            if !matches!(req, RegionRequest::Put(_)) {
                all_puts = false;
                break;
            }
        }

        if !all_puts {
            return Ok(Either::Right(requests));
        }

        // Now extract Put requests by consuming ownership (zero clone!)
        let put_requests = requests.into_iter().map(|(region_id, req)| {
            if let RegionRequest::Put(put) = req {
                (region_id, put)
            } else {
                unreachable!("Already checked all are Put")
            }
        });

        // Downcast to MetricEngine and call batch API
        let metric_engine =
            engine
                .as_any()
                .downcast_ref::<MetricEngine>()
                .context(UnexpectedSnafu {
                    violated: "Failed to downcast to MetricEngine",
                })?;

        let tracing_context = TracingContext::from_current_span();
        let batch_size = put_requests.len();
        let span = tracing_context.attach(info_span!(
            "RegionServer::handle_metric_batch_puts",
            batch_size = batch_size,
        ));
        let result = metric_engine
            .put_regions_batch(put_requests)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(HandleRegionRequestSnafu {
                region_id: first_region_id,
            });

        match result {
            Ok(total_affected) => {
                crate::metrics::REGION_CHANGED_ROW_COUNT
                    .with_label_values(&[request_type])
                    .inc_by(total_affected as u64);
                Ok(Either::Left(RegionResponse::new(total_affected)))
            }
            Err(err) => {
                crate::metrics::REGION_SERVER_INSERT_FAIL_COUNT
                    .with_label_values(&[request_type])
                    .inc_by(batch_size as u64);
                Err(err)
            }
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

    async fn handle_remote_dyn_filter_request(
        &self,
        request: &RemoteDynFilterRequest,
    ) -> Result<RegionResponse> {
        if request.query_id.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "query_id" }.fail();
        }

        let query_id = request.query_id.parse::<QueryId>();
        ensure!(
            query_id.is_ok(),
            UnexpectedSnafu {
                violated: "remote dynamic filter query_id must be a valid QueryId"
            }
        );
        let query_id = query_id.unwrap();

        match request
            .action
            .as_ref()
            .context(error::MissingRequiredFieldSnafu { name: "action" })?
        {
            Action::Update(update) => {
                self.handle_remote_dyn_filter_update(&query_id, update)
                    .await
            }
            Action::Unregister(unregister) => {
                self.handle_remote_dyn_filter_unregister(&query_id, unregister)
                    .await
            }
        }
    }

    async fn handle_remote_dyn_filter_update(
        &self,
        query_id: &QueryId,
        request: &api::v1::region::RemoteDynFilterUpdate,
    ) -> Result<RegionResponse> {
        if request.filter_id.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "filter_id" }.fail();
        }

        if request.payload.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "payload" }.fail();
        }

        let filter_id = RemoteDynFilterId::new(request.filter_id.clone());
        let outcome = apply_remote_dyn_filter_update(
            &self.inner.initial_remote_dyn_filter_registrations,
            query_id,
            &filter_id,
            &request.payload,
            request.generation,
            request.is_complete,
        );
        self.log_remote_dyn_filter_update_outcome(query_id, &filter_id, outcome);

        Ok(RegionResponse::new(0))
    }

    async fn handle_remote_dyn_filter_unregister(
        &self,
        query_id: &QueryId,
        request: &api::v1::region::RemoteDynFilterUnregister,
    ) -> Result<RegionResponse> {
        if request.filter_id.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "filter_id" }.fail();
        }

        let filter_id = RemoteDynFilterId::new(request.filter_id.clone());
        let outcome = unregister_remote_dyn_filter(
            &self.inner.initial_remote_dyn_filter_registrations,
            query_id,
            &filter_id,
        );
        self.log_remote_dyn_filter_update_outcome(query_id, &filter_id, outcome);

        Ok(RegionResponse::new(0))
    }

    fn log_remote_dyn_filter_update_outcome(
        &self,
        query_id: &QueryId,
        filter_id: &RemoteDynFilterId,
        outcome: RemoteDynFilterUpdateOutcome,
    ) {
        match outcome {
            RemoteDynFilterUpdateOutcome::MissingRegistration => {
                warn!(
                    "Remote dynamic filter update had no active registration, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
            RemoteDynFilterUpdateOutcome::Buffered => {
                debug!(
                    "Buffered early remote dynamic filter update, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
            RemoteDynFilterUpdateOutcome::Applied => {
                debug!(
                    "Applied remote dynamic filter update, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
            RemoteDynFilterUpdateOutcome::Idempotent => {
                debug!(
                    "Ignored idempotent remote dynamic filter update, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
            RemoteDynFilterUpdateOutcome::Stale => {
                debug!(
                    "Discarded stale remote dynamic filter update, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
            RemoteDynFilterUpdateOutcome::AlreadyComplete => {
                warn!(
                    "Discarded remote dynamic filter update after completion, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
            RemoteDynFilterUpdateOutcome::PayloadTooLarge => {
                warn!(
                    "Discarded oversized remote dynamic filter update, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
            RemoteDynFilterUpdateOutcome::DecodeFailed => {
                warn!(
                    "Remote dynamic filter update failed to decode or apply, query_id: {}, filter_id: {}",
                    query_id, filter_id
                );
            }
        }
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

fn wrap_remote_dyn_filter_cleanup_stream(
    stream: SendableRecordBatchStream,
    cleanup: RemoteDynFilterCleanup,
) -> SendableRecordBatchStream {
    Box::pin(RemoteDynFilterCleanupStream { stream, cleanup })
}

/// Removes query-scoped remote dynamic filter subscriptions unless ownership is moved elsewhere.
struct RemoteDynFilterCleanup {
    server: RegionServer,
    query_id: QueryId,
    region_id: RegionId,
    filter_ids: Vec<RemoteDynFilterId>,
    cleaned: bool,
}

impl RemoteDynFilterCleanup {
    fn new(
        server: RegionServer,
        query_id: QueryId,
        region_id: RegionId,
        filter_ids: Vec<RemoteDynFilterId>,
    ) -> Self {
        Self {
            server,
            query_id,
            region_id,
            filter_ids,
            cleaned: false,
        }
    }

    fn cleanup_once(&mut self) {
        if self.cleaned {
            return;
        }

        remove_initial_dyn_filter_regs(
            &self.server.inner.initial_remote_dyn_filter_registrations,
            &self.query_id,
            self.region_id,
            &self.filter_ids,
        );
        self.cleaned = true;
    }
}

impl Drop for RemoteDynFilterCleanup {
    fn drop(&mut self) {
        self.cleanup_once();
    }
}

/// Removes query-scoped remote dynamic filter subscriptions when a remote read stream is done.
struct RemoteDynFilterCleanupStream {
    stream: SendableRecordBatchStream,
    cleanup: RemoteDynFilterCleanup,
}

impl RecordBatchStream for RemoteDynFilterCleanupStream {
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
        self.stream.metrics()
    }
}

impl Stream for RemoteDynFilterCleanupStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(None) => {
                self.cleanup.cleanup_once();
                Poll::Ready(None)
            }
            other => other,
        }
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
    async fn handle(&self, request: region_request::Body) -> ServerResult<RegionResponseV1> {
        let failed_requests_cnt = crate::metrics::REGION_SERVER_REQUEST_FAILURE_COUNT
            .with_label_values(&[request.as_ref()]);
        let response = match &request {
            region_request::Body::Creates(_)
            | region_request::Body::Drops(_)
            | region_request::Body::Alters(_) => self.handle_batch_ddl_requests(request).await,
            region_request::Body::Inserts(_) | region_request::Body::Deletes(_) => {
                self.handle_requests_in_parallel(request).await
            }
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
        .inspect_err(|_| {
            failed_requests_cnt.inc();
        })
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
        })
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
            .map(|h| Arc::new(QueryContext::from(h)))
            .unwrap_or(QueryContext::arc());

        let result = self
            .handle_remote_read(request, query_ctx.clone())
            .trace(tracing_context.attach(info_span!("RegionServer::handle_read")))
            .await?;

        let stream = Box::pin(FlightRecordBatchStream::new(
            result,
            tracing_context,
            self.flight_compression,
            query_ctx,
        ));
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
                            self.unset_region_status(region_id, &engine, *region_change);
                            if e.status_code() == StatusCode::RegionNotFound
                                && ignore_nonexistent_region
                            {
                                warn!("Region {} not found, ignore it, source: {:?}", region_id, e);
                            } else {
                                error!(e; "Failed to open region: {}", region_id);
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
                .with_context(|| RegionEngineNotFoundSnafu { name: &engine })?
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
    pub async fn handle_batch_request(
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

        match engine
            .handle_request(region_id, request)
            .await
            .with_context(|_| HandleRegionRequestSnafu { region_id })
        {
            Ok(result) => {
                // Update metrics
                if matches!(region_change, RegionChange::Ingest) {
                    crate::metrics::REGION_CHANGED_ROW_COUNT
                        .with_label_values(&[request_type])
                        .inc_by(result.affected_rows as u64);
                }
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
                if matches!(region_change, RegionChange::Ingest) {
                    crate::metrics::REGION_SERVER_INSERT_FAIL_COUNT
                        .with_label_values(&[request_type])
                        .inc();
                }
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
            RegionChange::None | RegionChange::Ingest => {}
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
            RegionChange::None | RegionChange::Ingest => {}
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

        for (region_id, engine) in regions {
            let closed = engine
                .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
                .await;
            match closed {
                Ok(_) => debug!("Region {region_id} is closed"),
                Err(e) => warn!("Failed to close region {region_id}, err: {e}"),
            }
        }
        self.region_map.clear();
        info!("closed {num_regions} regions");

        drop(self.mito_engine.write().unwrap().take());
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
    Ingest,
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

    use std::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::SemanticType;
    use api::v1::region::{
        RemoteDynFilterRequest, RemoteDynFilterUnregister, RemoteDynFilterUpdate,
        remote_dyn_filter_request,
    };
    use common_error::ext::ErrorExt;
    use common_query::request::{
        DynFilterPayload, INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
        InitialDynFilterReg, InitialDynFilterRegs, InitialDynFilterSnapshot,
    };
    use common_recordbatch::RecordBatches;
    use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
    use datafusion::arrow::datatypes::Schema as ArrowSchema;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::{DynamicFilterPhysicalExpr, lit as physical_lit};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::StreamExt;
    use mito2::test_util::CreateRequestBuilder;
    use query::options::FLOW_RETURN_REGION_SEQ;
    use session::context::REMOTE_QUERY_ID_EXTENSION_KEY;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::region_engine::{
        PrepareRequest, QueryScanContext, RegionEngine, RegionScanner, ScannerProperties,
        SinglePartitionScanner,
    };
    use store_api::region_request::{
        PathType, RegionDropRequest, RegionOpenRequest, RegionTruncateRequest,
    };
    use store_api::storage::{RegionId, ScanRequest};
    use table::table::scan::RegionScanExec;

    use super::*;
    use crate::error::Result;
    use crate::region_server::registrations::REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES;
    use crate::tests::{MockRegionEngine, mock_region_server};

    fn test_remote_query_id() -> QueryId {
        QueryId::new()
    }

    fn test_remote_dyn_filter_region_id() -> RegionId {
        RegionId::new(1024, 7)
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

    #[test]
    fn initial_dyn_filter_regs_can_be_read_from_query_context() {
        let mut query_ctx = QueryContext::with("greptime", "public");
        query_ctx.set_extension(
            INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
            InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
                "filter-1",
                vec![vec![1, 2, 3]],
            )])
            .to_extension_value()
            .unwrap(),
        );

        let regs = initial_dyn_filter_regs_from_query_ctx(&Arc::new(query_ctx)).unwrap();

        assert_eq!(regs.regs.len(), 1);
        assert_eq!(regs.regs[0].filter_id, "filter-1");
    }

    #[test]
    fn initial_dyn_filter_regs_from_query_context_rejects_duplicate_filter_ids() {
        let mut query_ctx = QueryContext::with("greptime", "public");
        query_ctx.set_extension(
            INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
            InitialDynFilterRegs::new(vec![
                InitialDynFilterReg::new("filter-1", vec![vec![1, 2, 3]]),
                InitialDynFilterReg::new("filter-1", vec![vec![4, 5, 6]]),
            ])
            .to_extension_value()
            .unwrap(),
        );

        let regs = initial_dyn_filter_regs_from_query_ctx(&Arc::new(query_ctx));

        assert!(regs.is_none());
    }

    #[test]
    fn register_initial_dyn_filter_regs_creates_query_scoped_entries() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-1", vec![vec![1, 2, 3]]),
            InitialDynFilterReg::new("filter-2", vec![vec![4, 5, 6]]),
        ]);
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        let registered_filter_ids =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);

        let query_regs = regs_by_query.get(&query_id).unwrap();
        assert_eq!(query_regs.len(), 2);
        assert_eq!(
            registered_filter_ids,
            vec![
                RemoteDynFilterId::new("filter-1"),
                RemoteDynFilterId::new("filter-2")
            ]
        );
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.filter_id, RemoteDynFilterId::new("filter-1"));
        assert_eq!(registered.child_exprs_datafusion_proto, vec![vec![1, 2, 3]]);
        assert_eq!(registered.subscriber_regions.len(), 1);
        assert!(registered.subscriber_regions.contains(&region_id));
    }

    #[test]
    fn register_initial_dyn_filter_regs_same_region_duplicate_is_idempotent() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        let first = register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let duplicate =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);

        let query_regs = regs_by_query.get(&query_id).unwrap();
        assert_eq!(query_regs.len(), 1);
        assert_eq!(first, vec![RemoteDynFilterId::new("filter-1")]);
        assert!(duplicate.is_empty());
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 1);
        assert!(registered.subscriber_regions.contains(&region_id));
    }

    #[test]
    fn register_initial_dyn_filter_regs_ignores_invalid_duplicate_payload_set() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-1", vec![vec![1, 2, 3]]),
            InitialDynFilterReg::new("filter-1", vec![vec![4, 5, 6]]),
        ]);
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);

        assert!(regs_by_query.get(&query_id).is_none());
    }

    #[test]
    fn register_initial_dyn_filter_regs_tracks_different_region_subscribers_for_same_filter() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let query_id = test_remote_query_id();
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);

        let query_regs = regs_by_query.get(&query_id).unwrap();
        assert_eq!(query_regs.len(), 1);
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 2);
        assert!(registered.subscriber_regions.contains(&first_region_id));
        assert!(registered.subscriber_regions.contains(&second_region_id));
    }

    #[test]
    fn remove_initial_dyn_filter_regs_removes_registered_filter_entries() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let other_query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        let registered_filter_ids = register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
                "filter-1",
                vec![vec![1, 2, 3]],
            )]),
        );
        register_initial_dyn_filter_regs(
            &regs_by_query,
            &other_query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
                "filter-2",
                vec![vec![4, 5, 6]],
            )]),
        );

        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &registered_filter_ids,
        );

        assert!(regs_by_query.get(&query_id).is_none());
        let other_query_regs = regs_by_query.get(&other_query_id).unwrap();
        assert_eq!(other_query_regs.len(), 1);
    }

    #[test]
    fn remove_initial_dyn_filter_regs_keeps_other_subscribers() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);

        let first_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);

        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            first_region_id,
            &first_subscription,
        );

        let query_regs = regs_by_query.get(&query_id).unwrap();
        assert_eq!(query_regs.len(), 1);
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 1);
        assert!(registered.subscriber_regions.contains(&second_region_id));
    }

    #[test]
    fn remove_initial_remote_dyn_filter_regs_keeps_other_filters_for_same_query() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        let first_subscription = register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]),
        );
        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-2", vec![])]),
        );

        remove_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &first_subscription);

        let query_regs = regs_by_query.get(&query_id).unwrap();
        assert!(
            query_regs
                .get(&RemoteDynFilterId::new("filter-1"))
                .is_none()
        );
        assert!(
            query_regs
                .get(&RemoteDynFilterId::new("filter-2"))
                .is_some()
        );
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
            .handle_request(
                region_id,
                RegionRequest::Truncate(RegionTruncateRequest::All),
            )
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

        let status = mock_region_server.inner.region_map.get(&region_id);
        assert!(status.is_some());
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
        assert_eq!(err.status_code(), StatusCode::Unexpected);
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

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_request_requires_query_id() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: String::new(),
                action: Some(remote_dyn_filter_request::Action::Unregister(
                    RemoteDynFilterUnregister {
                        filter_id: "filter-1".to_string(),
                    },
                )),
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "query_id"
        );
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_request_requires_action() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: None,
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "action"
        );
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_update_requires_filter_id() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: Some(remote_dyn_filter_request::Action::Update(
                    RemoteDynFilterUpdate {
                        filter_id: String::new(),
                        payload: vec![1],
                        generation: 1,
                        is_complete: false,
                    },
                )),
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "filter_id"
        );
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_update_requires_payload() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: Some(remote_dyn_filter_request::Action::Update(
                    RemoteDynFilterUpdate {
                        filter_id: "filter-1".to_string(),
                        payload: Vec::new(),
                        generation: 1,
                        is_complete: false,
                    },
                )),
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "payload"
        );
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_update_no_registration() {
        let mock_region_server = mock_region_server();

        let response = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: Some(remote_dyn_filter_request::Action::Update(
                    RemoteDynFilterUpdate {
                        filter_id: "filter-1".to_string(),
                        payload: vec![1],
                        generation: 1,
                        is_complete: false,
                    },
                )),
            })
            .await
            .unwrap();

        assert_eq!(response.affected_rows, 0);
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_unregister_no_registration() {
        let mock_region_server = mock_region_server();

        let response = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: Some(remote_dyn_filter_request::Action::Unregister(
                    RemoteDynFilterUnregister {
                        filter_id: "filter-1".to_string(),
                    },
                )),
            })
            .await
            .unwrap();

        assert_eq!(response.affected_rows, 0);
    }

    #[test]
    fn test_apply_remote_dyn_filter_update_missing_registration() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");

        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[1], 1, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::MissingRegistration);

        let outcome = unregister_remote_dyn_filter(&regs_by_query, &query_id, &filter_id);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::MissingRegistration);
    }

    #[test]
    fn test_apply_remote_dyn_filter_update_buffering_before_scan() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");

        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            test_remote_dyn_filter_region_id(),
            &regs,
        );

        // First update with generation 1: should be Buffered (no runtime installed yet)
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[1], 1, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Buffered);

        // Update with generation 0: should be Stale (older than pending generation 1)
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[2], 0, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Stale);

        // Update with generation 1 again: should be Idempotent (same generation)
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[3], 1, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Idempotent);
    }

    fn datafusion_payload_bytes(expr: Arc<dyn PhysicalExpr>) -> Vec<u8> {
        match DynFilterPayload::from_datafusion_expr(&expr, REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES)
            .unwrap()
        {
            DynFilterPayload::Datafusion(bytes) => bytes,
            _ => unreachable!(
                "DynFilterPayload::from_datafusion_expr only returns datafusion payloads"
            ),
        }
    }

    fn empty_arrow_schema() -> ArrowSchema {
        ArrowSchema::empty()
    }

    fn register_empty_remote_dyn_filter(
        regs_by_query: &RemoteDynFilterRegistry,
        query_id: &QueryId,
    ) -> Vec<RemoteDynFilterId> {
        register_empty_remote_dyn_filter_for_region(
            regs_by_query,
            query_id,
            test_remote_dyn_filter_region_id(),
        )
    }

    fn register_empty_remote_dyn_filter_for_region(
        regs_by_query: &RemoteDynFilterRegistry,
        query_id: &QueryId,
        region_id: RegionId,
    ) -> Vec<RemoteDynFilterId> {
        register_initial_dyn_filter_regs(
            regs_by_query,
            query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]),
        )
    }

    #[test]
    fn initial_remote_dyn_filter_snapshot_initializes_runtime_filter() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let payload = DynFilterPayload::Datafusion(datafusion_payload_bytes(physical_lit(false)));
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-1", vec![])
                .with_initial_snapshot(InitialDynFilterSnapshot::new(payload, 7, false)),
        ]);

        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            test_remote_dyn_filter_region_id(),
            &regs,
        );
        let exprs = remote_dyn_filter_exprs_for_initial_regs(
            &regs_by_query,
            &query_id,
            &regs,
            &empty_arrow_schema(),
        );

        assert_eq!(exprs.len(), 1);
        assert_eq!(format!("{}", exprs[0]), "DynamicFilter [ false ]");
    }

    struct DynFilterAcceptingScanner {
        inner: SinglePartitionScanner,
    }

    impl std::fmt::Debug for DynFilterAcceptingScanner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DynFilterAcceptingScanner")
        }
    }

    impl DisplayAs for DynFilterAcceptingScanner {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            self.inner.fmt_as(t, f)
        }
    }

    impl RegionScanner for DynFilterAcceptingScanner {
        fn name(&self) -> &str {
            self.inner.name()
        }

        fn properties(&self) -> &ScannerProperties {
            self.inner.properties()
        }

        fn schema(&self) -> Arc<Schema> {
            self.inner.schema()
        }

        fn metadata(&self) -> Arc<RegionMetadata> {
            self.inner.metadata()
        }

        fn prepare(&mut self, request: PrepareRequest) -> std::result::Result<(), BoxedError> {
            self.inner.prepare(request)
        }

        fn scan_partition(
            &self,
            ctx: &QueryScanContext,
            metrics_set: &ExecutionPlanMetricsSet,
            partition: usize,
        ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
            self.inner.scan_partition(ctx, metrics_set, partition)
        }

        fn has_predicate_without_region(&self) -> bool {
            self.inner.has_predicate_without_region()
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Vec<bool> {
            vec![true; filter_exprs.len()]
        }

        fn set_logical_region(&mut self, logical_region: bool) {
            self.inner.set_logical_region(logical_region)
        }
    }

    fn region_scan_exec(region_id: RegionId) -> Arc<RegionScanExec> {
        let scanner = Box::new(DynFilterAcceptingScanner {
            inner: SinglePartitionScanner::new(
                single_value_stream(),
                false,
                Arc::new(mock_region_metadata(region_id)),
                None,
            ),
        });
        Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap())
    }

    fn remote_dyn_filter_query_ctx(
        query_id: &QueryId,
        regs: &InitialDynFilterRegs,
    ) -> QueryContextRef {
        Arc::new(
            QueryContextBuilder::default()
                .set_extension(
                    REMOTE_QUERY_ID_EXTENSION_KEY.to_string(),
                    query_id.to_string(),
                )
                .set_extension(
                    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY.to_string(),
                    regs.to_extension_value().unwrap(),
                )
                .build(),
        )
    }

    fn wrap_remote_dyn_filter_plan(
        server: &RegionServer,
        plan: Arc<dyn ExecutionPlan>,
        query_id: &QueryId,
        regs: &InitialDynFilterRegs,
    ) -> Arc<dyn ExecutionPlan> {
        let wrapper = RemoteDynFilterPhysicalPlanWrapper {
            server: Arc::downgrade(&server.inner),
        };
        wrapper.wrap(
            plan,
            remote_dyn_filter_query_ctx(query_id, regs),
            &ConfigOptions::default(),
        )
    }

    fn contains_filter_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.as_any().is::<FilterExec>()
            || plan
                .children()
                .into_iter()
                .any(|child| contains_filter_exec(child))
    }

    fn sort_with_fetch(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(
                Arc::new(Column::new("timestamp", 0)) as Arc<dyn PhysicalExpr>,
            )])
            .unwrap();
        Arc::new(SortExec::new(ordering, input).with_fetch(Some(1)))
    }

    fn only_remote_dyn_filter(
        regs_by_query: &RemoteDynFilterRegistry,
        query_id: &QueryId,
    ) -> Arc<DynamicFilterPhysicalExpr> {
        let initial_regs =
            InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);
        let exprs = remote_dyn_filter_exprs_for_initial_regs(
            regs_by_query,
            query_id,
            &initial_regs,
            &empty_arrow_schema(),
        );
        assert_eq!(1, exprs.len());
        let expr = exprs.into_iter().next().unwrap();
        let expr = expr as Arc<dyn std::any::Any + Send + Sync>;
        expr.downcast::<DynamicFilterPhysicalExpr>().unwrap()
    }

    #[test]
    fn test_remote_dyn_filter_filter_exec_pushes_through_transparent_node() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);
        register_empty_remote_dyn_filter(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
        );

        let scan = region_scan_exec(region_id);
        let plan = Arc::new(CoalescePartitionsExec::new(scan)) as Arc<dyn ExecutionPlan>;
        let wrapped = wrap_remote_dyn_filter_plan(&mock_region_server, plan, &query_id, &regs);

        assert!(wrapped.as_any().is::<CoalescePartitionsExec>());
        assert!(!contains_filter_exec(&wrapped));

        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
            &RemoteDynFilterId::new("filter-1"),
            &payload,
            0,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
    }

    #[test]
    fn test_remote_dyn_filter_filter_exec_stays_above_pushdown_barrier() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let region_id = RegionId::new(1024, 7);
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);
        register_empty_remote_dyn_filter(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
        );

        let scan: Arc<dyn ExecutionPlan> = region_scan_exec(region_id);
        let plan = sort_with_fetch(scan);
        let wrapped = wrap_remote_dyn_filter_plan(&mock_region_server, plan, &query_id, &regs);

        let filter = wrapped.as_any().downcast_ref::<FilterExec>().unwrap();
        assert!(filter.input().as_any().is::<SortExec>());

        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
            &RemoteDynFilterId::new("filter-1"),
            &payload,
            0,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
    }

    #[test]
    fn test_remote_dyn_filter_rejects_oversized_payload_before_buffering() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        let oversized = vec![0; REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES + 1];
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &oversized,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::PayloadTooLarge);

        // The rejected generation must not become the pending generation.
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[1], 0, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Buffered);
    }

    #[test]
    fn test_remote_dyn_filter_generation_zero_applies_as_first_update() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        // Installing the wrapper before the update exercises the runtime apply path directly.
        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);
        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            0,
            false,
        );

        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert!(dyn_filter.snapshot_generation() > 1);
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");
    }

    #[test]
    fn test_buffered_update_applies_when_scan_installs_wrapper() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Buffered);

        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);
        assert!(dyn_filter.snapshot_generation() > 1);
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");
    }

    #[test]
    fn test_unregister_completes_installed_remote_dyn_filter_without_relaxing() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);
        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");

        let outcome = unregister_remote_dyn_filter(&regs_by_query, &query_id, &filter_id);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert!(regs_by_query.get(&query_id).is_none());
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");
    }

    #[test]
    fn test_remote_dyn_filter_unregister_removes_all_region_subscribers_for_filter() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);

        let first_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        let second_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);
        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);

        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);

        let outcome = unregister_remote_dyn_filter(&regs_by_query, &query_id, &filter_id);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert!(regs_by_query.get(&query_id).is_none());
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");

        // Stream-local cleanup may run after FE's peer-deduplicated unregister. It should be benign.
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            first_region_id,
            &first_subscription,
        );
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            second_region_id,
            &second_subscription,
        );
        assert!(regs_by_query.get(&query_id).is_none());
    }

    #[test]
    fn test_remote_dyn_filter_unregister_keeps_other_filters_for_same_query() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]),
        );
        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-2", vec![])]),
        );

        let outcome = unregister_remote_dyn_filter(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-1"),
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);

        let query_regs = regs_by_query.get(&query_id).unwrap();
        assert!(
            query_regs
                .get(&RemoteDynFilterId::new("filter-1"))
                .is_none()
        );
        assert!(
            query_regs
                .get(&RemoteDynFilterId::new("filter-2"))
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_remote_dyn_filter_cleanup_stream_removes_on_eof() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();
        let registered_filter_ids = register_empty_remote_dyn_filter(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
        );
        let cleanup = RemoteDynFilterCleanup::new(
            mock_region_server.clone(),
            query_id,
            region_id,
            registered_filter_ids,
        );

        let stream = wrap_remote_dyn_filter_cleanup_stream(single_value_stream(), cleanup);
        let mut pinned = Box::pin(stream);
        while pinned.next().await.is_some() {}

        assert!(
            mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations
                .get(&query_id)
                .is_none()
        );
    }

    #[test]
    fn test_remote_dyn_filter_cleanup_stream_removes_on_drop() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();
        let registered_filter_ids = register_empty_remote_dyn_filter(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
        );
        let cleanup = RemoteDynFilterCleanup::new(
            mock_region_server.clone(),
            query_id,
            region_id,
            registered_filter_ids,
        );

        let stream = wrap_remote_dyn_filter_cleanup_stream(single_value_stream(), cleanup);
        drop(stream);

        assert!(
            mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations
                .get(&query_id)
                .is_none()
        );
    }

    #[test]
    fn test_remote_dyn_filter_cleanup_guard_removes_on_drop_before_stream_wrap() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();
        let registered_filter_ids = register_empty_remote_dyn_filter(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
        );

        let cleanup = RemoteDynFilterCleanup::new(
            mock_region_server.clone(),
            query_id,
            region_id,
            registered_filter_ids,
        );
        drop(cleanup);

        assert!(
            mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations
                .get(&query_id)
                .is_none()
        );
    }

    #[test]
    fn test_remote_dyn_filter_multi_region_subscription_cleanup() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let query_id = test_remote_query_id();
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);

        // Register the same logical filter for two regions on the same datanode.
        let first_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        let second_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);

        // Verify only one filter entry exists and both region subscribers are tracked explicitly.
        {
            let query_regs = regs_by_query.get(&query_id).unwrap();
            assert_eq!(query_regs.len(), 1);
            let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
            assert_eq!(registered.subscriber_regions.len(), 2);
            assert!(registered.subscriber_regions.contains(&first_region_id));
            assert!(registered.subscriber_regions.contains(&second_region_id));
        }

        // One region cleanup should not drop the entry while another region is subscribed.
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            first_region_id,
            &first_subscription,
        );
        assert!(regs_by_query.get(&query_id).is_some());
        {
            let query_regs = regs_by_query.get(&query_id).unwrap();
            let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
            assert_eq!(registered.subscriber_regions.len(), 1);
            assert!(registered.subscriber_regions.contains(&second_region_id));
        }

        // Last region cleanup should drop the entry.
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            second_region_id,
            &second_subscription,
        );
        assert!(regs_by_query.get(&query_id).is_none());
    }
}
