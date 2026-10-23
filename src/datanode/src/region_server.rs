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
use std::time::Duration;

use api::region::RegionResponse;
use api::v1::meta::TopicStat;
use api::v1::region::sync_request::ManifestInfo;
use api::v1::region::{
    ListMetadataRequest, RegionResponse as RegionResponseV1, SyncRequest, region_request,
};
use api::v1::{ResponseHeader, Status};
use arrow_flight::{FlightData, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_meta::datanode::TopicStatsReporter;
use common_query::OutputData;
use common_query::request::{QueryRequest, RAW_SERIES_READ_EXTENSION_KEY, RegionReadKind};
use common_query::series_frame::raw_series_frame_ordering;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{
    DfRecordBatch, OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream,
};
use common_runtime::Runtime;
use common_telemetry::tracing::{self, info_span};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, error, info, warn};
use dashmap::DashMap;
use datafusion::datasource::TableProvider;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::LogicalPlan;
use datatypes::schema::SchemaRef;
use datatypes::series::frame::{
    RawLabelMode, RawSeriesFrameDecoder, raw_series_frame_label_decode,
    raw_series_frame_series_schema,
};
use either::Either;
use futures_util::future::try_join_all;
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
use crate::region_server::catalog::{
    InternalTableKind, NameAwareCatalogList, NameAwareDataSourceInjectorBuilder,
};
use crate::region_server::registrations::RemoteDynFilterRegistry;
use crate::region_server::remote_dyn_filter::wrap_remote_dyn_filter_guarded_stream;

const QUERY_RUNTIME_STREAM_BUFFER_SIZE: usize = 8;

/// Returns the metric a raw-series read should frame, or `None` when this read is not one.
///
/// The answer is the table the request's plan scans, which is the one thing about a frame the
/// region cannot infer: a frame declares its metric name in its label decode description, and a
/// metric name is the logical table's identity rather than a column of any region.
///
/// Only a plan that *is* that scan, and asks for all of it, qualifies. A frame leaf answers with a
/// frame where the row leaf would have answered with rows, so every operator above the scan would
/// read a frame as rows, and a pushed-down fetch would be dropped by a leaf that returns every
/// chunk of every series. Such raw-series requests are rejected at the read boundary.
fn raw_series_metric_name(request: &QueryRequest) -> Option<String> {
    if request.read_kind != RegionReadKind::RawSeries {
        return None;
    }
    let LogicalPlan::TableScan(scan) = &request.plan else {
        return None;
    };
    if scan.fetch.is_some() {
        return None;
    }
    Some(scan.table_name.table().to_string())
}

/// Publishes the read kind the request asked for, and nothing a caller could have written.
///
/// A request header can carry arbitrary query-context extensions, so a context arriving here may
/// already hold [`RAW_SERIES_READ_EXTENSION_KEY`] with whatever the caller chose. Every read
/// rewrites the key unconditionally, from the decoded request alone: an ordinary read sets it to
/// the empty value the region's table provider reads as "no metric", so naming the key in a header
/// cannot opt a read into frames, and a raw read sets it to the metric derived from its own plan
/// rather than to a caller-supplied name.
fn sanitize_remote_read_context(mut ctx: QueryContext) -> QueryContext {
    ctx.set_extension(RAW_SERIES_READ_EXTENSION_KEY, "");
    ctx
}

fn validate_raw_series_request(request: &QueryRequest) -> Result<()> {
    if request.read_kind != RegionReadKind::RawSeries {
        return Ok(());
    }
    ensure!(
        matches!(&request.plan, LogicalPlan::TableScan(_)),
        UnexpectedSnafu {
            violated: "RawSeries read requires a bare TableScan plan",
        }
    );
    let LogicalPlan::TableScan(scan) = &request.plan else {
        unreachable!("plan was checked to be a TableScan");
    };
    // A bare scan may carry an empty table name, and an empty name is also the sentinel the
    // region's table provider reads as "no metric": such a read would silently be answered
    // with rows. A frame names the series it frames, so an unnamed scan is rejected here.
    ensure!(
        !scan.table_name.table().is_empty(),
        UnexpectedSnafu {
            violated: "RawSeries read requires a named table scan",
        }
    );
    ensure!(
        scan.fetch.is_none(),
        UnexpectedSnafu {
            violated: "RawSeries read does not support fetch",
        }
    );
    ensure!(
        InternalTableKind::from_table_name(scan.table_name.table()).is_none(),
        UnexpectedSnafu {
            violated: "RawSeries read does not support inspection tables",
        }
    );
    Ok(())
}

fn apply_raw_series_read_key(mut ctx: QueryContext, request: &QueryRequest) -> QueryContext {
    ctx.set_extension(
        RAW_SERIES_READ_EXTENSION_KEY,
        raw_series_metric_name(request).unwrap_or_default(),
    );
    ctx
}

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
            return common_runtime::spawn_ingest(async move {
                inner.handle_request(region_id, request).await
            })
            .await
            .context(RuntimeJoinSnafu { request_type })?;
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

        // Remote reads are always row reads; discard any caller-supplied frame opt-in before
        // constructing the catalog, decoder, or physical providers.
        let query_ctx = Arc::new(sanitize_remote_read_context(query_ctx.as_ref().clone()));

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

        let cleanup = self.register_initial_remote_dyn_filter_cleanup(&query_ctx, region_id);

        let stream = self
            .inner
            .handle_read(
                QueryRequest {
                    header: request.header,
                    region_id,
                    plan,
                    // The cross-process request has no way to ask for frames yet, so a remote read
                    // keeps the ordinary row answer it has always produced.
                    read_kind: RegionReadKind::Rows,
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
        validate_raw_series_request(&request)?;
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
        // A read that asked for raw series frames names the metric those frames belong to here,
        // because the region's table provider cannot see the plan and the metric name is not part
        // of any region's metadata. Everything else a frame declares is derived region-side. The
        // key is rewritten on every read, so this is also what stops a caller-supplied header from
        // asking for frames.
        let query_ctx = Arc::new(apply_raw_series_read_key(
            ctx.unwrap_or_else(|| QueryContextBuilder::default().build()),
            &request,
        ));

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
        let metric_engine = engine
            .as_any()
            .downcast_ref::<MetricEngine>()
            .context(UnexpectedSnafu {
                violated: "Failed to downcast to MetricEngine",
            })?
            .clone();

        let tracing_context = TracingContext::from_current_span();
        let batch_size = put_requests.len();
        let span = tracing_context.attach(info_span!(
            "RegionServer::handle_metric_batch_puts",
            batch_size = batch_size,
        ));
        let result = common_runtime::spawn_ingest(async move {
            metric_engine
                .put_regions_batch(put_requests)
                .trace(span)
                .await
        })
        .await
        .context(RuntimeJoinSnafu { request_type })?
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
                    .with_label_values(&[request_type, err.status_code().as_ref()])
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
                        .with_label_values(&[request_type, err.status_code().as_ref()])
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
        //
        // Captured before the plan moves into the engine: the answer to a raw-series read is
        // checked against the frame contract below.
        let raw_series = request.read_kind == RegionReadKind::RawSeries;

        let result = self
            .query_engine
            .execute(request.plan, query_ctx)
            .await
            .context(ExecuteLogicalPlanSnafu)?;

        match result.data {
            OutputData::AffectedRows(_) | OutputData::RecordBatches(_) => {
                UnsupportedOutputSnafu { expected: "stream" }.fail()
            }
            OutputData::Stream(stream) if !raw_series => Ok(stream),
            OutputData::Stream(stream) => {
                // The table provider comes from configuration, so the request being a bare scan
                // says nothing about the answer's shape: a factory could serve an ordinary row
                // table for the same name. The declared Arrow schema must therefore be a raw
                // series frame. The empty batch is decoded to compare the exact fields, types and
                // order; no row is polled. Rows are left untouched.
                let arrow = stream.schema().arrow_schema().clone();
                raw_series_frame_label_decode(&arrow)
                    .and_then(|decode| {
                        let mode = if decode.is_some() {
                            RawLabelMode::Key
                        } else {
                            RawLabelMode::Explicit
                        };
                        let series = raw_series_frame_series_schema(&arrow)?;
                        RawSeriesFrameDecoder::try_new(&series, mode)?
                            .decode(&DfRecordBatch::new_empty(arrow.clone()))?;
                        Ok(())
                    })
                    .map_err(|e| {
                        UnexpectedSnafu {
                            violated: format!(
                                "RawSeries read answered with a non-frame stream: {e}"
                            ),
                        }
                        .build()
                    })?;
                raw_series_frame_ordering(&arrow).map_err(|e| {
                    UnexpectedSnafu {
                        violated: format!("RawSeries read answered with unusable ordering: {e}"),
                    }
                    .build()
                })?;
                Ok(stream)
            }
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
                .handle_request(
                    region_id,
                    RegionRequest::Close(RegionCloseRequest::default()),
                )
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
        let mut first_error = None;
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
        MITO_ENGINE_NAME | METRIC_ENGINE_NAME | FILE_ENGINE_NAME => engine,
        _ => "unknown",
    };
    crate::metrics::REGION_OPEN_FAILURES_TOTAL
        .with_label_values(&[engine, status_code.as_ref()])
        .inc_by(count as u64);
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

    use api::v1::{Rows, SemanticType};
    use common_error::ext::{ErrorExt, RetryHint};
    use common_query::series_frame::unpack_raw_series;
    use common_recordbatch::RecordBatches;
    use common_recordbatch::adapter::{
        DfRecordBatchStreamAdapter, RecordBatchMetrics, RegionWatermarkEntry,
    };
    use common_recordbatch::util as recordbatch_util;
    use common_runtime::runtime::{BuilderBuild, RuntimeTrait};
    use datafusion::catalog::MemTable;
    use datatypes::arrow::array::{Array, Int32Array, UInt64Array};
    use datatypes::arrow::datatypes::Schema as ArrowSchema;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::series::SeriesOrdering;
    use datatypes::series::frame::{
        LABEL_DECODE_VERSION, LabelDecodeEncoding, LabelDecodeSpec, LabelDecodeTag,
    };
    use datatypes::series::raw::RawSeriesChunk;
    use datatypes::series::samples::{FieldSpec, SampleRef, SeriesSchema};
    use datatypes::vectors::Int32Vector;
    use futures_util::{StreamExt, TryStreamExt};
    use mito2::config::MitoConfig;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use query::QueryEngineFactory;
    use query::options::{FLOW_RETURN_REGION_SEQ, QueryOptions};
    use query::query_engine::DefaultSerializer;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::region_engine::{RegionEngine, RegionRole};
    use store_api::region_info::RegionInfoEntry;
    use store_api::region_request::{
        PathType, RegionCleanUpRequest, RegionCompactRequest, RegionCreateRequest,
        RegionDeleteRequest, RegionDropRequest, RegionOpenRequest, RegionPutRequest,
        RegionTruncateRequest,
    };
    use store_api::sst_entry::{ManifestSstEntry, PuffinIndexMetaEntry, StorageSstEntry};
    use store_api::storage::RegionId;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

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

    fn scan_plan(table: &str) -> LogicalPlan {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        datafusion_expr::table_scan(
            Some(table),
            &Schema::new(vec![Field::new("v", DataType::Int32, true)]),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    /// A real Mito region behind a real query engine, so a read exercises the whole
    /// `RegionServer::handle_read` path instead of a mocked provider.
    struct RawSeriesFixture {
        server: RegionServer,
        engine: Arc<MitoEngine>,
        region_id: RegionId,
        create: RegionCreateRequest,
        arrow_schema: ArrowSchema,
        /// Kept last so the temporary storage outlives the server and engine that use it.
        _env: TestEnv,
    }

    impl RawSeriesFixture {
        async fn new(prefix: &str, field_num: usize) -> Self {
            Self::with_factory(prefix, field_num, Arc::new(DummyTableProviderFactory)).await
        }

        async fn with_factory(
            prefix: &str,
            field_num: usize,
            table_provider_factory: TableProviderFactoryRef,
        ) -> Self {
            let mut env = TestEnv::with_prefix(prefix).await;
            let engine = Arc::new(env.create_engine(MitoConfig::default()).await);
            let query_engine = QueryEngineFactory::new(
                ::catalog::memory::MemoryCatalogManager::new(),
                None,
                None,
                None,
                None,
                false,
                QueryOptions::default(),
            )
            .query_engine();
            let mut server = RegionServer::with_table_provider(
                query_engine,
                Runtime::builder().build().unwrap(),
                Box::new(crate::event_listener::NoopRegionServerEventListener),
                table_provider_factory,
                0,
                Duration::from_millis(0),
                FlightCompression::default(),
            );
            server.register_engine(engine.clone());
            let region_id = RegionId::new(2048, 1);
            let create = CreateRequestBuilder::new().field_num(field_num).build();
            server
                .handle_request(region_id, RegionRequest::Create(create.clone()))
                .await
                .unwrap();
            server
                .set_region_role(region_id, RegionRole::Leader)
                .unwrap();
            let arrow_schema = Schema::new(
                create
                    .column_metadatas
                    .iter()
                    .map(|column| column.column_schema.clone())
                    .collect(),
            )
            .arrow_schema()
            .as_ref()
            .clone();
            Self {
                server,
                engine,
                region_id,
                create,
                arrow_schema,
                _env: env,
            }
        }

        fn scan_plan(&self, table: &str, projection: Option<Vec<usize>>) -> LogicalPlan {
            datafusion_expr::table_scan(Some(table), &self.arrow_schema, projection)
                .unwrap()
                .build()
                .unwrap()
        }

        fn request(&self, plan: LogicalPlan, read_kind: RegionReadKind) -> QueryRequest {
            QueryRequest {
                header: None,
                region_id: self.region_id,
                plan,
                read_kind,
            }
        }

        async fn put(&self, key: &str, timestamps: &[i64], fields: &[(Option<i64>, Option<i64>)]) {
            mito2::test_util::put_rows(
                &self.engine,
                self.region_id,
                Rows {
                    schema: mito2::test_util::rows_schema(&self.create),
                    rows: mito2::test_util::build_rows_with_fields(key, timestamps, fields),
                },
            )
            .await;
        }

        async fn stop(&self) {
            self.server.stop().await.unwrap();
        }
    }

    /// A configured table provider factory that always serves one ordinary row table.
    ///
    /// `RegionServer::with_table_provider` accepts such factories from configuration, so the
    /// region's metadata does not determine the answer's shape on its own.
    struct RowsTableProviderFactory(Arc<dyn TableProvider>);

    #[async_trait]
    impl query::dummy_catalog::TableProviderFactory for RowsTableProviderFactory {
        async fn create(
            &self,
            _region_id: RegionId,
            _engine: RegionEngineRef,
            _ctx: Option<QueryContextRef>,
        ) -> query::error::Result<Arc<dyn TableProvider>> {
            Ok(self.0.clone())
        }
    }

    /// Converts a region read into validated raw-series chunks and the layout it declared.
    async fn unpack_frames(
        stream: SendableRecordBatchStream,
        fields: &[&str],
    ) -> (
        Vec<String>,
        LabelDecodeSpec,
        SeriesOrdering,
        Vec<RawSeriesChunk>,
    ) {
        let expected = Arc::new(SeriesSchema {
            fields: fields
                .iter()
                .map(|name| FieldSpec {
                    name: Arc::from(*name),
                })
                .collect(),
            eval_grid: None,
        });
        let frames = unpack_raw_series(
            Box::pin(DfRecordBatchStreamAdapter::new(stream)),
            Some(expected),
        )
        .unwrap();
        let field_names = frames
            .schema()
            .fields
            .iter()
            .map(|field| field.name.to_string())
            .collect();
        let decode = frames
            .label_decode_spec()
            .cloned()
            .expect("a key-encoded frame declares its label decode description");
        let ordering = frames.ordering();
        let chunks = frames.try_collect::<Vec<_>>().await.unwrap();
        (field_names, decode, ordering, chunks)
    }

    fn float_at(chunk: &RawSeriesChunk, field: usize, row: usize) -> Option<f64> {
        match chunk.fields.as_slice()[field].sample(row) {
            SampleRef::Float(value) => Some(value),
            SampleRef::Missing => None,
            SampleRef::Histogram(_) => panic!("the fixture stores float samples"),
        }
    }

    fn timestamps(chunk: &RawSeriesChunk) -> Vec<i64> {
        chunk.time.as_buffer().iter().copied().collect()
    }

    fn inspection_scan_plan() -> LogicalPlan {
        datafusion_expr::table_scan(
            Some(RegionInfoEntry::reserved_table_name_for_inspection()),
            RegionInfoEntry::schema().arrow_schema().as_ref(),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn query_request(plan: LogicalPlan, read_kind: RegionReadKind) -> QueryRequest {
        QueryRequest {
            header: None,
            region_id: RegionId::new(2048, 1),
            plan,
            read_kind,
        }
    }

    #[tokio::test]
    async fn raw_series_inspection_table_is_rejected_but_rows_read_region_info() {
        let fixture = RawSeriesFixture::new("raw-series-inspection", 1).await;
        let plan = inspection_scan_plan();

        // The same bare scan that is a valid inspection read as rows must not be answered as a
        // frame: the reserved table is replaced by a row `MemTable`, so no frame can come back.
        let Err(err) = fixture
            .server
            .handle_read(fixture.request(plan.clone(), RegionReadKind::RawSeries))
            .await
        else {
            panic!("a RawSeries read of an inspection table must be rejected");
        };
        let crate::error::Error::Unexpected { violated, .. } = &err else {
            panic!("expected an inspection-specific rejection, got {err}");
        };
        assert!(
            violated.contains("inspection tables"),
            "unexpected rejection cause: {violated}"
        );

        let rows = fixture
            .server
            .handle_read(fixture.request(plan, RegionReadKind::Rows))
            .await
            .unwrap();
        let batches = recordbatch_util::collect(rows).await.unwrap();
        assert_eq!(1, batches.len());
        let batch = &batches[0];
        assert_eq!(
            batch
                .schema
                .column_schemas()
                .iter()
                .map(|column| (column.name.clone(), column.data_type.clone()))
                .collect::<Vec<_>>(),
            RegionInfoEntry::schema()
                .column_schemas()
                .iter()
                .map(|column| (column.name.clone(), column.data_type.clone()))
                .collect::<Vec<_>>()
        );
        assert_eq!(1, batch.num_rows());
        let region_id_column: &dyn Array = batch.column(0).as_ref();
        let region_ids = region_id_column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(fixture.region_id.as_u64(), region_ids.value(0));

        fixture.stop().await;
    }

    #[tokio::test]
    async fn raw_series_handle_read_answers_region_frames() {
        let fixture = RawSeriesFixture::new("raw-series-handle-read", 2).await;
        fixture
            .put(
                "host-a",
                &[1, 2],
                &[(Some(1), Some(10)), (Some(2), Some(20))],
            )
            .await;
        fixture
            .put("host-b", &[1, 2], &[(Some(3), None), (Some(4), Some(40))])
            .await;

        let stream = fixture
            .server
            .handle_read(fixture.request(
                fixture.scan_plan("cpu_usage", None),
                RegionReadKind::RawSeries,
            ))
            .await
            .unwrap();
        let (fields, decode, ordering, chunks) =
            unpack_frames(stream, &["field_0", "field_1"]).await;

        assert_eq!(vec!["field_0".to_string(), "field_1".to_string()], fields);
        assert_eq!(SeriesOrdering::KeyThenTime, ordering);
        assert_eq!(LABEL_DECODE_VERSION, decode.version);
        assert_eq!("cpu_usage", decode.metric_name);
        assert_eq!(LabelDecodeEncoding::Dense, decode.encoding);
        assert_eq!(1, decode.primary_key.len());
        assert_eq!(0, decode.primary_key[0].column_id);
        assert_eq!("tag_0", decode.primary_key[0].name);
        assert_eq!(
            ConcreteDataType::string_datatype(),
            decode.primary_key[0].physical_type
        );
        assert_eq!(
            vec![LabelDecodeTag {
                column_id: 0,
                name: "tag_0".to_string(),
            }],
            decode.logical_tags
        );

        // A producer may split a series across chunks, so group by encoded key and compare the
        // complete per-series payload rather than assuming one chunk per series.
        let mut by_key: HashMap<Vec<u8>, Vec<&RawSeriesChunk>> = HashMap::new();
        for chunk in &chunks {
            assert!(chunk.metric.is_none(), "key labels stay undecoded");
            assert!(!chunk.primary_key.is_empty());
            assert_eq!(2, chunk.fields.len());
            by_key
                .entry(chunk.primary_key.to_vec())
                .or_default()
                .push(chunk);
        }
        assert_eq!(2, by_key.len(), "each series keeps its own encoded key");
        let series_values = |group: &[&RawSeriesChunk], field: usize| {
            group
                .iter()
                .flat_map(|chunk| (0..chunk.time.len()).map(move |row| float_at(chunk, field, row)))
                .collect::<Vec<_>>()
        };
        let mut payloads = Vec::new();
        for group in by_key.values() {
            assert!(group.last().unwrap().is_last);
            assert_eq!(
                vec![1000, 2000],
                group
                    .iter()
                    .flat_map(|chunk| timestamps(chunk))
                    .collect::<Vec<_>>()
            );
            payloads.push((series_values(group, 0), series_values(group, 1)));
        }
        payloads.sort_by(|left, right| left.0[0].unwrap().total_cmp(&right.0[0].unwrap()));
        assert_eq!(
            vec![
                (vec![Some(1.0), Some(2.0)], vec![Some(10.0), Some(20.0)]),
                (vec![Some(3.0), Some(4.0)], vec![None, Some(40.0)]),
            ],
            payloads
        );

        fixture.stop().await;
    }

    #[tokio::test]
    async fn raw_series_handle_read_answers_empty_region_with_its_layout() {
        let fixture = RawSeriesFixture::new("raw-series-handle-read-empty", 2).await;
        let stream = fixture
            .server
            .handle_read(fixture.request(
                fixture.scan_plan("cpu_usage", None),
                RegionReadKind::RawSeries,
            ))
            .await
            .unwrap();
        let (fields, decode, ordering, chunks) =
            unpack_frames(stream, &["field_0", "field_1"]).await;

        assert_eq!(vec!["field_0".to_string(), "field_1".to_string()], fields);
        assert_eq!(SeriesOrdering::KeyThenTime, ordering);
        assert_eq!("cpu_usage", decode.metric_name);
        assert!(chunks.is_empty(), "an empty region frames no chunk");

        fixture.stop().await;
    }

    #[tokio::test]
    async fn raw_series_handle_read_applies_projection_order_and_pushed_predicate() {
        let fixture = RawSeriesFixture::new("raw-series-handle-read-projection", 2).await;
        fixture
            .put(
                "host-a",
                &[1, 2],
                &[(Some(1), Some(10)), (Some(2), Some(20))],
            )
            .await;
        fixture
            .put("host-b", &[1, 2], &[(Some(3), None), (Some(4), Some(40))])
            .await;

        // Read the unfiltered frames once to learn host-a's encoded key.
        let stream = fixture
            .server
            .handle_read(fixture.request(
                fixture.scan_plan("cpu_usage", None),
                RegionReadKind::RawSeries,
            ))
            .await
            .unwrap();
        let (_, _, _, all) = unpack_frames(stream, &["field_0", "field_1"]).await;
        let host_a_key = all
            .iter()
            .find(|chunk| float_at(chunk, 0, 0) == Some(1.0))
            .expect("host-a's unfiltered chunk")
            .primary_key
            .to_vec();

        // A reordered projection selects field_1 before field_0, and a pushed-down tag predicate
        // keeps only host-a, whose frame must still carry the same complete encoded key.
        let mut plan = fixture.scan_plan("cpu_usage", Some(vec![3, 2, 1, 0]));
        let LogicalPlan::TableScan(scan) = &mut plan else {
            panic!("the fixture builds a table scan");
        };
        scan.filters = vec![datafusion_expr::col("tag_0").eq(datafusion_expr::lit("host-a"))];
        let stream = fixture
            .server
            .handle_read(fixture.request(plan, RegionReadKind::RawSeries))
            .await
            .unwrap();
        let (fields, _, _, chunks) = unpack_frames(stream, &["field_1", "field_0"]).await;

        assert_eq!(vec!["field_1".to_string(), "field_0".to_string()], fields);
        assert!(!chunks.is_empty(), "the pushed predicate keeps one series");
        assert!(chunks.last().unwrap().is_last);
        for chunk in &chunks {
            assert_eq!(host_a_key, chunk.primary_key.to_vec());
        }
        let times = chunks.iter().flat_map(timestamps).collect::<Vec<_>>();
        let field_1 = chunks
            .iter()
            .flat_map(|chunk| (0..chunk.time.len()).map(move |row| float_at(chunk, 0, row)))
            .collect::<Vec<_>>();
        let field_0 = chunks
            .iter()
            .flat_map(|chunk| (0..chunk.time.len()).map(move |row| float_at(chunk, 1, row)))
            .collect::<Vec<_>>();
        assert_eq!(vec![1000, 2000], times);
        assert_eq!(vec![Some(10.0), Some(20.0)], field_1);
        assert_eq!(vec![Some(1.0), Some(2.0)], field_0);

        fixture.stop().await;
    }

    /// A configured factory can serve ordinary rows for the same table name, so a raw-series read
    /// must reject the answer by its declared shape instead of returning rows.
    #[tokio::test]
    async fn raw_series_read_rejects_a_configured_factory_that_answers_with_rows() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            true,
        )]));
        let values: VectorRef = Arc::new(Int32Vector::from_slice([42]));
        let batch = RecordBatch::new(schema.clone(), vec![values]).unwrap();
        let table = MemTable::try_new(
            schema.arrow_schema().clone(),
            vec![vec![batch.into_df_record_batch()]],
        )
        .unwrap();
        let fixture = RawSeriesFixture::with_factory(
            "raw-series-row-factory",
            2,
            Arc::new(RowsTableProviderFactory(Arc::new(table))),
        )
        .await;

        // `scan_plan`'s single Int32 column `v` is exactly the row table's schema.
        let plan = scan_plan("cpu");
        let Err(err) = fixture
            .server
            .handle_read(fixture.request(plan.clone(), RegionReadKind::RawSeries))
            .await
        else {
            panic!("a row answer must not satisfy a raw-series read");
        };
        assert!(
            err.to_string().contains("non-frame stream"),
            "unexpected rejection cause: {err}"
        );

        // The identical request as an ordinary read keeps the configured provider's row.
        let stream = fixture
            .server
            .handle_read(fixture.request(plan, RegionReadKind::Rows))
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(1, batches.iter().count());
        let batch = batches.iter().next().unwrap();
        assert_eq!(1, batch.num_rows());
        let column: &dyn Array = batch.column(0).as_ref();
        assert_eq!(
            42,
            column
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );

        fixture.stop().await;
    }

    /// A remote read is sanitized from its own context, never from the caller's extensions: even a
    /// context that already names a raw-series metric must receive ordinary rows.
    #[tokio::test]
    async fn remote_read_cannot_be_spoofed_into_raw_series() {
        let fixture = RawSeriesFixture::new("raw-series-remote-read", 2).await;
        fixture
            .put(
                "host-a",
                &[1, 2],
                &[(Some(1), Some(10)), (Some(2), Some(20))],
            )
            .await;
        fixture
            .put("host-b", &[1, 2], &[(Some(3), None), (Some(4), Some(40))])
            .await;

        let plan = DFLogicalSubstraitConvertor
            .encode(&fixture.scan_plan("cpu_usage", None), DefaultSerializer)
            .unwrap();
        let spoofed = Arc::new(
            QueryContextBuilder::default()
                .extensions(HashMap::from([(
                    RAW_SERIES_READ_EXTENSION_KEY.to_string(),
                    "spoofed_metric".to_string(),
                )]))
                .build(),
        );
        let stream = fixture
            .server
            .handle_remote_read(
                api::v1::region::QueryRequest {
                    header: None,
                    region_id: fixture.region_id.as_u64(),
                    plan: plan.to_vec(),
                },
                spoofed,
            )
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();

        let schema = batches.schema();
        assert_eq!(
            vec!["tag_0", "field_0", "field_1", "ts"],
            schema
                .column_schemas()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![
                ConcreteDataType::dictionary_datatype(
                    ConcreteDataType::uint32_datatype(),
                    ConcreteDataType::string_datatype(),
                ),
                ConcreteDataType::float64_datatype(),
                ConcreteDataType::float64_datatype(),
                ConcreteDataType::timestamp_millisecond_datatype(),
            ],
            schema
                .column_schemas()
                .iter()
                .map(|column| column.data_type.clone())
                .collect::<Vec<_>>()
        );
        assert!(
            schema
                .arrow_schema()
                .metadata()
                .keys()
                .all(|key| !key.starts_with("greptime.series.")),
            "a spoofed remote read must not answer with a raw frame"
        );
        let expected = "\
+--------+---------+---------+---------------------+
| tag_0  | field_0 | field_1 | ts                  |
+--------+---------+---------+---------------------+
| host-a | 1.0     | 10.0    | 1970-01-01T00:00:01 |
| host-a | 2.0     | 20.0    | 1970-01-01T00:00:02 |
| host-b | 3.0     |         | 1970-01-01T00:00:01 |
| host-b | 4.0     | 40.0    | 1970-01-01T00:00:02 |
+--------+---------+---------+---------------------+";
        assert_eq!(expected, batches.pretty_print().unwrap());

        fixture.stop().await;
    }

    #[test]
    fn raw_series_metric_is_the_table_scan_the_plan_is() {
        // An ordinary read names no metric, so the region keeps its row answer.
        assert_eq!(
            raw_series_metric_name(&query_request(scan_plan("cpu"), RegionReadKind::Rows)),
            None
        );
        // A raw read takes the metric from the scan it was asked to answer, which is the one part
        // of a frame no region holds.
        assert_eq!(
            raw_series_metric_name(&query_request(scan_plan("cpu"), RegionReadKind::RawSeries)),
            Some("cpu".to_string())
        );
    }

    #[test]
    fn raw_series_read_key_cannot_be_opted_into_by_a_request_header() {
        // A header's extensions reach the query context verbatim, so this is exactly what a caller
        // that named the key would have on the ordinary read path.
        let spoofed = QueryContextBuilder::default()
            .extensions(HashMap::from([(
                RAW_SERIES_READ_EXTENSION_KEY.to_string(),
                "spoofed_metric".to_string(),
            )]))
            .build();

        // An ordinary read overwrites it with the empty value, which the region's table provider
        // reads as "no metric": the answer is the same `RegionScanExec` it always was.
        let ctx = apply_raw_series_read_key(
            spoofed.clone(),
            &query_request(scan_plan("cpu"), RegionReadKind::Rows),
        );
        assert_eq!(ctx.extension(RAW_SERIES_READ_EXTENSION_KEY), Some(""));

        // A raw read overwrites it with the metric its own plan names, never the spoofed one.
        let ctx = apply_raw_series_read_key(
            spoofed,
            &query_request(scan_plan("cpu"), RegionReadKind::RawSeries),
        );
        assert_eq!(ctx.extension(RAW_SERIES_READ_EXTENSION_KEY), Some("cpu"));
    }

    #[test]
    fn remote_read_sanitizes_spoofed_raw_series_extension() {
        let spoofed = QueryContextBuilder::default()
            .extensions(HashMap::from([(
                RAW_SERIES_READ_EXTENSION_KEY.to_string(),
                "spoofed_metric".to_string(),
            )]))
            .build();
        let ctx = sanitize_remote_read_context(spoofed);
        assert_eq!(ctx.extension(RAW_SERIES_READ_EXTENSION_KEY), Some(""));
    }

    #[test]
    fn raw_series_request_rejects_an_unnamed_scan() {
        // An empty table name is also the table provider's "no metric" sentinel, so answering it
        // would silently downgrade a frame request to rows. The plan builder refuses an empty name,
        // so this is the hand-built request the check exists for.
        let mut plan = scan_plan("cpu");
        let LogicalPlan::TableScan(scan) = &mut plan else {
            panic!("the fixture is a table scan");
        };
        scan.table_name = datafusion_common::TableReference::bare("");
        let request = query_request(plan, RegionReadKind::RawSeries);
        let err = validate_raw_series_request(&request).unwrap_err();
        assert!(
            err.to_string().contains("named table scan"),
            "unexpected rejection cause: {err}"
        );
    }

    #[test]
    fn raw_series_request_rejects_a_plan_that_is_not_just_the_scan() {
        use datafusion_expr::LogicalPlanBuilder;

        let scan = scan_plan("cpu");
        let projected = LogicalPlanBuilder::from(scan)
            .project(vec![datafusion_expr::col("v")])
            .unwrap()
            .build()
            .unwrap();
        let request = query_request(projected, RegionReadKind::RawSeries);
        assert!(validate_raw_series_request(&request).is_err());
    }

    #[test]
    fn raw_series_request_rejects_every_inspection_table_name_case_insensitively() {
        for name in [
            ManifestSstEntry::reserved_table_name_for_inspection(),
            StorageSstEntry::reserved_table_name_for_inspection(),
            PuffinIndexMetaEntry::reserved_table_name_for_inspection(),
            RegionInfoEntry::reserved_table_name_for_inspection(),
        ] {
            for candidate in [name.to_string(), name.to_ascii_uppercase()] {
                let plan = scan_plan(&candidate);
                let request = query_request(plan.clone(), RegionReadKind::RawSeries);
                let err = validate_raw_series_request(&request).unwrap_err();
                assert!(
                    err.to_string().contains("inspection tables"),
                    "{candidate} must be rejected as an inspection table: {err}"
                );
                // The identical scan stays a valid ordinary row read.
                let request = query_request(plan, RegionReadKind::Rows);
                assert!(validate_raw_series_request(&request).is_ok());
            }
        }
    }

    #[test]
    fn raw_series_request_rejects_a_scan_that_already_limits_its_rows() {
        // A pushed-down fetch cannot be honoured by a frame leaf and is rejected.
        let mut limited = scan_plan("cpu");
        let LogicalPlan::TableScan(scan) = &mut limited else {
            panic!("the fixture is a table scan");
        };
        scan.fetch = Some(1);
        let request = query_request(limited, RegionReadKind::RawSeries);
        assert!(validate_raw_series_request(&request).is_err());
    }
}
