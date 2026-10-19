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

//! impl `FlowNode` trait for FlowNodeManager so standalone can call them
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use api::v1::flow::{
    CreateRequest, DirtyWindowRequests, DropRequest, FlowRequest, FlowResponse, FlushFlow,
    flow_request,
};
use api::v1::region::InsertRequests;
use catalog::CatalogManager;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_meta::ddl::create_flow::{
    FlowType, INTERNAL_EVAL_SCHEDULE_KEY, effective_eval_schedule_from_flow_info,
};
use common_meta::error::Result as MetaResult;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::flow::flow_info::FlowScheduleConfig;
use common_meta::key::flow::flow_state::FlowStat;
use common_runtime::JoinHandle;
use common_telemetry::{error, info, warn};
use futures::TryStreamExt;
use itertools::Itertools;
use operator::utils::try_to_session_query_context;
use session::context::QueryContextBuilder;
use snafu::{OptionExt, ResultExt, ensure};
use tokio::sync::{Mutex, RwLock};

use crate::batching_mode::engine::BatchingEngine;
use crate::engine::{FlowEngine, FlowStatProvider};
use crate::error::{
    CreateFlowSnafu, ExternalSnafu, FlowNotFoundSnafu, FlowNotRecoveredSnafu,
    IllegalCheckTaskStateSnafu, InternalSnafu, ListFlowsSnafu, NoAvailableFrontendSnafu,
    SyncCheckTaskSnafu, UnsupportedSnafu,
};
use crate::metrics::{METRIC_FLOW_ROWS, METRIC_FLOW_TASK_COUNT};
use crate::utils::StateReportHandler;
use crate::{CreateFlowArgs, Error, FlowId};

/// Ref to [`FlownodeEngine`]
pub type FlownodeEngineRef = Arc<FlownodeEngine>;

/// Manage the batching flow engine
///
/// including create/drop/flush flow
/// and redirect insert requests to the batching engine
pub struct FlownodeEngine {
    batching_engine: Arc<BatchingEngine>,
    /// receive a oneshot sender to send state report
    state_report_handler: RwLock<Option<StateReportHandler>>,
    flow_metadata_manager: Arc<FlowMetadataManager>,
    catalog_manager: Arc<dyn CatalogManager>,
    check_task: tokio::sync::Mutex<Option<ConsistentCheckTask>>,
    plugins: Plugins,
    /// `Some` in distributed mode, `None` in standalone mode
    node_id: Option<u32>,
    done_recovering: AtomicBool,
}

impl FlownodeEngine {
    pub fn new(
        node_id: Option<u32>,
        batching_engine: Arc<BatchingEngine>,
        flow_metadata_manager: Arc<FlowMetadataManager>,
        catalog_manager: Arc<dyn CatalogManager>,
        plugins: Plugins,
    ) -> Self {
        Self {
            batching_engine,
            state_report_handler: Default::default(),
            flow_metadata_manager,
            catalog_manager,
            check_task: Mutex::new(None),
            plugins,
            node_id,
            done_recovering: AtomicBool::new(false),
        }
    }

    /// Set `done_recovering` to true
    /// indicate that we are ready to handle requests
    pub fn set_done_recovering(&self) {
        info!("FlownodeEngine done recovering");
        self.done_recovering
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Check if `done_recovering` is true
    pub fn is_recover_done(&self) -> bool {
        self.done_recovering
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// wait for recovering to be done, this will only happen when flownode just started
    async fn wait_for_all_flow_recover(&self, waiting_req_cnt: usize) -> Result<(), Error> {
        if self.is_recover_done() {
            return Ok(());
        }

        warn!(
            "FlownodeEngine is not done recovering, {} insert request waiting for recovery",
            waiting_req_cnt
        );
        // wait 3 seconds, check every 1 second
        // TODO(discord9): make this configurable
        let mut retry = 0;
        let max_retry = 3;
        while retry < max_retry && !self.is_recover_done() {
            warn!(
                "FlownodeEngine is not done recovering, retry {} in 1s",
                retry
            );
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            retry += 1;
        }
        if retry == max_retry {
            return FlowNotRecoveredSnafu.fail();
        } else {
            info!("FlownodeEngine is done recovering");
        }
        // TODO(discord9): also put to centralized logging for flow once it implemented
        Ok(())
    }

    pub fn plugins(&self) -> &Plugins {
        &self.plugins
    }

    /// Determine if the engine is in distributed mode
    pub fn is_distributed(&self) -> bool {
        self.node_id.is_some()
    }

    pub fn batching_engine(&self) -> Arc<BatchingEngine> {
        self.batching_engine.clone()
    }

    pub async fn set_state_report_handler(&self, handler: StateReportHandler) {
        *self.state_report_handler.write().await = Some(handler);
    }

    pub async fn gen_state_report(&self) -> FlowStat {
        self.batching_engine.flow_stat().await
    }

    /// Start state report task, which receives a sender from heartbeat task and sends report back.
    ///
    /// if heartbeat task is shutdown, this future exits too.
    pub async fn start_state_report_task(self: Arc<Self>) -> Option<JoinHandle<()>> {
        let state_report_handler = self.state_report_handler.write().await.take();
        if let Some(mut handler) = state_report_handler {
            let zelf = self.clone();
            let handler = common_runtime::spawn_global(async move {
                while let Some(ret_handler) = handler.recv().await {
                    let state_report = zelf.gen_state_report().await;
                    ret_handler.send(state_report).unwrap_or_else(|err| {
                        common_telemetry::error!(err; "Send state report error");
                    });
                }
            });
            Some(handler)
        } else {
            None
        }
    }

    /// In distributed mode, scan periodically(1s) until all advertised frontends
    /// accept unauthenticated queries, or timeout. In standalone mode, return
    /// immediately.
    async fn wait_for_available_frontend(&self, timeout: std::time::Duration) -> Result<(), Error> {
        if !self.is_distributed() {
            return Ok(());
        }
        let frontend_client = self.batching_engine().frontend_client.clone();
        let sleep_duration = std::time::Duration::from_millis(1_000);
        let now = std::time::Instant::now();
        loop {
            let frontend_list = frontend_client.scan_for_frontend().await?;
            if !frontend_list.is_empty() {
                let fe_list = frontend_list
                    .iter()
                    .map(|peer| &peer.addr)
                    .collect::<Vec<_>>();
                let probe_failures = frontend_client
                    .check_all_frontends_without_auth(&frontend_list)
                    .await?;
                if probe_failures.is_empty() {
                    info!(
                        "Available frontend found and unauthenticated probe succeeded: {:?}",
                        fe_list
                    );
                    return Ok(());
                }
                warn!(
                    "Unauthenticated frontend probe failed, will retry. frontends={:?}, failures={:?}",
                    fe_list, probe_failures
                );
            }
            let elapsed = now.elapsed();
            tokio::time::sleep(sleep_duration).await;
            info!("Waiting for available frontend, elapsed={:?}", elapsed);
            if elapsed >= timeout {
                return NoAvailableFrontendSnafu {
                    timeout,
                    context: "No frontend accepted unauthenticated flownode probe",
                }
                .fail();
            }
        }
    }

    /// Try to sync with check task, this is only used in drop flow&flush flow, so a flow id is required
    ///
    /// the need to sync is to make sure flush flow actually get called
    async fn try_sync_with_check_task(
        &self,
        flow_id: FlowId,
        allow_drop: bool,
    ) -> Result<(), Error> {
        // this function rarely get called so adding some log is helpful
        info!("Try to sync with check task for flow {}", flow_id);
        let mut retry = 0;
        let max_retry = 10;
        // keep trying to trigger consistent check
        while retry < max_retry {
            if let Some(task) = self.check_task.lock().await.as_ref() {
                task.trigger(false, allow_drop).await?;
                break;
            }
            retry += 1;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        if retry == max_retry {
            error!(
                "Can't sync with check task for flow {} with allow_drop={}",
                flow_id, allow_drop
            );
            return SyncCheckTaskSnafu {
                flow_id,
                allow_drop,
            }
            .fail();
        }
        info!("Successfully sync with check task for flow {}", flow_id);

        Ok(())
    }

    /// Spawn a task to consistently check if all flow tasks in metasrv is created on flownode,
    /// so on startup, this will create all missing flow tasks, and constantly check at a interval
    async fn check_flow_consistent(
        &self,
        allow_create: bool,
        allow_drop: bool,
    ) -> Result<(), Error> {
        // use nodeid to determine if this is standalone/distributed mode, and retrieve all flows in this node(in distributed mode)/or all flows(in standalone mode)
        let nodeid = self.node_id;
        let should_exists: Vec<_> = if let Some(nodeid) = nodeid {
            // nodeid is available, so we only need to check flows on this node
            // which also means we are in distributed mode
            let to_be_recover = self
                .flow_metadata_manager
                .flownode_flow_manager()
                .flows(nodeid.into())
                .try_collect::<Vec<_>>()
                .await
                .context(ListFlowsSnafu {
                    id: Some(nodeid.into()),
                })?;
            to_be_recover.into_iter().map(|(id, _)| id).collect()
        } else {
            // nodeid is not available, so we need to check all flows
            // which also means we are in standalone mode
            let all_catalogs = self
                .catalog_manager
                .catalog_names()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            let mut all_flow_ids = vec![];
            for catalog in all_catalogs {
                let flows = self
                    .flow_metadata_manager
                    .flow_name_manager()
                    .flow_names(&catalog)
                    .await
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;

                all_flow_ids.extend(flows.into_iter().map(|(_, id)| id.flow_id()));
            }
            all_flow_ids
        };
        let should_exists = should_exists
            .into_iter()
            .map(|i| i as FlowId)
            .collect::<HashSet<_>>();
        let actual_exists = self.list_flows().await?.into_iter().collect::<HashSet<_>>();
        let to_be_created = should_exists
            .iter()
            .filter(|id| !actual_exists.contains(id))
            .collect::<Vec<_>>();
        let to_be_dropped = actual_exists
            .iter()
            .filter(|id| !should_exists.contains(id))
            .collect::<Vec<_>>();

        if !to_be_created.is_empty() {
            if allow_create {
                info!(
                    "Recovering {} flows: {:?}",
                    to_be_created.len(),
                    to_be_created
                );
                let mut errors = vec![];
                for flow_id in to_be_created.clone() {
                    let flow_id = *flow_id;
                    let info = self
                        .flow_metadata_manager
                        .flow_info_manager()
                        .get(flow_id as u32)
                        .await
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?
                        .context(FlowNotFoundSnafu { id: flow_id })?;

                    let sink_table_name = [
                        info.sink_table_name().catalog_name.clone(),
                        info.sink_table_name().schema_name.clone(),
                        info.sink_table_name().table_name.clone(),
                    ];
                    let args = CreateFlowArgs {
                        flow_id,
                        sink_table_name,
                        source_table_ids: info.source_table_ids().to_vec(),
                        // because recover should only happen on restart the `create_if_not_exists` and `or_replace` can be arbitrary value(since flow doesn't exist)
                        // but for the sake of consistency and to make sure recover of flow actually happen, we set both to true
                        // (which is also fine since checks for not allow both to be true is on metasrv and we already pass that)
                        create_if_not_exists: true,
                        or_replace: true,
                        expire_after: info.expire_after(),
                        eval_interval: info.eval_interval(),
                        comment: Some(info.comment().clone()),
                        sql: info.raw_sql().clone(),
                        flow_options: info.options().clone(),
                        eval_schedule: effective_eval_schedule_from_flow_info(&info)
                            .map_err(BoxedError::new)
                            .context(ExternalSnafu)?,
                        query_ctx: info
                            .query_context()
                            .clone()
                            .map(|ctx| {
                                try_to_session_query_context(ctx)
                                    .map_err(BoxedError::new)
                                    .context(ExternalSnafu)
                            })
                            .transpose()?
                            // or use default QueryContext with catalog_name from info
                            // to keep compatibility with old version
                            .or_else(|| {
                                Some(
                                    QueryContextBuilder::default()
                                        .current_catalog(info.catalog_name().clone())
                                        .build(),
                                )
                            }),
                    };
                    if let Err(err) = self
                        .create_flow(args)
                        .await
                        .map_err(BoxedError::new)
                        .with_context(|_| CreateFlowSnafu {
                            sql: info.raw_sql().clone(),
                        })
                    {
                        errors.push((flow_id, err));
                    }
                }
                if errors.is_empty() {
                    info!("Recover flows successfully, flows: {:?}", to_be_created);
                }

                for (flow_id, err) in errors {
                    warn!("Failed to recreate flow {}, err={:#?}", flow_id, err);
                }
            } else {
                warn!(
                    "Flows do not exist in flownode for node {:?}, flow_ids={:?}",
                    nodeid, to_be_created
                );
            }
        }
        if !to_be_dropped.is_empty() {
            if allow_drop {
                info!("Dropping flows: {:?}", to_be_dropped);
                let mut errors = vec![];
                for flow_id in to_be_dropped {
                    let flow_id = *flow_id;
                    if let Err(err) = self.remove_flow(flow_id).await {
                        errors.push((flow_id, err));
                    }
                }
                for (flow_id, err) in errors {
                    warn!("Failed to drop flow {}, err={:#?}", flow_id, err);
                }
            } else {
                warn!(
                    "Flows do not exist in metadata for node {:?}, flow_ids={:?}",
                    nodeid, to_be_dropped
                );
            }
        }
        Ok(())
    }

    // TODO(discord9): consider sync this with heartbeat(might become necessary in the future)
    pub async fn start_flow_consistent_check_task(self: &Arc<Self>) -> Result<(), Error> {
        let mut check_task = self.check_task.lock().await;
        ensure!(
            check_task.is_none(),
            IllegalCheckTaskStateSnafu {
                reason: "Flow consistent check task already exists",
            }
        );
        let task = ConsistentCheckTask::start_check_task(self).await?;
        *check_task = Some(task);
        Ok(())
    }

    pub async fn stop_flow_consistent_check_task(&self) -> Result<(), Error> {
        info!("Stopping flow consistent check task");
        let mut check_task = self.check_task.lock().await;

        ensure!(
            check_task.is_some(),
            IllegalCheckTaskStateSnafu {
                reason: "Flow consistent check task does not exist",
            }
        );

        check_task.take().unwrap().stop().await?;
        info!("Stopped flow consistent check task");
        Ok(())
    }

    /// Reconciles in-memory flow tasks from persisted metadata.
    pub async fn reconcile_flows_from_metadata(&self) -> Result<(), Error> {
        self.check_flow_consistent(true, true).await
    }

    /// TODO(discord9): also add a `exists` api using flow metadata manager's `exists` method
    async fn flow_exist_in_metadata(&self, flow_id: FlowId) -> Result<bool, Error> {
        self.flow_metadata_manager
            .flow_info_manager()
            .get(flow_id as u32)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
            .map(|info| info.is_some())
    }
}

struct ConsistentCheckTask {
    handle: JoinHandle<()>,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    trigger_tx: tokio::sync::mpsc::Sender<(bool, bool, tokio::sync::oneshot::Sender<()>)>,
}

impl ConsistentCheckTask {
    async fn start_check_task(engine: &Arc<FlownodeEngine>) -> Result<Self, Error> {
        let engine = engine.clone();
        let min_refresh_duration = engine
            .batching_engine()
            .batch_opts
            .experimental_min_refresh_duration;
        let frontend_scan_timeout = engine
            .batching_engine()
            .batch_opts
            .experimental_frontend_scan_timeout;
        engine
            .wait_for_available_frontend(frontend_scan_timeout)
            .await?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let (trigger_tx, mut trigger_rx) =
            tokio::sync::mpsc::channel::<(bool, bool, tokio::sync::oneshot::Sender<()>)>(10);
        let handle = common_runtime::spawn_global(async move {
            // Recover flows after the startup frontend probe succeeds.
            let mut recover_retry = 0;
            while let Err(err) = engine.check_flow_consistent(true, false).await {
                recover_retry += 1;
                error!(
                    "Failed to recover flows:\n {err:?}, retry {} in {}s",
                    recover_retry,
                    min_refresh_duration.as_secs()
                );
                tokio::time::sleep(min_refresh_duration).await;
            }

            engine.set_done_recovering();

            // then do check flows, with configurable allow_create and allow_drop
            let (mut allow_create, mut allow_drop) = (false, false);
            let mut ret_signal: Option<tokio::sync::oneshot::Sender<()>> = None;
            loop {
                if let Err(err) = engine.check_flow_consistent(allow_create, allow_drop).await {
                    error!(err; "Failed to check flow consistent");
                }
                if let Some(done) = ret_signal.take() {
                    let _ = done.send(());
                }
                tokio::select! {
                    _ = rx.recv() => break,
                    incoming = trigger_rx.recv() => if let Some(incoming) = incoming {
                        (allow_create, allow_drop) = (incoming.0, incoming.1);
                        ret_signal = Some(incoming.2);
                    },
                    _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                        (allow_create, allow_drop) = (false, false);
                    },
                }
            }
        });
        Ok(ConsistentCheckTask {
            handle,
            shutdown_tx: tx,
            trigger_tx,
        })
    }

    async fn trigger(&self, allow_create: bool, allow_drop: bool) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.trigger_tx
            .send((allow_create, allow_drop, tx))
            .await
            .map_err(|_| {
                IllegalCheckTaskStateSnafu {
                    reason: "Failed to send trigger signal",
                }
                .build()
            })?;
        rx.await.map_err(|_| {
            IllegalCheckTaskStateSnafu {
                reason: "Failed to receive trigger signal",
            }
            .build()
        })?;
        Ok(())
    }

    async fn stop(self) -> Result<(), Error> {
        self.shutdown_tx.send(()).await.map_err(|_| {
            IllegalCheckTaskStateSnafu {
                reason: "Failed to send shutdown signal",
            }
            .build()
        })?;
        // abort so no need to wait
        self.handle.abort();
        Ok(())
    }
}

impl FlowEngine for FlownodeEngine {
    async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        if let Some(flow_type) = args.flow_options.get(FlowType::FLOW_TYPE_KEY) {
            match flow_type.as_str() {
                FlowType::BATCHING => {}
                FlowType::STREAMING => {
                    return UnsupportedSnafu {
                        reason: format!(
                            "Flow {} was created with the legacy streaming flow type, which is no longer supported; drop and recreate the flow so it runs with the batching scheduler",
                            args.flow_id
                        ),
                    }
                    .fail();
                }
                other => {
                    return UnsupportedSnafu {
                        reason: format!(
                            "Flow {} has unknown flow type `{other}`; only batching flows are supported, drop and recreate the flow",
                            args.flow_id
                        ),
                    }
                    .fail();
                }
            }
        }

        self.batching_engine.create_flow(args).await
    }

    async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        if !self.batching_engine.flow_exist(flow_id).await? {
            // this can happen if flownode just restart, and is stilling creating the flow
            // since now that this flow should dropped, we need to trigger the consistent check and allow drop
            // this rely on drop flow ddl delete metadata first, see src/common/meta/src/ddl/drop_flow.rs
            warn!(
                "Flow {} is not exist in the underlying engine, but exist in metadata",
                flow_id
            );
            self.try_sync_with_check_task(flow_id, true).await?;

            return Ok(());
        }

        self.batching_engine.remove_flow(flow_id).await
    }

    async fn flush_flow(&self, flow_id: FlowId) -> Result<usize, Error> {
        // sync with check task
        self.try_sync_with_check_task(flow_id, false).await?;
        if !self.batching_engine.flow_exist(flow_id).await? {
            warn!("Currently flow={flow_id} doesn't exist in flownode, ignore flush_flow request");
            return Ok(0);
        }

        self.batching_engine.flush_flow(flow_id).await
    }

    async fn flow_exist(&self, flow_id: FlowId) -> Result<bool, Error> {
        self.batching_engine.flow_exist(flow_id).await
    }

    async fn list_flows(&self) -> Result<impl IntoIterator<Item = FlowId>, Error> {
        self.batching_engine.list_flows().await
    }

    /// Handle mirrored inserts: the batching engine uses them to mark dirty windows, so this
    /// path must be kept even though there is no streaming executor anymore.
    async fn handle_flow_inserts(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<(), Error> {
        self.wait_for_all_flow_recover(request.requests.len())
            .await?;
        let row_cnt = request
            .requests
            .iter()
            .map(|req| req.rows.as_ref().map(|rows| rows.rows.len()).unwrap_or(0))
            .sum::<usize>();
        METRIC_FLOW_ROWS
            .with_label_values(&["in-batching"])
            .inc_by(row_cnt as u64);

        self.batching_engine.handle_flow_inserts(request).await
    }

    async fn handle_mark_window_dirty(
        &self,
        req: api::v1::flow::DirtyWindowRequests,
    ) -> Result<(), Error> {
        self.batching_engine.handle_mark_window_dirty(req).await
    }
}

#[async_trait::async_trait]
impl common_meta::node_manager::Flownode for FlownodeEngine {
    async fn handle(&self, request: FlowRequest) -> MetaResult<FlowResponse> {
        let query_ctx = request
            .header
            .and_then(|h| h.query_context)
            .map(session::context::QueryContext::try_from)
            .transpose()
            .map_err(|err| {
                to_meta_err(snafu::location!())(
                    crate::error::InvalidQuerySnafu {
                        reason: err.to_string(),
                    }
                    .build(),
                )
            })?;
        match request.body {
            Some(flow_request::Body::Create(CreateRequest {
                flow_id: Some(task_id),
                source_table_ids,
                sink_table_name: Some(sink_table_name),
                create_if_not_exists,
                expire_after,
                eval_interval,
                comment,
                sql,
                mut flow_options,
                or_replace,
            })) => {
                let source_table_ids = source_table_ids.into_iter().map(|id| id.id).collect_vec();
                let sink_table_name = [
                    sink_table_name.catalog_name,
                    sink_table_name.schema_name,
                    sink_table_name.table_name,
                ];
                let expire_after = expire_after.map(|e| e.value);

                let eval_schedule = decode_internal_eval_schedule(&mut flow_options)
                    .map_err(to_meta_err(snafu::location!()))?;

                let args = CreateFlowArgs {
                    flow_id: task_id.id as u64,
                    sink_table_name,
                    source_table_ids,
                    create_if_not_exists,
                    or_replace,
                    expire_after,
                    eval_interval: eval_interval.map(|e| e.seconds),
                    comment: Some(comment),
                    sql: sql.clone(),
                    flow_options,
                    query_ctx,
                    eval_schedule,
                };
                let ret = self
                    .create_flow(args)
                    .await
                    .map_err(BoxedError::new)
                    .with_context(|_| CreateFlowSnafu { sql: sql.clone() })
                    .map_err(to_meta_err(snafu::location!()))?;
                METRIC_FLOW_TASK_COUNT.inc();
                Ok(FlowResponse {
                    affected_flows: ret
                        .map(|id| greptime_proto::v1::FlowId { id: id as u32 })
                        .into_iter()
                        .collect_vec(),
                    ..Default::default()
                })
            }
            Some(flow_request::Body::Drop(DropRequest {
                flow_id: Some(flow_id),
            })) => {
                self.remove_flow(flow_id.id as u64)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                METRIC_FLOW_TASK_COUNT.dec();
                Ok(Default::default())
            }
            Some(flow_request::Body::Flush(FlushFlow {
                flow_id: Some(flow_id),
            })) => {
                let row = self
                    .flush_flow(flow_id.id as u64)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                Ok(FlowResponse {
                    affected_flows: vec![flow_id],
                    affected_rows: row as u64,
                    ..Default::default()
                })
            }
            other => common_meta::error::InvalidFlowRequestBodySnafu { body: other }.fail(),
        }
    }

    async fn handle_inserts(&self, request: InsertRequests) -> MetaResult<FlowResponse> {
        FlowEngine::handle_flow_inserts(self, request)
            .await
            .map(|_| Default::default())
            .map_err(to_meta_err(snafu::location!()))
    }

    async fn handle_mark_window_dirty(&self, req: DirtyWindowRequests) -> MetaResult<FlowResponse> {
        self.batching_engine()
            .handle_mark_dirty_time_window(req)
            .await
            .map(|_| FlowResponse::default())
            .map_err(to_meta_err(snafu::location!()))
    }
}

/// Decode typed schedule config from the internal transient key emitted by metasrv.
/// Malformed JSON is an internal error rather than a reason to silently fall back.
fn decode_internal_eval_schedule(
    flow_options: &mut HashMap<String, String>,
) -> Result<Option<FlowScheduleConfig>, Error> {
    match flow_options.remove(INTERNAL_EVAL_SCHEDULE_KEY) {
        Some(json) => serde_json::from_str::<FlowScheduleConfig>(&json)
            .map(Some)
            .map_err(|err| {
                InternalSnafu {
                    reason: format!("Invalid internal eval schedule payload: {err}"),
                }
                .build()
            }),
        None => Ok(None),
    }
}

/// return a function to convert `crate::error::Error` to `common_meta::error::Error`
fn to_meta_err(
    location: snafu::Location,
) -> impl FnOnce(crate::error::Error) -> common_meta::error::Error {
    move |err: crate::error::Error| -> common_meta::error::Error {
        match err {
            crate::error::Error::FlowNotFound { id, .. } => {
                common_meta::error::Error::FlowNotFound {
                    flow_name: format!("flow_id={id}"),
                    location,
                }
            }
            _ => common_meta::error::Error::External {
                location,
                source: BoxedError::new(err),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use catalog::memory::new_memory_catalog_manager;
    use common_base::Plugins;
    use common_meta::ddl::create_flow::INTERNAL_EVAL_SCHEDULE_KEY;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::flow::FlowMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use query::options::QueryOptions;

    use super::{FlownodeEngine, decode_internal_eval_schedule};
    use crate::CreateFlowArgs;
    use crate::batching_mode::BatchingModeOptions;
    use crate::batching_mode::engine::BatchingEngine;
    use crate::batching_mode::frontend_client::FrontendClient;
    use crate::engine::FlowEngine;
    use crate::error::Error;

    async fn new_test_engine() -> FlownodeEngine {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_meta = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        table_meta.init().await.unwrap();
        let flow_meta = Arc::new(FlowMetadataManager::new(kv_backend));
        let catalog_manager = new_memory_catalog_manager().unwrap();
        let query_engine = crate::test_utils::create_test_query_engine();
        let (frontend_client, _handler) =
            FrontendClient::from_empty_grpc_handler(QueryOptions::default());
        let batching = Arc::new(BatchingEngine::new(
            Arc::new(frontend_client),
            query_engine,
            flow_meta.clone(),
            table_meta,
            catalog_manager.clone(),
            BatchingModeOptions::default(),
        ));

        FlownodeEngine::new(None, batching, flow_meta, catalog_manager, Plugins::new())
    }

    fn create_flow_args(flow_type: Option<&str>) -> CreateFlowArgs {
        let mut flow_options = HashMap::new();
        if let Some(flow_type) = flow_type {
            flow_options.insert("flow_type".to_string(), flow_type.to_string());
        }

        CreateFlowArgs {
            flow_id: 1024,
            sink_table_name: [
                "greptime_public".to_string(),
                "public".to_string(),
                "old_sink".to_string(),
            ],
            source_table_ids: vec![],
            create_if_not_exists: false,
            or_replace: false,
            expire_after: None,
            eval_interval: Some(60),
            comment: None,
            sql: "SELECT 1".to_string(),
            flow_options,
            query_ctx: None,
            eval_schedule: None,
        }
    }

    #[tokio::test]
    async fn test_old_streaming_metadata_asks_for_recreation() {
        let engine = new_test_engine().await;

        // A flow persisted by the removed streaming executor must not run as batching: the
        // recovery path goes through `create_flow`, so it reports that the flow needs recreation
        // instead of silently executing different SQL semantics.
        let err = engine
            .create_flow(create_flow_args(Some("streaming")))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported { .. }),
            "unexpected error: {err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("no longer supported"), "unexpected: {msg}");
        assert!(msg.contains("recreate"), "unexpected: {msg}");
        assert!(!engine.flow_exist(1024).await.unwrap());

        // `batching` and absent `flow_type` still reach the batching engine.
        for flow_type in [Some("batching"), None] {
            let err = engine.create_flow(create_flow_args(flow_type)).await;
            let msg = err.err().map(|err| err.to_string()).unwrap_or_default();
            assert!(!msg.contains("no longer supported"), "unexpected: {msg}");
        }
    }

    #[test]
    fn test_malformed_internal_eval_schedule_json_is_error() {
        let mut flow_options = HashMap::new();
        flow_options.insert(
            INTERNAL_EVAL_SCHEDULE_KEY.to_string(),
            "not-json".to_string(),
        );

        let err = decode_internal_eval_schedule(&mut flow_options).unwrap_err();
        assert!(matches!(
            err,
            Error::Internal { reason, .. }
                if reason.contains("Invalid internal eval schedule payload")
        ));
        assert!(!flow_options.contains_key(INTERNAL_EVAL_SCHEDULE_KEY));
    }
}
