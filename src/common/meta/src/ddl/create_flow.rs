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

mod metadata;

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use api::v1::ExpireAfter;
use api::v1::flow::flow_request::Body as PbFlowRequest;
use api::v1::flow::{CreateRequest, FlowRequest, FlowRequestHeader};
use async_trait::async_trait;
use common_catalog::format_full_flow_name;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::info;
use common_telemetry::tracing_context::TracingContext;
use futures::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use strum::AsRefStr;
use table::metadata::TableId;
use table::table_name::TableName;

use crate::cache_invalidator::Context;
use crate::ddl::DdlContext;
use crate::ddl::utils::{add_peer_context_if_needed, map_to_procedure_error};
use crate::error::{self, Result, UnexpectedSnafu};
use crate::instruction::{CacheIdent, CreateFlow, DropFlow};
use crate::key::flow::flow_info::{FlowInfoValue, FlowScheduleConfig, FlowStatus};
use crate::key::flow::flow_route::FlowRouteValue;
use crate::key::table_name::TableNameKey;
use crate::key::{DeserializedValueWithBytes, FlowId, FlowPartitionId};
use crate::lock_key::{CatalogLock, FlowNameLock};
use crate::metrics;
use crate::peer::Peer;
use crate::rpc::ddl::{CreateFlowTask, FlowQueryContext, QueryContext};

/// The procedure of flow creation.
pub struct CreateFlowProcedure {
    pub context: DdlContext,
    pub data: CreateFlowData,
}

impl CreateFlowProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateFlow";

    /// Returns a new [CreateFlowProcedure].
    pub fn new(task: CreateFlowTask, query_context: QueryContext, context: DdlContext) -> Self {
        Self {
            context,
            data: CreateFlowData {
                task,
                flow_id: None,
                peers: vec![],
                source_table_ids: vec![],
                unresolved_source_table_names: vec![],
                flow_context: query_context.into(), // Convert to FlowQueryContext
                state: CreateFlowState::Prepare,
                prev_flow_info_value: None,
                did_replace: false,
                flow_type: None,
            },
        }
    }

    /// Deserializes from `json`.
    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(CreateFlowProcedure { context, data })
    }

    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let catalog_name = &self.data.task.catalog_name;
        let flow_name = &self.data.task.flow_name;
        let sink_table_name = &self.data.task.sink_table_name;
        let create_if_not_exists = self.data.task.create_if_not_exists;
        let or_replace = self.data.task.or_replace;

        validate_flow_options(&self.data.task)?;

        let flow_name_value = self
            .context
            .flow_metadata_manager
            .flow_name_manager()
            .get(catalog_name, flow_name)
            .await?;

        if create_if_not_exists && or_replace {
            // this is forbidden because not clear what does that mean exactly
            return error::UnsupportedSnafu {
                operation: "Create flow with both `IF NOT EXISTS` and `OR REPLACE`",
            }
            .fail();
        }

        if let Some(value) = flow_name_value {
            ensure!(
                create_if_not_exists || or_replace,
                error::FlowAlreadyExistsSnafu {
                    flow_name: format_full_flow_name(catalog_name, flow_name),
                }
            );

            let flow_id = value.flow_id();
            if create_if_not_exists {
                info!("Flow already exists, flow_id: {}", flow_id);
                return Ok(Status::done_with_output(flow_id));
            }

            let flow_id = value.flow_id();
            let peers = self
                .context
                .flow_metadata_manager
                .flow_route_manager()
                .routes(flow_id)
                .await?
                .into_iter()
                .map(|(_, value)| value.peer)
                .collect::<Vec<_>>();
            self.data.flow_id = Some(flow_id);
            self.data.peers = peers;
            info!("Replacing flow, flow_id: {}", flow_id);

            let flow_info_value = self
                .context
                .flow_metadata_manager
                .flow_info_manager()
                .get_raw(flow_id)
                .await?;

            ensure!(
                flow_info_value.is_some(),
                error::FlowNotFoundSnafu {
                    flow_name: format_full_flow_name(catalog_name, flow_name),
                }
            );

            self.data.prev_flow_info_value = flow_info_value;
        }

        //  Ensures sink table doesn't exist.
        let exists = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                &sink_table_name.catalog_name,
                &sink_table_name.schema_name,
                &sink_table_name.table_name,
            ))
            .await?;
        // TODO(discord9): due to undefined behavior in flow's plan in how to transform types in mfp, sometime flow can't deduce correct schema
        // and require manually create sink table
        if exists {
            common_telemetry::warn!("Table already exists, table: {}", sink_table_name);
        }

        self.collect_source_tables().await?;
        ensure!(
            self.data.unresolved_source_table_names.is_empty()
                || defer_on_missing_source(&self.data.task)?,
            error::UnsupportedSnafu {
                operation: format!(
                    "Create flow with missing source tables requires WITH ('{DEFER_ON_MISSING_SOURCE_KEY}'='true'): {}",
                    self.data
                        .unresolved_source_table_names
                        .iter()
                        .map(ToString::to_string)
                        .join(", ")
                )
            }
        );
        self.ensure_supported_replace_transition()?;

        // Validate that source and sink tables are not the same
        let sink_table_name = &self.data.task.sink_table_name;
        if self
            .data
            .task
            .source_table_names
            .iter()
            .any(|source| source == sink_table_name)
        {
            return error::UnsupportedSnafu {
                operation: format!(
                    "Creating flow with source and sink table being the same: {}",
                    sink_table_name
                ),
            }
            .fail();
        }

        if self.data.flow_id.is_none() {
            self.allocate_flow_id().await?;
        }
        self.data.flow_type = Some(get_flow_type_from_options(&self.data.task)?);

        // Resolve schedule defaults into task.eval_schedule once, before
        // CreateRequest / FlowInfoValue are constructed. This ensures the same
        // typed schedule config is sent to flownodes and persisted in metadata.
        // Idempotent: if eval_schedule is already present we do not recompute.
        resolve_schedule_defaults_into_task(
            &mut self.data.task,
            self.data
                .prev_flow_info_value
                .as_ref()
                .map(|v| v.get_inner_ref()),
        );

        self.data.state = if self.data.is_pending() {
            self.data.peers.clear();
            CreateFlowState::CreateMetadata
        } else {
            CreateFlowState::CreateFlows
        };

        Ok(Status::executing(true))
    }

    fn ensure_supported_replace_transition(&self) -> Result<()> {
        if !self.data.task.or_replace {
            return Ok(());
        }

        let Some(prev_flow_info) = self.data.prev_flow_info_value.as_ref() else {
            return Ok(());
        };
        let prev_pending = prev_flow_info.get_inner_ref().is_pending();
        let new_pending = self.data.is_pending();
        ensure!(
            prev_pending == new_pending,
            error::UnsupportedSnafu {
                operation: "Replacing between pending and active flow states is not supported yet"
            }
        );

        Ok(())
    }

    async fn on_flownode_create_flows(&mut self) -> Result<Status> {
        // Safety: must be allocated.
        let mut create_flow = Vec::with_capacity(self.data.peers.len());
        for peer in &self.data.peers {
            let requester = self.context.node_manager.flownode(peer).await;
            let request = FlowRequest {
                header: Some(FlowRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    // Convert FlowQueryContext to QueryContext
                    query_context: Some(QueryContext::from(self.data.flow_context.clone()).into()),
                }),
                body: Some(PbFlowRequest::Create((&self.data).into())),
            };
            create_flow.push(async move {
                requester
                    .handle(request)
                    .await
                    .map_err(add_peer_context_if_needed(peer.clone()))
            });
        }
        info!(
            "Creating flow({:?}, type={:?}) on flownodes with peers={:?}",
            self.data.flow_id, self.data.flow_type, self.data.peers
        );
        join_all(create_flow)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.data.state = CreateFlowState::CreateMetadata;
        Ok(Status::executing(true))
    }

    /// Creates flow metadata.
    ///
    /// Abort(not-retry):
    /// - Failed to create table metadata.
    async fn on_create_metadata(&mut self) -> Result<Status> {
        // Safety: The flow id must be allocated.
        let flow_id = self.data.flow_id.unwrap();
        let (flow_info, flow_routes) = (&self.data).into();
        if let Some(prev_flow_value) = self.data.prev_flow_info_value.as_ref()
            && self.data.task.or_replace
        {
            self.context
                .flow_metadata_manager
                .update_flow_metadata(flow_id, prev_flow_value, &flow_info, flow_routes)
                .await?;
            info!("Replaced flow metadata for flow {flow_id}");
            self.data.did_replace = true;
        } else {
            self.context
                .flow_metadata_manager
                .create_flow_metadata(flow_id, flow_info, flow_routes)
                .await?;
            info!("Created flow metadata for flow {flow_id}");
        }

        self.data.state = CreateFlowState::InvalidateFlowCache;
        Ok(Status::executing(true))
    }

    async fn on_broadcast(&mut self) -> Result<Status> {
        debug_assert!(self.data.state == CreateFlowState::InvalidateFlowCache);
        // Safety: The flow id must be allocated.
        let flow_id = self.data.flow_id.unwrap();
        let did_replace = self.data.did_replace;
        let ctx = Context {
            subject: Some("Invalidate flow cache by creating flow".to_string()),
        };

        let mut caches = vec![];

        // if did replaced, invalidate the flow cache with drop the old flow
        if did_replace {
            let old_flow_info = self.data.prev_flow_info_value.as_ref().unwrap();

            // only drop flow is needed, since flow name haven't changed, and flow id already invalidated below
            caches.extend([CacheIdent::DropFlow(DropFlow {
                flow_id,
                source_table_ids: old_flow_info.source_table_ids.clone(),
                flow_part2node_id: old_flow_info.flownode_ids().clone().into_iter().collect(),
            })]);
        }

        let (_flow_info, flow_routes) = (&self.data).into();
        let flow_part2peers = flow_routes
            .into_iter()
            .map(|(part_id, route)| (part_id, route.peer))
            .collect();

        caches.extend([
            CacheIdent::CreateFlow(CreateFlow {
                flow_id,
                source_table_ids: self.data.source_table_ids.clone(),
                partition_to_peer_mapping: flow_part2peers,
            }),
            CacheIdent::FlowId(flow_id),
        ]);

        self.context
            .cache_invalidator
            .invalidate(&ctx, &caches)
            .await?;

        Ok(Status::done_with_output(flow_id))
    }
}

#[async_trait]
impl Procedure for CreateFlowProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_FLOW
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateFlowState::Prepare => self.on_prepare().await,
            CreateFlowState::CreateFlows => self.on_flownode_create_flows().await,
            CreateFlowState::CreateMetadata => self.on_create_metadata().await,
            CreateFlowState::InvalidateFlowCache => self.on_broadcast().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog_name = &self.data.task.catalog_name;
        let flow_name = &self.data.task.flow_name;

        LockKey::new(vec![
            CatalogLock::Read(catalog_name).into(),
            FlowNameLock::new(catalog_name, flow_name).into(),
        ])
    }
}

pub fn get_flow_type_from_options(flow_task: &CreateFlowTask) -> Result<FlowType> {
    let flow_type = flow_task
        .flow_options
        .get(FlowType::FLOW_TYPE_KEY)
        .map(|s| s.as_str());
    match flow_type {
        Some(FlowType::BATCHING) => Ok(FlowType::Batching),
        Some(FlowType::STREAMING) => Ok(FlowType::Streaming),
        Some(unknown) => UnexpectedSnafu {
            err_msg: format!("Unknown flow type: {}", unknown),
        }
        .fail(),
        None => Ok(FlowType::Batching),
    }
}

/// The flow option key for creating pending flow metadata when source tables do not exist.
pub const DEFER_ON_MISSING_SOURCE_KEY: &str = "defer_on_missing_source";

/// Internal transient key used to pass the serialized `FlowScheduleConfig` from
/// meta to flownode through `CreateRequest.flow_options`. This key must never
/// be accepted as a user-provided option and must never be persisted into
/// `FlowInfoValue.options`.
pub const INTERNAL_EVAL_SCHEDULE_KEY: &str = "__greptime_internal_eval_schedule";

pub fn defer_on_missing_source(flow_task: &CreateFlowTask) -> Result<bool> {
    flow_task
        .flow_options
        .get(DEFER_ON_MISSING_SOURCE_KEY)
        .map(|value| {
            value
                .trim()
                .to_ascii_lowercase()
                .parse::<bool>()
                .map_err(|_| {
                    error::UnexpectedSnafu {
                        err_msg: format!(
                            "Invalid flow option '{DEFER_ON_MISSING_SOURCE_KEY}': {value}"
                        ),
                    }
                    .build()
                })
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

pub fn validate_flow_options(flow_task: &CreateFlowTask) -> Result<()> {
    // Reject non-positive eval_interval_secs (zero or negative).
    if let Some(secs) = flow_task.eval_interval_secs
        && secs <= 0
    {
        return UnexpectedSnafu {
            err_msg: format!("EVAL INTERVAL must be positive, got {secs} seconds"),
        }
        .fail();
    }

    for key in flow_task.flow_options.keys() {
        match key.as_str() {
            DEFER_ON_MISSING_SOURCE_KEY
            | FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY
            | FlowType::FLOW_TYPE_KEY => {
                // flow_type is internal-only; allow it through as the
                // operator inserts it. Schedule keys and
                // __greptime_internal_eval_schedule fall through to the
                // unknown branch naturally since they are not in the
                // allowlist.
            }
            unknown => {
                return UnexpectedSnafu {
                    err_msg: format!(
                        "Unknown flow option '{unknown}', supported user options: {DEFER_ON_MISSING_SOURCE_KEY}, {FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY}"
                    ),
                }
                .fail();
            }
        }
    }

    defer_on_missing_source(flow_task)?;
    get_flow_type_from_options(flow_task)?;
    Ok(())
}

/// Computes the ceiling of `time` to the next schedule boundary aligned with `anchor + k * interval`.
/// All values are Unix timestamps in seconds.
fn ceil_to_boundary(time: i64, anchor: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return time;
    }
    if time <= anchor {
        return anchor;
    }

    let diff = i128::from(time) - i128::from(anchor);
    let interval = i128::from(interval);
    let k = (diff + interval - 1) / interval;
    let boundary = i128::from(anchor) + k * interval;

    boundary.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Returns the effective typed schedule config for flow metadata.
///
/// New metadata should carry `FlowInfoValue.eval_schedule`. For older metadata
/// that lacks the typed field, derive a deterministic config from `created_time`
/// and defaults. This avoids recovery-time wall-clock drift.
pub fn effective_eval_schedule_from_flow_info(
    flow_info: &FlowInfoValue,
) -> Option<FlowScheduleConfig> {
    if let Some(schedule) = &flow_info.eval_schedule {
        return Some(schedule.clone());
    }

    let eval_interval_secs = flow_info.eval_interval_secs?;
    if eval_interval_secs <= 0 {
        return None;
    }

    let start_secs = ceil_to_boundary(
        flow_info.created_time.timestamp(),
        FlowScheduleConfig::DEFAULT_ANCHOR_SECS,
        eval_interval_secs,
    );

    Some(FlowScheduleConfig::default_with_start(
        start_secs,
        eval_interval_secs,
    ))
}

/// Resolve `FlowScheduleConfig` into `task.eval_schedule`.
///
/// This must be called in `on_prepare` after `prev_flow_info` is loaded so that
/// `CreateRequest` and `FlowInfoValue` both see the same resolved defaults.
///
/// The function is idempotent: if `task.eval_schedule` is already `Some`,
/// it returns immediately (important for procedure retry / dump-restore).
///
/// Schedule configuration is NOT read from `task.flow_options` (those keys are
/// no longer user-facing options). For new flows, pure defaults are computed.
/// For OR REPLACE, the previous typed config is preserved when interval+anchor
/// are unchanged; otherwise defaults are recomputed.
pub(crate) fn resolve_schedule_defaults_into_task(
    task: &mut CreateFlowTask,
    prev_flow_info: Option<&FlowInfoValue>,
) {
    // Idempotent: if already computed, skip recomputation.
    if task.eval_schedule.is_some() {
        return;
    }

    let Some(eval_interval_secs) = task.eval_interval_secs else {
        return;
    };
    if eval_interval_secs <= 0 {
        return;
    }

    let anchor_secs = FlowScheduleConfig::DEFAULT_ANCHOR_SECS;

    // For OR REPLACE: if interval+anchor unchanged, preserve the entire
    // existing typed config so start / policy / limits are stable.
    if task.or_replace
        && let Some(prev) = prev_flow_info
        && let Some(old_sched) = effective_eval_schedule_from_flow_info(prev)
    {
        let old_interval = prev.eval_interval_secs.unwrap_or(0);
        if old_interval == eval_interval_secs && old_sched.anchor_secs == anchor_secs {
            task.eval_schedule = Some(old_sched);
            return;
        }
    }

    // --- Compute start ---
    let start_secs =
        // New flow, or OR REPLACE with changed interval/anchor: start at the next
        // aligned boundary after prepare time. The value is written into
        // task.eval_schedule once so procedure retry does not recompute it.
        ceil_to_boundary(chrono::Utc::now().timestamp(), anchor_secs, eval_interval_secs);

    task.eval_schedule = Some(FlowScheduleConfig::default_with_start(
        start_secs,
        eval_interval_secs,
    ));
}

fn user_runtime_flow_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    let mut options = options.clone();
    options.remove(DEFER_ON_MISSING_SOURCE_KEY);
    options.remove(INTERNAL_EVAL_SCHEDULE_KEY);
    options
}

/// The state of [CreateFlowProcedure].
#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr, PartialEq)]
pub enum CreateFlowState {
    /// Prepares to create the flow.
    Prepare,
    /// Creates flows on the flownode.
    CreateFlows,
    /// Invalidate flow cache.
    InvalidateFlowCache,
    /// Create metadata.
    CreateMetadata,
}

/// The type of flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum FlowType {
    /// The flow is a batching task.
    #[default]
    Batching,
    /// The flow is a streaming task.
    Streaming,
}

pub const FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY: &str =
    "experimental_enable_incremental_read";

impl FlowType {
    pub const BATCHING: &str = "batching";
    pub const STREAMING: &str = "streaming";
    pub const FLOW_TYPE_KEY: &str = "flow_type";
}

impl fmt::Display for FlowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowType::Batching => write!(f, "{}", FlowType::BATCHING),
            FlowType::Streaming => write!(f, "{}", FlowType::STREAMING),
        }
    }
}

/// The serializable data.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateFlowData {
    pub(crate) state: CreateFlowState,
    pub(crate) task: CreateFlowTask,
    pub(crate) flow_id: Option<FlowId>,
    pub(crate) peers: Vec<Peer>,
    pub(crate) source_table_ids: Vec<TableId>,
    #[serde(default)]
    pub(crate) unresolved_source_table_names: Vec<TableName>,
    /// Use alias for backward compatibility with QueryContext serialized data
    #[serde(alias = "query_context")]
    pub(crate) flow_context: FlowQueryContext,
    /// For verify if prev value is consistent when need to update flow metadata.
    /// only set when `or_replace` is true.
    pub(crate) prev_flow_info_value: Option<DeserializedValueWithBytes<FlowInfoValue>>,
    /// Only set to true when replace actually happened.
    /// This is used to determine whether to invalidate the cache.
    #[serde(default)]
    pub(crate) did_replace: bool,
    pub(crate) flow_type: Option<FlowType>,
}

impl CreateFlowData {
    pub(crate) fn is_pending(&self) -> bool {
        !self.unresolved_source_table_names.is_empty()
    }

    pub(crate) fn is_active(&self) -> bool {
        !self.is_pending()
    }
}

impl From<&CreateFlowData> for CreateRequest {
    fn from(value: &CreateFlowData) -> Self {
        let flow_id = value.flow_id.unwrap();
        let source_table_ids = &value.source_table_ids;

        let mut req = CreateRequest {
            flow_id: Some(api::v1::FlowId { id: flow_id }),
            source_table_ids: source_table_ids
                .iter()
                .map(|table_id| api::v1::TableId { id: *table_id })
                .collect_vec(),
            sink_table_name: Some(value.task.sink_table_name.clone().into()),
            // Always be true to ensure idempotent in case of retry
            create_if_not_exists: true,
            or_replace: value.task.or_replace,
            expire_after: value.task.expire_after.map(|value| ExpireAfter { value }),
            eval_interval: value
                .task
                .eval_interval_secs
                .map(|seconds| api::v1::EvalInterval { seconds }),
            comment: value.task.comment.clone(),
            sql: value.task.sql.clone(),
            flow_options: user_runtime_flow_options(&value.task.flow_options),
        };

        let flow_type = value.flow_type.unwrap_or_default().to_string();
        req.flow_options
            .insert(FlowType::FLOW_TYPE_KEY.to_string(), flow_type);

        // Pass typed schedule config via internal transient key in flow_options.
        if let Some(ref sched) = value.task.eval_schedule {
            let json = serde_json::to_string(sched)
                .expect("FlowScheduleConfig serialization should not fail");
            req.flow_options
                .insert(INTERNAL_EVAL_SCHEDULE_KEY.to_string(), json);
        }

        req
    }
}

impl From<&CreateFlowData> for (FlowInfoValue, Vec<(FlowPartitionId, FlowRouteValue)>) {
    fn from(value: &CreateFlowData) -> Self {
        let catalog_name = value.task.catalog_name.clone();
        let flow_name = value.task.flow_name.clone();
        let sink_table_name = value.task.sink_table_name.clone();
        let expire_after = value.task.expire_after;
        let eval_interval = value.task.eval_interval_secs;
        let comment = value.task.comment.clone();
        let sql = value.task.sql.clone();
        let eval_schedule = value.task.eval_schedule.clone();

        // Start with a clean options map. The transient schedule payload is
        // only for the meta→flownode CreateRequest boundary and must not be
        // persisted in FlowInfoValue.options.
        let mut options: HashMap<String, String> = value
            .task
            .flow_options
            .iter()
            .filter(|(k, _)| k.as_str() != INTERNAL_EVAL_SCHEDULE_KEY)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let flownode_ids = value
            .peers
            .iter()
            .enumerate()
            .map(|(idx, peer)| (idx as u32, peer.id))
            .collect::<BTreeMap<_, _>>();
        let flow_routes = value
            .peers
            .iter()
            .enumerate()
            .map(|(idx, peer)| (idx as u32, FlowRouteValue { peer: peer.clone() }))
            .collect::<Vec<_>>();

        let flow_type = value.flow_type.unwrap_or_default().to_string();
        options.insert(FlowType::FLOW_TYPE_KEY.to_string(), flow_type);

        let mut create_time = chrono::Utc::now();
        if let Some(prev_flow_value) = value.prev_flow_info_value.as_ref()
            && value.task.or_replace
        {
            create_time = prev_flow_value.get_inner_ref().created_time;
        }

        // This conversion borrows the procedure data: the procedure keeps using
        // `self.data` after metadata creation (for cache invalidation, retry and
        // procedure dump/restore), while `FlowInfoValue` owns the persisted
        // metadata. Cloning these owned fields is therefore intentional.
        let flow_info: FlowInfoValue = FlowInfoValue {
            source_table_ids: value.source_table_ids.clone(),
            all_source_table_names: value.task.source_table_names.clone(),
            unresolved_source_table_names: value.unresolved_source_table_names.clone(),
            sink_table_name: sink_table_name.clone(),
            flownode_ids,
            catalog_name: catalog_name.clone(),
            // Convert FlowQueryContext back to QueryContext for storage
            query_context: Some(QueryContext::from(value.flow_context.clone())),
            flow_name: flow_name.clone(),
            raw_sql: sql.clone(),
            expire_after,
            eval_interval_secs: eval_interval,
            comment: comment.clone(),
            options,
            status: if value.is_active() {
                FlowStatus::Active
            } else {
                FlowStatus::PendingSources
            },
            created_time: create_time,
            updated_time: chrono::Utc::now(),
            eval_schedule: eval_schedule.clone(),
        };

        (flow_info, flow_routes)
    }
}
