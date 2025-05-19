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

use std::collections::BTreeMap;
use std::fmt;

use api::v1::flow::flow_request::Body as PbFlowRequest;
use api::v1::flow::{CreateRequest, FlowRequest, FlowRequestHeader};
use api::v1::ExpireAfter;
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
use snafu::{ensure, ResultExt};
use strum::AsRefStr;
use table::metadata::TableId;

use crate::cache_invalidator::Context;
use crate::ddl::utils::{add_peer_context_if_needed, handle_retry_error};
use crate::ddl::DdlContext;
use crate::error::{self, Result, UnexpectedSnafu};
use crate::instruction::{CacheIdent, CreateFlow, DropFlow};
use crate::key::flow::flow_info::FlowInfoValue;
use crate::key::flow::flow_route::FlowRouteValue;
use crate::key::table_name::TableNameKey;
use crate::key::{DeserializedValueWithBytes, FlowId, FlowPartitionId};
use crate::lock_key::{CatalogLock, FlowNameLock, TableNameLock};
use crate::metrics;
use crate::peer::Peer;
use crate::rpc::ddl::{CreateFlowTask, QueryContext};

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
                query_context,
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
        if self.data.flow_id.is_none() {
            self.allocate_flow_id().await?;
        }
        self.data.state = CreateFlowState::CreateFlows;
        // determine flow type
        self.data.flow_type = Some(get_flow_type_from_options(&self.data.task)?);

        Ok(Status::executing(true))
    }

    async fn on_flownode_create_flows(&mut self) -> Result<Status> {
        // Safety: must be allocated.
        let mut create_flow = Vec::with_capacity(self.data.peers.len());
        for peer in &self.data.peers {
            let requester = self.context.node_manager.flownode(peer).await;
            let request = FlowRequest {
                header: Some(FlowRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    query_context: Some(self.data.query_context.clone().into()),
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
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog_name = &self.data.task.catalog_name;
        let flow_name = &self.data.task.flow_name;
        let sink_table_name = &self.data.task.sink_table_name;

        LockKey::new(vec![
            CatalogLock::Read(catalog_name).into(),
            TableNameLock::new(
                &sink_table_name.catalog_name,
                &sink_table_name.schema_name,
                &sink_table_name.catalog_name,
            )
            .into(),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlowType {
    /// The flow is a batching task.
    Batching,
    /// The flow is a streaming task.
    Streaming,
}

impl FlowType {
    pub const BATCHING: &str = "batching";
    pub const STREAMING: &str = "streaming";
    pub const FLOW_TYPE_KEY: &str = "flow_type";
}

impl Default for FlowType {
    fn default() -> Self {
        Self::Batching
    }
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
    pub(crate) query_context: QueryContext,
    /// For verify if prev value is consistent when need to update flow metadata.
    /// only set when `or_replace` is true.
    pub(crate) prev_flow_info_value: Option<DeserializedValueWithBytes<FlowInfoValue>>,
    /// Only set to true when replace actually happened.
    /// This is used to determine whether to invalidate the cache.
    #[serde(default)]
    pub(crate) did_replace: bool,
    pub(crate) flow_type: Option<FlowType>,
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
            comment: value.task.comment.clone(),
            sql: value.task.sql.clone(),
            flow_options: value.task.flow_options.clone(),
        };

        let flow_type = value.flow_type.unwrap_or_default().to_string();
        req.flow_options
            .insert(FlowType::FLOW_TYPE_KEY.to_string(), flow_type);
        req
    }
}

impl From<&CreateFlowData> for (FlowInfoValue, Vec<(FlowPartitionId, FlowRouteValue)>) {
    fn from(value: &CreateFlowData) -> Self {
        let CreateFlowTask {
            catalog_name,
            flow_name,
            sink_table_name,
            expire_after,
            comment,
            sql,
            flow_options: mut options,
            ..
        } = value.task.clone();

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
        options.insert("flow_type".to_string(), flow_type);

        let mut create_time = chrono::Utc::now();
        if let Some(prev_flow_value) = value.prev_flow_info_value.as_ref()
            && value.task.or_replace
        {
            create_time = prev_flow_value.get_inner_ref().created_time;
        }

        let flow_info: FlowInfoValue = FlowInfoValue {
            source_table_ids: value.source_table_ids.clone(),
            sink_table_name,
            flownode_ids,
            catalog_name,
            query_context: Some(value.query_context.clone()),
            flow_name,
            raw_sql: sql,
            expire_after,
            comment,
            options,
            created_time: create_time,
            updated_time: chrono::Utc::now(),
        };

        (flow_info, flow_routes)
    }
}
