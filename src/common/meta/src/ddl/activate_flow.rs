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

use std::collections::BTreeMap;

use api::v1::ExpireAfter;
use api::v1::flow::flow_request::Body as PbFlowRequest;
use api::v1::flow::{CreateRequest, FlowRequest, FlowRequestHeader};
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::tracing_context::TracingContext;
use futures::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;
use table::table_name::TableName;

use crate::cache_invalidator::Context;
use crate::ddl::DdlContext;
use crate::ddl::create_flow::FlowType;
use crate::ddl::utils::{add_peer_context_if_needed, map_to_procedure_error};
use crate::error::{self, Result};
use crate::instruction::{CacheIdent, CreateFlow};
use crate::key::flow::flow_info::{FlowInfoValue, FlowStatus};
use crate::key::flow::flow_route::FlowRouteValue;
use crate::key::table_name::TableNameKey;
use crate::key::{DeserializedValueWithBytes, FlowId, FlowPartitionId};
use crate::lock_key::{CatalogLock, FlowLock};
use crate::metrics;
use crate::peer::Peer;

pub struct ActivatePendingFlowProcedure {
    pub context: DdlContext,
    pub data: ActivatePendingFlowData,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ActivatePendingFlowState {
    Prepare,
    CreateFlows,
    UpdateMetadata,
    InvalidateFlowCache,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActivatePendingFlowData {
    pub(crate) state: ActivatePendingFlowState,
    pub(crate) flow_id: FlowId,
    pub(crate) catalog_name: String,
    #[serde(default)]
    pub(crate) peers: Vec<Peer>,
    #[serde(default)]
    pub(crate) resolved_table_ids: Vec<TableId>,
    pub(crate) prev_flow_info_value: Option<DeserializedValueWithBytes<FlowInfoValue>>,
}

pub struct PendingFlowResolution {
    resolved_table_ids: Vec<TableId>,
    unresolved_source_table_names: Vec<TableName>,
}

impl ActivatePendingFlowProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ActivatePendingFlow";

    pub fn new(flow_id: FlowId, catalog_name: String, context: DdlContext) -> Self {
        Self {
            context,
            data: ActivatePendingFlowData {
                state: ActivatePendingFlowState::Prepare,
                flow_id,
                catalog_name,
                peers: vec![],
                resolved_table_ids: vec![],
                prev_flow_info_value: None,
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        let Some(current_flow_info) = self
            .context
            .flow_metadata_manager
            .flow_info_manager()
            .get_raw(self.data.flow_id)
            .await?
        else {
            return Ok(Status::done());
        };

        if current_flow_info.get_inner_ref().is_active() {
            return Ok(Status::done());
        }

        let resolution =
            resolve_pending_flow_sources(&self.context, current_flow_info.get_inner_ref()).await?;
        if !resolution.unresolved_source_table_names.is_empty() {
            update_pending_flow_metadata(
                &self.context,
                self.data.flow_id,
                &current_flow_info,
                resolution.resolved_table_ids,
                resolution.unresolved_source_table_names,
                None,
            )
            .await?;
            return Ok(Status::done());
        }

        if let Some(reason) =
            validate_batching_activation(&self.context, &resolution.resolved_table_ids).await?
        {
            update_pending_flow_metadata(
                &self.context,
                self.data.flow_id,
                &current_flow_info,
                resolution.resolved_table_ids,
                vec![],
                Some(reason),
            )
            .await?;
            return Ok(Status::done());
        }

        self.data.peers = self.context.flow_metadata_allocator.alloc_peers(1).await?;
        self.data.resolved_table_ids = resolution.resolved_table_ids;
        self.data.prev_flow_info_value = Some(current_flow_info);
        self.data.state = ActivatePendingFlowState::CreateFlows;

        Ok(Status::executing(true))
    }

    async fn on_create_flows(&mut self) -> Result<Status> {
        let flow_info =
            self.data
                .prev_flow_info_value
                .as_ref()
                .context(error::UnexpectedSnafu {
                    err_msg: "missing previous flow info for activation",
                })?;
        let request = build_create_request(
            self.data.flow_id,
            flow_info.get_inner_ref(),
            &self.data.resolved_table_ids,
        );
        create_flow_on_peers(
            &self.context,
            &self.data.peers,
            request,
            flow_info.get_inner_ref(),
        )
        .await?;
        self.data.state = ActivatePendingFlowState::UpdateMetadata;
        Ok(Status::executing(true))
    }

    async fn on_update_metadata(&mut self) -> Result<Status> {
        let current_flow_info =
            self.data
                .prev_flow_info_value
                .as_ref()
                .context(error::UnexpectedSnafu {
                    err_msg: "missing previous flow info for metadata update",
                })?;
        let (new_flow_info, flow_routes) = build_active_flow_info(
            current_flow_info.get_inner_ref(),
            &self.data.peers,
            &self.data.resolved_table_ids,
        );
        self.context
            .flow_metadata_manager
            .update_flow_metadata(
                self.data.flow_id,
                current_flow_info,
                &new_flow_info,
                flow_routes,
            )
            .await?;
        self.data.state = ActivatePendingFlowState::InvalidateFlowCache;
        Ok(Status::executing(true))
    }

    async fn on_broadcast(&mut self) -> Result<Status> {
        invalidate_flow_cache(
            &self.context,
            self.data.flow_id,
            &self.data.resolved_table_ids,
            &self.data.peers,
        )
        .await?;
        Ok(Status::done_with_output(self.data.flow_id))
    }
}

#[async_trait]
impl Procedure for ActivatePendingFlowProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;
        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_FLOW
            .with_label_values(&[match state {
                ActivatePendingFlowState::Prepare => "activate_prepare",
                ActivatePendingFlowState::CreateFlows => "activate_create_flows",
                ActivatePendingFlowState::UpdateMetadata => "activate_update_metadata",
                ActivatePendingFlowState::InvalidateFlowCache => "activate_invalidate_flow_cache",
            }])
            .start_timer();

        match self.data.state {
            ActivatePendingFlowState::Prepare => self.on_prepare().await,
            ActivatePendingFlowState::CreateFlows => self.on_create_flows().await,
            ActivatePendingFlowState::UpdateMetadata => self.on_update_metadata().await,
            ActivatePendingFlowState::InvalidateFlowCache => self.on_broadcast().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(vec![
            CatalogLock::Read(&self.data.catalog_name).into(),
            FlowLock::Write(self.data.flow_id).into(),
        ])
    }
}

async fn resolve_pending_flow_sources(
    context: &DdlContext,
    flow_info: &FlowInfoValue,
) -> Result<PendingFlowResolution> {
    let keys = flow_info
        .all_source_table_names()
        .iter()
        .map(|name| TableNameKey::new(&name.catalog_name, &name.schema_name, &name.table_name))
        .collect::<Vec<_>>();
    let resolved_tables = context
        .table_metadata_manager
        .table_name_manager()
        .batch_get(keys)
        .await?;

    let mut resolved_table_ids = Vec::with_capacity(flow_info.all_source_table_names().len());
    let mut unresolved_source_table_names = Vec::new();
    for (name, table_id) in flow_info
        .all_source_table_names()
        .iter()
        .zip(resolved_tables.into_iter())
    {
        match table_id {
            Some(table_id) => resolved_table_ids.push(table_id.table_id()),
            None => unresolved_source_table_names.push(name.clone()),
        }
    }

    Ok(PendingFlowResolution {
        resolved_table_ids,
        unresolved_source_table_names,
    })
}

async fn update_pending_flow_metadata(
    context: &DdlContext,
    flow_id: FlowId,
    current_flow_info: &DeserializedValueWithBytes<FlowInfoValue>,
    resolved_table_ids: Vec<TableId>,
    unresolved_source_table_names: Vec<TableName>,
    last_activation_error: Option<String>,
) -> Result<()> {
    let mut new_flow_info = current_flow_info.get_inner_ref().clone();
    new_flow_info.source_table_ids = resolved_table_ids;
    new_flow_info.unresolved_source_table_names = unresolved_source_table_names;
    new_flow_info.last_activation_error = last_activation_error;
    new_flow_info.updated_time = chrono::Utc::now();
    context
        .flow_metadata_manager
        .update_flow_metadata(flow_id, current_flow_info, &new_flow_info, vec![])
        .await
}

async fn validate_batching_activation(
    context: &DdlContext,
    resolved_table_ids: &[TableId],
) -> Result<Option<String>> {
    let table_infos = context
        .table_metadata_manager
        .table_info_manager()
        .batch_get(resolved_table_ids)
        .await?;
    for source_table_id in resolved_table_ids {
        let Some(table_info) = table_infos.get(source_table_id) else {
            return Ok(Some(format!(
                "Source table metadata is not ready yet, table_id={source_table_id}",
            )));
        };
        if table_info.table_info.meta.options.ttl == Some(common_time::TimeToLive::Instant) {
            return Ok(Some(format!(
                "Source table {} requires streaming activation because ttl=instant",
                table_info.table_name()
            )));
        }
    }
    Ok(None)
}

fn build_create_request(
    flow_id: FlowId,
    flow_info: &FlowInfoValue,
    resolved_table_ids: &[TableId],
) -> CreateRequest {
    let mut flow_options = flow_info.options.clone();
    flow_options.insert(
        FlowType::FLOW_TYPE_KEY.to_string(),
        FlowType::Batching.to_string(),
    );
    CreateRequest {
        flow_id: Some(api::v1::FlowId { id: flow_id }),
        source_table_ids: resolved_table_ids
            .iter()
            .map(|table_id| api::v1::TableId { id: *table_id })
            .collect_vec(),
        sink_table_name: Some(flow_info.sink_table_name.clone().into()),
        create_if_not_exists: true,
        or_replace: false,
        expire_after: flow_info.expire_after.map(|value| ExpireAfter { value }),
        eval_interval: flow_info
            .eval_interval_secs
            .map(|seconds| api::v1::EvalInterval { seconds }),
        comment: flow_info.comment.clone(),
        sql: flow_info.raw_sql.clone(),
        flow_options,
    }
}

async fn create_flow_on_peers(
    context: &DdlContext,
    peers: &[Peer],
    request: CreateRequest,
    flow_info: &FlowInfoValue,
) -> Result<()> {
    let query_context = flow_info.query_context.clone().unwrap_or_default();
    let mut create_flow_tasks = Vec::with_capacity(peers.len());
    for peer in peers {
        let requester = context.node_manager.flownode(peer).await;
        let request = FlowRequest {
            header: Some(FlowRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                query_context: Some(query_context.clone().into()),
            }),
            body: Some(PbFlowRequest::Create(request.clone())),
        };
        create_flow_tasks.push(async move {
            requester
                .handle(request)
                .await
                .map_err(add_peer_context_if_needed(peer.clone()))
        });
    }
    join_all(create_flow_tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(())
}

fn build_active_flow_info(
    current_flow_info: &FlowInfoValue,
    peers: &[Peer],
    resolved_table_ids: &[TableId],
) -> (FlowInfoValue, Vec<(FlowPartitionId, FlowRouteValue)>) {
    let flownode_ids = peers
        .iter()
        .enumerate()
        .map(|(idx, peer)| (idx as u32, peer.id))
        .collect::<BTreeMap<_, _>>();
    let flow_routes = peers
        .iter()
        .enumerate()
        .map(|(idx, peer)| (idx as u32, FlowRouteValue { peer: peer.clone() }))
        .collect::<Vec<_>>();

    let mut new_flow_info = current_flow_info.clone();
    new_flow_info.source_table_ids = resolved_table_ids.to_vec();
    new_flow_info.unresolved_source_table_names = vec![];
    new_flow_info.flownode_ids = flownode_ids;
    new_flow_info.status = FlowStatus::Active;
    new_flow_info.last_activation_error = None;
    new_flow_info.options.insert(
        FlowType::FLOW_TYPE_KEY.to_string(),
        FlowType::Batching.to_string(),
    );
    new_flow_info.updated_time = chrono::Utc::now();

    (new_flow_info, flow_routes)
}

async fn invalidate_flow_cache(
    context: &DdlContext,
    flow_id: FlowId,
    resolved_table_ids: &[TableId],
    peers: &[Peer],
) -> Result<()> {
    let ctx = Context {
        subject: Some("Invalidate flow cache by activating pending flow".to_string()),
    };
    let flow_part2peers = peers
        .iter()
        .enumerate()
        .map(|(idx, peer)| (idx as u32, peer.clone()))
        .collect();
    context
        .cache_invalidator
        .invalidate(
            &ctx,
            &[
                CacheIdent::CreateFlow(CreateFlow {
                    flow_id,
                    source_table_ids: resolved_table_ids.to_vec(),
                    partition_to_peer_mapping: flow_part2peers,
                }),
                CacheIdent::FlowId(flow_id),
            ],
        )
        .await
}
