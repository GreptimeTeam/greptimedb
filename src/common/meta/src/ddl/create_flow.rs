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

mod check;
mod metadata;

use std::collections::BTreeMap;

use api::v1::flow::flow_request::Body as PbFlowRequest;
use api::v1::flow::{CreateRequest, FlowRequest};
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::info;
use futures::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use strum::AsRefStr;
use table::metadata::TableId;

use super::utils::add_peer_context_if_needed;
use crate::ddl::utils::handle_retry_error;
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::key::flow_task::flow_task_info::FlowTaskInfoValue;
use crate::key::FlowTaskId;
use crate::lock_key::{CatalogLock, FlowTaskNameLock, TableNameLock};
use crate::peer::Peer;
use crate::rpc::ddl::CreateFlowTask;
use crate::{metrics, ClusterId};

/// The procedure of flow task creation.
pub struct CreateFlowProcedure {
    pub context: DdlContext,
    pub data: CreateFlowTaskData,
}

impl CreateFlowProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateFlow";

    /// Returns a new [CreateFlowProcedure].
    pub fn new(cluster_id: ClusterId, task: CreateFlowTask, context: DdlContext) -> Self {
        Self {
            context,
            data: CreateFlowTaskData {
                cluster_id,
                task,
                flow_task_id: None,
                peers: vec![],
                source_table_ids: vec![],
                state: CreateFlowTaskState::CreateMetadata,
            },
        }
    }

    /// Deserializes from `json`.
    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(CreateFlowProcedure { context, data })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        self.check_creation().await?;
        self.collect_source_tables().await?;
        self.allocate_flow_task_id().await?;
        self.data.state = CreateFlowTaskState::CreateFlows;

        Ok(Status::executing(true))
    }

    async fn on_flownode_create_flows(&mut self) -> Result<Status> {
        // Safety: must be allocated.
        let mut create_flow_task = Vec::with_capacity(self.data.peers.len());
        for peer in &self.data.peers {
            let requester = self.context.node_manager.flownode(peer).await;
            let request = FlowRequest {
                body: Some(PbFlowRequest::Create((&self.data).into())),
            };
            create_flow_task.push(async move {
                requester
                    .handle(request)
                    .await
                    .map_err(add_peer_context_if_needed(peer.clone()))
            });
        }

        join_all(create_flow_task)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.data.state = CreateFlowTaskState::CreateMetadata;
        Ok(Status::executing(true))
    }

    /// Creates flow task metadata.
    ///
    /// Abort(not-retry):
    /// - Failed to create table metadata.
    async fn on_create_metadata(&mut self) -> Result<Status> {
        // Safety: The flow task id must be allocated.
        let flow_task_id = self.data.flow_task_id.unwrap();
        // TODO(weny): Support `or_replace`.
        self.context
            .flow_task_metadata_manager
            .create_flow_task_metadata(flow_task_id, (&self.data).into())
            .await?;
        info!("Created flow task metadata for flow {flow_task_id}");
        Ok(Status::done_with_output(flow_task_id))
    }
}

#[async_trait]
impl Procedure for CreateFlowProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_FLOW_TASK
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateFlowTaskState::Prepare => self.on_prepare().await,
            CreateFlowTaskState::CreateFlows => self.on_flownode_create_flows().await,
            CreateFlowTaskState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog_name = &self.data.task.catalog_name;
        let task_name = &self.data.task.task_name;
        let sink_table_name = &self.data.task.sink_table_name;

        LockKey::new(vec![
            CatalogLock::Read(catalog_name).into(),
            TableNameLock::new(
                &sink_table_name.catalog_name,
                &sink_table_name.schema_name,
                &sink_table_name.catalog_name,
            )
            .into(),
            FlowTaskNameLock::new(catalog_name, task_name).into(),
        ])
    }
}

/// The state of [CreateFlowTaskProcedure].
#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr, PartialEq)]
pub enum CreateFlowTaskState {
    /// Prepares to create the flow.
    Prepare,
    /// Creates flows on the flownode.
    CreateFlows,
    /// Create metadata.
    CreateMetadata,
}

/// The serializable data.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateFlowTaskData {
    pub(crate) cluster_id: ClusterId,
    pub(crate) state: CreateFlowTaskState,
    pub(crate) task: CreateFlowTask,
    pub(crate) flow_task_id: Option<FlowTaskId>,
    pub(crate) peers: Vec<Peer>,
    pub(crate) source_table_ids: Vec<TableId>,
}

impl From<&CreateFlowTaskData> for CreateRequest {
    fn from(value: &CreateFlowTaskData) -> Self {
        let flow_task_id = value.flow_task_id.unwrap();
        let source_table_ids = &value.source_table_ids;

        CreateRequest {
            task_id: Some(api::v1::flow::TaskId { id: flow_task_id }),
            source_table_ids: source_table_ids
                .iter()
                .map(|table_id| api::v1::TableId { id: *table_id })
                .collect_vec(),
            sink_table_name: Some(value.task.sink_table_name.clone().into()),
            // Always be true
            create_if_not_exists: true,
            expire_when: value.task.expire_when.clone(),
            comment: value.task.comment.clone(),
            sql: value.task.sql.clone(),
            task_options: value.task.options.clone(),
        }
    }
}

impl From<&CreateFlowTaskData> for FlowTaskInfoValue {
    fn from(value: &CreateFlowTaskData) -> Self {
        let CreateFlowTask {
            catalog_name,
            task_name,
            sink_table_name,
            expire_when,
            comment,
            sql,
            options,
            ..
        } = value.task.clone();

        let flownode_ids = value
            .peers
            .iter()
            .enumerate()
            .map(|(idx, peer)| (idx as u32, peer.id))
            .collect::<BTreeMap<_, _>>();

        FlowTaskInfoValue {
            source_table_ids: value.source_table_ids.clone(),
            sink_table_name,
            flownode_ids,
            catalog_name,
            task_name,
            raw_sql: sql,
            expire_when,
            comment,
            options,
        }
    }
}
