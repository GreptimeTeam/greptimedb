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
use crate::lock_key::{CatalogLock, FlowTaskNameLock};
use crate::peer::Peer;
use crate::rpc::ddl::CreateFlowTask;
use crate::{metrics, ClusterId};

/// The procedure of flow task creation.
pub struct CreateFlowTaskProcedure {
    pub context: DdlContext,
    pub data: CreateFlowTaskData,
}

impl CreateFlowTaskProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateFlowTask";

    /// Returns a new [CreateFlowTaskProcedure].
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
        Ok(CreateFlowTaskProcedure { context, data })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        self.check_creation().await?;
        self.collect_source_tables().await?;
        self.allocate_flow_task_id().await?;
        self.data.state = CreateFlowTaskState::FlownodeCreateFlows;

        Ok(Status::executing(true))
    }

    async fn on_flownode_create_flow(&mut self) -> Result<Status> {
        self.data.state = CreateFlowTaskState::CreateMetadata;
        // Safety: must be allocated.
        let mut create_flow_task = Vec::with_capacity(self.data.peers.len());
        for peer in &self.data.peers {
            let requester = self.context.datanode_manager.flownode(peer).await;
            let request = FlowRequest {
                body: Some(PbFlowRequest::Create(self.data.to_create_flow_request())),
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
            .create_flow_task_metadata(flow_task_id, self.data.to_flow_task_info_value())
            .await?;
        info!("Created flow task metadata for flow task {flow_task_id}");
        Ok(Status::done_with_output(flow_task_id))
    }
}

#[async_trait]
impl Procedure for CreateFlowTaskProcedure {
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
            CreateFlowTaskState::FlownodeCreateFlows => self.on_flownode_create_flow().await,
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

        LockKey::new(vec![
            CatalogLock::Read(catalog_name).into(),
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
    FlownodeCreateFlows,
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

impl CreateFlowTaskData {
    /// Converts to [CreateRequest]
    /// # Panic
    /// Panic if the `flow_task_id` is None.
    fn to_create_flow_request(&self) -> CreateRequest {
        let flow_task_id = self.flow_task_id.unwrap();
        let source_table_ids = &self.source_table_ids;

        CreateRequest {
            task_id: Some(api::v1::flow::TaskId { id: flow_task_id }),
            source_table_ids: source_table_ids
                .iter()
                .map(|table_id| api::v1::TableId { id: *table_id })
                .collect_vec(),
            sink_table_name: Some(self.task.sink_table_name.clone().into()),
            // Always be true
            create_if_not_exists: true,
            expire_when: self.task.expire_when.clone(),
            comment: self.task.comment.clone(),
            sql: self.task.sql.clone(),
            task_options: self.task.options.clone(),
        }
    }

    /// Converts to [FlowTaskInfoValue].
    fn to_flow_task_info_value(&self) -> FlowTaskInfoValue {
        let CreateFlowTask {
            catalog_name,
            task_name,
            sink_table_name,
            expire_when,
            comment,
            sql,
            options,
            ..
        } = self.task.clone();

        let flownode_ids = self
            .peers
            .iter()
            .enumerate()
            .map(|(idx, peer)| (idx as u32, peer.id))
            .collect::<BTreeMap<_, _>>();

        FlowTaskInfoValue {
            source_table_ids: self.source_table_ids.clone(),
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
