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

use api::v1::flow::{flow_request, DropRequest, FlowRequest};
use async_trait::async_trait;
use common_catalog::format_full_flow_name;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::info;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use strum::AsRefStr;

use crate::cache_invalidator::Context;
use crate::ddl::utils::{add_peer_context_if_needed, handle_retry_error};
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::flow_name::FlowName;
use crate::instruction::{CacheIdent, DropFlow};
use crate::key::flow::flow_info::FlowInfoValue;
use crate::key::flow::flow_route::FlowRouteValue;
use crate::lock_key::{CatalogLock, FlowLock};
use crate::metrics;
use crate::rpc::ddl::DropFlowTask;

/// The procedure for dropping a flow.
pub struct DropFlowProcedure {
    /// The context of procedure runtime.
    pub(crate) context: DdlContext,
    /// The serializable data.
    pub(crate) data: DropFlowData,
}

impl DropFlowProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::DropFlow";

    pub fn new(task: DropFlowTask, context: DdlContext) -> Self {
        Self {
            context,
            data: DropFlowData {
                state: DropFlowState::Prepare,
                task,
                flow_info_value: None,
                flow_route_values: vec![],
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: DropFlowData = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self { context, data })
    }

    /// Checks whether flow exists.
    /// - Early returns if flow not exists and `drop_if_exists` is `true`.
    /// - Throws an error if flow not exists and `drop_if_exists` is `false`.
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let catalog_name = &self.data.task.catalog_name;
        let flow_name = &self.data.task.flow_name;
        let exists = self
            .context
            .flow_metadata_manager
            .flow_name_manager()
            .exists(catalog_name, flow_name)
            .await?;

        if !exists && self.data.task.drop_if_exists {
            return Ok(Status::done());
        }

        ensure!(
            exists,
            error::FlowNotFoundSnafu {
                flow_name: format_full_flow_name(catalog_name, flow_name)
            }
        );

        self.fill_flow_metadata().await?;
        self.data.state = DropFlowState::DeleteMetadata;
        Ok(Status::executing(true))
    }

    async fn on_flownode_drop_flows(&self) -> Result<Status> {
        // Safety: checked
        let flownode_ids = &self.data.flow_info_value.as_ref().unwrap().flownode_ids;
        let flow_id = self.data.task.flow_id;
        let mut drop_flow_tasks = Vec::with_capacity(flownode_ids.len());

        for FlowRouteValue { peer } in &self.data.flow_route_values {
            let requester = self.context.node_manager.flownode(peer).await;
            let request = FlowRequest {
                body: Some(flow_request::Body::Drop(DropRequest {
                    flow_id: Some(api::v1::FlowId { id: flow_id }),
                })),
                ..Default::default()
            };

            drop_flow_tasks.push(async move {
                if let Err(err) = requester.handle(request).await {
                    if err.status_code() != StatusCode::FlowNotFound {
                        return Err(add_peer_context_if_needed(peer.clone())(err));
                    }
                }
                Ok(())
            });
        }

        join_all(drop_flow_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(Status::done())
    }

    async fn on_delete_metadata(&mut self) -> Result<Status> {
        let flow_id = self.data.task.flow_id;
        self.context
            .flow_metadata_manager
            .destroy_flow_metadata(
                flow_id,
                // Safety: checked
                self.data.flow_info_value.as_ref().unwrap(),
            )
            .await?;
        info!("Deleted flow metadata for flow {flow_id}");
        self.data.state = DropFlowState::InvalidateFlowCache;
        Ok(Status::executing(true))
    }

    async fn on_broadcast(&mut self) -> Result<Status> {
        let flow_id = self.data.task.flow_id;
        let ctx = Context {
            subject: Some("Invalidate flow cache by dropping flow".to_string()),
        };
        let flow_info_value = self.data.flow_info_value.as_ref().unwrap();

        let flow_part2nodes = flow_info_value
            .flownode_ids()
            .clone()
            .into_iter()
            .collect::<Vec<_>>();

        self.context
            .cache_invalidator
            .invalidate(
                &ctx,
                &[
                    CacheIdent::FlowId(flow_id),
                    CacheIdent::FlowName(FlowName {
                        catalog_name: flow_info_value.catalog_name.to_string(),
                        flow_name: flow_info_value.flow_name.to_string(),
                    }),
                    CacheIdent::DropFlow(DropFlow {
                        flow_id,
                        source_table_ids: flow_info_value.source_table_ids.clone(),
                        flow_part2node_id: flow_part2nodes,
                    }),
                ],
            )
            .await?;
        self.data.state = DropFlowState::DropFlows;
        Ok(Status::executing(true))
    }
}

#[async_trait]
impl Procedure for DropFlowProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;
        let _timer = metrics::METRIC_META_PROCEDURE_DROP_FLOW
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match self.data.state {
            DropFlowState::Prepare => self.on_prepare().await,
            DropFlowState::DeleteMetadata => self.on_delete_metadata().await,
            DropFlowState::InvalidateFlowCache => self.on_broadcast().await,
            DropFlowState::DropFlows => self.on_flownode_drop_flows().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog_name = &self.data.task.catalog_name;
        let flow_id = self.data.task.flow_id;

        let lock_key = vec![
            CatalogLock::Read(catalog_name).into(),
            FlowLock::Write(flow_id).into(),
        ];

        LockKey::new(lock_key)
    }
}

/// The serializable data
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropFlowData {
    state: DropFlowState,
    task: DropFlowTask,
    pub(crate) flow_info_value: Option<FlowInfoValue>,
    pub(crate) flow_route_values: Vec<FlowRouteValue>,
}

/// The state of drop flow
#[derive(Debug, Serialize, Deserialize, AsRefStr, PartialEq)]
enum DropFlowState {
    /// Prepares to drop the flow
    Prepare,
    /// Deletes metadata
    DeleteMetadata,
    /// Invalidate flow cache
    InvalidateFlowCache,
    /// Drop flows on flownode
    DropFlows,
    // TODO(weny): support to rollback
}
