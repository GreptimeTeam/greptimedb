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

//! This module contains the flow migration procedure, which is responsible for
//! migrating flow data between different flownodes in the system.
//!

use api::v1::flow::{flow_request, CreateRequest, DropRequest, FlowRequest, FlushFlow};
use api::v1::meta::Peer;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::ddl::utils::{add_peer_context_if_needed, handle_retry_error};
use common_meta::ddl::DdlContext;
use common_meta::instruction::{CacheIdent, CreateFlow, DropFlow};
use common_meta::key::flow::flow_info::FlowInfoValue;
use common_meta::key::{FlowId, FlowPartitionId};
use common_meta::lock_key::{CatalogLock, FlowLock};
use common_meta::rpc::ddl::QueryContext;
use common_procedure::error::FromJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{info, warn};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use strum::AsRefStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowMigrationData {
    pub(crate) catalog: String,
    pub(crate) flow_id: FlowId,
    pub(crate) partition_id: FlowPartitionId,
    pub(crate) src_flownode: Peer,
    pub(crate) dest_flownode: Peer,
    pub(crate) state: FlowMigrationState,
    pub(crate) query_ctx: QueryContext,
    pub(crate) old_flow_info: Option<FlowInfoValue>,
    pub(crate) old_flow_routes: Option<Vec<(FlowPartitionId, Peer)>>,
    pub(crate) new_flow_info: Option<FlowInfoValue>,
    pub(crate) new_flow_routes: Option<Vec<(FlowPartitionId, Peer)>>,
}

/// The state of [CreateFlowProcedure].
#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr, PartialEq)]
pub enum FlowMigrationState {
    /// Prepare the flow migration. Validate the flow and flownode.
    Prepare,
    /// Create flow on the destination flownode.
    CreateFlowOnDest,
    /// Alter metadata for flow, include FlowInfo, FlowRoute, FlownodeFlow and TableFlow.
    AlterMetadata,
    /// Invalidate flow cache for all above metadata.
    InvalidateFlowCache,
    /// Remove old flow in source flownode
    DropFlowOnSrc,
}

pub struct FlowMigrationProcedure {
    pub context: DdlContext,
    pub data: FlowMigrationData,
}

impl FlowMigrationProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::FlowMigration";

    pub fn new(
        catalog: String,
        flow_id: FlowId,
        partition_id: FlowPartitionId,
        src_flownode: Peer,
        dest_flownode: Peer,
        query_ctx: QueryContext,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: FlowMigrationData {
                catalog,
                flow_id,
                partition_id,
                src_flownode,
                dest_flownode,
                query_ctx,
                state: FlowMigrationState::Prepare,
                old_flow_info: None,
                old_flow_routes: None,
                new_flow_info: None,
                new_flow_routes: None,
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: FlowMigrationData = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self { context, data })
    }

    /// Check if the flow exists on the source flownode.
    pub(crate) async fn on_prepare(&mut self) -> common_meta::error::Result<Status> {
        debug_assert!(self.data.state == FlowMigrationState::Prepare);
        let flow_id = self.data.flow_id;
        let partition_id = self.data.partition_id;
        let src_flownode = self.data.src_flownode.clone();
        // Check if the flow exists on the source flownode.
        let flow_exist = self
            .context
            .flow_metadata_manager
            .flownode_flow_manager()
            .exist(src_flownode.id, flow_id, partition_id)
            .await?;

        if !flow_exist {
            warn!(
                "Flow (flow_id={}, partition_id={}) does not exist on flownode {}",
                flow_id, partition_id, src_flownode.id
            );

            common_meta::error::FlowNotExistOnFlownodeSnafu {
                flow_id,
                flownode_id: src_flownode.id,
                partition_id,
            }
            .fail()?;
        }

        self.data.state = FlowMigrationState::CreateFlowOnDest;
        Ok(Status::executing(true))
    }

    async fn on_create_flow_on_dest(&mut self) -> common_meta::error::Result<Status> {
        debug_assert!(self.data.state == FlowMigrationState::CreateFlowOnDest);
        let flow_id = self.data.flow_id;
        let partition_id = self.data.partition_id;
        let src_flownode = self.data.src_flownode.clone();
        let dest_flownode = self.data.dest_flownode.clone();
        let query_ctx = &self.data.query_ctx;

        // Create flow on the destination flownode.
        let flow_exist = self
            .context
            .flow_metadata_manager
            .flownode_flow_manager()
            .exist(dest_flownode.id, flow_id, partition_id)
            .await?;

        if flow_exist {
            warn!(
                "Flow (flow_id={}) already exists on flownode {}",
                flow_id, dest_flownode.id
            );
            return Ok(Status::done());
        }

        let flow_info = self
            .context
            .flow_metadata_manager
            .flow_info_manager()
            .get(flow_id)
            .await?
            .with_context(|| common_meta::error::FlowNotExistOnFlownodeSnafu {
                flow_id,
                flownode_id: src_flownode.id,
                partition_id,
            })?;

        let req = info_to_create_request(flow_id, &flow_info);

        let requester = self.context.node_manager.flownode(&dest_flownode).await;
        let request = api::v1::flow::FlowRequest {
            header: Some(api::v1::flow::FlowRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                query_context: Some(query_ctx.clone().into()),
            }),
            body: Some(api::v1::flow::flow_request::Body::Create(req)),
        };

        requester
            .handle(request)
            .await
            .map_err(add_peer_context_if_needed(dest_flownode.clone()))?;

        info!(
            "Duplicate flow (flow_id={}) from flownode {} to flownode {}",
            flow_id, src_flownode.id, dest_flownode.id
        );
        self.data.state = FlowMigrationState::AlterMetadata;

        Ok(Status::executing(true))
    }

    /// Alter related metadata, including FlowInfo, FlowRoute, FlownodeFlow and TableFlow
    async fn on_alter_metadata(&mut self) -> common_meta::error::Result<Status> {
        debug_assert!(self.data.state == FlowMigrationState::AlterMetadata);
        let flow_id = self.data.flow_id;
        let partition_id = self.data.partition_id;
        let src_flownode = self.data.src_flownode.clone();
        let dst_flownode = self.data.dest_flownode.clone();

        let old_flow_info = self
            .context
            .flow_metadata_manager
            .flow_info_manager()
            .get_raw(flow_id)
            .await?
            .with_context(|| common_meta::error::FlowNotExistOnFlownodeSnafu {
                flow_id,
                flownode_id: src_flownode.id,
                partition_id,
            })?;

        let old_flow_routes = self
            .context
            .flow_metadata_manager
            .flow_route_manager()
            .routes(flow_id)
            .await?;

        self.data.old_flow_info = Some(old_flow_info.clone().into_inner());
        self.data.old_flow_routes = Some(
            old_flow_routes
                .clone()
                .into_iter()
                .map(|(k, v)| (k.partition_id(), v.peer().clone()))
                .collect(),
        );

        let new_flow_info = {
            let mut info = old_flow_info.clone();
            let old_flownode = info.migrate_flow(partition_id, dst_flownode.id);
            if old_flownode != Some(src_flownode.id) {
                common_meta::error::UnexpectedSnafu{
                    err_msg: format!("Expect flow(id={}, partition={})'s old flownode to be node_id={}, found node_id={:?}", flow_id, partition_id, src_flownode, old_flownode)
                }.fail()?;
            }
            info
        }.into_inner();

        let new_flow_routes: Vec<(FlowPartitionId, _)> = old_flow_routes
            .clone()
            .into_iter()
            .map(|(k, mut v)| {
                if k.partition_id() == partition_id {
                    v = dst_flownode.clone().into();
                }
                (k.partition_id(), v)
            })
            .collect();

        self.context
            .flow_metadata_manager
            .update_flow_metadata(
                flow_id,
                &old_flow_info,
                &new_flow_info,
                new_flow_routes.clone(),
            )
            .await?;

        self.data.new_flow_info = Some(new_flow_info);
        self.data.new_flow_routes = Some(
            new_flow_routes
                .into_iter()
                .map(|(k, v)| (k, v.peer().clone()))
                .collect(),
        );

        self.data.state = FlowMigrationState::InvalidateFlowCache;

        Ok(Status::executing(true))
    }

    async fn on_broadcast(&mut self) -> common_meta::error::Result<Status> {
        debug_assert!(self.data.state == FlowMigrationState::InvalidateFlowCache);
        // Safety: The flow id must be allocated.
        let flow_id = self.data.flow_id;
        let old_flow_info = self.data.old_flow_info.as_ref().unwrap();
        let new_flow_info = self.data.new_flow_info.as_ref().unwrap();
        let new_flow_routes = self.data.new_flow_routes.as_ref().unwrap();

        let ctx = common_meta::cache_invalidator::Context {
            subject: Some(
                format!(
                    "Invalidate flow cache by migrating flow(id={}, partition={}) from flownode {:?} to {:?}",
                    flow_id,
                    self.data.partition_id,
                    self.data.src_flownode,
                    self.data.dest_flownode
                )
            ),
        };

        self.context
            .cache_invalidator
            .invalidate(
                &ctx,
                &[
                    CacheIdent::DropFlow(DropFlow {
                        flow_id,
                        source_table_ids: old_flow_info.source_table_ids().to_vec(),
                        flow_part2node_id: old_flow_info
                            .flownode_ids()
                            .clone()
                            .into_iter()
                            .collect(),
                    }),
                    CacheIdent::CreateFlow(CreateFlow {
                        flow_id,
                        source_table_ids: new_flow_info.source_table_ids().to_vec(),
                        partition_to_peer_mapping: new_flow_routes.clone(),
                    }),
                    CacheIdent::FlowId(flow_id),
                ],
            )
            .await?;

        self.data.state = FlowMigrationState::DropFlowOnSrc;

        Ok(Status::executing(true))
    }

    async fn drop_flow_on_src_node(&mut self) -> common_meta::error::Result<Status> {
        // first flush flow then drop flow
        let src_flownode = self.data.src_flownode.clone();
        let flow_id = self.data.flow_id;

        let requester = self.context.node_manager.flownode(&src_flownode).await;
        let flush_request = FlowRequest {
            body: Some(flow_request::Body::Flush(FlushFlow {
                flow_id: Some(api::v1::FlowId { id: flow_id }),
            })),
            ..Default::default()
        };
        let drop_request = FlowRequest {
            body: Some(flow_request::Body::Drop(DropRequest {
                flow_id: Some(api::v1::FlowId { id: flow_id }),
            })),
            ..Default::default()
        };

        let flush_and_drop_result = {
            if let Err(err) = requester.handle(flush_request).await {
                Err(err)
            } else {
                requester.handle(drop_request).await
            }
        };

        if let Err(err) = flush_and_drop_result {
            if err.status_code() != StatusCode::FlowNotFound {
                return Err(add_peer_context_if_needed(src_flownode.clone())(err));
            }
        }

        Ok(Status::done())
    }

    async fn rollback_metadata_if_needed(&mut self) -> common_meta::error::Result<()> {
        let flow_id = self.data.flow_id;
        let partition_id = self.data.partition_id;
        let dest_flownode = self.data.dest_flownode.clone();

        let Some(new_flow_info) = self.data.new_flow_info.as_ref() else {
            return Ok(());
        };
        let current_flow_info = self
            .context
            .flow_metadata_manager
            .flow_info_manager()
            .get_raw(flow_id)
            .await?
            .with_context(|| common_meta::error::FlowNotExistOnFlownodeSnafu {
                flow_id,
                flownode_id: dest_flownode.id,
                partition_id,
            })?;
        if *current_flow_info != *new_flow_info {
            return Ok(());
        }
        let Some(old_flow_info) = self.data.old_flow_info.as_ref() else {
            return Ok(());
        };
        let Some(old_flow_routes) = self.data.old_flow_routes.as_ref() else {
            return Ok(());
        };

        // rollback metadata
        self.context
            .flow_metadata_manager
            .update_flow_metadata(
                flow_id,
                &current_flow_info,
                old_flow_info,
                old_flow_routes
                    .iter()
                    .map(|(k, v)| (*k, v.clone().into()))
                    .collect(),
            )
            .await?;

        Ok(())
    }

    /// Invalidate all table flow cache mapping if needed.
    async fn invalid_table_flow_cache_if_needed(&mut self) -> common_meta::error::Result<()> {
        // if cache invalid went wrong, then invalid all related entries just in case
        let old_flow_info = self.data.old_flow_info.as_ref().unwrap();
        let new_flow_info = self.data.new_flow_info.as_ref().unwrap();
        let flow_id = self.data.flow_id;

        let invalid_old = CacheIdent::DropFlow(DropFlow {
            flow_id,
            source_table_ids: old_flow_info.source_table_ids().to_vec(),
            flow_part2node_id: old_flow_info.flownode_ids().clone().into_iter().collect(),
        });

        let invalid_new = CacheIdent::DropFlow(DropFlow {
            flow_id,
            source_table_ids: new_flow_info.source_table_ids().to_vec(),
            flow_part2node_id: new_flow_info.flownode_ids().clone().into_iter().collect(),
        });

        let ctx = common_meta::cache_invalidator::Context {
                    subject: Some(
                        format!(
                            "Rollback after failed, invalid all related entries: Invalidate flow cache by migrating flow(id={}, partition={}) from flownode {:?} to {:?}",
                            flow_id,
                            self.data.partition_id,
                            self.data.src_flownode,
                            self.data.dest_flownode
                        )
                    ),
                };

        self.context
            .cache_invalidator
            .invalidate(
                &ctx,
                &[invalid_old, invalid_new, CacheIdent::FlowId(flow_id)],
            )
            .await?;
        Ok(())
    }

    async fn rollback_inner(&mut self) -> common_meta::error::Result<()> {
        match self.data.state {
            FlowMigrationState::Prepare => {
                // No need to rollback since no change was done
                Ok(())
            }
            FlowMigrationState::CreateFlowOnDest => {
                // drop flow on dest
                let dest_flownode = self.data.dest_flownode.clone();
                let flow_id = self.data.flow_id;
                let requester = self.context.node_manager.flownode(&dest_flownode).await;
                let drop_request = FlowRequest {
                    body: Some(flow_request::Body::Drop(DropRequest {
                        flow_id: Some(api::v1::FlowId { id: flow_id }),
                    })),
                    ..Default::default()
                };
                if let Err(err) = requester.handle(drop_request).await {
                    // for case when error happen before create flow
                    if err.status_code() != StatusCode::FlowNotFound {
                        return Err(add_peer_context_if_needed(dest_flownode.clone())(err));
                    }
                }
                Ok(())
            }
            FlowMigrationState::AlterMetadata => {
                // if the txns failed, nothing will change
                // if something else failed, rollback the metadata
                self.rollback_metadata_if_needed().await
            }
            FlowMigrationState::InvalidateFlowCache => {
                self.rollback_metadata_if_needed().await?;
                self.invalid_table_flow_cache_if_needed().await
            }
            FlowMigrationState::DropFlowOnSrc => {
                // shouldn't fail,
                // since both flush_flow&drop_flow
                // shouldn't fail/only fail with FlowNotFound Error which are safely ignored
                // if somehow still failed, rollback the metadata anyway
                self.rollback_metadata_if_needed().await?;
                self.invalid_table_flow_cache_if_needed().await
            }
        }
    }
}

#[async_trait::async_trait]
impl Procedure for FlowMigrationProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = common_meta::metrics::METRIC_META_PROCEDURE_CREATE_FLOW
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            FlowMigrationState::Prepare => self.on_prepare().await,
            FlowMigrationState::CreateFlowOnDest => self.on_create_flow_on_dest().await,
            FlowMigrationState::AlterMetadata => self.on_alter_metadata().await,
            FlowMigrationState::InvalidateFlowCache => self.on_broadcast().await,
            FlowMigrationState::DropFlowOnSrc => self.drop_flow_on_src_node().await,
        }
        .map_err(handle_retry_error)
    }

    async fn rollback(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<()> {
        self.rollback_inner()
            .await
            .map_err(ProcedureError::external)
    }

    fn rollback_supported(&self) -> bool {
        true
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(common_procedure::error::ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog_name = &self.data.catalog;
        let flow_id = self.data.flow_id;

        let lock_key = vec![
            CatalogLock::Read(catalog_name).into(),
            FlowLock::Write(flow_id).into(),
        ];

        LockKey::new(lock_key)
    }
}

fn info_to_create_request(flow_id: FlowId, value: &FlowInfoValue) -> CreateRequest {
    CreateRequest {
        flow_id: Some(api::v1::FlowId { id: flow_id }),
        source_table_ids: value
            .source_table_ids()
            .iter()
            .map(|table_id| api::v1::TableId { id: *table_id })
            .collect_vec(),
        sink_table_name: Some(value.sink_table_name().clone().into()),
        // Always be true to ensure idempotent in case of retry
        create_if_not_exists: true,
        or_replace: false,
        expire_after: value
            .expire_after()
            .map(|value| api::v1::ExpireAfter { value }),
        comment: value.comment().clone(),
        sql: value.raw_sql().clone(),
        flow_options: value.options().clone(),
    }
}
