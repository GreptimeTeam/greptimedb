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

use std::sync::Arc;

use api::v1::flow::FlowRequestHeader;
use async_trait::async_trait;
use common_base::AffectedRows;
use common_error::ext::BoxedError;
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::key::FlowId;
use common_meta::node_manager::{Flownode, FlownodeRef, NodeManagerRef};
use common_meta::rpc::procedure::{MigrateRegionRequest, ProcedureStateResponse};
use common_query::error::Result;
use common_query::Output;
use common_telemetry::tracing_context::TracingContext;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, TryStreamExt};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::requests::{CompactTableRequest, DeleteRequest, FlushTableRequest, InsertRequest};

/// A trait for handling table mutations in `QueryEngine`.
#[async_trait]
pub trait TableMutationHandler: Send + Sync {
    /// Inserts rows into the table.
    async fn insert(&self, request: InsertRequest, ctx: QueryContextRef) -> Result<Output>;

    /// Delete rows from the table.
    async fn delete(&self, request: DeleteRequest, ctx: QueryContextRef) -> Result<AffectedRows>;

    /// Trigger a flush task for table.
    async fn flush(&self, request: FlushTableRequest, ctx: QueryContextRef)
        -> Result<AffectedRows>;

    /// Trigger a compaction task for table.
    async fn compact(
        &self,
        request: CompactTableRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows>;

    /// Trigger a flush task for a table region.
    async fn flush_region(&self, region_id: RegionId, ctx: QueryContextRef)
        -> Result<AffectedRows>;

    /// Trigger a compaction task for a table region.
    async fn compact_region(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows>;
}

/// A trait for handling procedure service requests in `QueryEngine`.
#[async_trait]
pub trait ProcedureServiceHandler: Send + Sync {
    /// Migrate a region from source peer to target peer, returns the procedure id if success.
    async fn migrate_region(&self, request: MigrateRegionRequest) -> Result<Option<String>>;

    /// Query the procedure' state by its id
    async fn query_procedure_state(&self, pid: &str) -> Result<ProcedureStateResponse>;
}

/// This flow service handler is only use for flush flow for now.
#[async_trait]
pub trait FlowServiceHandler: Send + Sync {
    async fn flush(&self, id: FlowId, ctx: QueryContextRef) -> Result<api::v1::flow::FlowResponse>;
}

#[async_trait]
impl<T: ?Sized + Flownode> FlowServiceHandler for T {
    async fn flush(&self, id: FlowId, ctx: QueryContextRef) -> Result<api::v1::flow::FlowResponse> {
        use api::v1::flow::{flow_request, FlowRequest, FlushFlow};
        let flush_req = FlowRequest {
            header: Some(FlowRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                query_context: Some(common_meta::rpc::ddl::QueryContext::from(ctx.clone()).into()),
            }),
            body: Some(flow_request::Body::Flush(FlushFlow {
                flow_id: Some(api::v1::FlowId { id }),
            })),
        };
        self.handle(flush_req)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)
    }
}

pub struct FlowServiceBroadcast {
    flow_metadata_manager: FlowMetadataManagerRef,
    node_manager: NodeManagerRef,
}

impl FlowServiceBroadcast {
    pub fn new(
        flow_metadata_manager: FlowMetadataManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        Self {
            flow_metadata_manager,
            node_manager,
        }
    }
}

#[async_trait]
impl FlowServiceHandler for FlowServiceBroadcast {
    async fn flush(&self, id: FlowId, ctx: QueryContextRef) -> Result<api::v1::flow::FlowResponse> {
        self.flush_inner(id, ctx).await
    }
}

impl FlowServiceBroadcast {
    async fn flush_inner(
        &self,
        id: FlowId,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        let all_flownode_peers = self
            .flow_metadata_manager
            .flow_route_manager()
            .routes(id)
            .try_collect::<Vec<_>>()
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?;
        // order of flownodes doens't matter here
        let all_flow_nodes = FuturesUnordered::from_iter(
            all_flownode_peers
                .iter()
                .map(|(_key, peer)| self.node_manager.flownode(peer.peer())),
        )
        .collect::<Vec<_>>()
        .await;
        let mut final_result: Option<api::v1::flow::FlowResponse> = None;
        for node in all_flow_nodes {
            let res = node.flush(id, ctx.clone()).await?;
            if let Some(prev) = &mut final_result {
                prev.affected_rows = res.affected_rows;
                prev.affected_flows.extend(res.affected_flows);
                prev.extension.extend(res.extension);
            } else {
                final_result = Some(res);
            }
        }
        final_result.context(common_query::error::FlownodeNotFoundSnafu)
    }
}

pub type TableMutationHandlerRef = Arc<dyn TableMutationHandler>;

pub type ProcedureServiceHandlerRef = Arc<dyn ProcedureServiceHandler>;

pub type FlowServiceHandlerRef = Arc<dyn FlowServiceHandler>;
