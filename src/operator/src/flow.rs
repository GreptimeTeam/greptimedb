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

use api::v1::flow::FlowRequestHeader;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_function::handlers::FlowServiceHandler;
use common_meta::flow_rpc::FlowRpcRef;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_query::error::Result;
use common_telemetry::tracing_context::TracingContext;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

/// The operator for flow service which implements [`FlowServiceHandler`].
pub struct FlowServiceOperator {
    flow_metadata_manager: FlowMetadataManagerRef,
    flow_rpc: FlowRpcRef,
}

impl FlowServiceOperator {
    pub fn new(flow_metadata_manager: FlowMetadataManagerRef, flow_rpc: FlowRpcRef) -> Self {
        Self {
            flow_metadata_manager,
            flow_rpc,
        }
    }

    pub fn flow_metadata_manager(&self) -> FlowMetadataManagerRef {
        self.flow_metadata_manager.clone()
    }
}

#[async_trait]
impl FlowServiceHandler for FlowServiceOperator {
    async fn flush(
        &self,
        catalog: &str,
        flow: &str,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        self.flush_inner(catalog, flow, ctx).await
    }
}

impl FlowServiceOperator {
    /// Flush the flownodes according to the flow id.
    async fn flush_inner(
        &self,
        catalog: &str,
        flow: &str,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        let id = self
            .flow_metadata_manager
            .flow_name_manager()
            .get(catalog, flow)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?
            .context(common_meta::error::FlowNotFoundSnafu {
                flow_name: format!("{}.{}", catalog, flow),
            })
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?
            .flow_id();

        let all_flownode_peers = self
            .flow_metadata_manager
            .flow_route_manager()
            .routes(id)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?;

        let mut final_result: Option<api::v1::flow::FlowResponse> = None;
        for (_key, peer) in all_flownode_peers.iter() {
            let peer = peer.peer();
            let res = {
                use api::v1::flow::{FlowRequest, FlushFlow, flow_request};
                let flush_req = FlowRequest {
                    header: Some(FlowRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        query_context: Some(
                            common_meta::rpc::ddl::QueryContext::from(ctx.clone()).into(),
                        ),
                    }),
                    body: Some(flow_request::Body::Flush(FlushFlow {
                        flow_id: Some(api::v1::FlowId { id }),
                    })),
                };
                self.flow_rpc
                    .handle_flow(peer, flush_req)
                    .await
                    .map_err(BoxedError::new)
                    .context(common_query::error::ExecuteSnafu)?
            };

            if let Some(prev) = &mut final_result {
                prev.affected_rows = res.affected_rows;
                prev.affected_flows.extend(res.affected_flows);
                prev.extensions.extend(res.extensions);
            } else {
                final_result = Some(res);
            }
        }

        final_result.context(common_query::error::FlownodeNotFoundSnafu)
    }
}
