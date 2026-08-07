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
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::node_manager::NodeManagerRef;
use common_query::error::Result;
use common_telemetry::tracing_context::TracingContext;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::utils::to_meta_query_context;

/// The operator for flow service which implements [`FlowServiceHandler`].
pub struct FlowServiceOperator {
    flow_metadata_manager: FlowMetadataManagerRef,
    node_manager: NodeManagerRef,
}

impl FlowServiceOperator {
    pub fn new(
        flow_metadata_manager: FlowMetadataManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        Self {
            flow_metadata_manager,
            node_manager,
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

        // order of flownodes doesn't matter here
        let all_flow_nodes = FuturesUnordered::from_iter(
            all_flownode_peers
                .iter()
                .map(|(_key, peer)| self.node_manager.flownode(peer.peer())),
        )
        .collect::<Vec<_>>()
        .await;

        let mut final_result: Option<api::v1::flow::FlowResponse> = None;
        for node in all_flow_nodes {
            let res = {
                use api::v1::flow::{FlowRequest, FlushFlow, flow_request};
                let flush_req = FlowRequest {
                    header: Some(FlowRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        query_context: Some(to_meta_query_context(ctx.clone()).into()),
                    }),
                    body: Some(flow_request::Body::Flush(FlushFlow {
                        flow_id: Some(api::v1::FlowId { id }),
                    })),
                };
                node.handle(flush_req)
                    .await
                    .map_err(BoxedError::new)
                    .context(common_query::error::ExecuteSnafu)?
            };

            if let Some(prev) = &mut final_result {
                prev.affected_rows += res.affected_rows;
                prev.affected_flows.extend(res.affected_flows);
                prev.extensions.extend(res.extensions);
            } else {
                final_result = Some(res);
            }
        }

        final_result.context(common_query::error::FlownodeNotFoundSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::FlowId;
    use api::v1::flow::{FlowRequest, FlowResponse};
    use api::v1::meta::Peer;
    use async_trait::async_trait;
    use common_meta::error::Result as MetaResult;
    use common_meta::key::flow::FlowMetadataManager;
    use common_meta::key::flow::flow_info::{FlowInfoValue, FlowStatus};
    use common_meta::key::flow::flow_route::FlowRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::node_manager::NodeManagerRef;
    use common_meta::test_util::{MockFlownodeHandler, MockFlownodeManager};
    use session::context::QueryContext;
    use table::table_name::TableName;

    use super::*;

    /// A mock flownode handler that returns a configurable `affected_rows` per flownode peer.
    #[derive(Clone)]
    struct MockFlushHandler {
        affected_rows: std::collections::HashMap<u64, u64>,
    }

    #[async_trait]
    impl MockFlownodeHandler for MockFlushHandler {
        async fn handle(&self, peer: &Peer, request: FlowRequest) -> MetaResult<FlowResponse> {
            // Sanity check: the flush request must target the expected flow id.
            let flow_id = match request.body {
                Some(api::v1::flow::flow_request::Body::Flush(flush)) => flush.flow_id.unwrap().id,
                _ => panic!("expected a flush flow request"),
            };
            let affected_rows = self
                .affected_rows
                .get(&peer.id)
                .copied()
                .unwrap_or_default();
            Ok(FlowResponse {
                header: None,
                affected_rows,
                affected_flows: vec![FlowId { id: flow_id }],
                extensions: [(format!("flownode-{}", peer.id), vec![peer.id as u8])]
                    .into_iter()
                    .collect(),
            })
        }
    }

    #[tokio::test]
    async fn flush_flow_merges_affected_rows_across_flownodes() {
        let flow_id: u32 = 42;
        let catalog = "greptime";
        let flow_name = "test_flow";

        let flow_metadata_manager =
            Arc::new(FlowMetadataManager::new(Arc::new(MemoryKvBackend::new())));
        let flow_info = FlowInfoValue {
            catalog_name: catalog.to_string(),
            query_context: None,
            flow_name: flow_name.to_string(),
            source_table_ids: vec![1024],
            all_source_table_names: vec![],
            unresolved_source_table_names: vec![],
            sink_table_name: TableName {
                catalog_name: catalog.to_string(),
                schema_name: "my_schema".to_string(),
                table_name: "sink_table".to_string(),
            },
            flownode_ids: [(0u32, 1u64), (1u32, 2u64)].into_iter().collect(),
            raw_sql: "SELECT * FROM source_table".to_string(),
            expire_after: None,
            eval_interval_secs: None,
            comment: String::new(),
            options: Default::default(),
            status: FlowStatus::Active,
            created_time: chrono::Utc::now(),
            updated_time: chrono::Utc::now(),
            eval_schedule: None,
        };
        // Two partitions routed to two different flownodes.
        let flow_routes = vec![
            (0u32, FlowRouteValue::from(Peer::new(1, "flownode-1"))),
            (1u32, FlowRouteValue::from(Peer::new(2, "flownode-2"))),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_info, flow_routes)
            .await
            .unwrap();

        let handler = MockFlushHandler {
            affected_rows: [(1, 100), (2, 200)].into_iter().collect(),
        };
        let node_manager: NodeManagerRef = Arc::new(MockFlownodeManager::new(handler));
        let operator = FlowServiceOperator::new(flow_metadata_manager, node_manager);

        let res = operator
            .flush_inner(catalog, flow_name, QueryContext::arc())
            .await
            .unwrap();

        // `affected_rows` must be the SUM across all flownodes, not the last response's value.
        assert_eq!(300, res.affected_rows);
        // `affected_flows` from every flownode are accumulated.
        assert_eq!(2, res.affected_flows.len());
        assert_eq!(2, res.extensions.len());
    }
}
