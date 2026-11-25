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
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_function::handlers::FlowServiceHandler;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::node_manager::{FlownodeRef, NodeManagerRef};
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

    async fn admin(
        &self,
        catalog: &str,
        flow: &str,
        function: &str,
        args: Vec<String>,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        use api::v1::flow::{AdminFlow, flow_request};

        let id = self.resolve_flow_id(catalog, flow).await?;
        let body = flow_request::Body::Admin(AdminFlow {
            catalog: catalog.to_string(),
            flow: flow.to_string(),
            function: function.to_string(),
            args,
        });

        self.handle_single_owner(id, ctx, body).await
    }
}

impl FlowServiceOperator {
    /// Resolves `catalog.flow` to its flow id.
    async fn resolve_flow_id(&self, catalog: &str, flow: &str) -> Result<u32> {
        Ok(self
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
            .flow_id())
    }

    /// Returns the flownodes that own the flow `id`.
    async fn owning_flownodes(&self, id: u32) -> Result<Vec<FlownodeRef>> {
        let all_flownode_peers = self
            .flow_metadata_manager
            .flow_route_manager()
            .routes(id)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?;

        // order of flownodes doesn't matter here
        Ok(FuturesUnordered::from_iter(
            all_flownode_peers
                .iter()
                .map(|(_key, peer)| self.node_manager.flownode(peer.peer())),
        )
        .collect::<Vec<_>>()
        .await)
    }

    /// Sends `body` to the one flownode that owns the flow `id`.
    ///
    /// A flow-admin call answers from process-local state of the execution that
    /// owns the flow (a backfill attempt and its status are the current
    /// functions). Broadcasting would create or read different attempts on
    /// different nodes and then merge them under the same extension key, so a
    /// multi-owner flow is refused before any RPC is sent.
    async fn handle_single_owner(
        &self,
        id: u32,
        ctx: QueryContextRef,
        body: api::v1::flow::flow_request::Body,
    ) -> Result<api::v1::flow::FlowResponse> {
        use api::v1::flow::FlowRequest;

        let nodes = self.owning_flownodes(id).await?;
        if nodes.len() != 1 {
            return Err(BoxedError::new(PlainError::new(
                format!(
                    "flow {id} is owned by {} flownodes; a flow admin call requires exactly one",
                    nodes.len()
                ),
                StatusCode::InvalidArguments,
            )))
            .context(common_query::error::ExecuteSnafu);
        }
        let node = nodes
            .into_iter()
            .next()
            .context(common_query::error::FlownodeNotFoundSnafu)?;
        let req = FlowRequest {
            header: Some(FlowRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                query_context: Some(to_meta_query_context(ctx).into()),
            }),
            body: Some(body),
        };
        node.handle(req)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)
    }

    /// Flush the flownodes according to the flow id.
    async fn flush_inner(
        &self,
        catalog: &str,
        flow: &str,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        let id = self.resolve_flow_id(catalog, flow).await?;

        let all_flow_nodes = self.owning_flownodes(id).await?;

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use api::v1::flow::{AdminFlow, FlowRequest, FlowResponse, flow_request};
    use async_trait::async_trait;
    use common_meta::key::flow::FlowMetadataManager;
    use common_meta::key::flow::flow_info::FlowInfoValue;
    use common_meta::key::flow::flow_route::FlowRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use common_meta::test_util::{MockFlownodeHandler, MockFlownodeManager};
    use session::context::QueryContext;
    use table::table_name::TableName;

    use super::*;

    /// The flow id the test flow `greptime.demo` resolves to.
    const FLOW_ID: u32 = 42;

    /// Records the requests a routed call sends and answers with canned extensions.
    #[derive(Clone, Default)]
    struct RecordingFlownode {
        requests: Arc<Mutex<Vec<FlowRequest>>>,
        extensions: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    impl RecordingFlownode {
        fn answer_with(&self, key: &str, value: &str) {
            self.extensions
                .lock()
                .unwrap()
                .insert(key.to_string(), value.as_bytes().to_vec());
        }

        fn take_requests(&self) -> Vec<FlowRequest> {
            std::mem::take(&mut *self.requests.lock().unwrap())
        }
    }

    #[async_trait]
    impl MockFlownodeHandler for RecordingFlownode {
        async fn handle(
            &self,
            _peer: &Peer,
            request: FlowRequest,
        ) -> common_meta::error::Result<FlowResponse> {
            self.requests.lock().unwrap().push(request);
            Ok(FlowResponse {
                extensions: self.extensions.lock().unwrap().clone(),
                ..Default::default()
            })
        }
    }

    /// Builds an operator over the flow `greptime.demo` (id 42) with `routes`.
    async fn new_operator_with_routes(
        handler: RecordingFlownode,
        routes: Vec<(u32, Peer)>,
    ) -> FlowServiceOperator {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend));
        let flownode_ids = routes.iter().map(|(id, _)| (*id, 1u64)).collect();
        flow_metadata_manager
            .create_flow_metadata(
                FLOW_ID,
                FlowInfoValue {
                    source_table_ids: vec![],
                    all_source_table_names: vec![],
                    unresolved_source_table_names: vec![],
                    sink_table_name: TableName::new("greptime", "public", "demo_sink"),
                    flownode_ids,
                    catalog_name: "greptime".to_string(),
                    query_context: None,
                    flow_name: "demo".to_string(),
                    raw_sql: "SELECT 1".to_string(),
                    expire_after: None,
                    eval_interval_secs: None,
                    comment: String::new(),
                    options: Default::default(),
                    status: Default::default(),
                    created_time: Default::default(),
                    updated_time: Default::default(),
                    eval_schedule: None,
                },
                routes
                    .into_iter()
                    .map(|(id, peer)| (id, FlowRouteValue::from(peer)))
                    .collect(),
            )
            .await
            .unwrap();

        FlowServiceOperator::new(
            flow_metadata_manager,
            Arc::new(MockFlownodeManager::new(handler)),
        )
    }

    /// Builds an operator over the flow `greptime.demo` routed to flownode 1.
    async fn new_operator(handler: RecordingFlownode) -> FlowServiceOperator {
        new_operator_with_routes(handler, vec![(1, Peer::empty(1))]).await
    }

    #[tokio::test]
    async fn test_admin_routes_the_request_and_keeps_the_extensions() {
        let handler = RecordingFlownode::default();
        handler.answer_with("flow_admin_result", "attempt-1");
        let operator = new_operator(handler.clone()).await;

        let response = operator
            .admin(
                "greptime",
                "demo",
                "flow_backfill",
                vec!["-1000".to_string(), "1000".to_string()],
                QueryContext::arc(),
            )
            .await
            .unwrap();

        assert_eq!(
            Some(&b"attempt-1".to_vec()),
            response.extensions.get("flow_admin_result")
        );

        let requests = handler.take_requests();
        assert_eq!(1, requests.len());
        assert!(
            requests[0]
                .header
                .as_ref()
                .and_then(|header| header.query_context.as_ref())
                .is_some(),
            "the query context must be carried to the flownode"
        );
        match requests[0].body.as_ref().unwrap() {
            flow_request::Body::Admin(AdminFlow {
                catalog,
                flow,
                function,
                args,
            }) => {
                assert_eq!("greptime", catalog);
                assert_eq!("demo", flow);
                assert_eq!("flow_backfill", function);
                assert_eq!(&vec!["-1000".to_string(), "1000".to_string()], args);
            }
            other => panic!("unexpected request body: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_admin_refuses_a_multi_owner_flow_before_sending() {
        let handler = RecordingFlownode::default();
        let operator = new_operator_with_routes(
            handler.clone(),
            vec![(1, Peer::empty(1)), (2, Peer::empty(2))],
        )
        .await;

        let err = operator
            .admin(
                "greptime",
                "demo",
                "flow_backfill",
                vec!["0".to_string(), "1000".to_string()],
                QueryContext::arc(),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("requires exactly one"),
            "multi-owner admin call must be refused: {err}"
        );
        assert!(
            handler.take_requests().is_empty(),
            "the refusal must happen before any flownode RPC"
        );
    }

    #[tokio::test]
    async fn test_admin_status_carries_the_function_and_no_args() {
        let handler = RecordingFlownode::default();
        handler.answer_with("flow_admin_result", "null");
        let operator = new_operator(handler.clone()).await;

        let response = operator
            .admin(
                "greptime",
                "demo",
                "flow_backfill_status",
                vec![],
                QueryContext::arc(),
            )
            .await
            .unwrap();

        assert_eq!(
            Some(&b"null".to_vec()),
            response.extensions.get("flow_admin_result")
        );

        let requests = handler.take_requests();
        assert_eq!(1, requests.len());
        match requests[0].body.as_ref().unwrap() {
            flow_request::Body::Admin(AdminFlow {
                catalog,
                flow,
                function,
                args,
            }) => {
                assert_eq!("greptime", catalog);
                assert_eq!("demo", flow);
                assert_eq!("flow_backfill_status", function);
                assert!(args.is_empty(), "a status call carries no args: {args:?}");
            }
            other => panic!("unexpected request body: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_admin_unknown_flow_is_an_execute_error() {
        let handler = RecordingFlownode::default();
        let operator = new_operator(handler.clone()).await;

        for response in [
            operator
                .admin(
                    "greptime",
                    "missing",
                    "flow_backfill",
                    vec!["0".to_string(), "1".to_string()],
                    QueryContext::arc(),
                )
                .await
                .unwrap_err(),
            operator
                .admin(
                    "greptime",
                    "missing",
                    "flow_backfill_status",
                    vec![],
                    QueryContext::arc(),
                )
                .await
                .unwrap_err(),
        ] {
            assert!(
                matches!(response, common_query::error::Error::Execute { .. }),
                "{response}"
            );
        }
        // Nothing is routed for a flow that does not exist.
        assert!(handler.take_requests().is_empty());
    }
}
