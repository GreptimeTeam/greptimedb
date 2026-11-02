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

use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use common_procedure::ProcedureContext;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::key::NodeSchedulingRole;
use crate::metasrv::Metasrv;
use crate::procedure::drain_datanode::{
    DEFAULT_PARALLELISM, DrainDatanodeProcedureTask, DrainDatanodeStrategy,
};
use crate::service::admin::util;

#[derive(Debug, Deserialize)]
pub(crate) struct DrainDatanodeRequest {
    node_id: u64,
    target_node_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DatanodeRequest {
    node_id: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct DrainDatanodeResponse {
    procedure_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DatanodeSchedulingResponse {
    node_id: u64,
    schedulable: bool,
}

/// Submits an asynchronous leader-region drain. Retries reuse the running procedure.
#[axum_macros::debug_handler]
pub(crate) async fn drain(
    State(metasrv): State<Arc<Metasrv>>,
    Json(request): Json<DrainDatanodeRequest>,
) -> Response {
    async {
        util::ensure_leader(&metasrv)?;
        let procedure_id = metasrv
            .drain_datanode_manager()
            .submit_procedure(
                ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: request.node_id,
                    target_node_id: request.target_node_id,
                    parallelism: DEFAULT_PARALLELISM,
                    strategy: DrainDatanodeStrategy::default(),
                    timeout: None,
                },
            )
            .await?;
        Ok::<_, crate::error::Error>(DrainDatanodeResponse {
            procedure_id: procedure_id.map(|id| id.to_string()),
        })
    }
    .await
    .map(Json)
    .map_err(util::admin_error_response)
    .into_response()
}

/// Restores scheduling; an active drain must finish before the node can receive regions.
#[axum_macros::debug_handler]
pub(crate) async fn mark_schedulable(
    State(metasrv): State<Arc<Metasrv>>,
    Json(request): Json<DatanodeRequest>,
) -> Response {
    async {
        util::ensure_leader(&metasrv)?;
        metasrv
            .drain_datanode_manager()
            .mark_schedulable(request.node_id)
            .await?;
        Ok::<_, crate::error::Error>(DatanodeSchedulingResponse {
            node_id: request.node_id,
            schedulable: true,
        })
    }
    .await
    .map(Json)
    .map_err(util::admin_error_response)
    .into_response()
}

/// Reads the node's scheduling state from the leader.
#[axum_macros::debug_handler]
pub(crate) async fn scheduling_status(
    State(metasrv): State<Arc<Metasrv>>,
    Query(request): Query<DatanodeRequest>,
) -> Response {
    scheduling_status_inner(&metasrv, request.node_id)
        .await
        .map(Json)
        .map_err(util::admin_error_response)
        .into_response()
}

async fn scheduling_status_inner(
    metasrv: &Metasrv,
    node_id: u64,
) -> Result<DatanodeSchedulingResponse> {
    util::ensure_leader(metasrv)?;
    let schedulable = metasrv
        .node_scheduling_manager()
        .is_schedulable(NodeSchedulingRole::Datanode, node_id)
        .await?;
    Ok(DatanodeSchedulingResponse {
        node_id,
        schedulable,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request, StatusCode};
    use common_meta::election::{Election, LeaderChangeMessage, LeaderValue, MetasrvNodeInfo};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::metasrv::builder::MetasrvBuilder;
    use crate::service::admin::admin_axum_router;

    struct TestElection(AtomicBool);

    #[async_trait::async_trait]
    impl Election for TestElection {
        type Leader = LeaderValue;
        fn is_leader(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }
        fn in_leader_infancy(&self) -> bool {
            false
        }
        async fn register_candidate(&self, _: &MetasrvNodeInfo) -> common_meta::error::Result<()> {
            Ok(())
        }
        async fn all_candidates(&self) -> common_meta::error::Result<Vec<MetasrvNodeInfo>> {
            Ok(vec![])
        }
        async fn campaign(&self) -> common_meta::error::Result<()> {
            Ok(())
        }
        async fn leader(&self) -> common_meta::error::Result<LeaderValue> {
            Ok(LeaderValue("leader:3002".into()))
        }
        async fn resign(&self) -> common_meta::error::Result<()> {
            Ok(())
        }
        fn subscribe_leader_change(&self) -> tokio::sync::broadcast::Receiver<LeaderChangeMessage> {
            tokio::sync::broadcast::channel(1).1
        }
    }

    async fn request(app: Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn test_http_scheduling_and_completed_drain_are_idempotent() {
        let metasrv = Arc::new(MetasrvBuilder::new().build().await.unwrap());
        let app = admin_axum_router(metasrv.clone());
        metasrv
            .node_scheduling_manager()
            .mark_unschedulable(NodeSchedulingRole::Datanode, 1)
            .await
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                (StatusCode::OK, json!({"procedure_id": null})),
                request(
                    app.clone(),
                    Method::POST,
                    "/admin/datanode/drain",
                    json!({"node_id": 1})
                )
                .await
            );
        }
        for _ in 0..2 {
            assert_eq!(
                (StatusCode::OK, json!({"node_id": 1, "schedulable": true})),
                request(
                    app.clone(),
                    Method::POST,
                    "/admin/datanode/mark-schedulable",
                    json!({"node_id": 1})
                )
                .await
            );
        }
        assert_eq!(
            (StatusCode::OK, json!({"node_id": 1, "schedulable": true})),
            request(
                app.clone(),
                Method::GET,
                "/admin/datanode/scheduling/status?node_id=1",
                Value::Null
            )
            .await
        );
        assert!(
            metasrv
                .procedure_manager()
                .list_procedures()
                .await
                .unwrap()
                .is_empty()
        );
        let (status, _) = request(
            app,
            Method::POST,
            "/admin/datanode/drain",
            json!({"node_id": 1, "target_node_id": 1}),
        )
        .await;
        assert_eq!(StatusCode::BAD_REQUEST, status);
    }

    #[tokio::test]
    async fn test_http_follower_rejects_admin_operations_without_side_effects() {
        let election = Arc::new(TestElection(AtomicBool::new(false)));
        let metasrv = Arc::new(
            MetasrvBuilder::new()
                .election(Some(election.clone()))
                .build()
                .await
                .unwrap(),
        );
        let app = admin_axum_router(metasrv.clone());
        metasrv
            .node_scheduling_manager()
            .mark_unschedulable(NodeSchedulingRole::Datanode, 1)
            .await
            .unwrap();
        for (method, uri, body) in [
            (
                Method::POST,
                "/admin/datanode/drain",
                json!({"node_id": 1, "target_node_id": 2}),
            ),
            (
                Method::POST,
                "/admin/datanode/mark-schedulable",
                json!({"node_id": 1}),
            ),
            (
                Method::GET,
                "/admin/datanode/scheduling/status?node_id=1",
                Value::Null,
            ),
            (
                Method::GET,
                "/admin/procedure/status?procedure_id=00000000-0000-0000-0000-000000000001",
                Value::Null,
            ),
        ] {
            let (status, body) = request(app.clone(), method, uri, body).await;
            assert_eq!(StatusCode::SERVICE_UNAVAILABLE, status, "{body}");
            assert!(body["error"].as_str().unwrap().contains("not leader"));
        }
        assert!(
            metasrv
                .procedure_manager()
                .list_procedures()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            !metasrv
                .node_scheduling_manager()
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
        election.0.store(true, Ordering::Relaxed);
        assert_eq!(
            (StatusCode::OK, json!({"node_id": 1, "schedulable": true})),
            request(
                app,
                Method::POST,
                "/admin/datanode/mark-schedulable",
                json!({"node_id": 1})
            )
            .await
        );
    }

    #[tokio::test]
    async fn test_http_procedure_status_and_request_validation() {
        let metasrv = Arc::new(MetasrvBuilder::new().build().await.unwrap());
        metasrv.procedure_manager().start().await.unwrap();
        let app = admin_axum_router(metasrv.clone());
        let (status, body) = request(
            app.clone(),
            Method::POST,
            "/admin/datanode/drain",
            json!({"node_id": 1}),
        )
        .await;
        assert_eq!(StatusCode::OK, status, "{body}");
        let pid = body["procedure_id"].as_str().unwrap();
        let uri = format!("/admin/procedure/status?procedure_id={pid}");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let (status, body) = request(app.clone(), Method::GET, &uri, Value::Null).await;
                assert_eq!(StatusCode::OK, status);
                assert_eq!(pid, body["procedure_id"]);
                if body["status"] == "Done" {
                    assert_eq!(
                        json!({"procedure_id": pid, "status": "Done", "error": null}),
                        body
                    );
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for (uri, expected) in [
            (
                "/admin/procedure/status?procedure_id=bad",
                StatusCode::BAD_REQUEST,
            ),
            (
                "/admin/procedure/status?procedure_id=00000000-0000-0000-0000-000000000001",
                StatusCode::NOT_FOUND,
            ),
        ] {
            assert_eq!(
                expected,
                request(app.clone(), Method::GET, uri, Value::Null).await.0
            );
        }
        for (method, uri, body, expected) in [
            (
                Method::GET,
                "/admin/datanode/drain",
                json!({"node_id": 1}),
                StatusCode::METHOD_NOT_ALLOWED,
            ),
            (
                Method::POST,
                "/admin/datanode/drain",
                json!({"node_id": -1}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                Method::POST,
                "/admin/datanode/drain",
                json!({}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                Method::GET,
                "/admin/datanode/scheduling/status",
                Value::Null,
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(expected, response.status());
        }
        metasrv.procedure_manager().stop().await.unwrap();
    }
}
