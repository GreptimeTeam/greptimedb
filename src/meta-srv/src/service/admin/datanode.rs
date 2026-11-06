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
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use common_procedure::ProcedureContext;
use serde::{Deserialize, Serialize};
use servers::http::result::error_result::ErrorResponse;

use crate::key::NodeSchedulingRole;
use crate::metasrv::Metasrv;
use crate::procedure::drain_datanode::{
    DEFAULT_DRAIN_TIMEOUT, DEFAULT_PARALLELISM, DrainDatanodeProcedureTask, DrainDatanodeStrategy,
};
use crate::service::admin::util;

#[derive(Debug, Deserialize)]
pub(crate) struct DrainDatanodeRequest {
    node_id: u64,
    target_node_id: Option<u64>,
    /// Concurrent region budget; defaults to [`DEFAULT_PARALLELISM`].
    #[serde(default = "default_parallelism")]
    parallelism: usize,
    /// Failure handling; defaults to [`DrainDatanodeStrategy::TryAll`].
    #[serde(default)]
    strategy: DrainStrategy,
    /// Whole-drain budget; defaults to [`DEFAULT_DRAIN_TIMEOUT`].
    #[serde(default = "default_timeout", with = "humantime_serde")]
    timeout: Duration,
}

/// HTTP strategy names follow the admin API's snake_case convention.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DrainStrategy {
    FailFast,
    #[default]
    TryAll,
}

impl From<DrainStrategy> for DrainDatanodeStrategy {
    fn from(strategy: DrainStrategy) -> Self {
        match strategy {
            DrainStrategy::FailFast => Self::FailFast,
            DrainStrategy::TryAll => Self::TryAll,
        }
    }
}

impl From<DrainDatanodeRequest> for DrainDatanodeProcedureTask {
    fn from(request: DrainDatanodeRequest) -> Self {
        Self {
            node_id: request.node_id,
            target_node_id: request.target_node_id,
            parallelism: request.parallelism,
            strategy: request.strategy.into(),
            timeout: Some(request.timeout),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct DatanodeRequest {
    node_id: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct DrainDatanodeResponse {
    procedure_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct DatanodeSchedulingResponse {
    node_id: u64,
    schedulable: bool,
}

fn default_parallelism() -> usize {
    DEFAULT_PARALLELISM
}

fn default_timeout() -> Duration {
    DEFAULT_DRAIN_TIMEOUT
}

/// Submits an asynchronous leader-region drain.
/// Rejects requests for a datanode already being drained.
#[axum_macros::debug_handler]
pub(crate) async fn drain(
    State(metasrv): State<Arc<Metasrv>>,
    Json(request): Json<DrainDatanodeRequest>,
) -> Response {
    async {
        util::ensure_leader(&metasrv)?;
        let procedure_id = metasrv
            .drain_datanode_manager()
            .submit_procedure(ProcedureContext::default(), request.into())
            .await?;
        Ok::<_, crate::error::Error>(DrainDatanodeResponse {
            procedure_id: procedure_id.to_string(),
        })
    }
    .await
    .map(Json)
    .map_err(ErrorResponse::from_error)
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
    .map_err(ErrorResponse::from_error)
    .into_response()
}

/// Reads the node's scheduling state from the leader.
#[axum_macros::debug_handler]
pub(crate) async fn scheduling_status(
    State(metasrv): State<Arc<Metasrv>>,
    Query(request): Query<DatanodeRequest>,
) -> Response {
    async {
        util::ensure_leader(&metasrv)?;
        let schedulable = metasrv
            .node_scheduling_manager()
            .is_schedulable(NodeSchedulingRole::Datanode, request.node_id)
            .await?;
        Ok::<_, crate::error::Error>(DatanodeSchedulingResponse {
            node_id: request.node_id,
            schedulable,
        })
    }
    .await
    .map(Json)
    .map_err(ErrorResponse::from_error)
    .into_response()
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

    #[test]
    fn test_drain_request_options() {
        for (body, parallelism, strategy) in [
            (json!({"node_id": 1}), 4, DrainDatanodeStrategy::TryAll),
            (
                json!({"node_id": 1, "parallelism": 8}),
                8,
                DrainDatanodeStrategy::TryAll,
            ),
            (
                json!({"node_id": 1, "strategy": "fail_fast"}),
                4,
                DrainDatanodeStrategy::FailFast,
            ),
            (
                json!({"node_id": 1, "parallelism": 1, "strategy": "fail_fast"}),
                1,
                DrainDatanodeStrategy::FailFast,
            ),
            (
                json!({"node_id": 1, "parallelism": 32, "strategy": "try_all"}),
                32,
                DrainDatanodeStrategy::TryAll,
            ),
        ] {
            let request: DrainDatanodeRequest = serde_json::from_value(body).unwrap();
            let task: DrainDatanodeProcedureTask = request.into();
            assert_eq!(parallelism, task.parallelism);
            assert_eq!(strategy, task.strategy);
            assert_eq!(1, task.node_id);
            assert_eq!(None, task.target_node_id);
            assert_eq!(Some(Duration::from_secs(20 * 60)), task.timeout);
        }
    }

    #[test]
    fn test_drain_request_timeout() {
        for (value, expected) in [
            (json!("30m"), Some(Duration::from_secs(1800))),
            (json!("1h 30m"), Some(Duration::from_secs(5400))),
            (json!("500ms"), Some(Duration::from_millis(500))),
        ] {
            let request: DrainDatanodeRequest =
                serde_json::from_value(json!({"node_id": 1, "timeout": value})).unwrap();
            let task: DrainDatanodeProcedureTask = request.into();
            assert_eq!(expected, task.timeout);
        }
    }

    #[tokio::test]
    async fn test_http_drain_rejects_invalid_options() {
        let metasrv = Arc::new(MetasrvBuilder::new().build().await.unwrap());
        metasrv.procedure_manager().start().await.unwrap();
        let app = admin_axum_router(metasrv.clone());
        for (options, expected) in [
            (json!({"timeout": null}), StatusCode::UNPROCESSABLE_ENTITY),
            (
                json!({"parallelism": null}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (json!({"strategy": null}), StatusCode::BAD_REQUEST),
            (json!({"timeout": "0s"}), StatusCode::BAD_REQUEST),
            (
                json!({"timeout": "invalid"}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (json!({"timeout": "-1s"}), StatusCode::UNPROCESSABLE_ENTITY),
            (json!({"parallelism": 0}), StatusCode::BAD_REQUEST),
            (json!({"parallelism": 33}), StatusCode::BAD_REQUEST),
            (json!({"parallelism": -1}), StatusCode::UNPROCESSABLE_ENTITY),
            (
                json!({"parallelism": 1.5}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                json!({"strategy": "unknown"}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                json!({"strategy": "FailFast"}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
        ] {
            let mut body = options;
            body["node_id"] = json!(1);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/admin/datanode/drain")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(expected, response.status(), "{body}");
        }
        assert!(
            metasrv
                .procedure_manager()
                .list_procedures()
                .await
                .unwrap()
                .is_empty()
        );
        metasrv.procedure_manager().stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_http_scheduling_is_idempotent() {
        let metasrv = Arc::new(MetasrvBuilder::new().build().await.unwrap());
        let app = admin_axum_router(metasrv.clone());
        metasrv.procedure_manager().start().await.unwrap();
        metasrv
            .node_scheduling_manager()
            .mark_unschedulable(NodeSchedulingRole::Datanode, 1)
            .await
            .unwrap();
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
        let (status, _) = request(
            app,
            Method::POST,
            "/admin/datanode/drain",
            json!({"node_id": 1, "target_node_id": 1}),
        )
        .await;
        assert_eq!(StatusCode::BAD_REQUEST, status);
        metasrv.procedure_manager().stop().await.unwrap();
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
        metasrv.procedure_manager().start().await.unwrap();
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
            assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, status, "{body}");
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
        metasrv.procedure_manager().stop().await.unwrap();
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
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let (status, body) = request(app.clone(), Method::GET, uri, Value::Null).await;
            assert_eq!(expected, status);
            if uri.ends_with("=bad") {
                assert!(
                    body["error"]
                        .as_str()
                        .unwrap()
                        .contains("invalid procedure id: bad")
                );
            }
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
