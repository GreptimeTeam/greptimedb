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

pub(crate) mod health;
pub(crate) mod heartbeat;
pub(crate) mod leader;
pub(crate) mod maintenance;
pub(crate) mod node_lease;
pub(crate) mod procedure;
pub(crate) mod recovery;
pub(crate) mod sequencer;
mod util;

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing, Router as AxumRouter};
use tonic::body::Body;
use tonic::codegen::{http, BoxFuture, Service};
use tonic::server::NamedService;

use crate::metasrv::Metasrv;
use crate::service::admin::health::HealthHandler;
use crate::service::admin::heartbeat::HeartBeatHandler;
use crate::service::admin::leader::LeaderHandler;
use crate::service::admin::maintenance::MaintenanceHandler;
use crate::service::admin::node_lease::NodeLeaseHandler;
use crate::service::admin::procedure::ProcedureManagerHandler;
use crate::service::admin::recovery::{
    get_recovery_mode, set_recovery_mode, unset_recovery_mode, RecoveryHandler,
};
use crate::service::admin::sequencer::TableIdSequenceHandler;
use crate::service::admin::util::{to_axum_json_response, to_axum_not_found_response};

pub fn make_admin_service(metasrv: Arc<Metasrv>) -> Admin {
    let router = Router::new().route("/health", health::HealthHandler);

    let router = router.route(
        "/node-lease",
        node_lease::NodeLeaseHandler {
            meta_peer_client: metasrv.meta_peer_client().clone(),
        },
    );

    let handler = heartbeat::HeartBeatHandler {
        meta_peer_client: metasrv.meta_peer_client().clone(),
    };
    let router = router
        .route("/heartbeat", handler.clone())
        .route("/heartbeat/help", handler);

    let router = router.route(
        "/leader",
        leader::LeaderHandler {
            election: metasrv.election().cloned(),
        },
    );

    let router = router.routes(
        &[
            "/maintenance",
            "/maintenance/status",
            "/maintenance/enable",
            "/maintenance/disable",
        ],
        maintenance::MaintenanceHandler {
            manager: metasrv.runtime_switch_manager().clone(),
        },
    );
    let router = router.routes(
        &[
            "/procedure-manager/pause",
            "/procedure-manager/resume",
            "/procedure-manager/status",
        ],
        procedure::ProcedureManagerHandler {
            manager: metasrv.runtime_switch_manager().clone(),
        },
    );
    let router = Router::nest("/admin", router);

    Admin::new(router)
}

#[async_trait::async_trait]
pub trait HttpHandler: Send + Sync {
    async fn handle(
        &self,
        path: &str,
        method: http::Method,
        params: &HashMap<String, String>,
    ) -> crate::Result<http::Response<String>>;
}

#[derive(Clone)]
pub struct Admin
where
    Self: Send,
{
    router: Arc<Router>,
}

impl Admin {
    pub fn new(router: Router) -> Self {
        Self {
            router: Arc::new(router),
        }
    }
}

impl NamedService for Admin {
    const NAME: &'static str = "admin";
}

impl Service<http::Request<Body>> for Admin {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let router = self.router.clone();
        let query_params = req
            .uri()
            .query()
            .map(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .into_owned()
                    .collect()
            })
            .unwrap_or_default();
        let path = req.uri().path().to_owned();
        let method = req.method().clone();
        Box::pin(async move { router.call(&path, method, query_params).await })
    }
}

#[derive(Default)]
pub struct Router {
    handlers: HashMap<String, Arc<dyn HttpHandler>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::default(),
        }
    }

    pub fn nest(path: &str, router: Router) -> Self {
        check_path(path);

        let handlers = router
            .handlers
            .into_iter()
            .map(|(url, handler)| (format!("{path}{url}"), handler))
            .collect();

        Self { handlers }
    }

    pub fn route(mut self, path: &str, handler: impl HttpHandler + 'static) -> Self {
        check_path(path);

        let _ = self.handlers.insert(path.to_owned(), Arc::new(handler));

        self
    }

    pub fn routes(mut self, paths: &[&str], handler: impl HttpHandler + 'static) -> Self {
        let handler = Arc::new(handler);
        for path in paths {
            check_path(path);
            let _ = self.handlers.insert(path.to_string(), handler.clone());
        }

        self
    }

    pub async fn call(
        &self,
        path: &str,
        method: http::Method,
        params: HashMap<String, String>,
    ) -> Result<http::Response<Body>, Infallible> {
        let handler = match self.handlers.get(path) {
            Some(handler) => handler,
            None => {
                return Ok(http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap())
            }
        };

        let res = match handler.handle(path, method, &params).await {
            Ok(res) => res.map(Body::new),
            Err(e) => http::Response::builder()
                .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::new(e.to_string()))
                .unwrap(),
        };

        Ok(res)
    }
}

fn check_path(path: &str) {
    if path.is_empty() || !path.starts_with('/') {
        panic!("paths must start with a `/`")
    }
}

/// Expose admin HTTP endpoints as an Axum router for the main HTTP server.
pub fn admin_axum_router(metasrv: Arc<Metasrv>) -> AxumRouter {
    let node_lease_handler = Arc::new(NodeLeaseHandler {
        meta_peer_client: metasrv.meta_peer_client().clone(),
    });
    let heartbeat_handler = Arc::new(HeartBeatHandler {
        meta_peer_client: metasrv.meta_peer_client().clone(),
    });
    let leader_handler = Arc::new(LeaderHandler {
        election: metasrv.election().cloned(),
    });
    let maintenance_handler = Arc::new(MaintenanceHandler {
        manager: metasrv.runtime_switch_manager().clone(),
    });
    let procedure_handler = Arc::new(ProcedureManagerHandler {
        manager: metasrv.runtime_switch_manager().clone(),
    });
    let recovery_handler = Arc::new(RecoveryHandler {
        manager: metasrv.runtime_switch_manager().clone(),
    });
    let table_id_sequence_handler = Arc::new(TableIdSequenceHandler {
        table_id_sequence: metasrv.table_id_sequence().clone(),
        runtime_switch_manager: metasrv.runtime_switch_manager().clone(),
    });
    let sequence_router = AxumRouter::new().nest(
        "/table",
        AxumRouter::new()
            .route("/next-id", routing::get(sequencer::get_next_table_id))
            .route("/set-next-id", routing::post(sequencer::set_next_table_id))
            .with_state(table_id_sequence_handler),
    );

    let health_router = AxumRouter::new().route(
        "/",
        routing::get({
            move || {
                let handler = HealthHandler;
                async move {
                    match handler
                        .handle("/health", http::Method::GET, &Default::default())
                        .await
                    {
                        Ok(status) => status.body().clone().into_response(),
                        Err(e) => {
                            common_telemetry::error!(e; "Health handler failed");
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                }
            }
        }),
    );

    let node_lease_router = AxumRouter::new().route(
        "/",
        routing::get({
            let handler = node_lease_handler.clone();
            move || async move {
                match handler
                    .handle("/node-lease", http::Method::GET, &Default::default())
                    .await
                {
                    Ok(resp) => resp.body().clone().into_response(),
                    Err(e) => {
                        common_telemetry::error!(e; "Node lease handler failed");
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }
        }),
    );

    let leader_router = AxumRouter::new().route(
        "/",
        routing::get({
            let handler = leader_handler.clone();
            move || async move {
                match handler
                    .handle("/leader", http::Method::GET, &Default::default())
                    .await
                {
                    Ok(resp) => resp.body().clone().into_response(),
                    Err(e) => {
                        common_telemetry::error!(e; "Leader handler failed");
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }
        }),
    );

    let heartbeat_router = AxumRouter::new()
        .route(
            "/",
            routing::get({
                let handler = heartbeat_handler.clone();
                move || async move {
                    match handler
                        .handle("/heartbeat", http::Method::GET, &Default::default())
                        .await
                    {
                        Ok(resp) => resp.body().clone().into_response(),
                        Err(e) => {
                            common_telemetry::error!(e; "Heartbeat handler failed");
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                }
            }),
        )
        .route(
            "/help",
            routing::get({
                let handler = heartbeat_handler.clone();
                move || async move {
                    match handler
                        .handle("/heartbeat/help", http::Method::GET, &Default::default())
                        .await
                    {
                        Ok(resp) => resp.body().clone().into_response(),
                        Err(e) => {
                            common_telemetry::error!(e; "Heartbeat help handler failed");
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                }
            }),
        );

    let maintenance_router = AxumRouter::new()
        .route(
            "/",
            routing::get({
                let handler = maintenance_handler.clone();
                move || async move {
                    match handler.get_maintenance().await {
                        Ok(resp) => to_axum_json_response(resp),
                        Err(e) => {
                            common_telemetry::error!(e; "Maintenance handler failed");
                            to_axum_not_found_response()
                        }
                    }
                }
            }),
        )
        .route(
            "/status",
            routing::get({
                let handler = maintenance_handler.clone();
                move || async move {
                    match handler.get_maintenance().await {
                        Ok(resp) => to_axum_json_response(resp),
                        Err(e) => {
                            common_telemetry::error!(e; "Maintenance status handler failed");
                            to_axum_not_found_response()
                        }
                    }
                }
            }),
        )
        .route(
            "/enable",
            routing::post({
                let handler = maintenance_handler.clone();
                move || async move {
                    match handler.set_maintenance().await {
                        Ok(resp) => to_axum_json_response(resp),
                        Err(e) => {
                            common_telemetry::error!(e; "Maintenance enable handler failed");
                            to_axum_not_found_response()
                        }
                    }
                }
            }),
        )
        .route(
            "/disable",
            routing::post({
                let handler = maintenance_handler.clone();
                move || async move {
                    match handler.unset_maintenance().await {
                        Ok(resp) => to_axum_json_response(resp),
                        Err(e) => {
                            common_telemetry::error!(e; "Maintenance disable handler failed");
                            to_axum_not_found_response()
                        }
                    }
                }
            }),
        );

    let procedure_router = AxumRouter::new()
        .route(
            "/status",
            routing::get({
                let handler = procedure_handler.clone();
                move || async move {
                    match handler.get_procedure_manager_status().await {
                        Ok(resp) => to_axum_json_response(resp),
                        Err(e) => {
                            common_telemetry::error!(e; "Procedure manager status handler failed");
                            to_axum_not_found_response()
                        }
                    }
                }
            }),
        )
        .route(
            "/pause",
            routing::post({
                let handler = procedure_handler.clone();
                move || async move {
                    match handler.pause_procedure_manager().await {
                        Ok(resp) => to_axum_json_response(resp),
                        Err(e) => {
                            common_telemetry::error!(e; "Procedure manager pause handler failed");
                            to_axum_not_found_response()
                        }
                    }
                }
            }),
        )
        .route(
            "/resume",
            routing::post({
                let handler = procedure_handler.clone();
                move || async move {
                    match handler.resume_procedure_manager().await {
                        Ok(resp) => to_axum_json_response(resp),
                        Err(e) => {
                            common_telemetry::error!(e; "Procedure manager resume handler failed");
                            to_axum_not_found_response()
                        }
                    }
                }
            }),
        );

    let recovery_router = AxumRouter::new()
        .route("/enable", routing::post(set_recovery_mode))
        .route("/disable", routing::post(unset_recovery_mode))
        .route("/status", routing::get(get_recovery_mode))
        .with_state(recovery_handler);

    let admin_router = AxumRouter::new()
        .nest("/health", health_router)
        .nest("/node-lease", node_lease_router)
        .nest("/leader", leader_router)
        .nest("/heartbeat", heartbeat_router)
        .nest("/maintenance", maintenance_router)
        .nest("/procedure-manager", procedure_router)
        .nest("/recovery", recovery_router)
        .nest("/sequence", sequence_router);

    AxumRouter::new().nest("/admin", admin_router)
}

#[cfg(test)]
mod tests {
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::KvBackendRef;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use super::*;
    use crate::metasrv::builder::MetasrvBuilder;
    use crate::metasrv::MetasrvOptions;
    use crate::{bootstrap, error};

    struct MockOkHandler;

    #[async_trait::async_trait]
    impl HttpHandler for MockOkHandler {
        async fn handle(
            &self,
            _: &str,
            _: http::Method,
            _: &HashMap<String, String>,
        ) -> crate::Result<http::Response<String>> {
            Ok(http::Response::builder()
                .status(http::StatusCode::OK)
                .body("Ok".to_string())
                .unwrap())
        }
    }
    struct MockEmptyKeyErrorHandler;

    #[async_trait::async_trait]
    impl HttpHandler for MockEmptyKeyErrorHandler {
        async fn handle(
            &self,
            _: &str,
            _: http::Method,
            _: &HashMap<String, String>,
        ) -> crate::Result<http::Response<String>> {
            error::EmptyKeySnafu {}.fail()
        }
    }

    #[test]
    fn test_route_nest() {
        let mock_handler = MockOkHandler {};
        let router = Router::new().route("/test_node", mock_handler);
        let router = Router::nest("/test_root", router);

        assert_eq!(1, router.handlers.len());
        assert!(router.handlers.contains_key("/test_root/test_node"));
    }

    #[should_panic]
    #[test]
    fn test_invalid_path() {
        check_path("test_node")
    }

    #[should_panic]
    #[test]
    fn test_empty_path() {
        check_path("")
    }

    #[tokio::test]
    async fn test_route_call_ok() {
        let mock_handler = MockOkHandler {};
        let router = Router::new().route("/test_node", mock_handler);
        let router = Router::nest("/test_root", router);

        let res = router
            .call(
                "/test_root/test_node",
                http::Method::GET,
                HashMap::default(),
            )
            .await
            .unwrap();

        assert!(res.status().is_success());
    }

    #[tokio::test]
    async fn test_route_call_no_handler() {
        let router = Router::new();

        let res = router
            .call(
                "/test_root/test_node",
                http::Method::GET,
                HashMap::default(),
            )
            .await
            .unwrap();

        assert_eq!(http::StatusCode::NOT_FOUND, res.status());
    }

    #[tokio::test]
    async fn test_route_call_err() {
        let mock_handler = MockEmptyKeyErrorHandler {};
        let router = Router::new().route("/test_node", mock_handler);
        let router = Router::nest("/test_root", router);

        let res = router
            .call(
                "/test_root/test_node",
                http::Method::GET,
                HashMap::default(),
            )
            .await
            .unwrap();

        assert_eq!(http::StatusCode::INTERNAL_SERVER_ERROR, res.status());
    }

    async fn test_metasrv(kv_backend: KvBackendRef) -> Metasrv {
        let opts = MetasrvOptions::default();
        let builder = MetasrvBuilder::new()
            .options(opts)
            .kv_backend(kv_backend.clone());

        let metasrv = builder.build().await.unwrap();
        metasrv
    }

    async fn send_request(client: &mut DuplexStream, request: &[u8]) -> String {
        client.write_all(request).await.unwrap();
        let mut buf = vec![0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_metasrv_maintenance_mode() {
        common_telemetry::init_default_ut_logging();
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let metasrv = test_metasrv(kv_backend).await;
        metasrv.try_start().await.unwrap();

        let (mut client, server) = tokio::io::duplex(1024);
        let metasrv = Arc::new(metasrv);
        let service = metasrv.clone();
        let _handle = tokio::spawn(async move {
            let router = bootstrap::router(service);
            router
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        // Get maintenance mode
        let response = send_request(
            &mut client,
            b"GET /admin/maintenance HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":false}"#));
        assert!(response.contains("200 OK"));

        // Set maintenance mode to true
        let response = send_request(
            &mut client,
            b"POST /admin/maintenance?enable=true HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":true}"#));
        assert!(response.contains("200 OK"));

        let enabled = metasrv
            .runtime_switch_manager()
            .maintenance_mode()
            .await
            .unwrap();
        assert!(enabled);

        // Get maintenance mode again
        let response = send_request(
            &mut client,
            b"GET /admin/maintenance HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":true}"#));
        assert!(response.contains("200 OK"));

        // Set maintenance mode to false
        let response = send_request(
            &mut client,
            b"POST /admin/maintenance?enable=false HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":false}"#));
        assert!(response.contains("200 OK"));

        let enabled = metasrv
            .runtime_switch_manager()
            .maintenance_mode()
            .await
            .unwrap();
        assert!(!enabled);

        // Set maintenance mode to true via GET request
        let response = send_request(
            &mut client,
            b"GET /admin/maintenance?enable=true HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":true}"#));
        assert!(response.contains("200 OK"));

        // Set maintenance mode to false via GET request
        let response = send_request(
            &mut client,
            b"PUT /admin/maintenance?enable=false HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":false}"#));
        assert!(response.contains("200 OK"));

        // Get maintenance mode via status path
        let response = send_request(
            &mut client,
            b"GET /admin/maintenance/status HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":false}"#));

        // Set maintenance mode via enable path
        let response = send_request(
            &mut client,
            b"POST /admin/maintenance/enable HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":true}"#));

        // Unset maintenance mode via disable path
        let response = send_request(
            &mut client,
            b"POST /admin/maintenance/disable HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(response.contains(r#"{"enabled":false}"#));

        // send POST request to status path
        let response = send_request(
            &mut client,
            b"POST /admin/maintenance/status HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(response.contains("404 Not Found"));

        // send GET request to enable path
        let response = send_request(
            &mut client,
            b"GET /admin/maintenance/enable HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(response.contains("404 Not Found"));

        // send GET request to disable path
        let response = send_request(
            &mut client,
            b"GET /admin/maintenance/disable HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(response.contains("404 Not Found"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_metasrv_procedure_manager_handler() {
        common_telemetry::init_default_ut_logging();
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let metasrv = test_metasrv(kv_backend).await;
        metasrv.try_start().await.unwrap();

        let (mut client, server) = tokio::io::duplex(1024);
        let metasrv = Arc::new(metasrv);
        let service = metasrv.clone();
        let _handle = tokio::spawn(async move {
            let router = bootstrap::router(service);
            router
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        // send GET request to procedure-manager/status path
        let response = send_request(
            &mut client,
            b"GET /admin/procedure-manager/status HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(
            response.contains(r#"{"status":"running"}"#),
            "response: {}",
            response
        );

        // send POST request to procedure-manager/pause path
        let response = send_request(
            &mut client,
            b"POST /admin/procedure-manager/pause HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains(r#"{"status":"paused"}"#));

        // send POST request to procedure-manager/resume path
        let response = send_request(
            &mut client,
            b"POST /admin/procedure-manager/resume HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(
            response.contains(r#"{"status":"running"}"#),
            "response: {}",
            response
        );

        // send GET request to procedure-manager/resume path
        let response = send_request(
            &mut client,
            b"GET /admin/procedure-manager/resume HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains("404 Not Found"));

        // send GET request to procedure-manager/pause path
        let response = send_request(
            &mut client,
            b"GET /admin/procedure-manager/pause HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.contains("404 Not Found"));
    }
}

#[cfg(test)]
mod axum_admin_tests {
    use std::sync::Arc;

    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request, StatusCode};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use tower::ServiceExt; // for `oneshot`

    use super::*;
    use crate::metasrv::builder::MetasrvBuilder;
    use crate::metasrv::MetasrvOptions;
    use crate::service::admin::sequencer::NextTableIdResponse;

    async fn setup_axum_app() -> AxumRouter {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let metasrv = MetasrvBuilder::new()
            .options(MetasrvOptions::default())
            .kv_backend(kv_backend)
            .build()
            .await
            .unwrap();
        let metasrv = Arc::new(metasrv);
        admin_axum_router(metasrv)
    }

    async fn get_body_string(resp: axum::response::Response) -> String {
        let body_bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        String::from_utf8_lossy(&body_bytes).to_string()
    }

    async fn into_bytes(resp: axum::response::Response) -> Vec<u8> {
        let body_bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        body_bytes.to_vec()
    }

    #[tokio::test]
    async fn test_admin_health() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.to_lowercase().contains("ok"));
    }

    #[tokio::test]
    async fn test_admin_node_lease() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/node-lease")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_admin_heartbeat() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/heartbeat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_admin_heartbeat_help() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/heartbeat/help")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_admin_leader() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/leader")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_admin_maintenance() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/maintenance")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("enabled"));
    }

    #[tokio::test]
    async fn test_admin_maintenance_status() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/maintenance/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("enabled"));
    }

    #[tokio::test]
    async fn test_admin_maintenance_enable_disable() {
        // Enable maintenance
        let response = setup_axum_app()
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/maintenance/enable")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("enabled"));
        // Disable maintenance
        let response = setup_axum_app()
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/maintenance/disable")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("enabled"));
    }

    #[tokio::test]
    async fn test_admin_procedure_manager_status() {
        let app = setup_axum_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/procedure-manager/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("status"));
    }

    #[tokio::test]
    async fn test_admin_procedure_manager_pause_resume() {
        // Pause
        let response = setup_axum_app()
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/procedure-manager/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("paused"));
        // Resume
        let response = setup_axum_app()
            .await
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/procedure-manager/resume")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("running"));
    }

    #[tokio::test]
    async fn test_admin_recovery() {
        let app = setup_axum_app().await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/recovery/status")
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("false"));

        // Enable recovery
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/recovery/enable")
                    .method(Method::POST)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("true"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/recovery/status")
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("true"));

        // Disable recovery
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/recovery/disable")
                    .method(Method::POST)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("false"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/recovery/status")
                    .method(Method::GET)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = get_body_string(response).await;
        assert!(body.contains("false"));
    }

    #[tokio::test]
    async fn test_admin_sequence_table_id() {
        common_telemetry::init_default_ut_logging();
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let metasrv = MetasrvBuilder::new()
            .options(MetasrvOptions::default())
            .kv_backend(kv_backend)
            .build()
            .await
            .unwrap();
        let metasrv = Arc::new(metasrv);
        let runtime_switch_manager = metasrv.runtime_switch_manager().clone();
        let app = admin_axum_router(metasrv);
        // Set recovery mode to true
        runtime_switch_manager.set_recovery_mode().await.unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/sequence/table/next-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = into_bytes(response).await;
        let resp: NextTableIdResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.next_table_id, 1024);

        // Bad request
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .uri("/admin/sequence/table/set-next-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Bad next id
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .uri("/admin/sequence/table/set-next-id")
                    .body(Body::from(r#"{"next_table_id": 0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = get_body_string(response).await;
        assert!(body.contains("is not greater than the current next value"));

        // Set next id
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .uri("/admin/sequence/table/set-next-id")
                    .body(Body::from(r#"{"next_table_id": 2048}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Set next id
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/sequence/table/next-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = into_bytes(response).await;
        let resp: NextTableIdResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.next_table_id, 2048);

        // Set recovery mode to false
        runtime_switch_manager.unset_recovery_mode().await.unwrap();
        // Set next id with recovery mode disabled
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .uri("/admin/sequence/table/set-next-id")
                    .body(Body::from(r#"{"next_table_id": 2049}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = get_body_string(response).await;
        assert!(body.contains("Setting next table id is only allowed in recovery mode"));
    }
}
