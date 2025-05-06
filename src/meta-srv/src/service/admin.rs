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

mod health;
mod heartbeat;
mod leader;
mod maintenance;
mod node_lease;
mod util;

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use tonic::body::BoxBody;
use tonic::codegen::{empty_body, http, BoxFuture, Service};
use tonic::server::NamedService;

use crate::metasrv::Metasrv;

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

    let router = router.route(
        "/maintenance",
        maintenance::MaintenanceHandler {
            manager: metasrv.maintenance_mode_manager().clone(),
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

impl<T> Service<http::Request<T>> for Admin
where
    T: Send,
{
    type Response = http::Response<BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<T>) -> Self::Future {
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
    handlers: HashMap<String, Box<dyn HttpHandler>>,
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

        let _ = self.handlers.insert(path.to_owned(), Box::new(handler));

        self
    }

    pub async fn call(
        &self,
        path: &str,
        method: http::Method,
        params: HashMap<String, String>,
    ) -> Result<http::Response<BoxBody>, Infallible> {
        let handler = match self.handlers.get(path) {
            Some(handler) => handler,
            None => {
                return Ok(http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(empty_body())
                    .unwrap())
            }
        };

        let res = match handler.handle(path, method, &params).await {
            Ok(res) => res.map(boxed),
            Err(e) => http::Response::builder()
                .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(boxed(e.to_string()))
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

/// Returns a [BoxBody] from a string.
/// The implementation follows [empty_body()].
fn boxed(body: String) -> BoxBody {
    Full::new(Bytes::from(body))
        .map_err(|err| match err {})
        .boxed_unsync()
}

#[cfg(test)]
mod tests {
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::KvBackendRef;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        let http_request = b"GET /admin/maintenance HTTP/1.1\r\nHost: localhost\r\n\r\n";
        client.write_all(http_request).await.unwrap();
        let mut buf = vec![0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains(r#"{"enabled":false}"#));
        assert!(response.contains("200 OK"));

        // Set maintenance mode to true
        let http_post = b"POST /admin/maintenance?enable=true HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
        client.write_all(http_post).await.unwrap();
        let mut buf = vec![0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains(r#"{"enabled":true}"#));
        assert!(response.contains("200 OK"));

        let enabled = metasrv
            .maintenance_mode_manager()
            .maintenance_mode()
            .await
            .unwrap();
        assert!(enabled);

        // Get maintenance mode again
        let http_request = b"GET /admin/maintenance HTTP/1.1\r\nHost: localhost\r\n\r\n";
        client.write_all(http_request).await.unwrap();
        let mut buf = vec![0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains(r#"{"enabled":true}"#));
        assert!(response.contains("200 OK"));

        // Set maintenance mode to false
        let http_post = b"POST /admin/maintenance?enable=false HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
        client.write_all(http_post).await.unwrap();
        let mut buf = vec![0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains(r#"{"enabled":false}"#));
        assert!(response.contains("200 OK"));

        let enabled = metasrv
            .maintenance_mode_manager()
            .maintenance_mode()
            .await
            .unwrap();
        assert!(!enabled);

        // Set maintenance mode to true via GET request
        let http_request =
            b"GET /admin/maintenance?enable=true HTTP/1.1\r\nHost: localhost\r\n\r\n";
        client.write_all(http_request).await.unwrap();
        let mut buf = vec![0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains(r#"{"enabled":true}"#));
        assert!(response.contains("200 OK"));

        // Set maintenance mode to false via GET request
        let http_request =
            b"PUT /admin/maintenance?enable=false HTTP/1.1\r\nHost: localhost\r\n\r\n";
        client.write_all(http_request).await.unwrap();
        let mut buf = vec![0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains(r#"{"enabled":false}"#));
        assert!(response.contains("200 OK"));
    }
}
