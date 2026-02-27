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

use std::net::SocketAddr;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use common_telemetry::warn;

/// Middleware that logs HTTP error responses (4xx/5xx) with client IP address.
///
/// Extracts client address from [`ConnectInfo`] if available.
/// If `ConnectInfo` is not present (e.g., in tests), logs without IP.
pub async fn log_error_with_client_ip(req: Request<Body>, next: Next) -> Response {
    let client_addr = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0);
    let response = next.run(req).await;

    if response.status().is_client_error() || response.status().is_server_error() {
        if let Some(addr) = client_addr {
            warn!(
                "HTTP error response {} from client {}",
                response.status(),
                addr
            );
        } else {
            warn!("HTTP error response {}", response.status(),);
        }
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::Router;
    use http::StatusCode;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_middleware_passes_error_response() {
        async fn not_found_handler() -> StatusCode {
            StatusCode::NOT_FOUND
        }

        let app = Router::new()
            .route("/not-found", get(not_found_handler))
            .layer(axum::middleware::from_fn(log_error_with_client_ip));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/not-found")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_middleware_passes_success_response() {
        async fn ok_handler() -> StatusCode {
            StatusCode::OK
        }

        let app = Router::new()
            .route("/ok", get(ok_handler))
            .layer(axum::middleware::from_fn(log_error_with_client_ip));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
