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

//! Aggregate memory admission for gRPC services.
//!
//! Tonic decompresses and decodes a complete request message (up to
//! `max_recv_message_size`, 512 MiB by default) *before* the typed handler is
//! invoked, so a handler that charges the [`ServerMemoryLimiter`] only sees
//! the request after the memory is already allocated. For transport-compressed
//! messages (`grpc-encoding: gzip/zstd`) a few KiB on the wire can expand to
//! hundreds of MiB during that window, which would bypass any aggregate quota.
//!
//! [`MemoryLimiterExtensionLayer`] therefore reserves the configured maximum
//! decoded message size for every compressed request *before* the inner
//! (tonic) service runs, and holds the reservation for the whole request.
//! Handlers can detect the reservation via [`PreDecodeMemoryReservation`] in
//! the request extensions and skip their own post-decode charge to avoid
//! double accounting.
//!
//! The reservation is intentionally conservative (it upper-bounds the decoded
//! size before it is known); with the default unlimited limiter it is a no-op.

use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::response::IntoResponse;
use common_memory_manager::MemoryGuard;
use futures::future::BoxFuture;
use http::Request;
use tonic::Status;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tower::Layer;

use crate::request_memory_limiter::ServerMemoryLimiter;
use crate::request_memory_metrics::RequestMemoryMetrics;

/// The gRPC request compression selector header.
const GRPC_ENCODING_HEADER: &str = "grpc-encoding";
/// The "no compression" encoding value.
const IDENTITY_ENCODING: &str = "identity";

/// Present in the request extensions when memory for the (compressed)
/// request's decoded message was reserved before tonic decompression.
///
/// Handlers that charge the [`ServerMemoryLimiter`] after decoding should skip
/// that charge when this marker is present: the reservation already covers the
/// peak decoding memory and stays alive for the whole request.
#[derive(Clone)]
pub(crate) struct PreDecodeMemoryReservation {
    _guard: Arc<MemoryGuard<RequestMemoryMetrics>>,
}

#[derive(Clone)]
pub struct MemoryLimiterExtensionLayer {
    limiter: ServerMemoryLimiter,
    max_decoding_message_size: usize,
}

impl MemoryLimiterExtensionLayer {
    pub fn new(limiter: ServerMemoryLimiter, max_decoding_message_size: usize) -> Self {
        Self {
            limiter,
            max_decoding_message_size,
        }
    }
}

impl<S> Layer<S> for MemoryLimiterExtensionLayer {
    type Service = MemoryLimiterExtensionService<S>;

    fn layer(&self, service: S) -> Self::Service {
        MemoryLimiterExtensionService {
            inner: service,
            limiter: self.limiter.clone(),
            max_decoding_message_size: self.max_decoding_message_size,
        }
    }
}

#[derive(Clone)]
pub struct MemoryLimiterExtensionService<S> {
    inner: S,
    limiter: ServerMemoryLimiter,
    max_decoding_message_size: usize,
}

impl<S: NamedService> NamedService for MemoryLimiterExtensionService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody> Service<Request<ReqBody>> for MemoryLimiterExtensionService<S>
where
    S: Service<Request<ReqBody>, Error = Infallible> + Clone + Send + 'static,
    S::Response: axum::response::IntoResponse,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = axum::response::Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        // Own a clone of the service so the returned future does not borrow
        // `self` (tower's load-shedding pattern).
        let mut this = self.clone();
        Box::pin(async move {
            req.extensions_mut().insert(this.limiter.clone());

            let compressed = req
                .headers()
                .get(GRPC_ENCODING_HEADER)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| !value.eq_ignore_ascii_case(IDENTITY_ENCODING));

            if compressed {
                // A compressed message can expand up to the decoding limit
                // inside tonic, before any handler runs. Reserve that worst
                // case against the aggregate quota so the decoding phase is
                // admitted as well. No-op for an unlimited limiter.
                let reservation = this.max_decoding_message_size as u64;
                match this.limiter.acquire(reservation).await {
                    Ok(guard) => {
                        req.extensions_mut().insert(PreDecodeMemoryReservation {
                            _guard: Arc::new(guard),
                        });
                    }
                    Err(e) => {
                        return Ok(Status::resource_exhausted(format!(
                            "request memory limit exceeded: {e}"
                        ))
                        .into_http::<axum::body::Body>()
                        .into_response());
                    }
                }
            }

            match this.inner.call(req).await {
                Ok(response) => Ok(response.into_response()),
                Err(e) => match e {},
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use axum::body::Body;
    use axum::response::IntoResponse;
    use common_memory_manager::OnExhaustedPolicy;
    use futures_util::future::BoxFuture;
    use http::{HeaderValue, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::grpc::memory_limit::MemoryLimiterExtensionLayer;

    #[derive(Clone)]
    struct EchoService;

    impl Service<Request<Body>> for EchoService {
        type Response = axum::response::Response;
        type Error = Infallible;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request<Body>) -> Self::Future {
            let saw_reservation = req
                .extensions()
                .get::<PreDecodeMemoryReservation>()
                .is_some();
            let saw_limiter = req.extensions().get::<ServerMemoryLimiter>().is_some();
            Box::pin(async move {
                Ok((
                    StatusCode::OK,
                    format!("reservation={saw_reservation} limiter={saw_limiter}"),
                )
                    .into_response())
            })
        }
    }

    impl NamedService for EchoService {
        const NAME: &'static str = "test.Echo";
    }

    #[tokio::test]
    async fn test_inserts_limiter_for_uncompressed_requests() {
        let limiter = ServerMemoryLimiter::new(1024 * 1024, OnExhaustedPolicy::Fail);
        let mut svc =
            MemoryLimiterExtensionLayer::new(limiter.clone(), 512 * 1024 * 1024).layer(EchoService);

        let req = Request::builder().body(Body::empty()).unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 1024).await.unwrap();
        assert_eq!(
            &body[..],
            &b"reservation=false limiter=true"[..],
            "uncompressed requests must not be pre-reserved"
        );
        assert_eq!(0, limiter.used_bytes());
    }

    #[tokio::test]
    async fn test_reserves_max_message_size_for_compressed_requests() {
        let max_size = 512 * 1024 * 1024;
        let limiter = ServerMemoryLimiter::new(1024 * 1024 * 1024, OnExhaustedPolicy::Fail);
        let mut svc =
            MemoryLimiterExtensionLayer::new(limiter.clone(), max_size).layer(EchoService);

        // The inner service observes the reservation; use a oneshot call and
        // check the limiter was charged while the call is in flight via the
        // response extensions observed by the echo service.
        let req = Request::builder()
            .header(GRPC_ENCODING_HEADER, HeaderValue::from_static("gzip"))
            .body(Body::empty())
            .unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 1024).await.unwrap();
        assert_eq!(
            &body[..],
            &b"reservation=true limiter=true"[..],
            "compressed requests must carry a pre-decode reservation"
        );
        // The reservation is released once the inner call (request) finishes.
        assert_eq!(0, limiter.used_bytes());
    }

    #[tokio::test]
    async fn test_rejects_compressed_request_when_quota_exhausted() {
        // Quota smaller than the decoding limit: the reservation cannot fit.
        let limiter = ServerMemoryLimiter::new(1024, OnExhaustedPolicy::Fail);
        let mut svc =
            MemoryLimiterExtensionLayer::new(limiter.clone(), 512 * 1024 * 1024).layer(EchoService);

        let req = Request::builder()
            .header(GRPC_ENCODING_HEADER, HeaderValue::from_static("zstd"))
            .body(Body::empty())
            .unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        // gRPC errors are HTTP 200 with a grpc-status header.
        assert_eq!(
            res.headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok()),
            Some("8"),
            "expected RESOURCE_EXHAUSTED grpc status"
        );
    }

    #[tokio::test]
    async fn test_identity_encoding_is_not_reserved() {
        let limiter = ServerMemoryLimiter::new(1024 * 1024, OnExhaustedPolicy::Fail);
        let mut svc =
            MemoryLimiterExtensionLayer::new(limiter.clone(), 512 * 1024 * 1024).layer(EchoService);

        let req = Request::builder()
            .header(
                GRPC_ENCODING_HEADER,
                HeaderValue::from_static(IDENTITY_ENCODING),
            )
            .body(Body::empty())
            .unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        let body = axum::body::to_bytes(res.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], &b"reservation=false limiter=true"[..]);
    }
}
