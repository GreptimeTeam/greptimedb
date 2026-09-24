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

//! Middleware for limiting total memory usage of concurrent HTTP request bodies.
//!
//! Admission works in two stages:
//!
//! 1. Upfront, the request's `Content-Length` (if present) is charged to the
//!    shared [`ServerMemoryLimiter`], preserving the configured wait/fail
//!    policy for regular requests.
//! 2. While the body is streamed, [`AccountedBody`] charges every byte beyond
//!    the upfront reservation. This closes two gaps of a header-only
//!    reservation: HTTP/1.1 chunked requests carry no `Content-Length` (and
//!    would otherwise be admitted for free), and a client can understate the
//!    header while sending a much larger body.
//!
//! Permits acquired while streaming are held by [`BodyMemoryAccounting`],
//! which is also inserted into the request extensions so they stay alive until
//! the whole request is finished (extractors collect the body into owned
//! buffers that outlive the body stream itself).

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use common_memory_manager::MemoryGuard;
use http::StatusCode;
use http_body::{Body as HttpBody, Frame};

use crate::error::Result;
use crate::request_memory_limiter::ServerMemoryLimiter;
use crate::request_memory_metrics::RequestMemoryMetrics;

/// Marker inserted by [`memory_limit_middleware`] when the request arrives
/// with a non-identity `Content-Encoding` header. Route-local
/// [`decoded_body_accounting_middleware`] uses it to account the decompressed
/// body: only such requests expand during request decompression.
#[derive(Clone, Copy)]
pub(crate) struct ContentEncoded;

pub async fn memory_limit_middleware(
    State(limiter): State<ServerMemoryLimiter>,
    req: Request,
    next: Next,
) -> Response {
    let content_length = req
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    let _guard = match limiter.acquire(content_length).await {
        Ok(guard) => guard,
        Err(e) => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                format!("Request body memory limit exceeded: {}", e),
            )
                .into_response();
        }
    };

    let content_encoded = req
        .headers()
        .get(http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| !v.eq_ignore_ascii_case("identity"));

    // Account whatever is actually streamed beyond the upfront reservation
    // (chunked transfer has none).
    let accounting = BodyMemoryAccounting::default();
    // The middleware keeps its own handle so the acquired permits stay held
    // for the whole request: the body wrapper and the extensions are dropped
    // once the extractors finish collecting the body, before the handler is
    // done with the decoded data.
    let retained_accounting = accounting.clone();
    let (mut parts, body) = req.into_parts();
    // Expose the limiter to handlers that decompress request bodies on their
    // own (e.g. the Loki protobuf push) so they can charge the decoded size.
    parts.extensions.insert(limiter.clone());
    parts.extensions.insert(accounting.clone());
    if content_encoded {
        parts.extensions.insert(ContentEncoded);
    }
    let accounted = AccountedBody::new(body, limiter, content_length, accounting);
    let req = Request::from_parts(parts, Body::new(accounted));

    let response = next.run(req).await;
    // `retained_accounting` stays alive until this function returns, holding
    // the permits across the handler's use of the collected body.
    quota_exceeded_response(&retained_accounting, response)
}

/// Rewrites the response of a request whose body accounting hit the quota:
/// the failure aborts the body mid-stream, so the extractors reject the
/// request with a generic body error (mapped to 400). Quota exhaustion must
/// surface as 429, matching the upfront admission path, so clients can tell
/// backpressure apart from malformed input.
fn quota_exceeded_response(accounting: &BodyMemoryAccounting, response: Response) -> Response {
    // The flag can only be set when the body failed before the handler ran,
    // so the response is the extractor rejection and can be replaced.
    if accounting.take_quota_exceeded() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "Request body memory limit exceeded",
        )
            .into_response();
    }
    response
}

/// Route-local counterpart of [`memory_limit_middleware`] for routes that
/// install `RequestDecompressionLayer`: it must be layered *inside* the
/// decompression layer, so the body it wraps is the **decompressed** stream.
///
/// Only requests marked [`ContentEncoded`] by the global middleware are
/// accounted; plain (uncompressed) requests are already covered by the global
/// wire-byte accounting, and charging them here as well would double-count.
/// For decompressed requests the `Content-Length` header has been stripped by
/// the decompression layer, so the full decoded size is charged as it streams.
pub(crate) async fn decoded_body_accounting_middleware(
    State(limiter): State<ServerMemoryLimiter>,
    req: Request,
    next: Next,
) -> Response {
    if req.extensions().get::<ContentEncoded>().is_none() {
        return next.run(req).await;
    }

    let content_length = req
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    let accounting = BodyMemoryAccounting::default();
    // Retain the permits for the whole request, like the global middleware.
    let retained_accounting = accounting.clone();
    let (mut parts, body) = req.into_parts();
    parts.extensions.insert(accounting.clone());
    let accounted = AccountedBody::new(body, limiter, content_length, accounting);
    let req = Request::from_parts(parts, Body::new(accounted));

    let response = next.run(req).await;
    // `retained_accounting` stays alive until this function returns, holding
    // the permits across the handler's use of the collected body.
    quota_exceeded_response(&retained_accounting, response)
}

/// Holds the memory guards acquired while a request body is streamed. Shared
/// between the [`AccountedBody`] wrapper, the request extensions and the
/// middleware itself, so the permits are only released when the request
/// (including any collected body buffers and the handler still using them)
/// is finished.
#[derive(Clone, Default)]
struct BodyMemoryAccounting {
    guards: Arc<Mutex<Vec<MemoryGuard<RequestMemoryMetrics>>>>,
    /// Set when an incremental charge hit the quota, so the rejection can be
    /// rewritten into a 429 after the middleware regains control.
    quota_exceeded: Arc<AtomicBool>,
}

impl BodyMemoryAccounting {
    fn hold(&self, guard: MemoryGuard<RequestMemoryMetrics>) {
        self.guards.lock().unwrap().push(guard);
    }

    fn mark_quota_exceeded(&self) {
        self.quota_exceeded.store(true, Ordering::Release);
    }

    fn take_quota_exceeded(&self) -> bool {
        self.quota_exceeded.swap(false, Ordering::AcqRel)
    }
}

type AcquireFuture =
    Pin<Box<dyn std::future::Future<Output = Result<MemoryGuard<RequestMemoryMetrics>>> + Send>>;

enum ChargeOutcome {
    Charged,
    Pending,
    Failed,
}

/// A request body wrapper that charges the shared limiter for every byte
/// streamed beyond `pre_charged` (the upfront `Content-Length` reservation).
struct AccountedBody {
    inner: Body,
    limiter: ServerMemoryLimiter,
    accounting: BodyMemoryAccounting,
    /// Bytes of body data streamed so far.
    streamed: u64,
    /// Bytes of the streamed body already covered by incremental permits.
    incrementally_charged: u64,
    /// Bytes covered by the upfront `Content-Length` reservation.
    pre_charged: u64,
    /// A data frame held back while its permit is being acquired, together
    /// with the number of extra bytes to charge for it.
    pending: Option<(Frame<Bytes>, u64)>,
    pending_acquire: Option<AcquireFuture>,
}

impl AccountedBody {
    fn new(
        inner: Body,
        limiter: ServerMemoryLimiter,
        pre_charged: u64,
        accounting: BodyMemoryAccounting,
    ) -> Self {
        Self {
            inner,
            limiter,
            accounting,
            streamed: 0,
            incrementally_charged: 0,
            pre_charged,
            pending: None,
            pending_acquire: None,
        }
    }

    /// Bytes that still need an incremental permit for the body streamed so far.
    fn uncharged(&self) -> u64 {
        self.streamed
            .saturating_sub(self.pre_charged)
            .saturating_sub(self.incrementally_charged)
    }

    /// Acquires a permit for `bytes`; only one acquisition may be in flight.
    fn charge(&mut self, bytes: u64, cx: &mut Context<'_>) -> ChargeOutcome {
        debug_assert!(bytes > 0);
        // Fast path: the permit is immediately available.
        if let Some(guard) = self.limiter.try_acquire(bytes) {
            self.accounting.hold(guard);
            self.incrementally_charged += bytes;
            return ChargeOutcome::Charged;
        }
        if let Some(fut) = self.pending_acquire.as_mut() {
            return match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(guard)) => {
                    self.accounting.hold(guard);
                    self.incrementally_charged += bytes;
                    self.pending_acquire = None;
                    ChargeOutcome::Charged
                }
                Poll::Ready(Err(_)) => {
                    self.pending_acquire = None;
                    ChargeOutcome::Failed
                }
                Poll::Pending => ChargeOutcome::Pending,
            };
        }
        let limiter = self.limiter.clone();
        let mut fut = Box::pin(async move { limiter.acquire(bytes).await });
        // Poll immediately so a ready result is not missed (returning
        // `Pending` without registering progress would park the task forever).
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(guard)) => {
                self.accounting.hold(guard);
                self.incrementally_charged += bytes;
                ChargeOutcome::Charged
            }
            Poll::Ready(Err(_)) => ChargeOutcome::Failed,
            Poll::Pending => {
                self.pending_acquire = Some(fut);
                ChargeOutcome::Pending
            }
        }
    }

    /// Hands out `frame` once `bytes` of it are charged, buffering the frame
    /// while the acquisition is pending.
    fn hand_out(
        &mut self,
        frame: Frame<Bytes>,
        bytes: u64,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        if bytes == 0 {
            return Poll::Ready(Some(Ok(frame)));
        }
        match self.charge(bytes, cx) {
            ChargeOutcome::Charged => Poll::Ready(Some(Ok(frame))),
            ChargeOutcome::Pending => {
                self.pending = Some((frame, bytes));
                Poll::Pending
            }
            ChargeOutcome::Failed => {
                self.accounting.mark_quota_exceeded();
                Poll::Ready(Some(Err(limit_exceeded_error())))
            }
        }
    }
}

impl HttpBody for AccountedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        // Finish a pending acquisition for a buffered frame first.
        if let Some((frame, bytes)) = this.pending.take() {
            return this.hand_out(frame, bytes, cx);
        }

        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.streamed += data.len() as u64;
                    let uncharged = this.uncharged();
                    this.hand_out(frame, uncharged, cx)
                } else {
                    // Trailers pass through uncharged.
                    Poll::Ready(Some(Ok(frame)))
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_none() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn limit_exceeded_error() -> axum::Error {
    axum::Error::new(std::io::Error::other(
        "request body exceeded the aggregate memory limit",
    ))
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, Bytes};
    use axum::http::{Request, StatusCode, header};
    use axum::routing::post;
    use axum::{Router, middleware};
    use common_memory_manager::OnExhaustedPolicy;
    use tower::ServiceExt;

    use super::memory_limit_middleware;
    use crate::request_memory_limiter::ServerMemoryLimiter;

    /// Streams `body` in `chunk_size` frames without a Content-Length header
    /// (like HTTP/1.1 chunked transfer).
    fn chunked_request(uri: &str, body: Vec<u8>, chunk_size: usize) -> Request<Body> {
        let stream = futures_util::stream::iter(
            body.chunks(chunk_size)
                .map(|c| Ok::<_, std::io::Error>(Bytes::copy_from_slice(c)))
                .collect::<Vec<_>>(),
        );
        Request::builder()
            .uri(uri)
            .method("POST")
            .body(Body::from_stream(stream))
            .unwrap()
    }

    fn counted_body_app(limiter: ServerMemoryLimiter) -> Router {
        Router::new()
            .route(
                "/echo",
                post(|body: Bytes| async move { (StatusCode::OK, body.len().to_string()) }),
            )
            .layer(middleware::from_fn_with_state(
                limiter,
                memory_limit_middleware,
            ))
    }

    #[tokio::test]
    async fn test_chunked_body_larger_than_quota_is_rejected() {
        // The quota cannot cover a fully streamed 64 KiB chunked body.
        let limiter = ServerMemoryLimiter::new(8 * 1024, OnExhaustedPolicy::Fail);
        let app = counted_body_app(limiter.clone());

        let res = app
            .oneshot(chunked_request("/echo", vec![b'x'; 64 * 1024], 1024))
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a chunked body larger than the quota must be rejected with 429, not 400"
        );
        assert_eq!(0, limiter.used_bytes(), "guards must be released");
    }

    #[tokio::test]
    async fn test_small_chunked_body_fits() {
        let limiter = ServerMemoryLimiter::new(8 * 1024, OnExhaustedPolicy::Fail);
        let app = counted_body_app(limiter.clone());

        let res = app
            .oneshot(chunked_request("/echo", vec![b'x'; 1024], 512))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], b"1024");
        assert_eq!(0, limiter.used_bytes(), "guards must be released");
    }

    #[tokio::test]
    async fn test_content_length_still_admitted_upfront() {
        // A Content-Length larger than the quota is rejected before the body
        // is read (429), unchanged from the previous behavior.
        let limiter = ServerMemoryLimiter::new(1024, OnExhaustedPolicy::Fail);
        let app = counted_body_app(limiter);

        let req = Request::builder()
            .uri("/echo")
            .method("POST")
            .header(header::CONTENT_LENGTH, "4096")
            .body(Body::from(vec![b'x'; 4096]))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn test_understated_content_length_is_charged_the_difference() {
        // Claim 1 KiB in Content-Length but stream 16 KiB: the extra 15 KiB
        // must be charged while streaming and rejected by the small quota.
        let limiter = ServerMemoryLimiter::new(2 * 1024, OnExhaustedPolicy::Fail);
        let app = counted_body_app(limiter.clone());

        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(
            Bytes::copy_from_slice(&vec![b'x'; 16 * 1024]),
        )]);
        let req = Request::builder()
            .uri("/echo")
            .method("POST")
            .header(header::CONTENT_LENGTH, "1024")
            .body(Body::from_stream(stream))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(0, limiter.used_bytes());
    }

    #[tokio::test]
    async fn test_honest_content_length_is_not_double_charged() {
        // With a quota large enough for the whole body, an accurate
        // Content-Length must admit the request without extra charges.
        let limiter = ServerMemoryLimiter::new(8 * 1024, OnExhaustedPolicy::Fail);
        let app = counted_body_app(limiter.clone());

        let req = Request::builder()
            .uri("/echo")
            .method("POST")
            .header(header::CONTENT_LENGTH, "4096")
            .body(Body::from(vec![b'x'; 4096]))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(0, limiter.used_bytes());
    }

    /// Mirrors the route-local stack: decoded-body accounting *inside* a
    /// request-decompression layer, like the ingestion routes in `http.rs`.
    fn decompressing_app(limiter: ServerMemoryLimiter) -> Router {
        use tower_http::decompression::RequestDecompressionLayer;

        Router::new()
            .route(
                "/echo",
                post(|body: Bytes| async move { (StatusCode::OK, body.len().to_string()) }),
            )
            .layer(middleware::from_fn_with_state(
                limiter,
                super::decoded_body_accounting_middleware,
            ))
            .layer(RequestDecompressionLayer::new().pass_through_unaccepted(true))
    }

    fn zstd_body(decoded: &[u8]) -> Body {
        let compressed = zstd::stream::encode_all(decoded, 3).unwrap();
        Body::from(compressed)
    }

    #[tokio::test]
    async fn test_decompressed_body_is_charged_for_content_encoded_requests() {
        // The decoded size (64 KiB) exceeds the 16 KiB quota: the accounting
        // layer must see the decompressed bytes and reject mid-stream.
        let limiter = ServerMemoryLimiter::new(16 * 1024, OnExhaustedPolicy::Fail);
        let app = decompressing_app(limiter.clone());

        let req = Request::builder()
            .uri("/echo")
            .method("POST")
            .header("content-encoding", "zstd")
            .extension(super::ContentEncoded)
            .body(zstd_body(&vec![b'x'; 64 * 1024]))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(
            res.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "decoded body larger than the quota must be rejected with 429"
        );
        assert_eq!(0, limiter.used_bytes(), "guards must be released");
    }

    #[tokio::test]
    async fn test_small_decompressed_body_fits() {
        let limiter = ServerMemoryLimiter::new(16 * 1024, OnExhaustedPolicy::Fail);
        let app = decompressing_app(limiter.clone());

        let req = Request::builder()
            .uri("/echo")
            .method("POST")
            .header("content-encoding", "zstd")
            .extension(super::ContentEncoded)
            .body(zstd_body(&vec![b'x'; 1024]))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], b"1024", "the handler must see the decoded body");
        assert_eq!(0, limiter.used_bytes(), "guards must be released");
    }

    #[tokio::test]
    async fn test_plain_requests_are_not_charged_by_the_decoded_layer() {
        // Without the ContentEncoded marker the decoded layer must be a
        // no-op: plain bodies are accounted by the global wire accounting.
        let limiter = ServerMemoryLimiter::new(16 * 1024, OnExhaustedPolicy::Fail);
        let app = decompressing_app(limiter.clone());

        // Plain body, no marker: passes through.
        let req = Request::builder()
            .uri("/echo")
            .method("POST")
            .body(Body::from(vec![b'x'; 1024]))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Compressed body WITHOUT the marker (e.g. a route that has the
        // decompression layer but no accounting): still passes through.
        let req = Request::builder()
            .uri("/echo")
            .method("POST")
            .header("content-encoding", "zstd")
            .body(zstd_body(&vec![b'x'; 1024]))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
