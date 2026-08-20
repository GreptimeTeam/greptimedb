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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::response::Response;
use http::StatusCode;
use pin_project::pin_project;
use tokio::time::{Instant, Sleep};
use tower::{Layer, Service};

use crate::http::header::constants::GREPTIME_DB_HEADER_TIMEOUT;

/// [`Timeout`] response future
///
/// [`Timeout`]: crate::timeout::Timeout
///
/// Modified from https://github.com/tower-rs/tower-http/blob/tower-http-0.5.2/tower-http/src/timeout/service.rs
#[derive(Debug)]
#[pin_project]
pub struct ResponseFuture<T> {
    #[pin]
    inner: T,
    #[pin]
    sleep: Sleep,
    status_code: StatusCode,
}

/// The resolved deadline of the in-flight HTTP request, inserted into request
/// extensions by [`DynamicTimeout`] so that downstream handlers can bound
/// their work (e.g. derive a query timeout) before the request is aborted.
#[derive(Debug, Clone, Copy)]
pub struct RequestDeadline(pub std::time::Instant);

impl<T> ResponseFuture<T> {
    pub(crate) fn new(inner: T, sleep: Sleep, status_code: StatusCode) -> Self {
        ResponseFuture {
            inner,
            sleep,
            status_code,
        }
    }
}

impl<F, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response, E>>,
{
    type Output = Result<Response, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // Poll the inner future first: if it can complete (e.g. with a query
        // timeout error carrying diagnostics) in the same tick the sleep
        // expires, prefer its response over a bare timeout response.
        if let Poll::Ready(output) = this.inner.poll(cx) {
            return Poll::Ready(output);
        }

        if this.sleep.poll(cx).is_ready() {
            let mut res = Response::default();
            *res.status_mut() = *this.status_code;
            return Poll::Ready(Ok(res));
        }

        Poll::Pending
    }
}

/// Applies a timeout to requests via the supplied inner service.
///
/// Modified from https://github.com/tower-rs/tower-http/blob/tower-http-0.5.2/tower-http/src/timeout/service.rs
#[derive(Debug, Clone)]
pub struct DynamicTimeoutLayer {
    default_timeout: Duration,
    status_code_fn: fn(&Request<Body>) -> StatusCode,
}

impl DynamicTimeoutLayer {
    /// Create a timeout from a duration
    pub fn new(default_timeout: Duration) -> Self {
        DynamicTimeoutLayer {
            default_timeout,
            status_code_fn: |_| StatusCode::REQUEST_TIMEOUT,
        }
    }

    /// Sets a function that selects the timeout response status for each request.
    pub fn with_status_code_fn(mut self, status_code_fn: fn(&Request<Body>) -> StatusCode) -> Self {
        self.status_code_fn = status_code_fn;
        self
    }
}

impl<S> Layer<S> for DynamicTimeoutLayer {
    type Service = DynamicTimeout<S>;

    fn layer(&self, service: S) -> Self::Service {
        DynamicTimeout::new(service, self.default_timeout, self.status_code_fn)
    }
}

/// Modified from https://github.com/tower-rs/tower-http/blob/tower-http-0.5.2/tower-http/src/timeout/service.rs
#[derive(Clone)]
pub struct DynamicTimeout<S> {
    inner: S,
    default_timeout: Duration,
    status_code_fn: fn(&Request<Body>) -> StatusCode,
}

impl<S> DynamicTimeout<S> {
    /// Create a new [`DynamicTimeout`] with the given timeout
    pub fn new(
        inner: S,
        default_timeout: Duration,
        status_code_fn: fn(&Request<Body>) -> StatusCode,
    ) -> Self {
        DynamicTimeout {
            inner,
            default_timeout,
            status_code_fn,
        }
    }
}

impl<S> Service<Request<Body>> for DynamicTimeout<S>
where
    S: Service<Request<Body>, Response = Response> + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.inner.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => Poll::Ready(r),
        }
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let status_code = (self.status_code_fn)(&request);
        let timeout = request
            .headers()
            .get(GREPTIME_DB_HEADER_TIMEOUT)
            .and_then(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|value| humantime::parse_duration(value).ok())
            })
            .unwrap_or(self.default_timeout);
        let mut request = request;
        if !timeout.is_zero() {
            // Expose the resolved deadline to downstream handlers so they can
            // bound their work before this layer aborts the request.
            let deadline = std::time::Instant::now() + timeout;
            let _ = request.extensions_mut().insert(RequestDeadline(deadline));
            let response = self.inner.call(request);
            let sleep = tokio::time::sleep(timeout);
            ResponseFuture::new(response, sleep, status_code)
        } else {
            let response = self.inner.call(request);
            // 30 years. See `Instant::far_future`.
            let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 30);
            ResponseFuture::new(
                response,
                tokio::time::sleep_until(far_future),
                status_code,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use axum::http::Request;
    use tower::Service;
    use tower::service_fn;

    use super::*;
    use crate::http::header::constants::GREPTIME_DB_HEADER_TIMEOUT;

    fn deadline_echo_service() -> DynamicTimeout<
        impl Service<Request<Body>, Response = Response, Error = Infallible> + Clone,
    > {
        let svc = service_fn(|req: Request<Body>| async move {
            let status = if req.extensions().get::<RequestDeadline>().is_some() {
                StatusCode::OK
            } else {
                StatusCode::NO_CONTENT
            };
            Ok::<_, Infallible>(Response::builder().status(status).body(Body::empty()).unwrap())
        });
        DynamicTimeout::new(svc, Duration::from_secs(30), |_| {
            StatusCode::REQUEST_TIMEOUT
        })
    }

    #[tokio::test]
    async fn test_request_deadline_extension_is_inserted() {
        let mut svc = deadline_echo_service();
        let res = svc
            .call(Request::new(Body::empty()))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_request_deadline_extension_skipped_when_timeout_disabled() {
        let mut svc = deadline_echo_service();
        let req = Request::builder()
            .header(GREPTIME_DB_HEADER_TIMEOUT, "0s")
            .body(Body::empty())
            .unwrap();
        let res = svc.call(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }
}
