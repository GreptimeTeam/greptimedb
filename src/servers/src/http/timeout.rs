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
}

impl<T> ResponseFuture<T> {
    pub(crate) fn new(inner: T, sleep: Sleep) -> Self {
        ResponseFuture { inner, sleep }
    }
}

impl<F, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response, E>>,
{
    type Output = Result<Response, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if this.sleep.poll(cx).is_ready() {
            let mut res = Response::default();
            *res.status_mut() = StatusCode::REQUEST_TIMEOUT;
            return Poll::Ready(Ok(res));
        }

        this.inner.poll(cx)
    }
}

/// Applies a timeout to requests via the supplied inner service.
///
/// Modified from https://github.com/tower-rs/tower-http/blob/tower-http-0.5.2/tower-http/src/timeout/service.rs
#[derive(Debug, Clone)]
pub struct DynamicTimeoutLayer {
    default_timeout: Duration,
}

impl DynamicTimeoutLayer {
    /// Create a timeout from a duration
    pub fn new(default_timeout: Duration) -> Self {
        DynamicTimeoutLayer { default_timeout }
    }
}

impl<S> Layer<S> for DynamicTimeoutLayer {
    type Service = DynamicTimeout<S>;

    fn layer(&self, service: S) -> Self::Service {
        DynamicTimeout::new(service, self.default_timeout)
    }
}

/// Modified from https://github.com/tower-rs/tower-http/blob/tower-http-0.5.2/tower-http/src/timeout/service.rs
#[derive(Clone)]
pub struct DynamicTimeout<S> {
    inner: S,
    default_timeout: Duration,
}

impl<S> DynamicTimeout<S> {
    /// Create a new [`DynamicTimeout`] with the given timeout
    pub fn new(inner: S, default_timeout: Duration) -> Self {
        DynamicTimeout {
            inner,
            default_timeout,
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
        let response = self.inner.call(request);

        if timeout.is_zero() {
            // 30 years. See `Instant::far_future`.
            let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 30);
            ResponseFuture::new(response, tokio::time::sleep_until(far_future))
        } else {
            let sleep = tokio::time::sleep(timeout);
            ResponseFuture::new(response, sleep)
        }
    }
}
