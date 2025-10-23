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

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::StatusCode;

use crate::metrics::{METRIC_HTTP_MEMORY_USAGE_BYTES, METRIC_HTTP_REQUESTS_REJECTED_TOTAL};
use crate::request_limiter::RequestMemoryLimiter;

pub async fn memory_limit_middleware(
    State(limiter): State<RequestMemoryLimiter>,
    req: Request,
    next: Next,
) -> Response {
    let content_length = req
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);

    let _guard = match limiter.try_acquire(content_length) {
        Ok(guard) => guard.inspect(|g| {
            METRIC_HTTP_MEMORY_USAGE_BYTES.set(g.current_usage() as i64);
        }),
        Err(e) => {
            METRIC_HTTP_REQUESTS_REJECTED_TOTAL.inc();
            return (
                StatusCode::TOO_MANY_REQUESTS,
                format!("Request body memory limit exceeded: {}", e),
            )
                .into_response();
        }
    };

    next.run(req).await
}
