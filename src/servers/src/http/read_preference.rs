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

use std::str::FromStr;

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use session::context::QueryContext;
use session::ReadPreference;

use crate::http::header::GREPTIME_DB_HEADER_READ_PREFERENCE;

/// Extract read preference from the request headers.
pub async fn extract_read_preference(mut request: Request<Body>, next: Next) -> Response {
    let read_preference = request
        .headers()
        .get(&GREPTIME_DB_HEADER_READ_PREFERENCE)
        .and_then(|header| header.to_str().ok())
        .and_then(|s| ReadPreference::from_str(s).ok())
        .unwrap_or_default();

    if let Some(query_ctx) = request.extensions_mut().get_mut::<QueryContext>() {
        query_ctx.set_read_preference(read_preference);
    }
    next.run(request).await
}
