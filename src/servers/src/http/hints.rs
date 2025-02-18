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

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use session::context::QueryContext;

use crate::hint_headers;

pub async fn extract_hints(mut request: Request<Body>, next: Next) -> Response {
    let hints = hint_headers::extract_hints(request.headers());
    let analyze_format = request
        .headers()
        .get(session::context::ANALYZE_FORMAT_HEADER_NAME)
        .map(|v| v.to_str().unwrap().to_string());
    if let Some(query_ctx) = request.extensions_mut().get_mut::<QueryContext>() {
        for (key, value) in hints {
            query_ctx.set_extension(key, value);
        }

        if let Some(value) = analyze_format {
            query_ctx.set_extension(session::context::ANALYZE_FORMAT_HEADER_NAME, value);
        }
    }
    next.run(request).await
}
