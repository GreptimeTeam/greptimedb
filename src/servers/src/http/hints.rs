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

use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use session::context::QueryContext;

use crate::GREPTIME_DB_HEADER_HINT_PREFIX;

pub async fn extract_hints<B>(mut request: Request<B>, next: Next<B>) -> Response {
    let hints = request
        .headers()
        .iter()
        .filter_map(|(key, value)| {
            let key = key.as_str();
            let new_key = key.strip_prefix(GREPTIME_DB_HEADER_HINT_PREFIX)?;
            let Ok(value) = value.to_str() else {
                // Simply return None for non-string values.
                return None;
            };
            Some((new_key.to_string(), value.trim().to_string()))
        })
        .collect::<Vec<_>>();
    if let Some(query_ctx) = request.extensions_mut().get_mut::<QueryContext>() {
        for (key, value) in hints {
            query_ctx.set_extension(key, value);
        }
    }
    next.run(request).await
}
