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
use session::context::{QueryContext, REMOTE_QUERY_ID_EXTENSION_KEY, is_reserved_extension_key};

use crate::hint_headers;

pub async fn extract_hints(mut request: Request<Body>, next: Next) -> Response {
    let hints = hint_headers::extract_hints(request.headers());
    if let Some(query_ctx) = request.extensions_mut().get_mut::<QueryContext>() {
        for (key, value) in hints {
            apply_hint(query_ctx, key, value);
        }
    }
    next.run(request).await
}

fn apply_hint(query_ctx: &mut QueryContext, key: String, value: String) {
    if is_reserved_extension_key(&key) {
        return;
    }
    query_ctx.set_extension(key, value);
}

#[cfg(test)]
mod tests {
    use session::context::{QueryContextBuilder, generate_remote_query_id};

    use super::*;

    #[test]
    fn test_apply_hint_ignores_remote_query_id() {
        let expected_remote_query_id = generate_remote_query_id();
        let mut query_ctx = QueryContextBuilder::default()
            .set_extension(
                REMOTE_QUERY_ID_EXTENSION_KEY.to_string(),
                expected_remote_query_id.clone(),
            )
            .build();

        apply_hint(
            &mut query_ctx,
            REMOTE_QUERY_ID_EXTENSION_KEY.to_string(),
            "spoofed-query-id".to_string(),
        );
        apply_hint(
            &mut query_ctx,
            "auto_create_table".to_string(),
            "true".to_string(),
        );

        assert_eq!(
            query_ctx.remote_query_id(),
            Some(expected_remote_query_id.as_str())
        );
        assert_eq!(query_ctx.extension("auto_create_table"), Some("true"));
    }
}
