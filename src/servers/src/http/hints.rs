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
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use session::context::QueryContext;

use crate::hint_headers;
use crate::hints::compatibility::{self, SelectedHints};
use crate::hints::protocol::{Endpoint, Protocol};

/// Selects sources at the HTTP boundary; source errors precede value validation.
pub async fn extract_hints(mut request: Request<Body>, next: Next) -> Response {
    match compatibility::select_http_inputs(
        hint_headers::extract_hints(request.headers()),
        request.headers(),
    ) {
        Ok(selected) => {
            request.extensions_mut().insert(selected);
        }
        Err(error) => return error.into_response(),
    }
    next.run(request).await
}

/// Applies a complete batch only after all registered values have been validated.
pub(crate) async fn apply_protocol_hints(
    State(protocol): State<Protocol>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    if let Some(selected) = request.extensions_mut().remove::<SelectedHints>() {
        let parsed = match crate::hints::parse_endpoint(Endpoint::Http(protocol), selected.hints) {
            Ok(parsed) => parsed,
            Err(error) => return error.into_response(),
        };
        if let Some(context) = request.extensions_mut().get_mut::<QueryContext>() {
            parsed.apply(context);
            for hint in selected.dedicated {
                hint.apply(context);
            }
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use crate::http::test_helpers::TestClient;
    use axum::Router;
    use axum::middleware;
    use axum::routing::get;
    use http::StatusCode;
    use session::hints;

    use super::*;
    use crate::http::header::constants::{
        GREPTIME_DB_HEADER_READ_PREFERENCE, GREPTIME_INSERT_SKIP_WAL_HEADER_NAME,
    };

    async fn context_probe(
        axum::Extension(context): axum::Extension<QueryContext>,
    ) -> axum::Json<serde_json::Value> {
        axum::Json(serde_json::json!({
            "wal": context.skip_wal(),
            "table_wal": context.extension(table::requests::SKIP_WAL_KEY),
            "unbound_wal": context.extension(hints::INSERT_SKIP_WAL_HINT),
            "custom": context.extension("custom.extension"),
            "remote_id": context.extension(hints::REMOTE_QUERY_ID_EXTENSION_KEY),
        }))
    }

    async fn client(protocol: Protocol) -> TestClient {
        let app = Router::new()
            .route("/probe", get(context_probe))
            .layer(middleware::from_fn_with_state(
                protocol,
                apply_protocol_hints,
            ))
            .layer(middleware::from_fn(extract_hints))
            .layer(middleware::from_fn(
                |mut request: Request<Body>, next: Next| async move {
                    request
                        .extensions_mut()
                        .insert(QueryContext::with("greptime", "public"));
                    next.run(request).await
                },
            ));
        TestClient::new(app).await
    }

    #[tokio::test]
    async fn test_http_hint_sources_reach_context() {
        let server = client(Protocol::Sql).await;
        let cases = [
            (vec![(hints::HINTS_KEY, "insert_skip_wal=true")], true),
            (vec![], false),
            (vec![(compatibility::INSERT_SKIP_WAL_HEADER, "true")], true),
            (vec![(GREPTIME_INSERT_SKIP_WAL_HEADER_NAME, "true")], true),
            (
                vec![
                    (hints::HINTS_KEY, "insert_skip_wal=invalid"),
                    (GREPTIME_INSERT_SKIP_WAL_HEADER_NAME, "false"),
                ],
                false,
            ),
            (
                vec![
                    (hints::HINTS_KEY, ""),
                    (compatibility::INSERT_SKIP_WAL_HEADER, "true"),
                ],
                false,
            ),
            (
                vec![(
                    hints::HINTS_KEY,
                    "insert_skip_wal=true,insert_skip_wal=false",
                )],
                false,
            ),
        ];
        for (headers, expected) in cases {
            let mut request = server.get("/probe");
            for (name, value) in headers {
                request = request.header(name, value);
            }
            let response = request.send().await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.json::<serde_json::Value>().await["wal"], expected);
        }
    }

    #[tokio::test]
    async fn test_http_hint_invalid_batch_and_source_priority() {
        let server = client(Protocol::Sql).await;
        for value in [
            "invalid",
            "invalid,insert_skip_wal=true",
            "true,insert_skip_wal=invalid",
        ] {
            let response = server
                .get("/probe")
                .header(hints::HINTS_KEY, format!("insert_skip_wal={value}"))
                .send()
                .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        let response = server
            .get("/probe")
            .header(hints::HINTS_KEY, "query.parallelism=invalid")
            .header(GREPTIME_INSERT_SKIP_WAL_HEADER_NAME, "invalid")
            .send()
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response.text().await.contains("must be true or false"));
    }

    #[tokio::test]
    async fn test_http_hint_extensions_and_legacy_routing() {
        let server = client(Protocol::Other).await;
        let response = server.get("/probe")
            .header(hints::HINTS_KEY, "custom.extension=value,skip_wal=true,insert_skip_wal=true,remote_query_id=spoofed,read_preference=invalid")
            .header(GREPTIME_DB_HEADER_READ_PREFERENCE, "invalid").send().await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.json::<serde_json::Value>().await;
        assert_eq!(body["custom"], "value");
        assert_eq!(body["table_wal"], "true");
        assert_eq!(body["wal"], false);
        assert_eq!(body["unbound_wal"], "true");
        assert_ne!(body["remote_id"], "spoofed");
    }
}
