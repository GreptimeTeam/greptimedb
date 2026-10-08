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
use axum::response::{IntoResponse, Response};
use session::context::QueryContext;

use crate::error::InvalidParameterSnafu;
use crate::http::header::GREPTIME_INSERT_SKIP_WAL_HEADER_NAME;
use crate::http::result::error_result::ErrorResponse;

/// Extract the request-level WAL policy from the dedicated HTTP header.
pub async fn extract_skip_wal(mut request: Request<Body>, next: Next) -> Response {
    let skip_wal = match request.headers().get(&GREPTIME_INSERT_SKIP_WAL_HEADER_NAME) {
        None => false,
        Some(value) => match value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
        {
            Some(skip_wal) => skip_wal,
            None => {
                return (
                    http::StatusCode::BAD_REQUEST,
                    ErrorResponse::from_error(
                        InvalidParameterSnafu {
                            reason: format!(
                                "{} must be true or false",
                                GREPTIME_INSERT_SKIP_WAL_HEADER_NAME
                            ),
                        }
                        .build(),
                    ),
                )
                    .into_response();
            }
        },
    };
    if let Some(query_ctx) = request.extensions_mut().get_mut::<QueryContext>() {
        query_ctx.set_skip_wal(skip_wal);
    }
    next.run(request).await
}
