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

use std::fmt::Debug;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use servers::http::result::error_result::ErrorResponse;
use snafu::{ResultExt, ensure};
use tonic::codegen::http;

use crate::error::{self, Result};
use crate::metasrv::Metasrv;

/// Returns a 200 response with a text body.
pub fn to_text_response(text: &str) -> Result<http::Response<String>> {
    http::Response::builder()
        .header("Content-Type", "text/plain")
        .status(http::StatusCode::OK)
        .body(text.to_string())
        .context(error::InvalidHttpBodySnafu)
}

/// Returns a 200 response with a JSON body.
pub fn to_json_response<T>(response: T) -> Result<http::Response<String>>
where
    T: Serialize + Debug,
{
    let response = serde_json::to_string(&response).context(error::SerializeToJsonSnafu {
        input: format!("{response:?}"),
    })?;
    http::Response::builder()
        .header("Content-Type", "application/json")
        .status(http::StatusCode::OK)
        .body(response)
        .context(error::InvalidHttpBodySnafu)
}

/// Returns a 404 response with an empty body.
pub fn to_not_found_response() -> Result<http::Response<String>> {
    http::Response::builder()
        .status(http::StatusCode::NOT_FOUND)
        .body("".to_string())
        .context(error::InvalidHttpBodySnafu)
}

/// A wrapper for error handling in admin services.
pub(crate) struct ErrorHandler<E>(E)
where
    E: std::error::Error + Send + Sync + Sized;

impl<E> ErrorHandler<E>
where
    E: std::error::Error + Send + Sync + Sized,
{
    pub(crate) fn new(error: E) -> Self {
        Self(error)
    }
}

impl<E> IntoResponse for ErrorHandler<E>
where
    E: std::error::Error + Send + Sync + Sized,
{
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

/// Uses the same leadership predicate as the metasrv gRPC procedure service.
pub(crate) fn ensure_leader(metasrv: &Metasrv) -> Result<()> {
    ensure!(
        metasrv.is_leader(),
        error::IsNotLeaderSnafu {
            node_addr: metasrv.options().grpc.server_addr.clone(),
        }
    );
    Ok(())
}

/// Preserves the shared JSON error format while identifying retry and conflict responses.
pub(crate) fn admin_error_response(error: error::Error) -> Response {
    let status = match &error {
        error::Error::IsNotLeader { .. } => Some(StatusCode::SERVICE_UNAVAILABLE),
        error::Error::DrainDatanodeRunning { .. } => Some(StatusCode::CONFLICT),
        error::Error::ProcedureNotFound { .. } => Some(StatusCode::NOT_FOUND),
        _ => None,
    };
    let mut response = ErrorResponse::from_error(error).into_response();
    if let Some(status) = status {
        *response.status_mut() = status;
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_admin_error_preserves_json_envelope() {
        let response =
            admin_error_response(error::DrainDatanodeRunningSnafu { node_id: 1_u64 }.build());
        assert_eq!(StatusCode::CONFLICT, response.status());
        assert_eq!("application/json", response.headers()["content-type"]);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            serde_json::json!({
                "code": common_error::status_code::StatusCode::Unexpected as u32,
                "error": "Another drain procedure is running for datanode: 1",
                "execution_time_ms": 0,
            }),
            body
        );
    }
}
