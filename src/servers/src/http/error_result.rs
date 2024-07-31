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

use axum::http::{HeaderValue, StatusCode as HttpStatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_telemetry::{debug, error};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::header::constants::GREPTIME_DB_HEADER_ERROR_CODE;
use crate::http::header::GREPTIME_DB_HEADER_EXECUTION_TIME;

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ErrorResponse {
    code: u32,
    error: String,
    execution_time_ms: u64,
}

impl ErrorResponse {
    pub fn from_error(error: impl ErrorExt) -> Self {
        let code = error.status_code();

        if code.should_log_error() {
            error!(error; "Failed to handle HTTP request");
        } else {
            debug!("Failed to handle HTTP request, err: {:?}", error);
        }

        Self::from_error_message(code, error.output_msg())
    }

    pub fn from_error_message(code: StatusCode, msg: String) -> Self {
        ErrorResponse {
            code: code as u32,
            error: msg,
            execution_time_ms: 0,
        }
    }

    pub fn with_execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time_ms = execution_time;
        self
    }

    pub fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn error(&self) -> &str {
        &self.error
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let code = self.code;
        let execution_time = self.execution_time_ms;
        let mut resp = Json(self).into_response();
        resp.headers_mut()
            .insert(GREPTIME_DB_HEADER_ERROR_CODE, HeaderValue::from(code));
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        let status = StatusCode::from_u32(code).unwrap_or(StatusCode::Unknown);
        let status_code = status_code_to_http_status(&status);

        (status_code, resp).into_response()
    }
}

pub fn status_code_to_http_status(status_code: &StatusCode) -> HttpStatusCode {
    match status_code {
        StatusCode::Success | StatusCode::Cancelled => HttpStatusCode::OK,

        StatusCode::Unsupported
        | StatusCode::InvalidArguments
        | StatusCode::InvalidSyntax
        | StatusCode::RequestOutdated
        | StatusCode::RegionAlreadyExists
        | StatusCode::TableColumnExists
        | StatusCode::TableAlreadyExists
        | StatusCode::RegionNotFound
        | StatusCode::DatabaseNotFound
        | StatusCode::TableNotFound
        | StatusCode::TableColumnNotFound
        | StatusCode::PlanQuery
        | StatusCode::DatabaseAlreadyExists
        | StatusCode::FlowNotFound
        | StatusCode::FlowAlreadyExists => HttpStatusCode::BAD_REQUEST,

        StatusCode::AuthHeaderNotFound
        | StatusCode::InvalidAuthHeader
        | StatusCode::UserNotFound
        | StatusCode::UnsupportedPasswordType
        | StatusCode::UserPasswordMismatch
        | StatusCode::RegionReadonly => HttpStatusCode::UNAUTHORIZED,

        StatusCode::PermissionDenied | StatusCode::AccessDenied => HttpStatusCode::FORBIDDEN,

        StatusCode::RateLimited => HttpStatusCode::TOO_MANY_REQUESTS,

        StatusCode::RegionNotReady
        | StatusCode::TableUnavailable
        | StatusCode::RegionBusy
        | StatusCode::StorageUnavailable => HttpStatusCode::SERVICE_UNAVAILABLE,

        StatusCode::Internal
        | StatusCode::Unexpected
        | StatusCode::IllegalState
        | StatusCode::Unknown
        | StatusCode::RuntimeResourcesExhausted
        | StatusCode::EngineExecuteQuery => HttpStatusCode::INTERNAL_SERVER_ERROR,
    }
}
