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

use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_error::{GREPTIME_DB_HEADER_ERROR_CODE, GREPTIME_DB_HEADER_ERROR_MSG};
use common_telemetry::logging::{debug, error};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::ResponseFormat;

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ErrorResponse {
    #[serde(skip)]
    ty: ResponseFormat,
    code: u32,
    error: String,
    execution_time_ms: u64,
}

impl ErrorResponse {
    pub fn from_error(ty: ResponseFormat, error: impl ErrorExt) -> Self {
        let code = error.status_code();

        if code.should_log_error() {
            error!(error; "Failed to handle HTTP request");
        } else {
            debug!("Failed to handle HTTP request, err: {:?}", error);
        }

        Self::from_error_message(ty, code, error.output_msg())
    }

    pub fn from_error_message(ty: ResponseFormat, code: StatusCode, msg: String) -> Self {
        ErrorResponse {
            ty,
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
        let ty = self.ty.as_str();
        let code = self.code;
        let msg = self.error.clone();
        let execution_time = self.execution_time_ms;
        let mut resp = Json(self).into_response();
        resp.headers_mut()
            .insert(GREPTIME_DB_HEADER_ERROR_CODE, HeaderValue::from(code));
        resp.headers_mut().insert(
            GREPTIME_DB_HEADER_ERROR_MSG,
            HeaderValue::from_str(&msg).expect("malformed error msg"),
        );
        resp.headers_mut()
            .insert(GREPTIME_DB_HEADER_FORMAT, HeaderValue::from_static(ty));
        resp.headers_mut().insert(
            GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        resp
    }
}
