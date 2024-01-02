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

use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use bytes::{BufMut, BytesMut};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_telemetry::logging::{debug, error};
use mime_guess::mime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ErrorResponse {
    // deprecated - backward compatible
    r#type: &'static str,

    code: u32,
    error: String,
    execution_time_ms: u64,
}

impl ErrorResponse {
    pub fn from_error(ty: &'static str, error: impl ErrorExt) -> Self {
        let code = error.status_code();

        if code.should_log_error() {
            error!(error; "Failed to handle HTTP request");
        } else {
            debug!("Failed to handle HTTP request, err: {:?}", error);
        }

        Self::from_error_message(ty, error.output_msg(), code)
    }

    pub fn from_error_message(ty: &'static str, msg: String, code: StatusCode) -> Self {
        ErrorResponse {
            r#type: ty,
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
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let mut buf = BytesMut::with_capacity(128).writer();
        if let Err(err) = serde_json::to_writer(&mut buf, &self) {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
                )],
                err.to_string(),
            )
                .into_response();
        }

        let mut headers = headers::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
        );
        headers.insert("X-GreptimeDB-Error-Code", HeaderValue::from(self.code));
        (headers, buf.into_inner().freeze()).into_response()
    }
}
