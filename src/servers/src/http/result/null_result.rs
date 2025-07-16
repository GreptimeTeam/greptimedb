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

use std::fmt::Write;

use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use common_error::status_code::StatusCode;
use common_query::Output;
use mime_guess::mime;
use serde::{Deserialize, Serialize};

use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::result::error_result::ErrorResponse;
use crate::http::{handler, GreptimeQueryOutput, HttpResponse, ResponseFormat};

#[derive(Serialize, Deserialize, Debug)]
enum Rows {
    Affected(usize),
    Queried(usize),
}

/// The null format is a simple text format that outputs the number of affected rows or queried rows
#[derive(Serialize, Deserialize, Debug)]
pub struct NullResponse {
    rows: Rows,
    execution_time_ms: u64,
}

impl NullResponse {
    pub async fn from_output(outputs: Vec<crate::error::Result<Output>>) -> HttpResponse {
        match handler::from_output(outputs).await {
            Err(err) => HttpResponse::Error(err),
            Ok((mut output, _)) => {
                if output.len() > 1 {
                    HttpResponse::Error(ErrorResponse::from_error_message(
                        StatusCode::InvalidArguments,
                        "cannot output multi-statements result in null format".to_string(),
                    ))
                } else {
                    match output.pop() {
                        Some(GreptimeQueryOutput::AffectedRows(rows)) => {
                            HttpResponse::Null(NullResponse {
                                rows: Rows::Affected(rows),
                                execution_time_ms: 0,
                            })
                        }

                        Some(GreptimeQueryOutput::Records(records)) => {
                            HttpResponse::Null(NullResponse {
                                rows: Rows::Queried(records.num_rows()),
                                execution_time_ms: 0,
                            })
                        }
                        _ => HttpResponse::Error(ErrorResponse::from_error_message(
                            StatusCode::Unexpected,
                            "unexpected output type".to_string(),
                        )),
                    }
                }
            }
        }
    }

    /// Returns the number of rows affected or queried.
    pub fn rows(&self) -> usize {
        match &self.rows {
            Rows::Affected(rows) => *rows,
            Rows::Queried(rows) => *rows,
        }
    }

    /// Consumes `self`, updates the execution time in milliseconds, and returns the updated instance.
    pub(crate) fn with_execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time_ms = execution_time;
        self
    }
}

impl IntoResponse for NullResponse {
    fn into_response(self) -> Response {
        let mut body = String::new();
        match self.rows {
            Rows::Affected(rows) => {
                let _ = writeln!(body, "{} rows affected.", rows);
            }
            Rows::Queried(rows) => {
                let _ = writeln!(body, "{} rows in set.", rows);
            }
        }
        let elapsed_secs = (self.execution_time_ms as f64) / 1000.0;
        let _ = writeln!(body, "Elapsed: {:.3} sec.", elapsed_secs);

        let mut resp = (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
            )],
            body,
        )
            .into_response();
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_FORMAT,
            HeaderValue::from_static(ResponseFormat::Null.as_str()),
        );
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(self.execution_time_ms),
        );

        resp
    }
}
#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http;

    use super::*;

    #[tokio::test]
    async fn test_into_response_format() {
        let result = NullResponse {
            rows: Rows::Queried(42),
            execution_time_ms: 1234,
        };
        let response = result.into_response();

        // Check status code
        assert_eq!(response.status(), http::StatusCode::OK);

        // Check headers
        let headers = response.headers();
        assert_eq!(
            headers.get(axum::http::header::CONTENT_TYPE).unwrap(),
            mime::TEXT_PLAIN_UTF_8.as_ref()
        );
        assert_eq!(
            headers.get(&GREPTIME_DB_HEADER_FORMAT).unwrap(),
            ResponseFormat::Null.as_str()
        );
        assert_eq!(
            headers.get(&GREPTIME_DB_HEADER_EXECUTION_TIME).unwrap(),
            "1234"
        );

        // Check body
        let body_bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        let body = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body.contains("42 rows in set."));
        assert!(body.contains("Elapsed: 1.234 sec."));
    }
}
