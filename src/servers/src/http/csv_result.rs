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
use itertools::Itertools;
use mime_guess::mime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::error_result::ErrorResponse;
use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::{handler, GreptimeQueryOutput, HttpResponse, ResponseFormat};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct CsvResponse {
    output: Vec<GreptimeQueryOutput>,
    execution_time_ms: u64,
}

impl CsvResponse {
    pub async fn from_output(outputs: Vec<crate::error::Result<Output>>) -> HttpResponse {
        match handler::from_output(ResponseFormat::Csv, outputs).await {
            Err(err) => HttpResponse::Error(err),
            Ok(output) => {
                if output.len() > 1 {
                    HttpResponse::Error(ErrorResponse::from_error_message(
                        ResponseFormat::Csv,
                        StatusCode::InvalidArguments,
                        "Multi-statements are not allowed".to_string(),
                    ))
                } else {
                    HttpResponse::Csv(CsvResponse {
                        output,
                        execution_time_ms: 0,
                    })
                }
            }
        }
    }

    pub fn output(&self) -> &[GreptimeQueryOutput] {
        &self.output
    }

    pub fn with_execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time_ms = execution_time;
        self
    }

    pub fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }
}

impl IntoResponse for CsvResponse {
    fn into_response(mut self) -> Response {
        debug_assert!(
            self.output.len() <= 1,
            "self.output has extra elements: {}",
            self.output.len()
        );

        let execution_time = self.execution_time_ms;
        let payload = match self.output.pop() {
            None => "".to_string(),
            Some(GreptimeQueryOutput::AffectedRows(n)) => {
                format!("{n}\n")
            }
            Some(GreptimeQueryOutput::Records(records)) => {
                let mut result = String::new();
                for row in records.rows {
                    let row = row.iter().map(|v| v.to_string()).join(",");
                    writeln!(result, "{row}").unwrap();
                }
                result
            }
        };

        let mut resp = (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(mime::TEXT_CSV_UTF_8.as_ref()),
            )],
            payload,
        )
            .into_response();
        resp.headers_mut()
            .insert(GREPTIME_DB_HEADER_FORMAT, HeaderValue::from_static("CSV"));
        resp.headers_mut().insert(
            GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        resp
    }
}
