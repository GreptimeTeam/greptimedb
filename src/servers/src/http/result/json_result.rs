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
use common_error::status_code::StatusCode;
use common_query::Output;
use mime_guess::mime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::result::error_result::ErrorResponse;
use crate::http::{handler, process_with_limit, GreptimeQueryOutput, HttpResponse, ResponseFormat};

/// The json format here is different from the default json output of `GreptimedbV1` result.
/// `JsonResponse` is intended to make it easier for user to consume data.
#[derive(Serialize, Deserialize, Debug)]
pub struct JsonResponse {
    output: Vec<GreptimeQueryOutput>,
    execution_time_ms: u64,
}

impl JsonResponse {
    pub async fn from_output(outputs: Vec<crate::error::Result<Output>>) -> HttpResponse {
        match handler::from_output(outputs).await {
            Err(err) => HttpResponse::Error(err),
            Ok((output, _)) => {
                if output.len() > 1 {
                    HttpResponse::Error(ErrorResponse::from_error_message(
                        StatusCode::InvalidArguments,
                        "cannot output multi-statements result in json format".to_string(),
                    ))
                } else {
                    HttpResponse::Json(JsonResponse {
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

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.output = process_with_limit(self.output, limit);
        self
    }
}

impl IntoResponse for JsonResponse {
    fn into_response(mut self) -> Response {
        debug_assert!(
            self.output.len() <= 1,
            "self.output has extra elements: {}",
            self.output.len()
        );

        let execution_time = self.execution_time_ms;
        let payload = match self.output.pop() {
            None => String::default(),
            Some(GreptimeQueryOutput::AffectedRows(n)) => json!({
                "data": [],
                "affected_rows": n,
                "execution_time_ms": execution_time,
            })
            .to_string(),

            Some(GreptimeQueryOutput::Records(records)) => {
                let schema = records.schema();

                let data: Vec<Map<String, Value>> = records
                    .rows
                    .iter()
                    .map(|row| {
                        schema
                            .column_schemas
                            .iter()
                            .enumerate()
                            .map(|(i, col)| (col.name.clone(), row[i].clone()))
                            .collect::<Map<String, Value>>()
                    })
                    .collect();

                json!({
                    "data": data,
                    "execution_time_ms": execution_time,
                })
                .to_string()
            }
        };

        (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
                ),
                (
                    GREPTIME_DB_HEADER_FORMAT.clone(),
                    HeaderValue::from_static(ResponseFormat::Json.as_str()),
                ),
                (
                    GREPTIME_DB_HEADER_EXECUTION_TIME.clone(),
                    HeaderValue::from(execution_time),
                ),
            ],
            payload,
        )
            .into_response()
    }
}
