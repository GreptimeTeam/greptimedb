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

use axum::response::{IntoResponse, Response};
use axum::Json;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::util;
use reqwest::header::HeaderValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::error_result::ErrorResponse;
use crate::http::{GreptimeQueryOutput, HttpRecordsOutput, HttpResponse};

pub(crate) const GREPTIME_V1_TYPE: &str = "greptime_v1";

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GreptimedbV1Response {
    // deprecated - backward compatible
    r#type: &'static str,
    code: u32,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) output: Vec<GreptimeQueryOutput>,
    pub(crate) execution_time_ms: u64,
}

impl GreptimedbV1Response {
    /// Create a response from query result
    pub async fn from_output(outputs: Vec<crate::error::Result<Output>>) -> HttpResponse {
        fn make_error_response(error: impl ErrorExt) -> HttpResponse {
            HttpResponse::Error(ErrorResponse::from_error(GREPTIME_V1_TYPE, error))
        }

        // TODO(sunng87): this api response structure cannot represent error well.
        //  It hides successful execution results from error response
        let mut results = Vec::with_capacity(outputs.len());
        for out in outputs {
            match out {
                Ok(Output::AffectedRows(rows)) => {
                    results.push(GreptimeQueryOutput::AffectedRows(rows));
                }
                Ok(Output::Stream(stream)) => {
                    // TODO(sunng87): streaming response
                    match util::collect(stream).await {
                        Ok(rows) => match HttpRecordsOutput::try_from(rows) {
                            Ok(rows) => {
                                results.push(GreptimeQueryOutput::Records(rows));
                            }
                            Err(err) => {
                                return make_error_response(err);
                            }
                        },
                        Err(err) => {
                            return make_error_response(err);
                        }
                    }
                }
                Ok(Output::RecordBatches(rbs)) => match HttpRecordsOutput::try_from(rbs.take()) {
                    Ok(rows) => {
                        results.push(GreptimeQueryOutput::Records(rows));
                    }
                    Err(err) => {
                        return make_error_response(err);
                    }
                },
                Err(err) => {
                    return make_error_response(err);
                }
            }
        }

        HttpResponse::GreptimedbV1(GreptimedbV1Response {
            r#type: GREPTIME_V1_TYPE,
            code: StatusCode::Success as u32,
            output: results,
            execution_time_ms: 0,
        })
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

impl IntoResponse for GreptimedbV1Response {
    fn into_response(self) -> Response {
        let mut resp = Json(self).into_response();
        resp.headers_mut().insert(
            "X-GreptimeDB-Format",
            HeaderValue::from_static("greptimedb_v1"),
        );
        resp
    }
}
