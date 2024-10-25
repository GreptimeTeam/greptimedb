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

use std::collections::HashMap;

use axum::headers::HeaderValue;
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_query::Output;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::http::header::{
    GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT, GREPTIME_DB_HEADER_METRICS,
};
use crate::http::{handler, process_with_limit, GreptimeQueryOutput, HttpResponse, ResponseFormat};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GreptimedbV1Response {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) output: Vec<GreptimeQueryOutput>,
    pub(crate) execution_time_ms: u64,

    // placeholder for header value
    #[serde(skip)]
    #[serde(default)]
    pub(crate) resp_metrics: HashMap<String, Value>,
}

impl GreptimedbV1Response {
    pub async fn from_output(outputs: Vec<crate::error::Result<Output>>) -> HttpResponse {
        match handler::from_output(outputs).await {
            Ok((output, resp_metrics)) => HttpResponse::GreptimedbV1(Self {
                output,
                execution_time_ms: 0,
                resp_metrics,
            }),
            Err(err) => HttpResponse::Error(err),
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

impl IntoResponse for GreptimedbV1Response {
    fn into_response(self) -> Response {
        let execution_time = self.execution_time_ms;
        let metrics = if self.resp_metrics.is_empty() {
            None
        } else {
            serde_json::to_string(&self.resp_metrics).ok()
        };

        let mut resp = Json(self).into_response();

        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_FORMAT,
            HeaderValue::from_static(ResponseFormat::GreptimedbV1.as_str()),
        );
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        if let Some(m) = metrics.and_then(|m| HeaderValue::from_str(&m).ok()) {
            resp.headers_mut().insert(&GREPTIME_DB_HEADER_METRICS, m);
        }

        resp
    }
}
