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

use axum::response::IntoResponse;
use axum::Json;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
use serde::{Deserialize, Serialize};

use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};

/// Greptimedb Manage Api Response struct
/// Currently we have `Pipelines` and `Scripts` as control panel api
#[derive(Serialize, Deserialize, Debug)]
pub struct GreptimedbManageResponse {
    #[serde(flatten)]
    pub(crate) manage_result: ManageResult,
    pub(crate) execution_time_ms: u64,
}

impl GreptimedbManageResponse {
    pub fn from_pipeline(name: String, version: String, execution_time_ms: u64) -> Self {
        GreptimedbManageResponse {
            manage_result: ManageResult::Pipelines {
                pipelines: vec![PipelineOutput { name, version }],
            },
            execution_time_ms,
        }
    }

    pub fn from_pipelines(pipelines: Vec<PipelineOutput>, execution_time_ms: u64) -> Self {
        GreptimedbManageResponse {
            manage_result: ManageResult::Pipelines { pipelines },
            execution_time_ms,
        }
    }

    pub fn with_execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time_ms = execution_time;
        self
    }

    pub fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum ManageResult {
    Pipelines { pipelines: Vec<PipelineOutput> },
    // todo(shuiyisong): refactor scripts api
    Scripts(),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PipelineOutput {
    name: String,
    version: String,
}

impl IntoResponse for GreptimedbManageResponse {
    fn into_response(self) -> axum::response::Response {
        let execution_time = self.execution_time_ms;

        let mut resp = Json(self).into_response();

        // We deliberately don't add this format into [`crate::http::ResponseFormat`]
        // because this is a format for manage api other than the data query api
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_FORMAT,
            HeaderValue::from_static("greptimedb_manage"),
        );
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        resp.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_str(mime_guess::mime::APPLICATION_JSON.as_ref()).unwrap(),
        );

        resp
    }
}

#[cfg(test)]
mod tests {
    use std::usize;

    use arrow::datatypes::ToByteSlice;
    use axum::body::to_bytes;

    use super::*;

    #[tokio::test]
    async fn test_into_response() {
        let resp = GreptimedbManageResponse {
            manage_result: ManageResult::Pipelines {
                pipelines: vec![PipelineOutput {
                    name: "test_name".to_string(),
                    version: "test_version".to_string(),
                }],
            },
            execution_time_ms: 42,
        };

        let re = resp.into_response();
        let data_str = format!("{:?}", re);
        assert_eq!(
            data_str,
            r#"Data(Response { status: 200, version: HTTP/1.1, headers: {"content-type": "application/json", "x-greptime-format": "greptimedb_manage", "x-greptime-execution-time": "42"}, body: UnsyncBoxBody })"#
        );

        let body_bytes = to_bytes(re.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8_lossy(body_bytes.to_byte_slice());
        assert_eq!(
            body_str,
            r#"{"pipelines":[{"name":"test_name","version":"test_version"}],"execution_time_ms":42}"#
        );
    }
}
