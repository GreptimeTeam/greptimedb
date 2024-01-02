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

use common_error::status_code::StatusCode;
use common_query::Output;

use crate::http::error_result::ErrorResponse;
use crate::http::greptime_result_v1::{GreptimedbV1Response, GREPTIME_V1_TYPE};
use crate::http::{GreptimeQueryOutput, QueryResponse};

#[derive(Debug)]
pub struct CsvResponse {
    output: Vec<GreptimeQueryOutput>,
    execution_time_ms: u64,
}

impl CsvResponse {
    pub async fn from_output(outputs: Vec<crate::error::Result<Output>>) -> QueryResponse {
        let response = match GreptimedbV1Response::from_output(outputs).await {
            QueryResponse::GreptimedbV1(resp) => resp,
            QueryResponse::Error(resp) => {
                return QueryResponse::Error(resp);
            }
            resp => unreachable!("neither greptime_v1 nor error: {:?}", resp),
        };

        if response.output.len() > 1 {
            QueryResponse::Error(ErrorResponse::from_error_message(
                GREPTIME_V1_TYPE,
                "Multi-statements are not allowed".to_string(),
                StatusCode::InvalidArguments,
            ))
        } else {
            QueryResponse::Csv(CsvResponse {
                output: response.output,
                execution_time_ms: response.execution_time_ms,
            })
        }
    }

    pub fn output(&self) -> &[GreptimeQueryOutput] {
        &self.output
    }

    pub fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }
}
