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

use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use common_error::status_code::StatusCode;
use common_query::Output;
use mime_guess::mime;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::result::error_result::ErrorResponse;
use crate::http::{GreptimeQueryOutput, HttpResponse, ResponseFormat, handler, process_with_limit};

/// The json format here is different from the default json output of `GreptimedbV1` result.
/// `JsonResponse` is intended to make it easier for user to consume data.
#[derive(Serialize, Deserialize, Debug)]
pub struct JsonResponse {
    output: Vec<GreptimeQueryOutput>,
    execution_time_ms: u64,
}
impl JsonResponse {
    pub async fn from_output(
        outputs: Vec<crate::error::Result<Output>>,
        max_result_rows: usize,
    ) -> HttpResponse {
        match handler::from_output(outputs, max_result_rows).await {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::status_code::StatusCode;
    use common_query::Output;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{UInt32Vector, VectorRef};

    use super::*;

    fn numbers_recordbatches() -> RecordBatches {
        let column_schemas = vec![ColumnSchema::new(
            "numbers",
            ConcreteDataType::uint32_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4]))];
        let recordbatch = RecordBatch::new(schema.clone(), columns).unwrap();
        RecordBatches::try_new(schema, vec![recordbatch]).unwrap()
    }

    #[tokio::test]
    async fn http_json_response_respects_max_result_rows() {
        // A result larger than the limit errors cleanly instead of being buffered whole.
        let recordbatches = numbers_recordbatches();
        let outputs = vec![Ok(Output::new_with_record_batches(recordbatches))];
        match JsonResponse::from_output(outputs, 2).await {
            HttpResponse::Error(err) => {
                assert_eq!(err.code(), StatusCode::RuntimeResourcesExhausted as u32);
                assert!(err.error().contains("maximum of 2"));
            }
            _ => panic!("expected error response"),
        }

        // A result within the limit still works.
        let recordbatches = numbers_recordbatches();
        let outputs = vec![Ok(Output::new_with_record_batches(recordbatches))];
        match JsonResponse::from_output(outputs, 4).await {
            HttpResponse::Json(resp) => {
                let records = match resp.output().first().unwrap() {
                    GreptimeQueryOutput::Records(records) => records,
                    _ => unreachable!(),
                };
                assert_eq!(records.num_rows(), 4);
            }
            _ => panic!("expected json response"),
        }
    }
}
