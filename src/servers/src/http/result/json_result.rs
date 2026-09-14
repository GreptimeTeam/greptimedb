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

                let data = Value::Array(data.into_iter().map(Value::Object).collect());
                let mut payload = Map::new();
                payload.insert("data".to_string(), data);
                payload.insert("execution_time_ms".to_string(), Value::from(execution_time));
                Value::Object(payload).to_string()
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
    use axum::body::to_bytes;
    use axum::http::{StatusCode, header};
    use serde_json::{Map, Value, json};

    use super::*;
    use crate::http::{ColumnSchema, HttpRecordsOutput, OutputSchema};

    fn records(column_names: &[&str], rows: Vec<Vec<Value>>) -> HttpRecordsOutput {
        let total_rows = rows.len();
        HttpRecordsOutput {
            schema: OutputSchema::new(
                column_names
                    .iter()
                    .map(|name| ColumnSchema::new((*name).to_string(), "Json".to_string()))
                    .collect(),
            ),
            rows,
            total_rows,
            metrics: Default::default(),
        }
    }

    fn old_records_payload(records: &HttpRecordsOutput, execution_time: u64) -> String {
        let schema = records.schema();
        let data: Vec<Map<String, Value>> = records
            .rows()
            .iter()
            .map(|row| {
                schema
                    .column_schemas
                    .iter()
                    .enumerate()
                    .map(|(i, col)| (col.name.clone(), row[i].clone()))
                    .collect()
            })
            .collect();

        json!({
            "data": data,
            "execution_time_ms": execution_time,
        })
        .to_string()
    }

    #[tokio::test]
    async fn test_records_response_matches_old_json_representation() {
        let records = records(
            &["z", "a", "z", "nested", "escaped", "nullable"],
            vec![
                vec![
                    json!(1),
                    json!("first"),
                    json!(2),
                    json!({"outer": {"items": [null, "value"]}}),
                    json!("line\nwith \"quotes\" and \\slashes"),
                    Value::Null,
                ],
                vec![
                    json!(3),
                    json!("second"),
                    json!(4),
                    json!({"array": [true, {"key": "value"}]}),
                    json!("\tescaped"),
                    Value::Null,
                ],
            ],
        );
        let execution_time = 42;
        let expected = old_records_payload(&records, execution_time);

        let response = JsonResponse {
            output: vec![GreptimeQueryOutput::Records(records)],
            execution_time_ms: execution_time,
        }
        .into_response();

        assert_eq!(StatusCode::OK, response.status());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(expected.as_bytes(), body.as_ref());
    }

    #[tokio::test]
    async fn test_empty_records_response_matches_old_json_representation_and_headers() {
        let records = records(&[], vec![]);
        let execution_time = 42;
        let expected = old_records_payload(&records, execution_time);

        let response = JsonResponse {
            output: vec![GreptimeQueryOutput::Records(records)],
            execution_time_ms: execution_time,
        }
        .into_response();

        assert_eq!(StatusCode::OK, response.status());
        assert_eq!(
            mime::APPLICATION_JSON.as_ref(),
            response.headers().get(header::CONTENT_TYPE).unwrap()
        );
        assert_eq!(
            ResponseFormat::Json.as_str(),
            response.headers().get(&GREPTIME_DB_HEADER_FORMAT).unwrap()
        );
        assert_eq!(
            execution_time.to_string(),
            response
                .headers()
                .get(&GREPTIME_DB_HEADER_EXECUTION_TIME)
                .unwrap()
                .to_str()
                .unwrap()
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(expected.as_bytes(), body.as_ref());
    }
}
