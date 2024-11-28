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

use std::cmp::max;
use std::fmt::{Display, Write};

use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use common_error::status_code::StatusCode;
use common_query::Output;
use itertools::Itertools;
use mime_guess::mime;
use serde::{Deserialize, Serialize};

use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::result::error_result::ErrorResponse;
use crate::http::{handler, process_with_limit, GreptimeQueryOutput, HttpResponse, ResponseFormat};

#[derive(Serialize, Deserialize, Debug)]
pub struct TableResponse {
    output: Vec<GreptimeQueryOutput>,
    execution_time_ms: u64,
}

impl TableResponse {
    pub async fn from_output(outputs: Vec<crate::error::Result<Output>>) -> HttpResponse {
        match handler::from_output(outputs).await {
            Err(err) => HttpResponse::Error(err),
            Ok((output, _)) => {
                if output.len() > 1 {
                    HttpResponse::Error(ErrorResponse::from_error_message(
                        StatusCode::InvalidArguments,
                        "cannot output multi-statements result in table format".to_string(),
                    ))
                } else {
                    HttpResponse::Table(TableResponse {
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

impl Display for TableResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = match self.output.first() {
            None => String::default(),
            Some(GreptimeQueryOutput::AffectedRows(n)) => {
                format!("{n}\n")
            }
            Some(GreptimeQueryOutput::Records(records)) => {
                let mut max_width = vec![0; records.num_cols()];
                let mut result = String::new();
                // Determine maximum width for each column
                for (i, column) in records.schema.column_schemas.iter().enumerate() {
                    max_width[i] = max(max_width[i], column.name.len());
                }
                for row in &records.rows {
                    for (i, v) in row.iter().enumerate() {
                        let s = v.to_string();
                        max_width[i] = max(max_width[i], s.len());
                    }
                }

                // Construct the header
                let head: String = records
                    .schema
                    .column_schemas
                    .iter()
                    .enumerate()
                    .map(|(i, column)| format!("─{:─<1$}─", column.name, max_width[i]))
                    .join("┬");
                writeln!(result, "┌{}┐", head).unwrap();

                // Construct rows
                for row in &records.rows {
                    let row = row
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            let s = v.to_string();
                            format!(" {:1$} ", s, max_width[i])
                        })
                        .join("│");
                    writeln!(result, "│{row}│").unwrap();
                }

                // Construct the footer
                let footer: String = max_width.iter().map(|v| "─".repeat(*v + 2)).join("┴");
                writeln!(result, "└{}┘", footer).unwrap();
                result
            }
        };
        write!(f, "{}", payload)
    }
}

impl IntoResponse for TableResponse {
    fn into_response(self) -> Response {
        debug_assert!(
            self.output.len() <= 1,
            "self.output has extra elements: {}",
            self.output.len()
        );

        let execution_time = self.execution_time_ms;

        let mut resp = (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
            )],
            self.to_string(),
        )
            .into_response();
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_FORMAT,
            HeaderValue::from_static(ResponseFormat::Table.as_str()),
        );
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        resp
    }
}
#[cfg(test)]
mod test {

    use super::TableResponse;

    #[tokio::test]
    async fn test_table_format() {
        let data = r#"{"output":[{"records":{"schema":{"column_schemas":[{"name":"host","data_type":"String"},{"name":"ts","data_type":"TimestampMillisecond"},{"name":"cpu","data_type":"Float64"},{"name":"memory","data_type":"Float64"}]},"rows":[["127.0.0.1",1702433141000,0.5,0.2],["127.0.0.1",1702433146000,0.3,0.2],["127.0.0.1",1702433151000,0.4,0.3],["127.0.0.2",1702433141000,0.3,0.1],["127.0.0.2",1702433146000,0.2,0.4],["127.0.0.2",1702433151000,0.2,0.4]]}}],"execution_time_ms":13}"#;
        let table_response: TableResponse = serde_json::from_str(data).unwrap();
        let payload = table_response.to_string();
        let expected_payload = r#"┌─host────────┬─ts────────────┬─cpu─┬─memory─┐
│ "127.0.0.1" │ 1702433141000 │ 0.5 │ 0.2    │
│ "127.0.0.1" │ 1702433146000 │ 0.3 │ 0.2    │
│ "127.0.0.1" │ 1702433151000 │ 0.4 │ 0.3    │
│ "127.0.0.2" │ 1702433141000 │ 0.3 │ 0.1    │
│ "127.0.0.2" │ 1702433146000 │ 0.2 │ 0.4    │
│ "127.0.0.2" │ 1702433151000 │ 0.2 │ 0.4    │
└─────────────┴───────────────┴─────┴────────┘
"#;
        assert_eq!(payload, expected_payload);
    }
}
