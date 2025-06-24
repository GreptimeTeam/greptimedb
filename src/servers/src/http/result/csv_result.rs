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
use serde_json::Value as JsonValue;

use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
// use super::process_with_limit;
use crate::http::result::error_result::ErrorResponse;
use crate::http::{handler, process_with_limit, GreptimeQueryOutput, HttpResponse, ResponseFormat};

#[derive(Serialize, Deserialize, Debug)]
pub struct CsvResponse {
    output: Vec<GreptimeQueryOutput>,
    execution_time_ms: u64,
    with_names: bool,
    with_types: bool,
}

impl CsvResponse {
    pub async fn from_output(
        outputs: Vec<crate::error::Result<Output>>,
        with_names: bool,
        with_types: bool,
    ) -> HttpResponse {
        match handler::from_output(outputs).await {
            Err(err) => HttpResponse::Error(err),
            Ok((output, _)) => {
                if output.len() > 1 {
                    HttpResponse::Error(ErrorResponse::from_error_message(
                        StatusCode::InvalidArguments,
                        "cannot output multi-statements result in csv format".to_string(),
                    ))
                } else {
                    let csv_resp = CsvResponse {
                        output,
                        execution_time_ms: 0,
                        with_names: false,
                        with_types: false,
                    };

                    HttpResponse::Csv(csv_resp.with_names(with_names).with_types(with_types))
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

    pub fn with_names(mut self, with_names: bool) -> Self {
        self.with_names = with_names;
        self
    }

    pub fn with_types(mut self, with_types: bool) -> Self {
        self.with_types = with_types;

        // If `with_type` is true, than always set `with_names` to be true.
        if with_types {
            self.with_names = true;
        }
        self
    }
}

macro_rules! http_try {
    ($handle: expr) => {
        match $handle {
            Ok(res) => res,
            Err(err) => {
                let msg = err.to_string();
                return HttpResponse::Error(ErrorResponse::from_error_message(
                    StatusCode::Unexpected,
                    msg,
                ))
                .into_response();
            }
        }
    };
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
            None => String::default(),
            Some(GreptimeQueryOutput::AffectedRows(n)) => {
                format!("{n}\n")
            }
            Some(GreptimeQueryOutput::Records(records)) => {
                let mut wtr = csv::WriterBuilder::new()
                    .terminator(csv::Terminator::CRLF)
                    .from_writer(Vec::new());

                if self.with_names {
                    let names = records
                        .schema
                        .column_schemas
                        .iter()
                        .map(|c| &c.name)
                        .collect::<Vec<_>>();
                    http_try!(wtr.serialize(names));
                }

                if self.with_types {
                    let types = records
                        .schema
                        .column_schemas
                        .iter()
                        .map(|c| &c.data_type)
                        .collect::<Vec<_>>();
                    http_try!(wtr.serialize(types));
                }

                for row in records.rows {
                    let row = row
                        .into_iter()
                        .map(|value| {
                            match value {
                                // Cast array and object to string
                                JsonValue::Array(a) => {
                                    JsonValue::String(serde_json::to_string(&a).unwrap_or_default())
                                }
                                JsonValue::Object(o) => {
                                    JsonValue::String(serde_json::to_string(&o).unwrap_or_default())
                                }
                                v => v,
                            }
                        })
                        .collect::<Vec<_>>();

                    http_try!(wtr.serialize(row));
                }

                http_try!(wtr.flush());

                let bytes = http_try!(wtr.into_inner());
                http_try!(String::from_utf8(bytes))
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
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_FORMAT,
            HeaderValue::from_static(
                ResponseFormat::Csv(self.with_names, self.with_types).as_str(),
            ),
        );
        resp.headers_mut().insert(
            &GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        resp
    }
}
