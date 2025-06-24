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
                    .terminator(csv::Terminator::CRLF) // RFC 4180
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::Output;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Float32Vector, StringVector, UInt32Vector, VectorRef};

    use super::*;
    #[tokio::test]
    async fn test_csv_response_with_names_and_types() {
        let (schema, columns) = create_test_data();

        // Test with_names=true, with_types=true
        {
            let body = get_csv_body(&schema, &columns, true, true).await;
            assert!(body.starts_with("col1,col2,col3\r\nUInt32,String,Float32\r\n"));
            assert!(body.contains("1,,-1000.1400146484375\r\n2,hello,1.9900000095367432"));
        }

        // Test with_names=true, with_types=false
        {
            let body = get_csv_body(&schema, &columns, true, false).await;
            assert!(body.starts_with("col1,col2,col3\r\n"));
            assert!(!body.contains("UInt32,String,Float3"));
            assert!(body.contains("1,,-1000.1400146484375\r\n2,hello,1.9900000095367432"));
        }

        // Test with_names=false, with_types=false
        {
            let body = get_csv_body(&schema, &columns, false, false).await;
            assert!(!body.starts_with("col1,col2,col3"));
            assert!(!body.contains("UInt32,String,Float3"));
            assert!(body.contains("1,,-1000.1400146484375\r\n2,hello,1.9900000095367432"));
        }
    }

    fn create_test_data() -> (Arc<Schema>, Vec<VectorRef>) {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("col2", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("col3", ConcreteDataType::float32_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));

        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2])),
            Arc::new(StringVector::from(vec![None, Some("hello")])),
            Arc::new(Float32Vector::from_slice(vec![-1000.14, 1.99])),
        ];

        (schema, columns)
    }

    async fn get_csv_body(
        schema: &Arc<Schema>,
        columns: &[VectorRef],
        with_names: bool,
        with_types: bool,
    ) -> String {
        let recordbatch = RecordBatch::new(schema.clone(), columns.to_vec()).unwrap();
        let recordbatches = RecordBatches::try_new(schema.clone(), vec![recordbatch]).unwrap();
        let output = Output::new_with_record_batches(recordbatches);
        let outputs = vec![Ok(output)];

        let resp = CsvResponse::from_output(outputs, with_names, with_types)
            .await
            .into_response();
        let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}
