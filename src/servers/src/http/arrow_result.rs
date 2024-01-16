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

use arrow_ipc::writer::FileWriter;
use axum::http::{header, HeaderName, HeaderValue};
use axum::response::{IntoResponse, Response};
use common_error::status_code::StatusCode;
use common_query::Output;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::error_result::ErrorResponse;
use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::{HttpResponse, ResponseFormat};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ArrowResponse {
    data: Vec<u8>,
    execution_time_ms: u64,
}

impl ArrowResponse {
    pub async fn from_output(mut outputs: Vec<crate::error::Result<Output>>) -> HttpResponse {
        if outputs.len() > 1 {
            return HttpResponse::Error(ErrorResponse::from_error_message(
                ResponseFormat::Arrow,
                StatusCode::InvalidArguments,
                "Multi-statements are not allowed".to_string(),
            ));
        }

        match outputs.remove(0) {
            Ok(output) => {
                let payload = match output {
                    Output::AffectedRows(_rows) => {
                        vec![]
                    }
                    Output::RecordBatches(recordbatches) => {
                        let schema = recordbatches.schema();
                        let schema = schema.arrow_schema();

                        // unwrap should be safe here
                        let mut bytes = Vec::new();
                        {
                            let mut writer = FileWriter::try_new(&mut bytes, schema).unwrap();

                            for recordbatch in recordbatches {
                                writer.write(&recordbatch.into_df_record_batch()).unwrap();
                            }

                            writer.finish().unwrap();
                        }

                        bytes
                    }

                    Output::Stream(mut recordbatches) => {
                        let schema = recordbatches.schema();
                        let schema = schema.arrow_schema();

                        let mut bytes = Vec::new();
                        {
                            let mut writer = FileWriter::try_new(&mut bytes, schema).unwrap();

                            while let Some(rb) = recordbatches.next().await {
                                let rb = rb.unwrap();
                                writer.write(&rb.into_df_record_batch()).unwrap();
                            }

                            writer.finish().unwrap();
                        }

                        bytes
                    }
                };
                HttpResponse::Arrow(ArrowResponse {
                    data: payload,
                    execution_time_ms: 0,
                })
            }
            Err(e) => HttpResponse::Error(ErrorResponse::from_error(ResponseFormat::Arrow, e)),
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

impl IntoResponse for ArrowResponse {
    fn into_response(self) -> Response {
        let execution_time = self.execution_time_ms;
        (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/arrow"),
                ),
                (
                    HeaderName::from_static(GREPTIME_DB_HEADER_FORMAT),
                    HeaderValue::from_static("ARROW"),
                ),
                (
                    HeaderName::from_static(GREPTIME_DB_HEADER_EXECUTION_TIME),
                    HeaderValue::from(execution_time),
                ),
            ],
            self.data,
        )
            .into_response()
    }
}
