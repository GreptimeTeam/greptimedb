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

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_ipc::writer::FileWriter;
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use common_error::status_code::StatusCode;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatchStream;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Error};
use crate::http::error_result::ErrorResponse;
use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::{HttpResponse, ResponseFormat};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ArrowResponse {
    pub(crate) data: Vec<u8>,
    pub(crate) execution_time_ms: u64,
}

async fn write_arrow_bytes(
    mut recordbatches: Pin<Box<dyn RecordBatchStream + Send>>,
    schema: &Arc<Schema>,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, schema).context(error::ArrowSnafu)?;

        while let Some(rb) = recordbatches.next().await {
            let rb = rb.context(error::CollectRecordbatchSnafu)?;
            writer
                .write(&rb.into_df_record_batch())
                .context(error::ArrowSnafu)?;
        }

        writer.finish().context(error::ArrowSnafu)?;
    }

    Ok(bytes)
}

impl ArrowResponse {
    pub async fn from_output(mut outputs: Vec<error::Result<Output>>) -> HttpResponse {
        if outputs.len() > 1 {
            return HttpResponse::Error(ErrorResponse::from_error_message(
                StatusCode::InvalidArguments,
                "cannot output multi-statements result in arrow format".to_string(),
            ));
        }

        match outputs.pop() {
            None => HttpResponse::Arrow(ArrowResponse {
                data: vec![],
                execution_time_ms: 0,
            }),
            Some(Ok(output)) => match output.data {
                OutputData::AffectedRows(_) => HttpResponse::Arrow(ArrowResponse {
                    data: vec![],
                    execution_time_ms: 0,
                }),
                OutputData::RecordBatches(batches) => {
                    let schema = batches.schema();
                    match write_arrow_bytes(batches.as_stream(), schema.arrow_schema()).await {
                        Ok(payload) => HttpResponse::Arrow(ArrowResponse {
                            data: payload,
                            execution_time_ms: 0,
                        }),
                        Err(e) => HttpResponse::Error(ErrorResponse::from_error(e)),
                    }
                }
                OutputData::Stream(batches) => {
                    let schema = batches.schema();
                    match write_arrow_bytes(batches, schema.arrow_schema()).await {
                        Ok(payload) => HttpResponse::Arrow(ArrowResponse {
                            data: payload,
                            execution_time_ms: 0,
                        }),
                        Err(e) => HttpResponse::Error(ErrorResponse::from_error(e)),
                    }
                }
            },
            Some(Err(e)) => HttpResponse::Error(ErrorResponse::from_error(e)),
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
                    &header::CONTENT_TYPE,
                    HeaderValue::from_static("application/arrow"),
                ),
                (
                    &GREPTIME_DB_HEADER_FORMAT,
                    HeaderValue::from_static(ResponseFormat::Arrow.as_str()),
                ),
                (
                    &GREPTIME_DB_HEADER_EXECUTION_TIME,
                    HeaderValue::from(execution_time),
                ),
            ],
            self.data,
        )
            .into_response()
    }
}
