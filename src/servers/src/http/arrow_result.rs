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
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_ipc::CompressionType;
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
    compression: Option<CompressionType>,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    {
        let options = IpcWriteOptions::default()
            .try_with_compression(compression)
            .context(error::ArrowSnafu)?;
        let mut writer = FileWriter::try_new_with_options(&mut bytes, schema, options)
            .context(error::ArrowSnafu)?;

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

fn compression_type(compression: Option<String>) -> Option<CompressionType> {
    match compression
        .map(|compression| compression.to_lowercase())
        .as_deref()
    {
        Some("zstd") => Some(CompressionType::ZSTD),
        Some("lz4") => Some(CompressionType::LZ4_FRAME),
        _ => None,
    }
}

impl ArrowResponse {
    pub async fn from_output(
        mut outputs: Vec<error::Result<Output>>,
        compression: Option<String>,
    ) -> HttpResponse {
        if outputs.len() > 1 {
            return HttpResponse::Error(ErrorResponse::from_error_message(
                StatusCode::InvalidArguments,
                "cannot output multi-statements result in arrow format".to_string(),
            ));
        }

        let compression = compression_type(compression);

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
                    match write_arrow_bytes(batches.as_stream(), schema.arrow_schema(), compression)
                        .await
                    {
                        Ok(payload) => HttpResponse::Arrow(ArrowResponse {
                            data: payload,
                            execution_time_ms: 0,
                        }),
                        Err(e) => HttpResponse::Error(ErrorResponse::from_error(e)),
                    }
                }
                OutputData::Stream(batches) => {
                    let schema = batches.schema();
                    match write_arrow_bytes(batches, schema.arrow_schema(), compression).await {
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

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use arrow_ipc::reader::FileReader;
    use arrow_schema::DataType;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{StringVector, UInt32Vector};

    use super::*;

    #[tokio::test]
    async fn test_arrow_output() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
                None,
            ])),
        ];

        for compression in [None, Some("zstd".to_string()), Some("lz4".to_string())].into_iter() {
            let recordbatch = RecordBatch::new(schema.clone(), columns.clone()).unwrap();
            let recordbatches =
                RecordBatches::try_new(schema.clone(), vec![recordbatch.clone()]).unwrap();
            let outputs = vec![Ok(Output::new_with_record_batches(recordbatches))];

            let http_resp = ArrowResponse::from_output(outputs, compression).await;
            match http_resp {
                HttpResponse::Arrow(resp) => {
                    let output = resp.data;
                    let mut reader =
                        FileReader::try_new(Cursor::new(output), None).expect("Arrow reader error");
                    let schema = reader.schema();
                    assert_eq!(schema.fields[0].name(), "numbers");
                    assert_eq!(schema.fields[0].data_type(), &DataType::UInt32);
                    assert_eq!(schema.fields[1].name(), "strings");
                    assert_eq!(schema.fields[1].data_type(), &DataType::Utf8);

                    let rb = reader.next().unwrap().expect("read record batch failed");
                    assert_eq!(rb.num_columns(), 2);
                    assert_eq!(rb.num_rows(), 4);
                }
                HttpResponse::Error(e) => {
                    panic!("unexpected {:?}", e);
                }
                _ => unreachable!(),
            }
        }
    }
}
