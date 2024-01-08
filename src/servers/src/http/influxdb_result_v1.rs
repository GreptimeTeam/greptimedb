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

use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_error::ext::ErrorExt;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{Error, ToJsonSnafu};
use crate::http::error_result::ErrorResponse;
use crate::http::header::{GREPTIME_DB_HEADER_EXECUTION_TIME, GREPTIME_DB_HEADER_FORMAT};
use crate::http::{Epoch, HttpResponse, ResponseFormat};

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SqlQuery {
    pub db: Option<String>,
    // Returns epoch timestamps with the specified precision.
    // Both u and µ indicate microseconds.
    // epoch = [ns,u,µ,ms,s],
    pub epoch: Option<String>,
    pub sql: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Eq, PartialEq)]
pub struct InfluxdbRecordsOutput {
    // The SQL query does not return the table name, but in InfluxDB,
    // we require the table name, so we set it to an empty string “”.
    name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<Vec<Value>>,
}

impl InfluxdbRecordsOutput {
    pub fn new(columns: Vec<String>, values: Vec<Vec<Value>>) -> Self {
        Self {
            name: "".to_string(),
            columns,
            values,
        }
    }
}

impl TryFrom<(Option<Epoch>, Vec<RecordBatch>)> for InfluxdbRecordsOutput {
    type Error = Error;

    fn try_from(
        (epoch, recordbatches): (Option<Epoch>, Vec<RecordBatch>),
    ) -> Result<InfluxdbRecordsOutput, Self::Error> {
        if recordbatches.is_empty() {
            Ok(InfluxdbRecordsOutput::new(vec![], vec![]))
        } else {
            // Safety: ensured by previous empty check
            let first = &recordbatches[0];
            let columns = first
                .schema
                .column_schemas()
                .iter()
                .map(|cs| cs.name.clone())
                .collect::<Vec<_>>();

            let mut rows =
                Vec::with_capacity(recordbatches.iter().map(|r| r.num_rows()).sum::<usize>());

            for recordbatch in recordbatches {
                for row in recordbatch.rows() {
                    let value_row = row
                        .into_iter()
                        .map(|value| {
                            let value = match (epoch, &value) {
                                (Some(epoch), datatypes::value::Value::Timestamp(ts)) => {
                                    if let Some(timestamp) = epoch.convert_timestamp(*ts) {
                                        datatypes::value::Value::Timestamp(timestamp)
                                    } else {
                                        value
                                    }
                                }
                                _ => value,
                            };
                            Value::try_from(value)
                        })
                        .collect::<Result<Vec<Value>, _>>()
                        .context(ToJsonSnafu)?;

                    rows.push(value_row);
                }
            }

            Ok(InfluxdbRecordsOutput::new(columns, rows))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Eq, PartialEq)]
pub struct InfluxdbOutput {
    pub statement_id: u32,
    pub series: Vec<InfluxdbRecordsOutput>,
}

impl InfluxdbOutput {
    pub fn num_rows(&self) -> usize {
        self.series.iter().map(|r| r.values.len()).sum()
    }

    pub fn num_cols(&self) -> usize {
        self.series
            .first()
            .map(|r| r.columns.len())
            .unwrap_or(0usize)
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct InfluxdbV1Response {
    results: Vec<InfluxdbOutput>,
    execution_time_ms: u64,
}

impl InfluxdbV1Response {
    pub fn with_execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time_ms = execution_time;
        self
    }

    /// Create a influxdb v1 response from query result
    pub async fn from_output(
        outputs: Vec<crate::error::Result<Output>>,
        epoch: Option<Epoch>,
    ) -> HttpResponse {
        fn make_error_response(error: impl ErrorExt) -> HttpResponse {
            HttpResponse::Error(ErrorResponse::from_error(ResponseFormat::InfluxdbV1, error))
        }

        // TODO(sunng87): this api response structure cannot represent error well.
        //  It hides successful execution results from error response
        let mut results = Vec::with_capacity(outputs.len());
        for (statement_id, out) in outputs.into_iter().enumerate() {
            let statement_id = statement_id as u32;
            match out {
                Ok(Output::AffectedRows(_)) => {
                    results.push(InfluxdbOutput {
                        statement_id,
                        series: vec![],
                    });
                }
                Ok(Output::Stream(stream)) => {
                    // TODO(sunng87): streaming response
                    match util::collect(stream).await {
                        Ok(rows) => match InfluxdbRecordsOutput::try_from((epoch, rows)) {
                            Ok(rows) => {
                                results.push(InfluxdbOutput {
                                    statement_id,
                                    series: vec![rows],
                                });
                            }
                            Err(err) => {
                                return make_error_response(err);
                            }
                        },
                        Err(err) => {
                            return make_error_response(err);
                        }
                    }
                }
                Ok(Output::RecordBatches(rbs)) => {
                    match InfluxdbRecordsOutput::try_from((epoch, rbs.take())) {
                        Ok(rows) => {
                            results.push(InfluxdbOutput {
                                statement_id,
                                series: vec![rows],
                            });
                        }
                        Err(err) => {
                            return make_error_response(err);
                        }
                    }
                }
                Err(err) => {
                    return make_error_response(err);
                }
            }
        }

        HttpResponse::InfluxdbV1(InfluxdbV1Response {
            results,
            execution_time_ms: 0,
        })
    }

    pub fn results(&self) -> &[InfluxdbOutput] {
        &self.results
    }

    pub fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }
}

impl IntoResponse for InfluxdbV1Response {
    fn into_response(self) -> Response {
        let execution_time = self.execution_time_ms;
        let mut resp = Json(self).into_response();
        resp.headers_mut().insert(
            GREPTIME_DB_HEADER_FORMAT,
            HeaderValue::from_static("influxdb_v1"),
        );
        resp.headers_mut().insert(
            GREPTIME_DB_HEADER_EXECUTION_TIME,
            HeaderValue::from(execution_time),
        );
        resp
    }
}
