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

use std::fmt::Display;
use std::time::Instant;

use axum::extract::{Query, State};
use axum::{Extension, Form, Json};
use common_error::ext::ErrorExt;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_telemetry::{debug, error};
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{EpochTimestampSnafu, Error, ToJsonSnafu};
use crate::http::ApiState;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Epoch {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
}

impl Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Epoch::Nanosecond => write!(f, "Epoch::Nanosecond"),
            Epoch::Microsecond => write!(f, "Epoch::Microsecond"),
            Epoch::Millisecond => write!(f, "Epoch::Millisecond"),
            Epoch::Second => write!(f, "Epoch::Second"),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SqlQuery {
    pub db: Option<String>,
    // Returns epoch timestamps with the specified precision.
    // Both u and µ indicate microseconds.
    // epoch = [ns,u,µ,ms,s],
    pub epoch: Option<String>,
    pub sql: Option<String>,
}

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql_with_influxdb_v1_result(
    State(state): State<ApiState>,
    Query(query_params): Query<SqlQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<SqlQuery>,
) -> Json<InfluxdbResponse> {
    let sql_handler = &state.sql_handler;

    let start = Instant::now();
    let sql = query_params.sql.or(form_params.sql);
    let db = query_ctx.get_db_string();
    let epoch_str = query_params.epoch.or(form_params.epoch);
    let mut epoch = None;
    if let Some(epoch_str) = epoch_str {
        match parse_epoch(epoch_str.as_str()) {
            Ok(ep) => epoch = Some(ep),
            Err(e) => {
                return Json(InfluxdbResponse::with_error(e));
            }
        }
    }

    let _timer = crate::metrics::METRIC_HTTP_SQL_WITH_INFLUXDB_V1_RESULT_FORMAT_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let resp = if let Some(sql) = &sql {
        if let Some(resp) = validate_schema(sql_handler.clone(), query_ctx.clone()).await {
            return Json(resp);
        }

        InfluxdbResponse::from_output(sql_handler.do_query(sql, query_ctx).await, epoch).await
    } else {
        InfluxdbResponse::with_error_message("sql parameter is required.".to_string())
    };

    Json(resp.with_execution_time(start.elapsed().as_millis()))
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InfluxdbRecordsOutput {
    // The SQL query does not return the table name, but in InfluxDB,
    // we require the table name, so we set it to an empty string “”.
    name: String,
    columns: Option<Vec<String>>,
    values: Vec<Vec<Value>>,
}

impl InfluxdbRecordsOutput {
    pub fn new(columns: Option<Vec<String>>, values: Vec<Vec<Value>>) -> Self {
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
            Ok(InfluxdbRecordsOutput::new(None, vec![]))
        } else {
            // safety ensured by previous empty check
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
                            if let Some(epoch) = epoch {
                                if let datatypes::value::Value::Timestamp(ts) = &value {
                                    if let Some(timestamp) = convert_timestamp_by_epoch(*ts, epoch)
                                    {
                                        return datatypes::value::Value::Timestamp(timestamp);
                                    }
                                }
                            }

                            value
                        })
                        .map(Value::try_from)
                        .collect::<Result<Vec<Value>, _>>()
                        .context(ToJsonSnafu)?;

                    rows.push(value_row);
                }
            }

            Ok(InfluxdbRecordsOutput::new(Some(columns), rows))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct InfluxdbOutput {
    pub statement_id: u32,
    pub series: Vec<InfluxdbRecordsOutput>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct InfluxdbResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<Vec<InfluxdbOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_time_ms: Option<u128>,
}

impl InfluxdbResponse {
    pub fn with_error(error: impl ErrorExt) -> Self {
        let code = error.status_code();
        if code.should_log_error() {
            error!(error; "Failed to handle HTTP request");
        } else {
            debug!("Failed to handle HTTP request, err: {:?}", error);
        }

        InfluxdbResponse {
            results: None,
            error: Some(error.output_msg()),
            execution_time_ms: None,
        }
    }

    fn with_error_message(err_msg: String) -> Self {
        InfluxdbResponse {
            results: None,
            error: Some(err_msg),
            execution_time_ms: None,
        }
    }

    fn with_output(results: Option<Vec<InfluxdbOutput>>) -> Self {
        InfluxdbResponse {
            results,
            error: None,
            execution_time_ms: None,
        }
    }

    fn with_execution_time(mut self, execution_time: u128) -> Self {
        self.execution_time_ms = Some(execution_time);
        self
    }

    /// Create a influxdb v1 response from query result
    pub async fn from_output(
        outputs: Vec<crate::error::Result<Output>>,
        epoch: Option<Epoch>,
    ) -> Self {
        // TODO(sunng87): this api response structure cannot represent error
        // well. It hides successful execution results from error response
        let mut results = Vec::with_capacity(outputs.len());
        for (statement_id, out) in outputs.into_iter().enumerate().map(|(i, o)| (i as u32, o)) {
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
                                return Self::with_error(err);
                            }
                        },

                        Err(e) => {
                            return Self::with_error(e);
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
                            return Self::with_error(err);
                        }
                    }
                }
                Err(e) => {
                    return Self::with_error(e);
                }
            }
        }
        Self::with_output(Some(results))
    }
}

async fn validate_schema(
    sql_handler: ServerSqlQueryHandlerRef,
    query_ctx: QueryContextRef,
) -> Option<InfluxdbResponse> {
    match sql_handler
        .is_valid_schema(query_ctx.current_catalog(), query_ctx.current_schema())
        .await
    {
        Ok(false) => Some(InfluxdbResponse::with_error_message(format!(
            "Database not found: {}",
            query_ctx.get_db_string()
        ))),
        Err(e) => Some(InfluxdbResponse::with_error_message(format!(
            "Error checking database: {}, {}",
            query_ctx.get_db_string(),
            e.output_msg(),
        ))),
        _ => None,
    }
}

fn parse_epoch(value: &str) -> crate::error::Result<Epoch> {
    // Both u and µ indicate microseconds.
    // epoch = [ns,u,µ,ms,s],
    // For details, see the Influxdb documents.
    // https://docs.influxdata.com/influxdb/v1/tools/api/#query-string-parameters-1
    match value {
        "ns" => Ok(Epoch::Nanosecond),
        "u" | "µ" => Ok(Epoch::Microsecond),
        "ms" => Ok(Epoch::Millisecond),
        "s" => Ok(Epoch::Second),
        unknown => EpochTimestampSnafu {
            name: unknown.to_string(),
        }
        .fail(),
    }
}

fn convert_timestamp_by_epoch(ts: Timestamp, epoch: Epoch) -> Option<Timestamp> {
    match epoch {
        Epoch::Nanosecond => ts.convert_to(TimeUnit::Nanosecond),
        Epoch::Microsecond => ts.convert_to(TimeUnit::Microsecond),
        Epoch::Millisecond => ts.convert_to(TimeUnit::Millisecond),
        Epoch::Second => ts.convert_to(TimeUnit::Second),
    }
}
