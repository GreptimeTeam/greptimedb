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

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Extension;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_grpc::writer::Precision;
use common_telemetry::timer;
use session::context::QueryContextRef;

use crate::error::{Result, TimePrecisionSnafu};
use crate::influxdb::InfluxdbRequest;
use crate::query_handler::InfluxdbLineProtocolHandlerRef;

// https://docs.influxdata.com/influxdb/v1.8/tools/api/#ping-http-endpoint
#[axum_macros::debug_handler]
pub async fn influxdb_ping() -> Result<impl IntoResponse> {
    Ok(StatusCode::NO_CONTENT)
}

// https://docs.influxdata.com/influxdb/v1.8/tools/api/#health-http-endpoint
#[axum_macros::debug_handler]
pub async fn influxdb_health() -> Result<impl IntoResponse> {
    Ok(StatusCode::OK)
}

#[axum_macros::debug_handler]
pub async fn influxdb_write_v1(
    State(handler): State<InfluxdbLineProtocolHandlerRef>,
    Query(mut params): Query<HashMap<String, String>>,
    Extension(query_ctx): Extension<QueryContextRef>,
    lines: String,
) -> Result<impl IntoResponse> {
    let db = params
        .remove("db")
        .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());

    let precision = params
        .get("precision")
        .map(|val| parse_time_precision(val))
        .transpose()?;

    influxdb_write(&db, precision, lines, handler, query_ctx).await
}

#[axum_macros::debug_handler]
pub async fn influxdb_write_v2(
    State(handler): State<InfluxdbLineProtocolHandlerRef>,
    Query(mut params): Query<HashMap<String, String>>,
    Extension(query_ctx): Extension<QueryContextRef>,
    lines: String,
) -> Result<impl IntoResponse> {
    let db = params
        .remove("bucket")
        .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());

    let precision = params
        .get("precision")
        .map(|val| parse_time_precision(val))
        .transpose()?;

    influxdb_write(&db, precision, lines, handler, query_ctx).await
}

pub async fn influxdb_write(
    db: &str,
    precision: Option<Precision>,
    lines: String,
    handler: InfluxdbLineProtocolHandlerRef,
    ctx: QueryContextRef,
) -> Result<impl IntoResponse> {
    let _timer = timer!(
        crate::metrics::METRIC_HTTP_INFLUXDB_WRITE_ELAPSED,
        &[(crate::metrics::METRIC_DB_LABEL, db.to_string())]
    );

    let request = InfluxdbRequest { precision, lines };
    handler.exec(request, ctx).await?;

    Ok((StatusCode::NO_CONTENT, ()))
}

fn parse_time_precision(value: &str) -> Result<Precision> {
    // Precision conversion needs to be compatible with influxdb v1 v2 api.
    // For details, see the Influxdb documents.
    // https://docs.influxdata.com/influxdb/v1.8/tools/api/#apiv2write-http-endpoint
    // https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
    match value {
        "n" | "ns" => Ok(Precision::Nanosecond),
        "u" | "us" => Ok(Precision::Microsecond),
        "ms" => Ok(Precision::Millisecond),
        "s" => Ok(Precision::Second),
        "m" => Ok(Precision::Minute),
        "h" => Ok(Precision::Hour),
        unknown => TimePrecisionSnafu {
            name: unknown.to_string(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use common_grpc::writer::Precision;

    use crate::http::influxdb::parse_time_precision;

    #[test]
    fn test_parse_time_precision() {
        assert_eq!(Precision::Nanosecond, parse_time_precision("n").unwrap());
        assert_eq!(Precision::Nanosecond, parse_time_precision("ns").unwrap());
        assert_eq!(Precision::Microsecond, parse_time_precision("u").unwrap());
        assert_eq!(Precision::Microsecond, parse_time_precision("us").unwrap());
        assert_eq!(Precision::Millisecond, parse_time_precision("ms").unwrap());
        assert_eq!(Precision::Second, parse_time_precision("s").unwrap());
        assert_eq!(Precision::Minute, parse_time_precision("m").unwrap());
        assert_eq!(Precision::Hour, parse_time_precision("h").unwrap());
        assert!(parse_time_precision("unknown").is_err());
    }
}
