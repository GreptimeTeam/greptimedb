// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_grpc::writer::Precision;

use crate::error::{Result, TimePrecisionSnafu};
use crate::influxdb::InfluxdbRequest;
use crate::query_handler::InfluxdbLineProtocolHandlerRef;

#[axum_macros::debug_handler]
pub async fn influxdb_write(
    State(handler): State<InfluxdbLineProtocolHandlerRef>,
    Query(mut params): Query<HashMap<String, String>>,
    lines: String,
) -> Result<(StatusCode, ())> {
    let db = params
        .remove("db")
        .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());

    let precision = params
        .get("precision")
        .map(|val| parse_time_precision(val))
        .transpose()?;
    let request = InfluxdbRequest {
        precision,
        lines,
        db,
    };
    handler.exec(&request).await?;
    Ok((StatusCode::NO_CONTENT, ()))
}

fn parse_time_precision(value: &str) -> Result<Precision> {
    match value {
        "n" => Ok(Precision::NANOSECOND),
        "u" => Ok(Precision::MICROSECOND),
        "ms" => Ok(Precision::MILLISECOND),
        "s" => Ok(Precision::SECOND),
        "m" => Ok(Precision::MINUTE),
        "h" => Ok(Precision::HOUR),
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
        assert_eq!(Precision::NANOSECOND, parse_time_precision("n").unwrap());
        assert_eq!(Precision::MICROSECOND, parse_time_precision("u").unwrap());
        assert_eq!(Precision::MILLISECOND, parse_time_precision("ms").unwrap());
        assert_eq!(Precision::SECOND, parse_time_precision("s").unwrap());
        assert_eq!(Precision::MINUTE, parse_time_precision("m").unwrap());
        assert_eq!(Precision::HOUR, parse_time_precision("h").unwrap());
        assert!(parse_time_precision("unknown").is_err());
    }
}
