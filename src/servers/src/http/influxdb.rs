use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use common_grpc::writer::Precision;

use crate::context::Context;
use crate::error::Result;
use crate::error::TimePrecisionSnafu;
use crate::influxdb::InfluxdbRequest;
use crate::query_handler::InfluxdbLineProtocolHandlerRef;

#[axum_macros::debug_handler]
pub async fn influxdb_write(
    State(handler): State<InfluxdbLineProtocolHandlerRef>,
    Query(params): Query<HashMap<String, String>>,
    lines: String,
) -> Result<(StatusCode, ())> {
    let precision = params
        .get("precision")
        .map(|val| parse_time_precision(val))
        .transpose()?;
    let request = InfluxdbRequest { precision, lines };
    handler.exec(&request, &Context::new()).await?;
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
