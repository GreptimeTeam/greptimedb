use std::collections::HashMap;

use axum::extract::State;
use axum::http::Request;
use axum::http::StatusCode as HttpStatusCode;
use hyper::Body;
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Error, Result};
use crate::http::HttpResponse;
use crate::opentsdb::codec::OpentsdbDataPoint;
use crate::query_handler::OpentsdbLineProtocolHandlerRef;

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, PartialEq)]
pub struct OpentsdbDataPointRequest {
    metric: String,
    timestamp: u64,
    value: f64,
    tags: HashMap<String, String>,
}

impl From<&OpentsdbDataPointRequest> for OpentsdbDataPoint {
    fn from(request: &OpentsdbDataPointRequest) -> Self {
        let ts_millis = OpentsdbDataPoint::timestamp_to_millis(request.timestamp);

        let tags = request
            .tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<(String, String)>>();

        OpentsdbDataPoint::new(request.metric.clone(), ts_millis, request.value, tags)
    }
}

// Please refer to the Opentsdb documents of ["api/put"](http://opentsdb.net/docs/build/html/api_http/put.html)
// for more details.
#[axum_macros::debug_handler]
pub async fn put(
    State(opentsdb_handler): State<OpentsdbLineProtocolHandlerRef>,
    request: Request<Body>,
) -> Result<(HttpStatusCode, HttpResponse)> {
    let (parts, body) = request.into_parts();

    let query = parts.uri.query();
    let params = query
        .map(|query| query.split('&').collect::<Vec<&str>>())
        .unwrap_or_default();
    let summary = params.iter().any(|&p| p == "summary");
    let details = params.iter().any(|&p| p == "details");

    let data_points = parse_data_points(body).await?;

    let mut response = OpentsdbDebuggingResponse {
        success: 0,
        failed: 0,
        errors: if details {
            Some(Vec::with_capacity(data_points.len()))
        } else {
            None
        },
    };

    for data_point in data_points.iter() {
        let result = opentsdb_handler.exec(&data_point.into()).await;
        match result {
            Ok(()) => response.on_success(),
            Err(e) => {
                if !summary && !details {
                    // Not debugging purpose, failed fast.
                    return error::InternalSnafu {
                        err_msg: e.to_string(),
                    }
                    .fail();
                }

                response.on_failed(data_point, e);
            }
        }
    }

    let response = if summary || details {
        let response = match serde_json::to_string::<OpentsdbDebuggingResponse>(&response) {
            Ok(x) => x,
            Err(e) => {
                return error::InternalSnafu {
                    err_msg: format!("Failed to serialize response, cause: {}", e),
                }
                .fail()
            }
        };
        (HttpStatusCode::OK, HttpResponse::Text(response))
    } else {
        (
            HttpStatusCode::NO_CONTENT,
            HttpResponse::Text("".to_string()),
        )
    };
    Ok(response)
}

async fn parse_data_points(body: Body) -> Result<Vec<OpentsdbDataPointRequest>> {
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;

    let first = body.first().context(error::InvalidQuerySnafu {
        reason: "Request is empty.",
    })?;
    let data_points = match first {
        // single data point put
        b'{' => vec![
            serde_json::from_slice::<OpentsdbDataPointRequest>(&body[..])
                .context(error::InvalidOpentsdbRequestSnafu)?,
        ],

        // multiple data point put
        b'[' => serde_json::from_slice::<Vec<OpentsdbDataPointRequest>>(&body[..])
            .context(error::InvalidOpentsdbRequestSnafu)?,

        _ => {
            let body = String::from_utf8(body.to_vec()).context(error::InvalidOpentsdbLineSnafu)?;
            return error::InvalidQuerySnafu {
                reason: format!("Illegal request: {}", body),
            }
            .fail();
        }
    };
    Ok(data_points)
}

#[derive(serde::Serialize)]
struct OpentsdbDetailError {
    datapoint: OpentsdbDataPointRequest,
    error: String,
}

#[derive(serde::Serialize)]
struct OpentsdbDebuggingResponse {
    success: i32,
    failed: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    errors: Option<Vec<OpentsdbDetailError>>,
}

impl OpentsdbDebuggingResponse {
    fn on_success(&mut self) {
        self.success += 1;
    }

    fn on_failed(&mut self, data_point: &OpentsdbDataPointRequest, error: Error) {
        self.failed += 1;

        if let Some(details) = self.errors.as_mut() {
            let error = OpentsdbDetailError {
                datapoint: data_point.clone(),
                error: error.to_string(),
            };
            details.push(error);
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_into_opentsdb_data_point() {
        let request = &OpentsdbDataPointRequest {
            metric: "hello".to_string(),
            timestamp: 1234,
            value: 1.0,
            tags: HashMap::from([("foo".to_string(), "a".to_string())]),
        };
        let data_point: OpentsdbDataPoint = request.into();
        assert_eq!(data_point.metric(), "hello");
        assert_eq!(data_point.ts_millis(), 1234000);
        assert_eq!(data_point.value(), 1.0);
        assert_eq!(
            data_point.tags(),
            &vec![("foo".to_string(), "a".to_string())]
        );
    }

    #[tokio::test]
    async fn test_parse_data_points() {
        let raw_data_point1 = r#"{
                "metric": "sys.cpu.nice",
                "timestamp": 1346846400,
                "value": 18,
                "tags": {
                    "host": "web01",
                    "dc": "lga"
                }
            }"#;
        let data_point1 =
            serde_json::from_str::<OpentsdbDataPointRequest>(raw_data_point1).unwrap();

        let raw_data_point2 = r#"{
                "metric": "sys.cpu.nice",
                "timestamp": 1346846400,
                "value": 9,
                "tags": {
                    "host": "web02",
                    "dc": "lga"
                }
            }"#;
        let data_point2 =
            serde_json::from_str::<OpentsdbDataPointRequest>(raw_data_point2).unwrap();

        let body = Body::from(raw_data_point1);
        let data_points = parse_data_points(body).await.unwrap();
        assert_eq!(data_points.len(), 1);
        assert_eq!(data_points[0], data_point1);

        let body = Body::from(format!("[{},{}]", raw_data_point1, raw_data_point2));
        let data_points = parse_data_points(body).await.unwrap();
        assert_eq!(data_points.len(), 2);
        assert_eq!(data_points[0], data_point1);
        assert_eq!(data_points[1], data_point2);

        let body = Body::from("hello world");
        let result = parse_data_points(body).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid query: Illegal request: hello world"
        );
    }
}
