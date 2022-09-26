use std::collections::HashMap;

use axum::extract::State;
use axum::http::Request;
use axum::http::StatusCode as HttpStatusCode;
use axum::Json;
use hyper::Body;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Error, Result};
use crate::opentsdb::codec::DataPoint;
use crate::query_handler::OpentsdbProtocolHandlerRef;

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Vec(Vec<T>),
}

impl<T> From<OneOrMany<T>> for Vec<T> {
    fn from(from: OneOrMany<T>) -> Self {
        match from {
            OneOrMany::One(val) => vec![val],
            OneOrMany::Vec(vec) => vec,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct OpentsdbDataPointRequest {
    metric: String,
    timestamp: i64,
    value: f64,
    tags: HashMap<String, String>,
}

impl From<&OpentsdbDataPointRequest> for DataPoint {
    fn from(request: &OpentsdbDataPointRequest) -> Self {
        let ts_millis = DataPoint::timestamp_to_millis(request.timestamp);

        let tags = request
            .tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<(String, String)>>();

        DataPoint::new(request.metric.clone(), ts_millis, request.value, tags)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum OpentsdbPutResponse {
    Empty,
    Debug(OpentsdbDebuggingResponse),
}

// Please refer to the OpenTSDB documents of ["api/put"](http://opentsdb.net/docs/build/html/api_http/put.html)
// for more details.
#[axum_macros::debug_handler]
pub async fn put(
    State(opentsdb_handler): State<OpentsdbProtocolHandlerRef>,
    request: Request<Body>,
) -> Result<(HttpStatusCode, Json<OpentsdbPutResponse>)> {
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
        (
            HttpStatusCode::OK,
            Json(OpentsdbPutResponse::Debug(response)),
        )
    } else {
        (HttpStatusCode::NO_CONTENT, Json(OpentsdbPutResponse::Empty))
    };
    Ok(response)
}

async fn parse_data_points(body: Body) -> Result<Vec<OpentsdbDataPointRequest>> {
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;
    let data_points = serde_json::from_slice::<OneOrMany<OpentsdbDataPointRequest>>(&body[..])
        .context(error::InvalidOpentsdbJsonRequestSnafu)?;
    Ok(data_points.into())
}

#[derive(Serialize, Deserialize, Debug)]
struct OpentsdbDetailError {
    datapoint: OpentsdbDataPointRequest,
    error: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OpentsdbDebuggingResponse {
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
        let data_point: DataPoint = request.into();
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

        let body = Body::from("");
        let result = parse_data_points(body).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid OpenTSDB Json request, source: EOF while parsing a value at line 1 column 0"
        );

        let body = Body::from("hello world");
        let result = parse_data_points(body).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid OpenTSDB Json request, source: expected value at line 1 column 1"
        );
    }
}
