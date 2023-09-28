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

use axum::extract::{Query, RawBody, State};
use axum::http::StatusCode as HttpStatusCode;
use axum::{Extension, Json};
use common_error::ext::ErrorExt;
use hyper::Body;
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{self, Result};
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
pub struct DataPointRequest {
    metric: String,
    timestamp: i64,
    value: f64,
    tags: HashMap<String, String>,
}

impl From<DataPointRequest> for DataPoint {
    fn from(request: DataPointRequest) -> Self {
        let ts_millis = DataPoint::timestamp_to_millis(request.timestamp);

        let tags = request
            .tags
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect::<Vec<(String, String)>>();

        DataPoint::new(request.metric, ts_millis, request.value, tags)
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
    Query(params): Query<HashMap<String, String>>,
    Extension(ctx): Extension<QueryContextRef>,
    RawBody(body): RawBody,
) -> Result<(HttpStatusCode, Json<OpentsdbPutResponse>)> {
    let summary = params.contains_key("summary");
    let details = params.contains_key("details");

    let data_points = parse_data_points(body).await?;

    let response = if !summary && !details {
        for data_point in data_points.into_iter() {
            if let Err(e) = opentsdb_handler.exec(&data_point.into(), ctx.clone()).await {
                // Not debugging purpose, failed fast.
                return error::InternalSnafu {
                    err_msg: e.to_string(),
                }
                .fail();
            }
        }
        (HttpStatusCode::NO_CONTENT, Json(OpentsdbPutResponse::Empty))
    } else {
        let mut response = OpentsdbDebuggingResponse {
            success: 0,
            failed: 0,
            errors: if details {
                Some(Vec::with_capacity(data_points.len()))
            } else {
                None
            },
        };

        for data_point in data_points.into_iter() {
            let result = opentsdb_handler
                .exec(&data_point.clone().into(), ctx.clone())
                .await;
            match result {
                Ok(()) => response.on_success(),
                Err(e) => {
                    response.on_failed(data_point, e);
                }
            }
        }
        (
            HttpStatusCode::OK,
            Json(OpentsdbPutResponse::Debug(response)),
        )
    };
    Ok(response)
}

async fn parse_data_points(body: Body) -> Result<Vec<DataPointRequest>> {
    let body = hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)?;
    let data_points = serde_json::from_slice::<OneOrMany<DataPointRequest>>(&body[..])
        .context(error::InvalidOpentsdbJsonRequestSnafu)?;
    Ok(data_points.into())
}

#[derive(Serialize, Deserialize, Debug)]
struct OpentsdbDetailError {
    datapoint: DataPointRequest,
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

    fn on_failed(&mut self, datapoint: DataPointRequest, error: impl ErrorExt) {
        self.failed += 1;

        if let Some(details) = self.errors.as_mut() {
            let error = OpentsdbDetailError {
                datapoint,
                error: error.output_msg(),
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
        let request = DataPointRequest {
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
        let data_point1 = serde_json::from_str::<DataPointRequest>(raw_data_point1).unwrap();

        let raw_data_point2 = r#"{
                "metric": "sys.cpu.nice",
                "timestamp": 1346846400,
                "value": 9,
                "tags": {
                    "host": "web02",
                    "dc": "lga"
                }
            }"#;
        let data_point2 = serde_json::from_str::<DataPointRequest>(raw_data_point2).unwrap();

        let body = Body::from(raw_data_point1);
        let data_points = parse_data_points(body).await.unwrap();
        assert_eq!(data_points.len(), 1);
        assert_eq!(data_points[0], data_point1);

        let body = Body::from(format!("[{raw_data_point1},{raw_data_point2}]"));
        let data_points = parse_data_points(body).await.unwrap();
        assert_eq!(data_points.len(), 2);
        assert_eq!(data_points[0], data_point1);
        assert_eq!(data_points[1], data_point2);

        let body = Body::from("");
        let result = parse_data_points(body).await;
        assert!(result.is_err());
        let err = result.unwrap_err().output_msg();
        assert!(err.contains("EOF while parsing a value at line 1 column 0"));

        let body = Body::from("hello world");
        let result = parse_data_points(body).await;
        assert!(result.is_err());
        let err = result.unwrap_err().output_msg();
        assert!(err.contains("expected value at line 1 column 1"));
    }
}
