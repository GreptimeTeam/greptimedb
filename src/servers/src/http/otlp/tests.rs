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

use axum::body::to_bytes;

use super::*;

#[tokio::test]
async fn metric_failure_is_a_protobuf_invalid_argument() {
    let response = OtlpMetricsResponse::Failure(MetricsIngestOutcome {
        error_message: Some("reserved temporality label".to_string()),
        ..Default::default()
    })
    .into_response();

    assert_eq!(StatusCode::BAD_REQUEST, response.status());
    assert_eq!(
        &CONTENT_TYPE_PROTOBUF,
        response.headers().get(header::CONTENT_TYPE).unwrap()
    );
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let status = GoogleRpcStatus::decode(body).unwrap();
    assert_eq!(tonic::Code::InvalidArgument as i32, status.code);
    assert_eq!("reserved temporality label", status.message);
}
