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

//! Splunk HTTP Event Collector (HEC) compatible ingestion endpoint.
//!
//! Clients point their base endpoint at `/v1/splunk`, so the full paths are
//! `/v1/splunk/services/collector/event` and `/v1/splunk/services/collector/health`.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

/// HEC status code returned by the health endpoint when the collector is available.
/// Reference: Splunk HEC returns `{"text":"HEC is healthy","code":17}`.
const HEC_HEALTHY_CODE: u32 = 17;

/// Handle `GET /services/collector/health`.
///
/// Splunk clients probe this endpoint before sending data, so it must return
/// `200 OK` without requiring authentication (the path is registered in
/// [`crate::http::PUBLIC_API_PREFIX`]).
#[axum_macros::debug_handler]
pub async fn handle_health() -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(json!({
            "text": "HEC is healthy",
            "code": HEC_HEALTHY_CODE,
        })),
    )
}
