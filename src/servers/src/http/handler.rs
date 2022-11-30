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
use std::sync::Arc;
use std::time::Instant;

use aide::transform::TransformOperation;
use axum::extract::{Json, Query, State};
use common_error::status_code::StatusCode;
use common_telemetry::metric;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use session::context::SessionContext;

use crate::http::{ApiState, JsonResponse};

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SqlQuery {
    pub database: Option<String>,
    pub sql: Option<String>,
}

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    State(state): State<ApiState>,
    Query(params): Query<SqlQuery>,
) -> Json<JsonResponse> {
    let sql_handler = &state.sql_handler;
    let start = Instant::now();
    let resp = if let Some(sql) = &params.sql {
        // TODO(LFC): Sessions in http server.
        let session_ctx = Arc::new(SessionContext::new());
        JsonResponse::from_output(sql_handler.do_query(sql, session_ctx).await).await
    } else {
        JsonResponse::with_error(
            "sql parameter is required.".to_string(),
            StatusCode::InvalidArguments,
        )
    };

    Json(resp.with_execution_time(start.elapsed().as_millis()))
}

pub(crate) fn sql_docs(op: TransformOperation) -> TransformOperation {
    op.response::<200, Json<JsonResponse>>()
}

/// Handler to export metrics
#[axum_macros::debug_handler]
pub async fn metrics(Query(_params): Query<HashMap<String, String>>) -> String {
    if let Some(handle) = metric::try_handle() {
        handle.render()
    } else {
        "Prometheus handle not initialized.".to_owned()
    }
}
