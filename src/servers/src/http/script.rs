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

use std::time::Instant;

use axum::extract::{Json, Query, RawBody, State};
use common_error::ext::ErrorExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::{ApiState, JsonResponse};

macro_rules! json_err {
    ($e: expr) => {{
        return Json(JsonResponse::with_error(
            format!("Invalid argument: {}", $e),
            common_error::status_code::StatusCode::InvalidArguments,
        ));
    }};

    ($msg: expr, $code: expr) => {{
        return Json(JsonResponse::with_error($msg.to_string(), $code));
    }};
}

macro_rules! unwrap_or_json_err {
    ($result: expr) => {
        match $result {
            Ok(result) => result,
            Err(e) => json_err!(e),
        }
    };
}

/// Handler to insert and compile script
#[axum_macros::debug_handler]
pub async fn scripts(
    State(state): State<ApiState>,
    Query(params): Query<ScriptQuery>,
    RawBody(body): RawBody,
) -> Json<JsonResponse> {
    if let Some(script_handler) = &state.script_handler {
        let name = params.name.as_ref();

        if name.is_none() || name.unwrap().is_empty() {
            json_err!("invalid name");
        }
        let bytes = unwrap_or_json_err!(hyper::body::to_bytes(body).await);

        let script = unwrap_or_json_err!(String::from_utf8(bytes.to_vec()));

        let body = match script_handler.insert_script(name.unwrap(), &script).await {
            Ok(()) => JsonResponse::with_output(None),
            Err(e) => json_err!(format!("Insert script error: {}", e), e.status_code()),
        };

        Json(body)
    } else {
        json_err!("Script execution not supported, missing script handler");
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ScriptQuery {
    pub name: Option<String>,
}

/// Handler to execute script
#[axum_macros::debug_handler]
pub async fn run_script(
    State(state): State<ApiState>,
    Query(params): Query<ScriptQuery>,
) -> Json<JsonResponse> {
    if let Some(script_handler) = &state.script_handler {
        let start = Instant::now();
        let name = params.name.as_ref();

        if name.is_none() || name.unwrap().is_empty() {
            json_err!("invalid name");
        }

        let output = script_handler.execute_script(name.unwrap()).await;
        let resp = JsonResponse::from_output(output).await;

        Json(resp.with_execution_time(start.elapsed().as_millis()))
    } else {
        json_err!("Script execution not supported, missing script handler");
    }
}
