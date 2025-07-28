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

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use serde::{Deserialize, Serialize};
use servers::http::result::error_result::ErrorResponse;

pub(crate) type RecoveryHandlerRef = Arc<RecoveryHandler>;

pub(crate) struct RecoveryHandler {
    pub(crate) manager: RuntimeSwitchManagerRef,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RecoveryResponse {
    pub enabled: bool,
}

/// Get the recovery mode.
#[axum_macros::debug_handler]
pub(crate) async fn get_recovery_mode(State(handler): State<RecoveryHandlerRef>) -> Response {
    let enabled = handler.manager.recovery_mode().await;

    match enabled {
        Ok(enabled) => (StatusCode::OK, Json(RecoveryResponse { enabled })).into_response(),
        Err(e) => ErrorResponse::from_error(e).into_response(),
    }
}

/// Set the recovery mode.
#[axum_macros::debug_handler]
pub(crate) async fn set_recovery_mode(State(handler): State<RecoveryHandlerRef>) -> Response {
    match handler.manager.set_recovery_mode().await {
        Ok(_) => (StatusCode::OK, Json(RecoveryResponse { enabled: true })).into_response(),
        Err(e) => ErrorResponse::from_error(e).into_response(),
    }
}

/// Unset the recovery mode.
#[axum_macros::debug_handler]
pub(crate) async fn unset_recovery_mode(State(handler): State<RecoveryHandlerRef>) -> Response {
    match handler.manager.unset_recovery_mode().await {
        Ok(_) => (StatusCode::OK, Json(RecoveryResponse { enabled: false })).into_response(),
        Err(e) => ErrorResponse::from_error(e).into_response(),
    }
}
