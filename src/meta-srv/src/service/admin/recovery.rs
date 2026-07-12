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

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use serde::{Deserialize, Serialize};

use crate::service::admin::util::ErrorHandler;

#[derive(Clone)]
pub(crate) struct RecoveryHandler {
    pub(crate) manager: RuntimeSwitchManagerRef,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RecoveryResponse {
    pub enabled: bool,
}

/// Get the recovery mode.
#[axum_macros::debug_handler]
pub(crate) async fn status(State(handler): State<RecoveryHandler>) -> Response {
    handler
        .manager
        .recovery_mode()
        .await
        .map(|enabled| Json(RecoveryResponse { enabled }))
        .map_err(ErrorHandler::new)
        .into_response()
}

/// Set the recovery mode.
#[axum_macros::debug_handler]
pub(crate) async fn set(State(handler): State<RecoveryHandler>) -> Response {
    handler
        .manager
        .set_recovery_mode()
        .await
        .map(|_| Json(RecoveryResponse { enabled: true }))
        .map_err(ErrorHandler::new)
        .into_response()
}

/// Unset the recovery mode.
#[axum_macros::debug_handler]
pub(crate) async fn unset(State(handler): State<RecoveryHandler>) -> Response {
    handler
        .manager
        .unset_recovery_mode()
        .await
        .map(|_| Json(RecoveryResponse { enabled: false }))
        .map_err(ErrorHandler::new)
        .into_response()
}
