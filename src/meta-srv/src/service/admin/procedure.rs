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
use std::sync::Arc;

use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_meta::rpc::procedure;
use common_procedure::ProcedureId;
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use servers::http::result::error_result::ErrorResponse;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use crate::error::{self, RuntimeSwitchManagerSnafu};
use crate::metasrv::Metasrv;
use crate::service::admin::HttpHandler;
use crate::service::admin::util;
use crate::service::admin::util::{ErrorHandler, to_json_response, to_not_found_response};

#[derive(Clone)]
pub struct ProcedureManagerHandler {
    pub manager: RuntimeSwitchManagerRef,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ProcedureManagerStatusResponse {
    status: ProcedureManagerStatus,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProcedureManagerStatus {
    Paused,
    Running,
}

/// Get the procedure manager status.
#[axum_macros::debug_handler]
pub(crate) async fn status(State(handler): State<ProcedureManagerHandler>) -> Response {
    handler
        .get_procedure_manager_status()
        .await
        .map(Json)
        .map_err(ErrorHandler::new)
        .into_response()
}

/// Pause the procedure manager.
#[axum_macros::debug_handler]
pub(crate) async fn pause(State(handler): State<ProcedureManagerHandler>) -> Response {
    handler
        .pause_procedure_manager()
        .await
        .map(Json)
        .map_err(ErrorHandler::new)
        .into_response()
}

/// Resume the procedure manager.
#[axum_macros::debug_handler]
pub(crate) async fn resume(State(handler): State<ProcedureManagerHandler>) -> Response {
    handler
        .resume_procedure_manager()
        .await
        .map(Json)
        .map_err(ErrorHandler::new)
        .into_response()
}

impl ProcedureManagerHandler {
    pub(crate) async fn pause_procedure_manager(
        &self,
    ) -> crate::Result<ProcedureManagerStatusResponse> {
        self.manager
            .pasue_procedure()
            .await
            .context(RuntimeSwitchManagerSnafu)?;
        // TODO(weny): Add a record to the system events.
        info!("Pause the procedure manager.");
        Ok(ProcedureManagerStatusResponse {
            status: ProcedureManagerStatus::Paused,
        })
    }

    pub(crate) async fn resume_procedure_manager(
        &self,
    ) -> crate::Result<ProcedureManagerStatusResponse> {
        self.manager
            .resume_procedure()
            .await
            .context(RuntimeSwitchManagerSnafu)?;
        // TODO(weny): Add a record to the system events.
        info!("Resume the procedure manager.");
        Ok(ProcedureManagerStatusResponse {
            status: ProcedureManagerStatus::Running,
        })
    }

    pub(crate) async fn get_procedure_manager_status(
        &self,
    ) -> crate::Result<ProcedureManagerStatusResponse> {
        let is_paused = self
            .manager
            .is_procedure_paused()
            .await
            .context(RuntimeSwitchManagerSnafu)?;
        let response = ProcedureManagerStatusResponse {
            status: if is_paused {
                ProcedureManagerStatus::Paused
            } else {
                ProcedureManagerStatus::Running
            },
        };

        Ok(response)
    }
}

#[async_trait::async_trait]
impl HttpHandler for ProcedureManagerHandler {
    async fn handle(
        &self,
        path: &str,
        method: http::Method,
        _: &HashMap<String, String>,
    ) -> crate::Result<http::Response<String>> {
        match method {
            http::Method::GET => {
                if path.ends_with("status") {
                    let response = self.get_procedure_manager_status().await?;
                    to_json_response(response)
                } else {
                    to_not_found_response()
                }
            }
            http::Method::POST => {
                if path.ends_with("pause") {
                    let response = self.pause_procedure_manager().await?;
                    to_json_response(response)
                } else if path.ends_with("resume") {
                    let response = self.resume_procedure_manager().await?;
                    to_json_response(response)
                } else {
                    to_not_found_response()
                }
            }
            _ => to_not_found_response(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProcedureStatusRequest {
    procedure_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProcedureStatusResponse {
    procedure_id: String,
    status: String,
    error: Option<String>,
}

/// Queries a submitted procedure, independently of the procedure-manager switch.
#[axum_macros::debug_handler]
pub(crate) async fn procedure_status(
    State(metasrv): State<Arc<Metasrv>>,
    Query(request): Query<ProcedureStatusRequest>,
) -> Response {
    async {
        util::ensure_leader(&metasrv)?;
        let pid = request.procedure_id.parse::<ProcedureId>().map_err(|_| {
            error::InvalidArgumentsSnafu {
                err_msg: "invalid procedure id",
            }
            .build()
        })?;
        let state = metasrv
            .procedure_manager()
            .procedure_state(pid)
            .await
            .context(error::QueryProcedureSnafu)?
            .with_context(|| error::ProcedureNotFoundSnafu {
                pid: request.procedure_id,
            })?;
        let (_, error) = procedure::procedure_state_to_pb_state(&state);
        Ok::<_, error::Error>(ProcedureStatusResponse {
            procedure_id: pid.to_string(),
            status: state.as_str_name().to_string(),
            error: (!error.is_empty()).then_some(error),
        })
    }
    .await
    .map(Json)
    .map_err(ErrorResponse::from_error)
    .into_response()
}
