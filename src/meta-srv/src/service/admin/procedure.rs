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

use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tonic::codegen::http;
use tonic::codegen::http::Response;

use crate::error::RuntimeSwitchManagerSnafu;
use crate::service::admin::util::{to_json_response, to_not_found_response};
use crate::service::admin::HttpHandler;

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
    ) -> crate::Result<Response<String>> {
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
