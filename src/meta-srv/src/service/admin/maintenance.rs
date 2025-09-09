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

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use crate::error::{
    self, MissingRequiredParameterSnafu, ParseBoolSnafu, Result, RuntimeSwitchManagerSnafu,
    UnsupportedSnafu,
};
use crate::service::admin::HttpHandler;
use crate::service::admin::util::{ErrorHandler, to_json_response, to_not_found_response};

#[derive(Clone)]
pub struct MaintenanceHandler {
    pub manager: RuntimeSwitchManagerRef,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MaintenanceResponse {
    enabled: bool,
}

/// Get the maintenance mode.
#[axum_macros::debug_handler]
pub(crate) async fn status(State(handler): State<MaintenanceHandler>) -> Response {
    handler
        .get_maintenance()
        .await
        .map(Json)
        .map_err(ErrorHandler::new)
        .into_response()
}

/// Set the maintenance mode.
#[axum_macros::debug_handler]
pub(crate) async fn set(State(handler): State<MaintenanceHandler>) -> Response {
    handler
        .set_maintenance()
        .await
        .map(Json)
        .map_err(ErrorHandler::new)
        .into_response()
}

/// Unset the maintenance mode.
#[axum_macros::debug_handler]
pub(crate) async fn unset(State(handler): State<MaintenanceHandler>) -> Response {
    handler
        .unset_maintenance()
        .await
        .map(Json)
        .map_err(ErrorHandler::new)
        .into_response()
}

impl TryFrom<MaintenanceResponse> for String {
    type Error = error::Error;

    fn try_from(response: MaintenanceResponse) -> Result<Self> {
        serde_json::to_string(&response).context(error::SerializeToJsonSnafu {
            input: format!("{response:?}"),
        })
    }
}

impl MaintenanceHandler {
    pub(crate) async fn get_maintenance(&self) -> crate::Result<MaintenanceResponse> {
        let enabled = self
            .manager
            .maintenance_mode()
            .await
            .context(RuntimeSwitchManagerSnafu)?;
        Ok(MaintenanceResponse { enabled })
    }

    pub(crate) async fn set_maintenance(&self) -> crate::Result<MaintenanceResponse> {
        self.manager
            .set_maintenance_mode()
            .await
            .context(RuntimeSwitchManagerSnafu)?;
        // TODO(weny): Add a record to the system events.
        info!("Enable the maintenance mode.");
        Ok(MaintenanceResponse { enabled: true })
    }

    pub(crate) async fn unset_maintenance(&self) -> crate::Result<MaintenanceResponse> {
        self.manager
            .unset_maintenance_mode()
            .await
            .context(RuntimeSwitchManagerSnafu)?;
        // TODO(weny): Add a record to the system events.
        info!("Disable the maintenance mode.");
        Ok(MaintenanceResponse { enabled: false })
    }

    async fn handle_legacy_maintenance(
        &self,
        params: &HashMap<String, String>,
    ) -> crate::Result<MaintenanceResponse> {
        let enable = get_enable_from_params(params)?;
        if enable {
            self.set_maintenance().await
        } else {
            self.unset_maintenance().await
        }
    }
}

fn get_enable_from_params(params: &HashMap<String, String>) -> crate::Result<bool> {
    params
        .get("enable")
        .map(|v| v.parse::<bool>())
        .context(MissingRequiredParameterSnafu { param: "enable" })?
        .context(ParseBoolSnafu {
            err_msg: "'enable' must be 'true' or 'false'",
        })
}

const MAINTENANCE_PATH: &str = "maintenance";
const ENABLE_SUFFIX: &str = "enable";
const DISABLE_SUFFIX: &str = "disable";
const STATUS_SUFFIX: &str = "status";

#[async_trait::async_trait]
impl HttpHandler for MaintenanceHandler {
    // TODO(weny): Remove the legacy version of the maintenance API.
    // However, we need to keep the legacy version for a while to avoid breaking the existing operators.
    async fn handle(
        &self,
        path: &str,
        method: http::Method,
        params: &HashMap<String, String>,
    ) -> crate::Result<http::Response<String>> {
        match method {
            http::Method::GET => {
                if path.ends_with(STATUS_SUFFIX) {
                    // Handle GET request to '/admin/maintenance/status'
                    let response = self.get_maintenance().await?;
                    to_json_response(response)
                } else if path.ends_with(MAINTENANCE_PATH) && params.is_empty() {
                    // Handle GET request to '/admin/maintenance'. (The legacy version)
                    let response = self.get_maintenance().await?;
                    to_json_response(response)
                } else if path.ends_with(MAINTENANCE_PATH) {
                    // Handle GET request to '/admin/maintenance' with URL parameters. (The legacy version)
                    warn!(
                        "Found URL parameters in '/admin/maintenance' request, it's deprecated, will be removed in the future"
                    );
                    // The old version operator will send GET request with URL parameters,
                    // so we need to support it.
                    let response = self.handle_legacy_maintenance(params).await?;
                    to_json_response(response)
                } else {
                    to_not_found_response()
                }
            }
            http::Method::PUT => {
                // Handle PUT request to '/admin/maintenance' with URL parameters. (The legacy version)
                if path.ends_with(MAINTENANCE_PATH) {
                    warn!(
                        "Found PUT request to '/admin/maintenance', it's deprecated, will be removed in the future"
                    );
                    let response = self.handle_legacy_maintenance(params).await?;
                    to_json_response(response)
                } else {
                    to_not_found_response()
                }
            }
            http::Method::POST => {
                // Handle POST request to '/admin/maintenance/enable'
                if path.ends_with(ENABLE_SUFFIX) {
                    let response = self.set_maintenance().await?;
                    to_json_response(response)
                } else if path.ends_with(DISABLE_SUFFIX) {
                    // Handle POST request to '/admin/maintenance/disable'
                    let response = self.unset_maintenance().await?;
                    to_json_response(response)
                } else if path.ends_with(MAINTENANCE_PATH) {
                    // Handle POST request to '/admin/maintenance' with URL parameters. (The legacy version)
                    warn!(
                        "Found PUT request to '/admin/maintenance', it's deprecated, will be removed in the future"
                    );
                    let response = self.handle_legacy_maintenance(params).await?;
                    to_json_response(response)
                } else {
                    to_not_found_response()
                }
            }
            _ => UnsupportedSnafu {
                operation: format!("http method {method}"),
            }
            .fail(),
        }
    }
}
