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

use common_meta::key::maintenance::MaintenanceModeManagerRef;
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;
use tonic::codegen::http::Response;

use crate::error::{
    self, InvalidHttpBodySnafu, MaintenanceModeManagerSnafu, MissingRequiredParameterSnafu,
    ParseBoolSnafu, Result, UnsupportedSnafu,
};
use crate::service::admin::HttpHandler;

#[derive(Clone)]
pub struct MaintenanceHandler {
    pub manager: MaintenanceModeManagerRef,
}

#[derive(Debug, Serialize, Deserialize)]
struct MaintenanceResponse {
    enabled: bool,
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
    async fn get_maintenance(&self) -> crate::Result<Response<String>> {
        let enabled = self
            .manager
            .maintenance_mode()
            .await
            .context(MaintenanceModeManagerSnafu)?;
        let response = MaintenanceResponse { enabled }.try_into()?;
        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(response)
            .context(InvalidHttpBodySnafu)
    }

    async fn set_maintenance(
        &self,
        params: &HashMap<String, String>,
    ) -> crate::Result<Response<String>> {
        let enable = params
            .get("enable")
            .map(|v| v.parse::<bool>())
            .context(MissingRequiredParameterSnafu { param: "enable" })?
            .context(ParseBoolSnafu {
                err_msg: "'enable' must be 'true' or 'false'",
            })?;

        if enable {
            self.manager
                .set_maintenance_mode()
                .await
                .context(MaintenanceModeManagerSnafu)?;
            info!("Enable the maintenance mode.");
        } else {
            self.manager
                .unset_maintenance_mode()
                .await
                .context(MaintenanceModeManagerSnafu)?;
            info!("Disable the maintenance mode.");
        };

        let response = MaintenanceResponse { enabled: enable }.try_into()?;
        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(response)
            .context(InvalidHttpBodySnafu)
    }
}

#[async_trait::async_trait]
impl HttpHandler for MaintenanceHandler {
    async fn handle(
        &self,
        _: &str,
        method: http::Method,
        params: &HashMap<String, String>,
    ) -> crate::Result<Response<String>> {
        match method {
            http::Method::GET => {
                if params.is_empty() {
                    self.get_maintenance().await
                } else {
                    warn!(
                        "Found URL parameters in '/admin/maintenance' request, it's deprecated, will be removed in the future"
                    );
                    // The old version operator will send GET request with URL parameters,
                    // so we need to support it.
                    self.set_maintenance(params).await
                }
            }
            http::Method::PUT => {
                warn!("Found PUT request to '/admin/maintenance', it's deprecated, will be removed in the future");
                self.set_maintenance(params).await
            }
            http::Method::POST => self.set_maintenance(params).await,
            _ => UnsupportedSnafu {
                operation: format!("http method {method}"),
            }
            .fail(),
        }
    }
}
