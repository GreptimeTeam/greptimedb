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

use common_meta::key::MAINTENANCE_KEY;
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::PutRequest;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;
use tonic::codegen::http::Response;

use crate::error::{
    InvalidHttpBodySnafu, KvBackendSnafu, MissingRequiredParameterSnafu, ParseBoolSnafu,
    UnsupportedSnafu,
};
use crate::service::admin::HttpHandler;

#[derive(Clone)]
pub struct MaintenanceHandler {
    pub kv_backend: KvBackendRef,
}

impl MaintenanceHandler {
    async fn get_maintenance(&self) -> crate::Result<Response<String>> {
        let enabled = self
            .kv_backend
            .exists(MAINTENANCE_KEY.as_bytes())
            .await
            .context(KvBackendSnafu)?;
        let response = if enabled {
            "Maintenance mode is enabled"
        } else {
            "Maintenance mode is disabled"
        };
        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(response.into())
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

        let response = if enable {
            let req = PutRequest {
                key: Vec::from(MAINTENANCE_KEY),
                value: vec![],
                prev_kv: false,
            };
            self.kv_backend
                .put(req.clone())
                .await
                .context(KvBackendSnafu)?;
            "Maintenance mode enabled"
        } else {
            self.kv_backend
                .delete(MAINTENANCE_KEY.as_bytes(), false)
                .await
                .context(KvBackendSnafu)?;
            "Maintenance mode disabled"
        };

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(response.into())
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
            http::Method::GET => self.get_maintenance().await,
            http::Method::PUT => self.set_maintenance(params).await,
            _ => UnsupportedSnafu {
                operation: format!("http method {method}"),
            }
            .fail(),
        }
    }
}
