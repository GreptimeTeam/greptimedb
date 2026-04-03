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
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tonic::codegen::http;

use crate::error::{self, Result};
use crate::metasrv::ElectionRef;
use crate::service::admin::HttpHandler;
use crate::service::admin::util::ErrorHandler;

#[derive(Clone)]
pub struct LeaderHandler {
    pub election: Option<ElectionRef>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LeaderInfo {
    pub leader_addr: Option<String>,
    pub is_leader: bool,
}

impl LeaderHandler {
    async fn get_leader_info(&self) -> Result<LeaderInfo> {
        let (leader_addr, is_leader) = if let Some(election) = &self.election {
            (
                Some(election.leader().await.context(error::KvBackendSnafu)?.0),
                election.is_leader(),
            )
        } else {
            (None, false)
        };

        Ok(LeaderInfo {
            leader_addr,
            is_leader,
        })
    }
}

/// Get the leader handler.
#[axum_macros::debug_handler]
pub(crate) async fn get(State(handler): State<LeaderHandler>) -> Response {
    handler
        .get_leader_info()
        .await
        .map(Json)
        .map_err(ErrorHandler::new)
        .into_response()
}

#[async_trait::async_trait]
impl HttpHandler for LeaderHandler {
    async fn handle(
        &self,
        _: &str,
        _: http::Method,
        _: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        if let Some(leader_addr) = self.get_leader_info().await?.leader_addr {
            return http::Response::builder()
                .status(http::StatusCode::OK)
                .body(leader_addr)
                .context(error::InvalidHttpBodySnafu);
        }
        http::Response::builder()
            .status(http::StatusCode::OK)
            .body("election info is None".to_string())
            .context(error::InvalidHttpBodySnafu)
    }
}
