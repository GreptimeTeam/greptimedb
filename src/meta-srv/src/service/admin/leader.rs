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

use snafu::ResultExt;
use tonic::codegen::http;

use crate::error::{self, Result};
use crate::metasrv::ElectionRef;
use crate::service::admin::HttpHandler;

pub struct LeaderHandler {
    pub election: Option<ElectionRef>,
}

#[async_trait::async_trait]
impl HttpHandler for LeaderHandler {
    async fn handle(&self, _: &str, _: &HashMap<String, String>) -> Result<http::Response<String>> {
        if let Some(election) = &self.election {
            let leader_addr = election.leader().await?.0;
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
