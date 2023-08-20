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
use common_meta::rpc::store::PutRequest;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use crate::error::{self, Result};
use crate::service::admin::HttpHandler;
use crate::service::store::kv::{KvStoreRef, ResettableKvStoreRef};
pub struct MaintenanceHandler {
    pub kv_store: KvStoreRef,
    pub in_memory: ResettableKvStoreRef,
}

#[async_trait::async_trait]
impl HttpHandler for MaintenanceHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let switch_on = params
            .get("switch_on")
            .map(|on| on.parse::<bool>())
            .context(error::MissingRequiredParameterSnafu { param: "switch-on" })?
            .context(error::ParseBoolSnafu {
                err_msg: "`switch_on` is not a valid bool",
            })?;

        let req = PutRequest {
            key: Vec::from(MAINTENANCE_KEY),
            value: vec![],
            prev_kv: false,
        };

        if switch_on {
            self.kv_store.put(req.clone()).await?;
            self.in_memory.put(req).await?;
        } else {
            self.kv_store.delete(MAINTENANCE_KEY, false).await?;
            self.in_memory.delete(MAINTENANCE_KEY, false).await?;
        }

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body("metasvc is succeed to be set maintenance mode".to_string())
            .context(error::InvalidHttpBodySnafu)
    }
}
