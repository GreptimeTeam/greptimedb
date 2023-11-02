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

use common_meta::kv_backend::ResettableKvBackendRef;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tonic::codegen::http;

use crate::error::{self, Result};
use crate::inactive_region_manager::InactiveRegionManager;
use crate::keys::InactiveRegionKey;
use crate::service::admin::{util, HttpHandler};

pub struct ViewInactiveRegionsHandler {
    pub store: ResettableKvBackendRef,
}

#[async_trait::async_trait]
impl HttpHandler for ViewInactiveRegionsHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let cluster_id = util::extract_cluster_id(params)?;

        let inactive_region_manager = InactiveRegionManager::new(&self.store);
        let inactive_regions = inactive_region_manager
            .scan_all_inactive_regions(cluster_id)
            .await?;
        let result = InactiveRegions { inactive_regions }.try_into()?;

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(result)
            .context(error::InvalidHttpBodySnafu)
    }
}

pub struct ClearInactiveRegionsHandler {
    pub store: ResettableKvBackendRef,
}

#[async_trait::async_trait]
impl HttpHandler for ClearInactiveRegionsHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let cluster_id = util::extract_cluster_id(params)?;

        let inactive_region_manager = InactiveRegionManager::new(&self.store);
        inactive_region_manager
            .clear_all_inactive_regions(cluster_id)
            .await?;

        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body("Success\n".to_owned())
            .unwrap())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
struct InactiveRegions {
    inactive_regions: Vec<InactiveRegionKey>,
}

impl TryFrom<InactiveRegions> for String {
    type Error = error::Error;

    fn try_from(value: InactiveRegions) -> Result<Self> {
        serde_json::to_string(&value).context(error::SerializeToJsonSnafu {
            input: format!("{value:?}"),
        })
    }
}
