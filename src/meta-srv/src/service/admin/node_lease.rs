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

use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tonic::codegen::http;

use crate::cluster::MetaPeerClientRef;
use crate::error::{self, Result};
use crate::keys::{LeaseKey, LeaseValue};
use crate::lease;
use crate::service::admin::{util, HttpHandler};

pub struct NodeLeaseHandler {
    pub meta_peer_client: MetaPeerClientRef,
}

#[async_trait::async_trait]
impl HttpHandler for NodeLeaseHandler {
    async fn handle(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let cluster_id = util::extract_cluster_id(params)?;

        let leases =
            lease::filter_datanodes(cluster_id, &self.meta_peer_client, |_, _| true).await?;
        let leases = leases
            .into_iter()
            .map(|(k, v)| HumanLease {
                name: k,
                human_time: common_time::DateTime::new(v.timestamp_millis).to_string(),
                lease: v,
            })
            .collect::<Vec<_>>();
        let result = LeaseValues { leases }.try_into()?;

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(result)
            .context(error::InvalidHttpBodySnafu)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HumanLease {
    pub name: LeaseKey,
    pub human_time: String,
    pub lease: LeaseValue,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseValues {
    pub leases: Vec<HumanLease>,
}

impl TryFrom<LeaseValues> for String {
    type Error = error::Error;

    fn try_from(vals: LeaseValues) -> Result<Self> {
        serde_json::to_string(&vals).context(error::SerializeToJsonSnafu {
            input: format!("{vals:?}"),
        })
    }
}
