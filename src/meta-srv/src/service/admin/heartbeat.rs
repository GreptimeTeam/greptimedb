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
use snafu::{OptionExt, ResultExt};
use tonic::codegen::http;

use crate::cluster::MetaPeerClient;
use crate::error::{self, Result};
use crate::keys::StatValue;
use crate::service::admin::HttpHandler;

pub struct HeartBeatHandler {
    pub meta_peer_client: Option<MetaPeerClient>,
}

#[async_trait::async_trait]
impl HttpHandler for HeartBeatHandler {
    async fn handle(
        &self,
        _: &str,
        map: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let meta_peer_client = self
            .meta_peer_client
            .as_ref()
            .context(error::NoMetaPeerClientSnafu)?;

        let stat_kvs = meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut stat_vals: Vec<StatValue> = stat_kvs.into_values().collect();

        if let Some(addr) = map.get("addr") {
            stat_vals.retain(|stat_val| {
                if let Some(stat) = stat_val.stats.get(0) {
                    stat.addr == addr.clone()
                } else {
                    false
                }
            });
        }
        let result = StatValues { stat_vals }.try_into()?;

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(result)
            .context(error::InvalidHttpBodySnafu)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StatValues {
    pub stat_vals: Vec<StatValue>,
}

impl TryFrom<StatValues> for String {
    type Error = error::Error;

    fn try_from(vals: StatValues) -> Result<Self> {
        serde_json::to_string(&vals).context(error::SerializeToJsonSnafu {
            input: format!("{vals:?}"),
        })
    }
}
