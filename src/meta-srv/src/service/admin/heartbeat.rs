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
use crate::keys::StatValue;
use crate::service::admin::{util, HttpHandler};

#[derive(Clone)]
pub struct HeartBeatHandler {
    pub meta_peer_client: MetaPeerClientRef,
}

#[async_trait::async_trait]
impl HttpHandler for HeartBeatHandler {
    async fn handle(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        if path.ends_with("/help") {
            return util::to_text_response(
                r#"
            - GET /heartbeat
            - GET /heartbeat?addr=127.0.0.1:3001
            "#,
            );
        }

        let stat_kvs = self.meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut stat_vals: Vec<StatValue> = stat_kvs.into_values().collect();

        if let Some(addr) = params.get("addr") {
            stat_vals = filter_by_addr(stat_vals, addr);
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

fn filter_by_addr(stat_vals: Vec<StatValue>, addr: &str) -> Vec<StatValue> {
    stat_vals
        .into_iter()
        .filter(|stat_val| stat_val.stats.iter().any(|stat| stat.addr == addr))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::handler::node_stat::Stat;
    use crate::keys::StatValue;
    use crate::service::admin::heartbeat::filter_by_addr;

    #[tokio::test]
    async fn test_filter_by_addr() {
        let stat_value1 = StatValue {
            stats: vec![
                Stat {
                    addr: "127.0.0.1:3001".to_string(),
                    timestamp_millis: 1,
                    ..Default::default()
                },
                Stat {
                    addr: "127.0.0.1:3001".to_string(),
                    timestamp_millis: 2,
                    ..Default::default()
                },
            ],
        };

        let stat_value2 = StatValue {
            stats: vec![
                Stat {
                    addr: "127.0.0.1:3002".to_string(),
                    timestamp_millis: 3,
                    ..Default::default()
                },
                Stat {
                    addr: "127.0.0.1:3002".to_string(),
                    timestamp_millis: 4,
                    ..Default::default()
                },
                Stat {
                    addr: "127.0.0.1:3002".to_string(),
                    timestamp_millis: 5,
                    ..Default::default()
                },
            ],
        };

        let mut stat_vals = vec![stat_value1, stat_value2];
        stat_vals = filter_by_addr(stat_vals, "127.0.0.1:3002");
        assert_eq!(stat_vals.len(), 1);
        assert_eq!(stat_vals.first().unwrap().stats.len(), 3);
        assert_eq!(
            stat_vals
                .first()
                .unwrap()
                .stats
                .first()
                .unwrap()
                .timestamp_millis,
            3
        );
    }
}
