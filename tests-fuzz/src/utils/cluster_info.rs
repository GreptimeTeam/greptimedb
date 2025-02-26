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

use std::time::Duration;

use common_telemetry::info;
use humantime::parse_duration;
use snafu::ResultExt;
use sqlx::MySqlPool;

use super::wait::wait_condition_fn;
use crate::error::{self, Result};

pub const PEER_TYPE_DATANODE: &str = "DATANODE";

#[derive(Debug, sqlx::FromRow)]
pub struct NodeInfo {
    pub peer_id: i64,
    pub peer_addr: String,
    pub peer_type: String,
    pub active_time: Option<String>,
}

/// Returns all [NodeInfo] in the cluster.
pub async fn fetch_nodes(db: &MySqlPool) -> Result<Vec<NodeInfo>> {
    let sql = "select * from information_schema.cluster_info";
    sqlx::query_as::<_, NodeInfo>(sql)
        .fetch_all(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}

/// Waits until all datanodes are online within a specified timeout period.
///
/// This function repeatedly checks the status of all datanodes and waits until all of them are online
/// or the timeout period elapses. A datanode is considered online if its `active_time` is less than 3 seconds.
pub async fn wait_for_all_datanode_online(greptime: MySqlPool, timeout: Duration) {
    wait_condition_fn(
        timeout,
        || {
            let greptime = greptime.clone();
            Box::pin(async move {
                let nodes = fetch_nodes(&greptime)
                    .await
                    .unwrap()
                    .into_iter()
                    .flat_map(|node| {
                        if node.peer_type == PEER_TYPE_DATANODE {
                            Some(node)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                info!("Waits for all datanode online: {nodes:?}");
                nodes
            })
        },
        |nodes| {
            nodes
                .into_iter()
                .map(|node| parse_duration(&node.active_time.unwrap()).unwrap())
                .all(|duration| duration < Duration::from_secs(3))
        },
        Duration::from_secs(5),
    )
    .await
}
