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

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use common_telemetry::info;
use snafu::ResultExt;
use sqlx::{MySql, Pool, Row};
use store_api::storage::RegionId;

use crate::error::{self};
use crate::ir::Ident;
use crate::utils::partition::{fetch_partitions, region_distribution};
use crate::utils::wait::wait_condition_fn;

/// Migrates a region from one peer to another within a specified timeout.
///
/// Returns the procedure id.
pub async fn migrate_region(
    e: &Pool<MySql>,
    region_id: u64,
    from_peer_id: u64,
    to_peer_id: u64,
    timeout_secs: u64,
) -> String {
    let sql =
        format!("admin migrate_region({region_id}, {from_peer_id}, {to_peer_id}, {timeout_secs});");
    let result = sqlx::query(&sql)
        .fetch_one(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })
        .unwrap();
    result.try_get(0).unwrap()
}

/// Waits until the region distribution matches the expected distribution within a specified timeout.
pub async fn wait_for_region_distribution(
    greptime: &Pool<MySql>,
    timeout: Duration,
    table_name: Ident,
    expected_region_distribution: BTreeMap<u64, HashSet<RegionId>>,
) {
    wait_condition_fn(
        timeout,
        || {
            let greptime = greptime.clone();
            let table_name = table_name.clone();
            Box::pin(async move {
                let partitions = fetch_partitions(&greptime, table_name).await.unwrap();
                region_distribution(partitions)
                    .into_iter()
                    .map(|(datanode, regions)| {
                        (datanode, regions.into_iter().collect::<HashSet<_>>())
                    })
                    .collect::<BTreeMap<_, _>>()
            })
        },
        move |region_distribution| {
            info!("region distribution: {:?}", region_distribution);
            if expected_region_distribution.keys().len() != region_distribution.keys().len() {
                return false;
            }

            for (datanode, expected_regions) in &expected_region_distribution {
                match region_distribution.get(datanode) {
                    Some(regions) => {
                        if expected_regions != regions {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        },
        Duration::from_secs(5),
    )
    .await
}
