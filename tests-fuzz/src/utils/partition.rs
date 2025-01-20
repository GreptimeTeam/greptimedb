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

use std::collections::BTreeMap;
use std::time::Duration;

use common_telemetry::info;
use snafu::ResultExt;
use sqlx::MySqlPool;
use store_api::storage::RegionId;

use super::wait::wait_condition_fn;
use crate::error::{self, Result};
use crate::ir::Ident;

#[derive(Debug, sqlx::FromRow)]
pub struct Partition {
    pub datanode_id: u64,
    pub region_id: u64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct PartitionCount {
    pub count: i64,
}

pub async fn count_partitions(db: &MySqlPool, datanode_id: u64) -> Result<PartitionCount> {
    let sql = "select count(1) as count from information_schema.region_peers where peer_id == ?";
    sqlx::query_as::<_, PartitionCount>(sql)
        .bind(datanode_id)
        .fetch_one(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}

/// Returns the [Partition] of the specific `region_id`
pub async fn fetch_partition(db: &MySqlPool, region_id: u64) -> Result<Partition> {
    let sql = "select region_id, peer_id as datanode_id from information_schema.region_peers where region_id = ?;";
    sqlx::query_as::<_, Partition>(sql)
        .bind(region_id)
        .fetch_one(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}

/// Returns all [Partition] of the specific `table`
pub async fn fetch_partitions(db: &MySqlPool, table_name: Ident) -> Result<Vec<Partition>> {
    let sql = "select b.peer_id as datanode_id, a.greptime_partition_id as region_id
from information_schema.partitions a left join information_schema.region_peers b
on a.greptime_partition_id = b.region_id where a.table_name= ? order by datanode_id asc;";
    sqlx::query_as::<_, Partition>(sql)
        .bind(table_name.value.to_string())
        .fetch_all(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}

/// Creates a distribution map of regions to datanodes based on the provided partitions.
///
/// This function iterates over the provided partitions and groups the regions by their associated datanode IDs.
pub fn region_distribution(partitions: Vec<Partition>) -> BTreeMap<u64, Vec<RegionId>> {
    let mut distribution: BTreeMap<u64, Vec<RegionId>> = BTreeMap::new();
    for partition in partitions {
        distribution
            .entry(partition.datanode_id)
            .or_default()
            .push(RegionId::from_u64(partition.region_id));
    }

    distribution
}

/// Pretty prints the region distribution for each datanode.
///
/// This function logs the number of regions for each datanode in the distribution map.
pub fn pretty_print_region_distribution(distribution: &BTreeMap<u64, Vec<RegionId>>) {
    for (node, regions) in distribution {
        info!("Datanode: {node}, num of regions: {}", regions.len());
    }
}

/// Waits until all regions are evicted from the specified datanode.
///
/// This function repeatedly checks the number of partitions on the specified datanode and waits until
/// the count reaches zero or the timeout period elapses. It logs the number of partitions on each check.
pub async fn wait_for_all_regions_evicted(
    greptime: MySqlPool,
    selected_datanode: u64,
    timeout: Duration,
) {
    wait_condition_fn(
        timeout,
        || {
            let greptime = greptime.clone();
            Box::pin(async move {
                let partition = count_partitions(&greptime, selected_datanode)
                    .await
                    .unwrap();
                info!(
                    "Datanode: {selected_datanode}, num of partitions: {}",
                    partition.count
                );
                partition.count
            })
        },
        |count| count == 0,
        Duration::from_secs(5),
    )
    .await;
}
