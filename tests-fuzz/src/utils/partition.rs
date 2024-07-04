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
use sqlx::database::HasArguments;
use sqlx::{ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, MySql, Pool, Type};
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

pub async fn count_partitions<'a, DB, E>(e: E, datanode_id: u64) -> Result<PartitionCount>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'c> E: 'a + Executor<'c, Database = DB>,
    for<'c> i64: Decode<'c, DB> + Type<DB>,
    for<'c> String: Decode<'c, DB> + Type<DB>,
    for<'c> u64: Encode<'c, DB> + Type<DB>,
    for<'c> &'c str: ColumnIndex<<DB as Database>::Row>,
{
    let sql = "select count(1) as count from information_schema.region_peers where peer_id == ?";
    Ok(sqlx::query_as::<_, PartitionCount>(sql)
        .bind(datanode_id)
        .fetch_all(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })?
        .remove(0))
}

/// Returns the [Partition] of the specific `region_id`
pub async fn fetch_partition<'a, DB, E>(e: E, region_id: u64) -> Result<Partition>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'c> E: 'a + Executor<'c, Database = DB>,
    for<'c> u64: Decode<'c, DB> + Type<DB>,
    for<'c> String: Decode<'c, DB> + Type<DB>,
    for<'c> u64: Encode<'c, DB> + Type<DB>,
    for<'c> &'c str: ColumnIndex<<DB as Database>::Row>,
{
    let sql = "select region_id, peer_id as datanode_id from information_schema.region_peers where region_id = ?;";
    sqlx::query_as::<_, Partition>(sql)
        .bind(region_id)
        .fetch_one(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}

/// Returns all [Partition] of the specific `table`
pub async fn fetch_partitions<'a, DB, E>(e: E, table_name: Ident) -> Result<Vec<Partition>>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'c> E: 'a + Executor<'c, Database = DB>,
    for<'c> u64: Decode<'c, DB> + Type<DB>,
    for<'c> String: Decode<'c, DB> + Type<DB>,
    for<'c> String: Encode<'c, DB> + Type<DB>,
    for<'c> &'c str: ColumnIndex<<DB as Database>::Row>,
{
    let sql = "select b.peer_id as datanode_id, a.greptime_partition_id as region_id
from information_schema.partitions a left join information_schema.region_peers b
on a.greptime_partition_id = b.region_id where a.table_name= ? order by datanode_id asc;";
    sqlx::query_as::<_, Partition>(sql)
        .bind(table_name.value.to_string())
        .fetch_all(e)
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
    greptime: Pool<MySql>,
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
