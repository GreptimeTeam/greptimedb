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

use api::v1::meta::Peer;
use common_telemetry::warn;

use crate::error::Result;
use crate::handler::node_stat::RegionStat;
use crate::keys::{LeaseKey, LeaseValue, StatKey, StatValue};
use crate::lease;
use crate::metasrv::SelectorContext;
use crate::selector::{Namespace, Selector};

const MAX_REGION_NUMBER: u64 = u64::MAX;

pub struct LoadBasedSelector;

#[async_trait::async_trait]
impl Selector for LoadBasedSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output> {
        // get alive datanodes
        let lease_kvs =
            lease::alive_datanodes(ns, &ctx.meta_peer_client, ctx.datanode_lease_secs).await?;
        if lease_kvs.is_empty() {
            return Ok(vec![]);
        }

        let stat_keys: Vec<StatKey> = lease_kvs.keys().map(|k| k.into()).collect();
        let stat_kvs = ctx.meta_peer_client.get_dn_stat_kvs(stat_keys).await?;

        let mut tuples: Vec<(LeaseKey, LeaseValue, u64)> = lease_kvs
            .into_iter()
            // The regions of a table need to be distributed on different datanode.
            .filter(|(lease_k, _)| {
                if let Some(stat_val) = stat_kvs.get(&lease_k.into()) {
                    if let (Some(catalog), Some(schema), Some(table)) =
                        (&ctx.catalog, &ctx.schema, &ctx.table)
                    {
                        return contains_table(stat_val, catalog, schema, table) != Some(true);
                    }
                }
                true
            })
            .map(|(lease_k, lease_v)| {
                let stat_key: StatKey = (&lease_k).into();

                let region_num = match stat_kvs
                    .get(&stat_key)
                    .and_then(|stat_val| stat_val.region_num())
                {
                    Some(region_num) => region_num,
                    None => {
                        warn!("Failed to get stat_val by stat_key {:?}", stat_key);
                        MAX_REGION_NUMBER
                    }
                };

                (lease_k, lease_v, region_num)
            })
            .collect();

        // sort the datanodes according to the number of regions
        tuples.sort_by(|a, b| a.2.cmp(&b.2));

        Ok(tuples
            .into_iter()
            .map(|(stat_key, lease_val, _)| Peer {
                id: stat_key.node_id,
                addr: lease_val.node_addr,
            })
            .collect())
    }
}

// Determine whether there is the table in datanode according to the heartbeats.
//
// Result:
// None indicates no heartbeats in stat_val;
// Some(true) indicates table exists in the datanode;
// Some(false) indicates that table not exists in datanode.
fn contains_table(
    stat_val: &StatValue,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
) -> Option<bool> {
    let may_latest = stat_val.stats.last();

    if let Some(latest) = may_latest {
        for RegionStat {
            catalog,
            schema,
            table,
            ..
        } in latest.region_stats.iter()
        {
            if catalog_name == catalog && schema_name == schema && table_name == table {
                return Some(true);
            }
        }
    } else {
        return None;
    }

    Some(false)
}

#[cfg(test)]
mod tests {
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::keys::StatValue;
    use crate::selector::load_based::contains_table;

    #[test]
    fn test_contains_table_from_stat_val() {
        let empty = StatValue { stats: vec![] };
        assert!(contains_table(&empty, "greptime_4", "public_4", "demo_5").is_none());

        let stat_val = StatValue {
            stats: vec![
                Stat {
                    region_stats: vec![
                        RegionStat {
                            catalog: "greptime_1".to_string(),
                            schema: "public_1".to_string(),
                            table: "demo_1".to_string(),
                            ..Default::default()
                        },
                        RegionStat {
                            catalog: "greptime_2".to_string(),
                            schema: "public_2".to_string(),
                            table: "demo_2".to_string(),
                            ..Default::default()
                        },
                        RegionStat {
                            catalog: "greptime_3".to_string(),
                            schema: "public_3".to_string(),
                            table: "demo_3".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                Stat {
                    region_stats: vec![
                        RegionStat {
                            catalog: "greptime_1".to_string(),
                            schema: "public_1".to_string(),
                            table: "demo_1".to_string(),
                            ..Default::default()
                        },
                        RegionStat {
                            catalog: "greptime_2".to_string(),
                            schema: "public_2".to_string(),
                            table: "demo_2".to_string(),
                            ..Default::default()
                        },
                        RegionStat {
                            catalog: "greptime_3".to_string(),
                            schema: "public_3".to_string(),
                            table: "demo_3".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                Stat {
                    region_stats: vec![
                        RegionStat {
                            catalog: "greptime_1".to_string(),
                            schema: "public_1".to_string(),
                            table: "demo_1".to_string(),
                            ..Default::default()
                        },
                        RegionStat {
                            catalog: "greptime_2".to_string(),
                            schema: "public_2".to_string(),
                            table: "demo_2".to_string(),
                            ..Default::default()
                        },
                        RegionStat {
                            catalog: "greptime_4".to_string(),
                            schema: "public_4".to_string(),
                            table: "demo_4".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
        };
        assert!(contains_table(&stat_val, "greptime_1", "public_1", "demo_1").unwrap());
        assert!(contains_table(&stat_val, "greptime_2", "public_2", "demo_2").unwrap());
        assert!(!contains_table(&stat_val, "greptime_3", "public_3", "demo_3").unwrap());
        assert!(contains_table(&stat_val, "greptime_4", "public_4", "demo_4").unwrap());
    }
}
