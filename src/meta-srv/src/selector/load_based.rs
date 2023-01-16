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

use api::v1::meta::{Peer, RangeRequest};
use common_time::util as time_util;

use super::{Namespace, Selector};
use crate::error::Result;
use crate::keys::{LeaseKey, LeaseValue, StatKey, StatValue, DN_STAT_PREFIX};
use crate::metasrv::Context;
use crate::service::store::kv::KvStoreRef;
use crate::{lease, util};

pub struct LoadBasedSelector;

#[async_trait::async_trait]
impl Selector for LoadBasedSelector {
    type Context = Context;
    type Output = Vec<Peer>;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output> {
        // get alive datanodes
        let lease_filter = |_: &LeaseKey, v: &LeaseValue| {
            time_util::current_time_millis() - v.timestamp_millis < ctx.datanode_lease_secs * 1000
        };
        let lease_kvs: HashMap<LeaseKey, LeaseValue> =
            lease::alive_datanodes(ns, &ctx.kv_store, lease_filter)
                .await?
                .into_iter()
                .collect();

        // get stats of datanodes
        let stat_kvs = all_stat_kvs(ns, &ctx.kv_store).await?;

        // filter out expired datanodes and nodes that cannot get region number
        let mut tuples: Vec<_> = stat_kvs
            .iter()
            .filter_map(|(stat_key, stat_val)| {
                match (
                    lease_kvs.get(&to_lease_key(stat_key)),
                    stat_val.region_num(),
                ) {
                    (Some(lease_val), Some(region_num)) => Some((stat_key, lease_val, region_num)),
                    _ => None,
                }
            })
            .collect();

        // sort the datanodes according to the number of regions
        tuples.sort_by(|a, b| a.2.cmp(&b.2));

        Ok(tuples
            .into_iter()
            .map(|(stat_key, lease_val, _)| Peer {
                id: stat_key.node_id,
                addr: lease_val.node_addr.clone(),
            })
            .collect())
    }
}

// get all stat kvs from store
pub async fn all_stat_kvs(
    cluster_id: u64,
    kv_store: &KvStoreRef,
) -> Result<Vec<(StatKey, StatValue)>> {
    let key = stat_prefix(cluster_id);
    let range_end = util::get_prefix_end_key(&key);
    let req = RangeRequest {
        key,
        range_end,
        ..Default::default()
    };

    let kvs = kv_store.range(req).await?.kvs;

    let mut stat_kvs = Vec::with_capacity(kvs.len());

    for kv in kvs {
        let key: StatKey = kv.key.try_into()?;
        let value: StatValue = kv.value.try_into()?;
        stat_kvs.push((key, value));
    }

    Ok(stat_kvs)
}

fn to_lease_key(k: &StatKey) -> LeaseKey {
    LeaseKey {
        cluster_id: k.cluster_id,
        node_id: k.node_id,
    }
}

fn stat_prefix(cluster_id: u64) -> Vec<u8> {
    format!("{DN_STAT_PREFIX}-{cluster_id}").into_bytes()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::PutRequest;

    use super::{all_stat_kvs, to_lease_key};
    use crate::handler::node_stat::Stat;
    use crate::keys::{StatKey, StatValue};
    use crate::selector::load_based::stat_prefix;
    use crate::service::store::kv::{KvStore, KvStoreRef};
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_all_stat_kvs() {
        let kv_store = Arc::new(MemStore::new()) as Arc<dyn KvStore>;
        let kvs = all_stat_kvs(0, &kv_store).await.unwrap();
        assert!(kvs.is_empty());

        let mut kv_store = Arc::new(MemStore::new()) as Arc<dyn KvStore>;
        put_stats_to_store(&mut kv_store).await;
        let kvs = all_stat_kvs(0, &kv_store).await.unwrap();
        assert_eq!(2, kvs.len());
        let kvs = all_stat_kvs(1, &kv_store).await.unwrap();
        assert_eq!(1, kvs.len());
    }

    #[test]
    fn test_to_lease_key() {
        let statkey = StatKey {
            cluster_id: 1,
            node_id: 101,
        };
        let lease_key = to_lease_key(&statkey);
        assert_eq!(1, lease_key.cluster_id);
        assert_eq!(101, lease_key.node_id);
    }

    #[test]
    fn test_stat_prefix() {
        assert_eq!(stat_prefix(1), b"__meta_dnstat-1");
    }

    async fn put_stats_to_store(store: &mut KvStoreRef) {
        let put1 = PutRequest {
            key: StatKey {
                cluster_id: 0,
                node_id: 101,
            }
            .into(),
            value: StatValue {
                stats: vec![Stat {
                    region_num: Some(100),
                    ..Default::default()
                }],
            }
            .try_into()
            .unwrap(),
            ..Default::default()
        };
        store.put(put1).await.unwrap();

        let put2 = PutRequest {
            key: StatKey {
                cluster_id: 0,
                node_id: 102,
            }
            .into(),
            value: StatValue {
                stats: vec![Stat {
                    region_num: Some(99),
                    ..Default::default()
                }],
            }
            .try_into()
            .unwrap(),
            ..Default::default()
        };
        store.put(put2).await.unwrap();

        let put3 = PutRequest {
            key: StatKey {
                cluster_id: 1,
                node_id: 103,
            }
            .into(),
            value: StatValue {
                stats: vec![Stat {
                    region_num: Some(98),
                    ..Default::default()
                }],
            }
            .try_into()
            .unwrap(),
            ..Default::default()
        };
        store.put(put3).await.unwrap();
    }
}
