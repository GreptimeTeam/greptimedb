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

use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue};
use common_meta::peer::Peer;
use itertools::{Itertools, MinMaxResult};

use crate::selector::weighted_choose::WeightedItem;

/// The [`WeightCompute`] trait is used to compute the weight array by heartbeats.
pub trait WeightCompute: Send + Sync {
    type Source;

    fn compute(&self, stat_kvs: &Self::Source) -> Vec<WeightedItem<Peer>>;
}

/// The ['RegionNumsBasedWeightCompute'] calculates weighted list based on region number obtained from the heartbeat.
///
/// # How to calculate the weighted list?
/// weight = max_region_num - current_region_num + (max_region_num - min_region_num);
///
/// # How to calculate the reverse weighted list?
/// reverse_weight = region_num - min_region_num + (max_region_num - min_region_num);
pub struct RegionNumsBasedWeightCompute;

impl WeightCompute for RegionNumsBasedWeightCompute {
    type Source = HashMap<DatanodeStatKey, DatanodeStatValue>;

    fn compute(
        &self,
        stat_kvs: &HashMap<DatanodeStatKey, DatanodeStatValue>,
    ) -> Vec<WeightedItem<Peer>> {
        let mut region_nums = Vec::with_capacity(stat_kvs.len());
        let mut peers = Vec::with_capacity(stat_kvs.len());

        for (stat_k, stat_v) in stat_kvs {
            let Some(region_num) = stat_v.region_num() else {
                continue;
            };
            let Some(node_addr) = stat_v.node_addr() else {
                continue;
            };

            let peer = Peer {
                id: stat_k.node_id,
                addr: node_addr,
            };

            region_nums.push(region_num);
            peers.push(peer);
        }

        if region_nums.is_empty() {
            return vec![];
        }

        let (min_weight, max_weight) = match region_nums.iter().minmax() {
            // unreachable safety: region_nums is not empty
            MinMaxResult::NoElements => unreachable!(),
            MinMaxResult::OneElement(minmax) => (*minmax, *minmax),
            MinMaxResult::MinMax(min, max) => (*min, *max),
        };

        let base_weight = match max_weight - min_weight {
            0 => 1,
            x => x,
        };

        peers
            .into_iter()
            .zip(region_nums)
            .map(|(peer, region_num)| WeightedItem {
                item: peer,
                weight: (max_weight - region_num + base_weight) as f64,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_meta::datanode::{
        DatanodeStatKey, DatanodeStatValue, RegionManifestInfo, RegionStat, Stat,
    };
    use common_meta::peer::Peer;
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use super::{RegionNumsBasedWeightCompute, WeightCompute};

    #[test]
    fn test_weight_compute() {
        let mut stat_kvs: HashMap<DatanodeStatKey, DatanodeStatValue> = HashMap::default();
        let stat_key = DatanodeStatKey { node_id: 1 };
        let stat_val = DatanodeStatValue {
            stats: vec![mock_stat_1()],
        };
        stat_kvs.insert(stat_key, stat_val);
        let stat_key = DatanodeStatKey { node_id: 2 };
        let stat_val = DatanodeStatValue {
            stats: vec![mock_stat_2()],
        };
        stat_kvs.insert(stat_key, stat_val);
        let stat_key = DatanodeStatKey { node_id: 3 };
        let stat_val = DatanodeStatValue {
            stats: vec![mock_stat_3()],
        };
        stat_kvs.insert(stat_key, stat_val);

        let compute = RegionNumsBasedWeightCompute;
        let weight_array = compute.compute(&stat_kvs);

        let mut expected = HashMap::new();
        expected.insert(
            Peer {
                id: 1,
                addr: "127.0.0.1:3001".to_string(),
            },
            4,
        );
        expected.insert(
            Peer {
                id: 2,
                addr: "127.0.0.1:3002".to_string(),
            },
            3,
        );
        expected.insert(
            Peer {
                id: 3,
                addr: "127.0.0.1:3003".to_string(),
            },
            2,
        );
        for weight in weight_array.iter() {
            assert_eq!(*expected.get(&weight.item).unwrap(), weight.weight as usize);
        }

        let mut expected = HashMap::new();
        expected.insert(
            Peer {
                id: 1,
                addr: "127.0.0.1:3001".to_string(),
            },
            2,
        );
        expected.insert(
            Peer {
                id: 2,
                addr: "127.0.0.1:3002".to_string(),
            },
            3,
        );
        expected.insert(
            Peer {
                id: 3,
                addr: "127.0.0.1:3003".to_string(),
            },
            4,
        );
    }

    fn mock_stat_1() -> Stat {
        Stat {
            addr: "127.0.0.1:3001".to_string(),
            region_num: 11,
            region_stats: vec![RegionStat {
                id: RegionId::from_u64(111),
                rcus: 1,
                wcus: 1,
                approximate_bytes: 1,
                engine: "mito2".to_string(),
                role: RegionRole::Leader,
                num_rows: 0,
                memtable_size: 0,
                manifest_size: 0,
                sst_size: 0,
                index_size: 0,
                region_manifest: RegionManifestInfo::Mito {
                    manifest_version: 0,
                    flushed_entry_id: 0,
                },
                data_topic_latest_entry_id: 0,
                metadata_topic_latest_entry_id: 0,
            }],
            ..Default::default()
        }
    }

    fn mock_stat_2() -> Stat {
        Stat {
            addr: "127.0.0.1:3002".to_string(),
            region_num: 12,
            region_stats: vec![RegionStat {
                id: RegionId::from_u64(112),
                rcus: 1,
                wcus: 1,
                approximate_bytes: 1,
                engine: "mito2".to_string(),
                role: RegionRole::Leader,
                num_rows: 0,
                memtable_size: 0,
                manifest_size: 0,
                sst_size: 0,
                index_size: 0,
                region_manifest: RegionManifestInfo::Mito {
                    manifest_version: 0,
                    flushed_entry_id: 0,
                },
                data_topic_latest_entry_id: 0,
                metadata_topic_latest_entry_id: 0,
            }],
            ..Default::default()
        }
    }

    fn mock_stat_3() -> Stat {
        Stat {
            addr: "127.0.0.1:3003".to_string(),
            region_num: 13,
            region_stats: vec![RegionStat {
                id: RegionId::from_u64(113),
                rcus: 1,
                wcus: 1,
                approximate_bytes: 1,
                engine: "mito2".to_string(),
                role: RegionRole::Leader,
                num_rows: 0,
                memtable_size: 0,
                manifest_size: 0,
                sst_size: 0,
                index_size: 0,
                region_manifest: RegionManifestInfo::Mito {
                    manifest_version: 0,
                    flushed_entry_id: 0,
                },
                data_topic_latest_entry_id: 0,
                metadata_topic_latest_entry_id: 0,
            }],
            ..Default::default()
        }
    }
}
