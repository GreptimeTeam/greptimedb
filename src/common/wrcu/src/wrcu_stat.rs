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
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use api::v1::InsertRequest as GrpcInsertRequest;
use dashmap::DashMap;

use crate::WcuCalc;

#[derive(Default, Clone)]
pub struct WrcuStat {
    wcu_counter: Arc<Counter<StatKey>>,
    rcu_counter: Arc<Counter<StatKey>>,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct StatKey {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub region_number: u32,
}

/// The statistics for wcus and rcus over a period of time in the Datanode
pub struct Statistics {
    pub wcus: u64,
    pub rcus: u64,
    pub region_wcu_map: HashMap<StatKey, u64>,
    pub region_rcu_map: HashMap<StatKey, u64>,
}

impl WrcuStat {
    pub fn add_grpc_insert_req(
        &self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        request: &GrpcInsertRequest,
    ) {
        let wcus = request.wcu_num();
        let table = request.table_name.clone();
        let region_number = request.region_number;
        let region = StatKey {
            catalog: catalog.into(),
            schema: schema.into(),
            table,
            region_number,
        };
        self.wcu_counter.add_count(region, wcus as u64);
    }

    pub fn add_wcu(
        &self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        region_number: u32,
        wcus: u64,
    ) {
        let region = StatKey {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
            region_number,
        };
        self.wcu_counter.add_count(region, wcus);
    }

    pub fn add_rcu(
        &self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        region_number: u32,
        wcus: u64,
    ) {
        let region = StatKey {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
            region_number,
        };
        self.rcu_counter.add_count(region, wcus);
    }

    // Get the statistics of wcu and rcu data in the latest cycle
    pub fn statistics_and_clear(&self) -> Statistics {
        let wcu_r = self.wcu_counter.as_ref();
        let rcu_r = self.rcu_counter.as_ref();

        let wcu_snapshot = wcu_r.snapshot();
        wcu_r.clear();

        let rcu_snapshot = rcu_r.snapshot();
        rcu_r.clear();

        Statistics::from(wcu_snapshot, rcu_snapshot)
    }
}

impl Statistics {
    fn from(wcu_snapshot: Snapshot<StatKey>, rcu_snapshot: Snapshot<StatKey>) -> Self {
        Statistics {
            wcus: wcu_snapshot.total,
            rcus: rcu_snapshot.total,
            region_wcu_map: wcu_snapshot.count_map,
            region_rcu_map: rcu_snapshot.count_map,
        }
    }
}

struct Counter<K> {
    total: AtomicU64,
    count_map: DashMap<K, u64>,
}

struct Snapshot<K> {
    total: u64,
    count_map: HashMap<K, u64>,
}

impl<K> Default for Counter<K>
where
    K: PartialEq + Eq + Hash,
{
    fn default() -> Self {
        Self {
            total: AtomicU64::default(),
            count_map: DashMap::default(),
        }
    }
}

impl<K> Counter<K>
where
    K: PartialEq + Eq + Hash + Clone,
{
    fn add_count(&self, key: K, count: u64) {
        let mut val = self.count_map.entry(key).or_insert(0);
        (*val) += count;

        self.total.fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Snapshot<K> {
        let count_map = self
            .count_map
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();

        let total = self.total.load(Ordering::Relaxed);

        Snapshot { total, count_map }
    }

    fn clear(&self) {
        self.total.store(0, Ordering::Relaxed);
        self.count_map.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{Counter, Snapshot, StatKey};
    use crate::WrcuStat;

    #[test]
    fn test_wrcu_stat() {
        let wrcu_stat = WrcuStat::default();

        let joins: Vec<_> = (0..4)
            .map(|_| {
                let wrcu_stat_clone = wrcu_stat.clone();

                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        wrcu_stat_clone.add_wcu("catalog", "schema", "table", 0, 10);
                        wrcu_stat_clone.add_rcu("catalog", "schema", "table", 0, 5);
                        wrcu_stat_clone.add_wcu("catalog", "schema", "table", 1, 20);
                        wrcu_stat_clone.add_rcu("catalog", "schema", "table", 1, 10);
                    }
                })
            })
            .collect();

        for join in joins {
            join.join().unwrap();
        }

        let stats = wrcu_stat.statistics_and_clear();

        assert_eq!(1000 * 4 * (10 + 20), stats.wcus);
        assert_eq!(1000 * 4 * (5 + 10), stats.rcus);

        let stat_key = &StatKey {
            catalog: "catalog".to_string(),
            schema: "schema".to_string(),
            table: "table".to_string(),
            region_number: 0,
        };
        assert_eq!(1000 * 4 * 10, *stats.region_wcu_map.get(stat_key).unwrap());
        assert_eq!(1000 * 4 * 5, *stats.region_rcu_map.get(stat_key).unwrap());

        let stat_key = &StatKey {
            catalog: "catalog".to_string(),
            schema: "schema".to_string(),
            table: "table".to_string(),
            region_number: 1,
        };
        assert_eq!(1000 * 4 * 20, *stats.region_wcu_map.get(stat_key).unwrap());
        assert_eq!(1000 * 4 * 10, *stats.region_rcu_map.get(stat_key).unwrap());

        let stats = wrcu_stat.statistics_and_clear();

        assert_eq!(0, stats.wcus);
        assert_eq!(0, stats.rcus);
        assert!(stats.region_wcu_map.is_empty());
        assert!(stats.region_rcu_map.is_empty());
    }

    #[test]
    fn test_counter() {
        let counter: Arc<Counter<String>> = Arc::new(Counter::default());
        let key = "key";

        let joins: Vec<_> = (0..4)
            .map(|_| {
                let counter_clone = counter.clone();

                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        counter_clone.add_count(key.to_string(), 1);
                    }
                })
            })
            .collect();

        for join in joins {
            join.join().unwrap();
        }

        let Snapshot { total, count_map } = counter.snapshot();

        assert_eq!(4000, total);
        assert_eq!(4000, count_map.get(key).unwrap().clone());

        counter.clear();

        let Snapshot { total, count_map } = counter.snapshot();

        assert_eq!(0, total);
        assert!(count_map.is_empty());
    }
}
