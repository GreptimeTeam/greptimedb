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

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Weak};

use common_cache::SyncCache;
use datatypes::arrow::array::RecordBatch;
use datatypes::value::Value;

use common_base::hash::partition_expr_version;
use common_error::ext::BoxedError;
use common_meta::cache::{CacheContainer, Initializer, TableRoute, TableRouteCacheRef};
use common_meta::instruction::CacheIdent;
use common_meta::rpc::router::RegionRoute;
use moka::future::Cache;
use snafu::ResultExt;
use store_api::storage::{RegionId, RegionNumber, TableId};

use crate::multi_dim::MultiDimPartitionRule;
use crate::partition::{PartitionRule, PartitionRuleRef, RegionMask};

use crate::expr::PartitionExpr;
use crate::manager::PartitionInfoWithVersion;

#[derive(Debug, Clone)]
pub struct PhysicalPartitionInfo {
    pub partitions: Vec<PartitionInfoWithVersion>,
}

#[derive(Debug, Clone)]
pub enum CachedPartitionInfo {
    Physical(Arc<PhysicalPartitionInfo>),
    Logical(TableId),
}

impl CachedPartitionInfo {
    /// Returns the physical partitions if the cached partition info is physical.
    pub fn into_physical(self) -> Option<Arc<PhysicalPartitionInfo>> {
        match self {
            CachedPartitionInfo::Physical(partitions) => Some(partitions),
            CachedPartitionInfo::Logical(_) => None,
        }
    }
}

pub type PartitionInfoCache = CacheContainer<TableId, CachedPartitionInfo, CacheIdent>;

pub type PartitionInfoCacheRef = Arc<PartitionInfoCache>;

pub fn create_partitions_with_version_from_region_routes(
    table_id: TableId,
    region_routes: &[RegionRoute],
) -> common_meta::error::Result<Vec<PartitionInfoWithVersion>> {
    let mut partitions = Vec::with_capacity(region_routes.len());
    for r in region_routes {
        // Ignore regions marked as reject-all-writes; they should not participate
        // in writable partition-cache construction.
        if r.is_ignore_all_writes() {
            continue;
        }

        let expr_json = r.region.partition_expr();
        let partition_expr_version = if expr_json.is_empty() {
            None
        } else {
            Some(partition_expr_version(Some(expr_json.as_str())))
        };
        let partition_expr = PartitionExpr::from_json_str(expr_json.as_str())
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)?;
        let id = RegionId::new(table_id, r.region.id.region_number());
        partitions.push(PartitionInfoWithVersion {
            id,
            partition_expr,
            partition_expr_version,
        });
    }

    Ok(partitions)
}

fn init_factory(
    table_route_cache: TableRouteCacheRef,
) -> Initializer<TableId, CachedPartitionInfo> {
    Arc::new(move |table_id: &TableId| {
        let table_route_cache = table_route_cache.clone();
        Box::pin(async move {
            let Some(table_route) = table_route_cache.get(*table_id).await? else {
                return Ok(None);
            };

            let cached = match table_route.as_ref() {
                TableRoute::Physical(physical) => {
                    let partitions = create_partitions_with_version_from_region_routes(
                        *table_id,
                        &physical.region_routes,
                    )?;

                    CachedPartitionInfo::Physical(Arc::new(PhysicalPartitionInfo { partitions }))
                }
                TableRoute::Logical(logical) => {
                    let table_id = logical.physical_table_id();
                    CachedPartitionInfo::Logical(table_id)
                }
            };

            Ok(Some(cached))
        })
    })
}

pub fn new_partition_info_cache(
    name: String,
    cache: Cache<(usize, TableId), CachedPartitionInfo>,
    table_route_cache: TableRouteCacheRef,
) -> PartitionInfoCache {
    CacheContainer::new_coordinated(
        name,
        cache,
        init_factory(table_route_cache),
        CacheIdent::table_id_key,
    )
}

/// Shared, bounded routing results. Snapshot identity fences late insertions after invalidation.
pub(crate) struct PartitionRoutingCache {
    cache: SyncCache<RoutingKey, RoutingResult>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct RoutingKey {
    table_id: TableId,
    values: Vec<Value>,
}

#[derive(Clone)]
struct RoutingResult {
    snapshot: Weak<PhysicalPartitionInfo>,
    region: RegionNumber,
}

impl PartitionRoutingCache {
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            cache: SyncCache::new(
                "partition_routing",
                capacity,
                |key: &RoutingKey, _: &RoutingResult| {
                    let bytes = key.values.iter().fold(
                        std::mem::size_of::<RoutingKey>() + std::mem::size_of::<RoutingResult>(),
                        |sum, value| {
                            sum.saturating_add(std::mem::size_of::<Value>())
                                .saturating_add(value.as_value_ref().data_size())
                        },
                    );
                    u32::try_from(bytes).unwrap_or(u32::MAX)
                },
                |_, _, _| {},
            ),
        }
    }

    pub(crate) fn rule(
        &self,
        table_id: TableId,
        snapshot: Arc<PhysicalPartitionInfo>,
    ) -> crate::error::Result<PartitionRuleRef> {
        let mut names = BTreeSet::new();
        let mut regions = Vec::new();
        let mut expressions = Vec::new();
        for part in &snapshot.partitions {
            if let Some(expr) = &part.partition_expr {
                let mut columns = std::collections::HashSet::new();
                expr.collect_column_names(&mut columns);
                names.extend(columns);
                regions.push(part.id.region_number());
                expressions.push(expr.clone());
            }
        }
        regions.extend(
            snapshot
                .partitions
                .iter()
                .filter(|part| part.partition_expr.is_none())
                .map(|part| part.id.region_number()),
        );
        let rule = MultiDimPartitionRule::try_new(
            names.into_iter().collect(),
            regions,
            expressions,
            false,
        )?;
        Ok(Arc::new(CachedPhysicalPartitionRule {
            table_id,
            snapshot,
            rule,
            cache: self.cache.clone(),
        }))
    }
}

struct CachedPhysicalPartitionRule {
    table_id: TableId,
    snapshot: Arc<PhysicalPartitionInfo>,
    rule: MultiDimPartitionRule,
    cache: SyncCache<RoutingKey, RoutingResult>,
}

impl PartitionRule for CachedPhysicalPartitionRule {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn partition_columns(&self) -> &[String] {
        self.rule.partition_columns()
    }

    fn find_region(&self, values: &[Value]) -> crate::error::Result<RegionNumber> {
        // Large variable-width keys should not displace many useful small entries.
        if values.iter().fold(0usize, |size, value| {
            size.saturating_add(value.as_value_ref().data_size())
        }) > 8192
        {
            return self.rule.find_region(values);
        }
        // Columns are deterministically ordered when building the rule from this snapshot.
        let key = RoutingKey {
            table_id: self.table_id,
            values: values.to_vec(),
        };
        if let Some(cached) = self.cache.get(&key)
            && cached.snapshot.ptr_eq(&Arc::downgrade(&self.snapshot))
        {
            return Ok(cached.region);
        }
        let region = self.rule.find_region(values)?;
        self.cache.insert(
            key,
            RoutingResult {
                snapshot: Arc::downgrade(&self.snapshot),
                region,
            },
        );
        Ok(region)
    }

    fn split_record_batch(
        &self,
        batch: &RecordBatch,
    ) -> crate::error::Result<HashMap<RegionNumber, RegionMask>> {
        self.rule.split_record_batch(batch)
    }
}

#[cfg(test)]
mod tests {
    use common_base::hash::partition_expr_version;
    use common_meta::rpc::router::{Region, RegionRoute, WriteRoutePolicy};
    use store_api::storage::RegionId;

    use super::create_partitions_with_version_from_region_routes;

    #[test]
    fn test_create_partitions_with_version_excludes_reject_all_writes() {
        let table_id = 1024;
        let expr_json =
            r#"{"Expr":{"lhs":{"Column":"a"},"op":"GtEq","rhs":{"Value":{"UInt32":10}}}}"#;
        let region_routes = vec![
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 1),
                    partition_expr: expr_json.to_string(),
                    ..Default::default()
                },
                leader_peer: None,
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: Some(WriteRoutePolicy::IgnoreAllWrites),
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 2),
                    partition_expr: expr_json.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];

        let partitions =
            create_partitions_with_version_from_region_routes(table_id, &region_routes).unwrap();
        assert_eq!(1, partitions.len());
        assert_eq!(RegionId::new(table_id, 2), partitions[0].id);
        assert_eq!(
            Some(partition_expr_version(Some(expr_json))),
            partitions[0].partition_expr_version
        );
    }
    fn routing_snapshot(region: u32) -> std::sync::Arc<super::PhysicalPartitionInfo> {
        use crate::expr::col;
        use datatypes::value::Value;
        std::sync::Arc::new(super::PhysicalPartitionInfo {
            partitions: vec![crate::manager::PartitionInfoWithVersion {
                id: RegionId::new(42, region),
                partition_expr: Some(
                    col("a")
                        .eq(Value::from("x"))
                        .and(col("b").gt_eq(Value::Int64(10))),
                ),
                partition_expr_version: None,
            }],
        })
    }

    #[test]
    fn test_routing_cache_snapshot_fences_late_publication() {
        use datatypes::value::Value;
        let cache = super::PartitionRoutingCache::new(4096);
        let old_snapshot = routing_snapshot(1);
        let old = cache.rule(42, old_snapshot.clone()).unwrap();
        let new_snapshot = routing_snapshot(2);
        let new = cache.rule(42, new_snapshot.clone()).unwrap();
        let values = [Value::from("x"), Value::Int64(10)];
        assert_eq!(old.partition_columns(), &["a", "b"]);
        assert_eq!(old.find_region(&values).unwrap(), 1);
        assert_eq!(new.find_region(&values).unwrap(), 2);
        // An in-flight request can publish its old answer after replacement.
        assert_eq!(old.find_region(&values).unwrap(), 1);
        assert_eq!(new.find_region(&values).unwrap(), 2);
        // Different physical tables cannot share a cached answer.
        let other = cache.rule(43, routing_snapshot(3)).unwrap();
        assert_eq!(other.find_region(&values).unwrap(), 3);
        assert_eq!(
            new.find_region(&[Value::from("y"), Value::Int64(10)])
                .unwrap(),
            0
        );
        assert!(new.find_region(&[Value::from("x")]).is_err());
        drop(old);
        assert_eq!(std::sync::Arc::strong_count(&old_snapshot), 1);
    }

    #[test]
    fn test_routing_cache_disabled_and_large_keys() {
        use datatypes::value::Value;
        let cache = super::PartitionRoutingCache::new(0);
        let rule = cache.rule(42, routing_snapshot(1)).unwrap();
        assert_eq!(
            rule.find_region(&[Value::from("x"), Value::Int64(11)])
                .unwrap(),
            1
        );
        cache.cache.run_pending_tasks();
        assert!(
            cache
                .cache
                .get(&super::RoutingKey {
                    table_id: 42,
                    values: vec![Value::from("x"), Value::Int64(11)]
                })
                .is_none()
        );
        assert_eq!(
            rule.find_region(&[Value::from("x".repeat(9000)), Value::Int64(11)])
                .unwrap(),
            0
        );
    }

    /// Identical rule and keys; compare interpretation with warm, shared routing results.
    #[test]
    #[ignore = "manual optimized routing benchmark"]
    #[allow(clippy::print_stdout)]
    fn bench_shared_partition_routing() {
        use crate::expr::col;
        use datatypes::value::Value;
        for partitions in [19u32, 64, 128] {
            let snapshot = std::sync::Arc::new(super::PhysicalPartitionInfo {
                partitions: (0..partitions)
                    .map(|i| crate::manager::PartitionInfoWithVersion {
                        id: RegionId::new(42, i + 1),
                        partition_expr: Some(
                            col("key")
                                .gt_eq(Value::UInt32(i * 100))
                                .and(col("key").lt(Value::UInt32((i + 1) * 100))),
                        ),
                        partition_expr_version: None,
                    })
                    .collect(),
            });
            let cache = super::PartitionRoutingCache::new(64 * 1024 * 1024);
            let rule = cache.rule(42, snapshot).unwrap();
            let cached = rule
                .as_any()
                .downcast_ref::<super::CachedPhysicalPartitionRule>()
                .unwrap();
            let keys: Vec<_> = (0..partitions * 100).map(|i| [Value::UInt32(i)]).collect();
            for key in &keys {
                rule.find_region(key).unwrap();
            }
            for round in 0..3 {
                for use_cache in [false, true] {
                    let start = std::time::Instant::now();
                    let mut checksum = 0u64;
                    for n in 0..200_000usize {
                        let key = std::hint::black_box(&keys[n % keys.len()]);
                        let result = if use_cache {
                            rule.find_region(key)
                        } else {
                            super::PartitionRule::find_region(&cached.rule, key)
                        };
                        checksum += result.unwrap() as u64;
                    }
                    println!(
                        "routing partitions={partitions} round={round} cached={use_cache} elapsed_us={} checksum={checksum}",
                        start.elapsed().as_micros()
                    );
                }
            }
        }
    }
}
