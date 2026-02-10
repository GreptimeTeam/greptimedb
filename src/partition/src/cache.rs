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

use std::sync::Arc;

use common_base::hash::partition_expr_version;
use common_error::ext::BoxedError;
use common_meta::cache::{CacheContainer, Initializer, TableRoute, TableRouteCacheRef};
use common_meta::instruction::CacheIdent;
use common_meta::rpc::router::RegionRoute;
use moka::future::Cache;
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};

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
    cache: Cache<TableId, CachedPartitionInfo>,
    table_route_cache: TableRouteCacheRef,
) -> PartitionInfoCache {
    CacheContainer::new(
        name,
        cache,
        Box::new(|cache, ident| {
            Box::pin(async move {
                if let CacheIdent::TableId(table_id) = ident {
                    cache.invalidate(table_id).await
                }
                Ok(())
            })
        }),
        init_factory(table_route_cache),
        |ident| matches!(ident, CacheIdent::TableId(_)),
    )
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
}
