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

use common_base::hash::partition_rule_version;
use common_error::ext::BoxedError;
use common_meta::cache::{CacheContainer, Initializer, TableRoute, TableRouteCacheRef};
use common_meta::instruction::CacheIdent;
use common_meta::rpc::router::RegionRoute;
use moka::future::Cache;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};

use crate::expr::PartitionExpr;

#[derive(Debug, Clone)]
pub struct PartitionInfoWithVersion {
    pub id: RegionId,
    pub partition_expr: Option<PartitionExpr>,
    pub partition_rule_version: u64,
}

#[derive(Clone)]
pub struct CachedPartitionInfo {
    pub table_route_version: u64,
    pub partitions: Vec<PartitionInfoWithVersion>,
}

pub type PartitionInfoCache = CacheContainer<TableId, Arc<CachedPartitionInfo>, CacheIdent>;

pub type PartitionInfoCacheRef = Arc<PartitionInfoCache>;

pub fn create_partitions_with_version_from_region_routes(
    table_id: TableId,
    region_routes: &[RegionRoute],
) -> common_meta::error::Result<Vec<PartitionInfoWithVersion>> {
    let mut partitions = Vec::with_capacity(region_routes.len());
    for r in region_routes {
        let expr_json = r.region.partition_expr();
        let partition_rule_version = if expr_json.is_empty() {
            0
        } else {
            partition_rule_version(Some(expr_json.as_str()))
        };
        let partition_expr = PartitionExpr::from_json_str(expr_json.as_str())
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)?;
        let id = RegionId::new(table_id, r.region.id.region_number());
        partitions.push(PartitionInfoWithVersion {
            id,
            partition_expr,
            partition_rule_version,
        });
    }

    Ok(partitions)
}

fn init_factory(
    table_route_cache: TableRouteCacheRef,
) -> Initializer<TableId, Arc<CachedPartitionInfo>> {
    Arc::new(move |table_id: &TableId| {
        let table_route_cache = table_route_cache.clone();
        Box::pin(async move {
            let Some(table_route) = table_route_cache.get(*table_id).await? else {
                return Ok(None);
            };

            let physical_route = match table_route.as_ref() {
                TableRoute::Physical(physical) => physical.clone(),
                TableRoute::Logical(logical) => {
                    let physical_table_id = logical.physical_table_id();
                    let Some(physical_table_route) =
                        table_route_cache.get(physical_table_id).await?
                    else {
                        return Ok(None);
                    };
                    physical_table_route
                        .as_physical_table_route_ref()
                        .context(common_meta::error::UnexpectedLogicalRouteTableSnafu {
                            err_msg: format!(
                                "Expected the physical table route, but got logical table route, table: {table_id}"
                            ),
                        })?
                        .clone()
                }
            };

            let partitions = create_partitions_with_version_from_region_routes(
                *table_id,
                &physical_route.region_routes,
            )?;
            Ok(Some(Arc::new(CachedPartitionInfo {
                table_route_version: physical_route.version(),
                partitions,
            })))
        })
    })
}

pub fn new_partition_info_cache(
    name: String,
    cache: Cache<TableId, Arc<CachedPartitionInfo>>,
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
        init_factory(table_route_cache.clone()),
        |ident| matches!(ident, CacheIdent::TableId(_)),
    )
}
