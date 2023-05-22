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

use api::v1::meta::{TableName, TableRouteValue};
use async_trait::async_trait;
use catalog::helper::TableGlobalKey;
use common_meta::peer::Peer;
use common_meta::router::TableRoute;
use common_meta::RegionIdent;
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use super::failover_end::RegionFailoverEnd;
use super::{RegionFailoverContext, State};
use crate::error::{
    CorruptedTableRouteSnafu, Result, RetryLaterSnafu, TableNotFoundSnafu,
    TableRouteConversionSnafu,
};
use crate::keys::TableRouteKey;
use crate::table_routes;

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct UpdateRegionMetadata {
    candidate: Peer,
}

impl UpdateRegionMetadata {
    pub(super) fn new(candidate: Peer) -> Self {
        Self { candidate }
    }

    async fn update_meta(
        &self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<()> {
        self.update_table_global_value(ctx, failed_region).await?;
        self.update_table_route(ctx, failed_region).await?;
        Ok(())
    }

    async fn update_table_global_value(
        &self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<()> {
        let key = TableGlobalKey {
            catalog_name: failed_region.catalog.clone(),
            schema_name: failed_region.schema.clone(),
            table_name: failed_region.table.clone(),
        };
        let mut value = table_routes::get_table_global_value(&ctx.selector_ctx.kv_store, &key)
            .await?
            .with_context(|| TableNotFoundSnafu {
                name: common_catalog::format_full_table_name(
                    &key.catalog_name,
                    &key.schema_name,
                    &key.table_name,
                ),
            })?;

        if let Some(mut region_numbers) = value.regions_id_map.remove(&failed_region.datanode_id) {
            region_numbers.retain(|x| *x != failed_region.region_number);

            if !region_numbers.is_empty() {
                value
                    .regions_id_map
                    .insert(failed_region.datanode_id, region_numbers);
            }
        }

        let region_numbers = value
            .regions_id_map
            .entry(self.candidate.id)
            .or_insert_with(Vec::new);
        region_numbers.push(failed_region.region_number);

        table_routes::put_table_global_value(&ctx.selector_ctx.kv_store, &key, &value).await?;
        info!(
            "Region mappings in table global value (key = '{key}') are updated to {:?}. \
            Failed region {} was on Datanode {}.",
            value.regions_id_map, failed_region.region_number, failed_region.datanode_id,
        );
        Ok(())
    }

    async fn update_table_route(
        &self,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<()> {
        let table_name = TableName {
            catalog_name: failed_region.catalog.clone(),
            schema_name: failed_region.schema.clone(),
            table_name: failed_region.table.clone(),
        };
        let key = TableRouteKey::with_table_name(failed_region.table_id as _, &table_name);
        let value = table_routes::get_table_route_value(&ctx.selector_ctx.kv_store, &key).await?;

        let table_route = value
            .table_route
            .with_context(|| CorruptedTableRouteSnafu {
                key: key.key(),
                reason: "'table_route' is empty",
            })?;
        let mut table_route = TableRoute::try_from_raw(&value.peers, table_route)
            .context(TableRouteConversionSnafu)?;

        for region_route in table_route.region_routes.iter_mut() {
            if region_route.region.id == failed_region.region_number as u64 {
                region_route.leader_peer = Some(self.candidate.clone());
                break;
            }
        }

        pretty_log_table_route_change(&key, &table_route, failed_region);

        let (peers, table_route) = table_route
            .try_into_raw()
            .context(TableRouteConversionSnafu)?;

        let value = TableRouteValue {
            peers,
            table_route: Some(table_route),
        };
        table_routes::put_table_route_value(&ctx.selector_ctx.kv_store, &key, value).await?;
        Ok(())
    }
}

fn pretty_log_table_route_change(
    key: &TableRouteKey,
    table_route: &TableRoute,
    failed_region: &RegionIdent,
) {
    let region_routes = table_route
        .region_routes
        .iter()
        .map(|x| {
            format!(
                "{{region: {}, leader: {}, followers: [{}]}}",
                x.region.id,
                x.leader_peer
                    .as_ref()
                    .map(|p| p.id.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                x.follower_peers
                    .iter()
                    .map(|p| p.id.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        })
        .collect::<Vec<_>>();

    info!(
        "Updating region routes in table route value (key = '{}') to [{}]. \
        Failed region {} was on Datanode {}.",
        key.key(),
        region_routes.join(", "),
        failed_region.region_number,
        failed_region.datanode_id,
    );
}

#[async_trait]
#[typetag::serde]
impl State for UpdateRegionMetadata {
    async fn next(
        mut self: Box<Self>,
        ctx: &RegionFailoverContext,
        failed_region: &RegionIdent,
    ) -> Result<Box<dyn State>> {
        self.update_meta(ctx, failed_region).await.map_err(|e| {
            RetryLaterSnafu {
                reason: format!(
                    "Failed to update metadata for failed region: {}, error: {}",
                    failed_region, e
                ),
            }
            .build()
        })?;
        Ok(Box::new(RegionFailoverEnd))
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::TableRouteValue;
    use catalog::helper::TableGlobalValue;

    use super::super::tests::{TestingEnv, TestingEnvBuilder};
    use super::*;
    use crate::table_routes::tests::new_region_route;

    #[tokio::test]
    async fn test_update_table_global_value() {
        common_telemetry::init_default_ut_logging();

        async fn test(env: TestingEnv, candidate: u64) -> TableGlobalValue {
            let TestingEnv {
                context,
                failed_region,
                heartbeat_receivers: _,
            } = env;

            let key = TableGlobalKey {
                catalog_name: failed_region.catalog.clone(),
                schema_name: failed_region.schema.clone(),
                table_name: failed_region.table.clone(),
            };

            let original =
                table_routes::get_table_global_value(&context.selector_ctx.kv_store, &key)
                    .await
                    .unwrap()
                    .unwrap();

            let state = UpdateRegionMetadata::new(Peer::new(candidate, ""));
            state
                .update_table_global_value(&context, &failed_region)
                .await
                .unwrap();

            let updated =
                table_routes::get_table_global_value(&context.selector_ctx.kv_store, &key)
                    .await
                    .unwrap()
                    .unwrap();

            // verifies that other data stay untouched
            assert_eq!(original.node_id, updated.node_id);
            assert_eq!(original.table_info, updated.table_info);
            updated
        }

        // Region distribution:
        // Datanode => Regions
        // 1 => 1, 2
        // 2 => 3
        // 3 => 4

        // Testing failed region 1 moves to Datanode 2.
        let env = TestingEnvBuilder::new().with_failed_region(1).build().await;
        let updated = test(env, 2).await;

        let new_region_id_map = updated.regions_id_map;
        assert_eq!(new_region_id_map.len(), 3);
        assert_eq!(new_region_id_map.get(&1), Some(&vec![2]));
        assert_eq!(new_region_id_map.get(&2), Some(&vec![3, 1]));
        assert_eq!(new_region_id_map.get(&3), Some(&vec![4]));

        // Testing failed region 3 moves to Datanode 3.
        let env = TestingEnvBuilder::new().with_failed_region(3).build().await;
        let updated = test(env, 3).await;

        let new_region_id_map = updated.regions_id_map;
        assert_eq!(new_region_id_map.len(), 2);
        assert_eq!(new_region_id_map.get(&1), Some(&vec![1, 2]));
        assert_eq!(new_region_id_map.get(&3), Some(&vec![4, 3]));

        // Testing failed region 1 moves to a new Datanode, 4.
        let env = TestingEnvBuilder::new().with_failed_region(1).build().await;
        let updated = test(env, 4).await;

        let new_region_id_map = updated.regions_id_map;
        assert_eq!(new_region_id_map.len(), 4);
        assert_eq!(new_region_id_map.get(&1), Some(&vec![2]));
        assert_eq!(new_region_id_map.get(&2), Some(&vec![3]));
        assert_eq!(new_region_id_map.get(&3), Some(&vec![4]));
        assert_eq!(new_region_id_map.get(&4), Some(&vec![1]));

        // Testing failed region 3 moves to a new Datanode, 4.
        let env = TestingEnvBuilder::new().with_failed_region(3).build().await;
        let updated = test(env, 4).await;

        let new_region_id_map = updated.regions_id_map;
        assert_eq!(new_region_id_map.len(), 3);
        assert_eq!(new_region_id_map.get(&1), Some(&vec![1, 2]));
        assert_eq!(new_region_id_map.get(&3), Some(&vec![4]));
        assert_eq!(new_region_id_map.get(&4), Some(&vec![3]));
    }

    #[tokio::test]
    async fn test_update_table_route() {
        common_telemetry::init_default_ut_logging();

        async fn test(env: TestingEnv, candidate: u64) -> TableRouteValue {
            let TestingEnv {
                context,
                failed_region,
                heartbeat_receivers: _,
            } = env;

            let state = UpdateRegionMetadata::new(Peer::new(candidate, ""));
            state
                .update_table_route(&context, &failed_region)
                .await
                .unwrap();

            let key = TableRouteKey {
                table_id: failed_region.table_id as u64,
                catalog_name: &failed_region.catalog,
                schema_name: &failed_region.schema,
                table_name: &failed_region.table,
            };
            table_routes::get_table_route_value(&context.selector_ctx.kv_store, &key)
                .await
                .unwrap()
        }

        // Original region routes:
        // region number => leader node
        // 1 => 1
        // 2 => 1
        // 3 => 2
        // 4 => 3

        // Testing failed region 1 moves to Datanode 2.
        let env = TestingEnvBuilder::new().with_failed_region(1).build().await;
        let updated = test(env, 2).await;
        let actual = &updated.table_route.as_ref().unwrap().region_routes;

        // Expected region routes:
        // region number => leader node
        // 1 => 2
        // 2 => 1
        // 3 => 2
        // 4 => 3
        let peers = &updated.peers;
        assert_eq!(peers.len(), 3);
        let expected = vec![
            new_region_route(1, peers, 2),
            new_region_route(2, peers, 1),
            new_region_route(3, peers, 2),
            new_region_route(4, peers, 3),
        ];
        assert_eq!(actual, &expected);

        // Testing failed region 3 moves to Datanode 3.
        let env = TestingEnvBuilder::new().with_failed_region(3).build().await;
        let updated = test(env, 3).await;
        let actual = &updated.table_route.as_ref().unwrap().region_routes;

        // Expected region routes:
        // region number => leader node
        // 1 => 1
        // 2 => 1
        // 3 => 3
        // 4 => 3
        let peers = &updated.peers;
        assert_eq!(peers.len(), 2);
        let expected = vec![
            new_region_route(1, peers, 1),
            new_region_route(2, peers, 1),
            new_region_route(3, peers, 3),
            new_region_route(4, peers, 3),
        ];
        assert_eq!(actual, &expected);

        // Testing failed region 1 moves to a new Datanode, 4.
        let env = TestingEnvBuilder::new().with_failed_region(1).build().await;
        let updated = test(env, 4).await;
        let actual = &updated.table_route.as_ref().unwrap().region_routes;

        // Expected region routes:
        // region number => leader node
        // 1 => 4
        // 2 => 1
        // 3 => 2
        // 4 => 3
        let peers = &updated.peers;
        assert_eq!(peers.len(), 4);
        let expected = vec![
            new_region_route(1, peers, 4),
            new_region_route(2, peers, 1),
            new_region_route(3, peers, 2),
            new_region_route(4, peers, 3),
        ];
        assert_eq!(actual, &expected);

        // Testing failed region 3 moves to a new Datanode, 4.
        let env = TestingEnvBuilder::new().with_failed_region(3).build().await;
        let updated = test(env, 4).await;
        let actual = &updated.table_route.as_ref().unwrap().region_routes;

        // Expected region routes:
        // region number => leader node
        // 1 => 1
        // 2 => 1
        // 3 => 4
        // 4 => 3
        let peers = &updated.peers;
        assert_eq!(peers.len(), 3);
        let expected = vec![
            new_region_route(1, peers, 1),
            new_region_route(2, peers, 1),
            new_region_route(3, peers, 4),
            new_region_route(4, peers, 3),
        ];
        assert_eq!(actual, &expected);
    }
}
