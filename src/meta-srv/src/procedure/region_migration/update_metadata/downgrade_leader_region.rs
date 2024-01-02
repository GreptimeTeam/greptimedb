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

use common_meta::rpc::router::RegionStatus;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::Context;

impl UpdateMetadata {
    /// Downgrades the leader region.
    ///
    /// Abort(non-retry):
    /// - TableRoute is not found.
    ///
    /// Retry:
    /// - Failed to update [TableRouteValue](common_meta::key::table_region::TableRegionValue).
    /// - Failed to retrieve the metadata of table.
    ///
    /// About the failure of updating the [TableRouteValue](common_meta::key::table_region::TableRegionValue):
    ///
    /// - There may be another [RegionMigrationProcedure](crate::procedure::region_migration::RegionMigrationProcedure)
    /// that is executed concurrently for **other region**.
    /// It will only update **other region** info. Therefore, It's safe to retry after failure.
    ///
    /// - There is no other DDL procedure executed concurrently for the current table.
    pub async fn downgrade_leader_region(&self, ctx: &mut Context) -> Result<()> {
        let table_metadata_manager = ctx.table_metadata_manager.clone();
        let from_peer_id = ctx.persistent_ctx.from_peer.id;
        let region_id = ctx.region_id();
        let table_id = region_id.table_id();
        let current_table_route_value = ctx.get_table_route_value().await?;

        // TODO(weny): ensures the leader region peer is the `from_peer`.
        if let Err(err) = table_metadata_manager
            .update_leader_region_status(table_id, current_table_route_value, |route| {
                if route.region.id == region_id
                    && route
                        .leader_peer
                        .as_ref()
                        .is_some_and(|leader_peer| leader_peer.id == from_peer_id)
                {
                    Some(Some(RegionStatus::Downgraded))
                } else {
                    None
                }
            })
            .await
            .context(error::TableMetadataManagerSnafu)
        {
            debug_assert!(ctx.remove_table_route_value());
            return error::RetryLaterSnafu {
                reason: format!("Failed to update the table route during the downgrading leader region, error: {err}")
            }.fail();
        }

        debug_assert!(ctx.remove_table_route_value());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute, RegionStatus};
    use store_api::storage::RegionId;

    use crate::error::Error;
    use crate::procedure::region_migration::downgrade_leader_region::DowngradeLeaderRegion;
    use crate::procedure::region_migration::test_util::{self, TestingEnv};
    use crate::procedure::region_migration::update_metadata::UpdateMetadata;
    use crate::procedure::region_migration::{ContextFactory, PersistentContext, State};

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    #[test]
    fn test_state_serialization() {
        let state = UpdateMetadata::Downgrade;
        let expected = r#"{"UpdateMetadata":"Downgrade"}"#;
        assert_eq!(expected, serde_json::to_string(&state).unwrap());
    }

    #[tokio::test]
    async fn test_table_route_is_not_found_error() {
        let state = UpdateMetadata::Downgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let err = state.downgrade_leader_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::TableRouteNotFound { .. });

        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_failed_to_update_table_route_error() {
        let state = UpdateMetadata::Downgrade;
        let persistent_context = new_persistent_context();
        let from_peer = persistent_context.from_peer.clone();

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_id = ctx.region_id().table_id();

        let table_info = new_test_table_info(1024, vec![1, 2]).into();
        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 1)),
                leader_peer: Some(from_peer.clone()),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 2)),
                leader_peer: Some(Peer::empty(4)),
                ..Default::default()
            },
        ];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_metadata_manager = env.table_metadata_manager();
        let original_table_route = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        // modifies the table route.
        table_metadata_manager
            .update_leader_region_status(table_id, &original_table_route, |route| {
                if route.region.id == RegionId::new(1024, 2) {
                    Some(Some(RegionStatus::Downgraded))
                } else {
                    None
                }
            })
            .await
            .unwrap();

        // sets the old table route.
        ctx.volatile_ctx.table_route = Some(original_table_route);

        let err = state.downgrade_leader_region(&mut ctx).await.unwrap_err();

        assert!(ctx.volatile_ctx.table_route.is_none());

        assert_matches!(err, Error::RetryLater { .. });

        assert!(err.is_retryable());
        assert!(err.to_string().contains("Failed to update the table route"));
    }

    #[tokio::test]
    async fn test_only_downgrade_from_peer() {
        let mut state = Box::new(UpdateMetadata::Downgrade);
        let persistent_context = new_persistent_context();

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_id = ctx.region_id().table_id();

        let table_info = new_test_table_info(1024, vec![1, 2]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(1024)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_metadata_manager = env.table_metadata_manager();

        let (next, _) = state.next(&mut ctx).await.unwrap();

        let _ = next
            .as_any()
            .downcast_ref::<DowngradeLeaderRegion>()
            .unwrap();

        let latest_table_route = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        // It should remain unchanged.
        assert_eq!(latest_table_route.version().unwrap(), 0);
        assert!(!latest_table_route.region_routes().unwrap()[0].is_leader_downgraded());
        assert!(ctx.volatile_ctx.table_route.is_none());
    }

    #[tokio::test]
    async fn test_next_downgrade_leader_region_state() {
        let mut state = Box::new(UpdateMetadata::Downgrade);
        let persistent_context = new_persistent_context();
        let from_peer = persistent_context.from_peer.clone();

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_id = ctx.region_id().table_id();

        let table_info = new_test_table_info(1024, vec![1, 2]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(from_peer.clone()),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_metadata_manager = env.table_metadata_manager();

        let (next, _) = state.next(&mut ctx).await.unwrap();

        let _ = next
            .as_any()
            .downcast_ref::<DowngradeLeaderRegion>()
            .unwrap();

        let latest_table_route = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        assert!(latest_table_route.region_routes().unwrap()[0].is_leader_downgraded());
        assert!(ctx.volatile_ctx.table_route.is_none());
    }
}
