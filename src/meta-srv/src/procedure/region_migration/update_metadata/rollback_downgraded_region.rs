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

use snafu::ResultExt;

use crate::error::{self, Result};
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::Context;

impl UpdateMetadata {
    /// Rollbacks the downgraded leader region if the candidate region is unreachable.
    ///
    /// Abort(non-retry):
    /// - TableRoute is not found.
    ///
    /// Retry:
    /// - Failed to update [TableRouteValue](common_meta::key::table_region::TableRegionValue).
    /// - Failed to retrieve the metadata of table.
    pub async fn rollback_downgraded_region(&self, ctx: &mut Context) -> Result<()> {
        let table_metadata_manager = ctx.table_metadata_manager.clone();
        let region_id = ctx.region_id();
        let table_id = region_id.table_id();
        let current_table_route_value = ctx.get_table_route_value().await?;

        if let Err(err) = table_metadata_manager
            .update_leader_region_status(table_id, current_table_route_value, |route| {
                if route.region.id == region_id {
                    Some(None)
                } else {
                    None
                }
            })
            .await
            .context(error::TableMetadataManagerSnafu)
        {
            debug_assert!(ctx.remove_table_route_value());
            return error::RetryLaterSnafu {
                reason: format!("Failed to update the table route during the rollback downgraded leader region, error: {err}")
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
    use crate::procedure::region_migration::migration_abort::RegionMigrationAbort;
    use crate::procedure::region_migration::test_util::{self, TestingEnv};
    use crate::procedure::region_migration::update_metadata::UpdateMetadata;
    use crate::procedure::region_migration::{ContextFactory, PersistentContext, State};

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    #[tokio::test]
    async fn test_table_route_is_not_found_error() {
        let state = UpdateMetadata::Rollback;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let err = state.downgrade_leader_region(&mut ctx).await.unwrap_err();

        assert_matches!(err, Error::TableRouteNotFound { .. });

        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_update_table_route_with_retry() {
        let state = UpdateMetadata::Rollback;
        let persistent_context = new_persistent_context();
        let from_peer = persistent_context.from_peer.clone();

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_id = ctx.region_id().table_id();

        let table_info = new_test_table_info(1024, vec![1, 2, 3]).into();
        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 1)),
                leader_peer: Some(from_peer.clone()),
                leader_status: Some(RegionStatus::Downgraded),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 2)),
                leader_peer: Some(Peer::empty(4)),
                leader_status: Some(RegionStatus::Downgraded),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 3)),
                leader_peer: Some(Peer::empty(5)),
                ..Default::default()
            },
        ];

        let expected_region_routes = {
            let mut region_routes = region_routes.clone();
            region_routes[0].leader_status = None;
            region_routes[1].leader_status = None;
            region_routes
        };

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_metadata_manager = env.table_metadata_manager();
        let old_table_route = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        // modifies the table route.
        table_metadata_manager
            .update_leader_region_status(table_id, &old_table_route, |route| {
                if route.region.id == RegionId::new(1024, 2) {
                    Some(None)
                } else {
                    None
                }
            })
            .await
            .unwrap();

        ctx.volatile_ctx.table_route = Some(old_table_route);

        let err = state
            .rollback_downgraded_region(&mut ctx)
            .await
            .unwrap_err();
        assert!(ctx.volatile_ctx.table_route.is_none());
        assert_matches!(err, Error::RetryLater { .. });
        assert!(err.is_retryable());
        assert!(err.to_string().contains("Failed to update the table route"));

        state.rollback_downgraded_region(&mut ctx).await.unwrap();

        let table_route = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(
            &expected_region_routes,
            table_route.region_routes().unwrap()
        );
    }

    #[tokio::test]
    async fn test_next_migration_end_state() {
        let mut state = Box::new(UpdateMetadata::Rollback);
        let persistent_context = new_persistent_context();
        let from_peer = persistent_context.from_peer.clone();

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_id = ctx.region_id().table_id();

        let table_info = new_test_table_info(1024, vec![1, 2, 3]).into();
        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 1)),
                leader_peer: Some(from_peer.clone()),
                leader_status: Some(RegionStatus::Downgraded),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 2)),
                leader_peer: Some(Peer::empty(4)),
                leader_status: Some(RegionStatus::Downgraded),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(1024, 3)),
                leader_peer: Some(Peer::empty(5)),
                ..Default::default()
            },
        ];

        let expected_region_routes = {
            let mut region_routes = region_routes.clone();
            region_routes[0].leader_status = None;
            region_routes
        };

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_metadata_manager = env.table_metadata_manager();

        let (next, _) = state.next(&mut ctx).await.unwrap();

        let _ = next
            .as_any()
            .downcast_ref::<RegionMigrationAbort>()
            .unwrap();

        assert!(ctx.volatile_ctx.table_route.is_none());

        let table_route = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(
            &expected_region_routes,
            table_route.region_routes().unwrap()
        );
    }
}
