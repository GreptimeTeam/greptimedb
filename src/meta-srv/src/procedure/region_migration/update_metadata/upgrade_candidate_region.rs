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

use common_meta::key::datanode_table::RegionInfo;
use common_meta::rpc::router::{region_distribution, RegionRoute};
use common_telemetry::{info, warn};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::Context;

impl UpdateMetadata {
    /// Returns new [Vec<RegionRoute>].
    async fn build_upgrade_candidate_region_metadata(
        &self,
        ctx: &mut Context,
    ) -> Result<Vec<RegionRoute>> {
        let region_id = ctx.region_id();
        let table_route_value = ctx.get_table_route_value().await?.clone();

        let mut region_routes = table_route_value
            .region_routes()
            .context(error::UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{self:?} is a non-physical TableRouteValue."),
            })?
            .clone();
        let region_route = region_routes
            .iter_mut()
            .find(|route| route.region.id == region_id)
            .context(error::RegionRouteNotFoundSnafu { region_id })?;

        // Removes downgraded status.
        region_route.set_leader_status(None);

        let candidate = &ctx.persistent_ctx.to_peer;
        let expected_old_leader = &ctx.persistent_ctx.from_peer;

        // Upgrades candidate to leader.
        ensure!(region_route
                .leader_peer
                .take_if(|old_leader| old_leader.id == expected_old_leader.id)
                .is_some(),
                error::UnexpectedSnafu{
                    violated: format!("Unexpected region leader: {:?} during the upgrading candidate metadata, expected: {:?}", region_route.leader_peer, expected_old_leader),
                }
            );

        region_route.leader_peer = Some(candidate.clone());
        info!(
            "Upgrading candidate region to leader region: {:?} for region: {}",
            candidate, region_id
        );

        // Removes the candidate region in followers.
        let removed = region_route
            .follower_peers
            .extract_if(|peer| peer.id == candidate.id)
            .collect::<Vec<_>>();

        if removed.len() > 1 {
            warn!(
                    "Removes duplicated regions: {removed:?} during the upgrading candidate metadata for region: {region_id}"
                );
        }

        Ok(region_routes)
    }

    /// Returns true if region metadata has been updated.
    async fn check_metadata_updated(&self, ctx: &mut Context) -> Result<bool> {
        let region_id = ctx.region_id();
        let table_route_value = ctx.get_table_route_value().await?.clone();

        let region_routes = table_route_value
            .region_routes()
            .context(error::UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{self:?} is a non-physical TableRouteValue."),
            })?
            .clone();
        let region_route = region_routes
            .into_iter()
            .find(|route| route.region.id == region_id)
            .context(error::RegionRouteNotFoundSnafu { region_id })?;

        let leader_peer = region_route
            .leader_peer
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: format!("The leader peer of region {region_id} is not found during the update metadata for upgrading"),
            })?;

        let candidate_peer_id = ctx.persistent_ctx.to_peer.id;

        if leader_peer.id == candidate_peer_id {
            ensure!(
                !region_route.is_leader_downgraded(),
                error::UnexpectedSnafu {
                    violated: format!("Unexpected intermediate state is found during the update metadata for upgrading region {region_id}"),
                }
            );

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Upgrades the candidate region.
    ///
    /// Abort(non-retry):
    /// - TableRoute or RegionRoute is not found.
    /// Typically, it's impossible, there is no other DDL procedure executed concurrently for the current table.
    ///
    /// Retry:
    /// - Failed to update [TableRouteValue](common_meta::key::table_region::TableRegionValue).
    /// - Failed to retrieve the metadata of table.
    pub async fn upgrade_candidate_region(&self, ctx: &mut Context) -> Result<()> {
        let region_id = ctx.region_id();
        let table_metadata_manager = ctx.table_metadata_manager.clone();

        if self.check_metadata_updated(ctx).await? {
            return Ok(());
        }

        let region_routes = self.build_upgrade_candidate_region_metadata(ctx).await?;
        let datanode_table_value = ctx.get_from_peer_datanode_table_value().await?;
        let RegionInfo {
            region_storage_path,
            region_options,
            region_wal_options,
            engine,
        } = datanode_table_value.region_info.clone();
        let table_route_value = ctx.get_table_route_value().await?;

        let region_distribution = region_distribution(&region_routes);
        info!(
            "Trying to update region routes to {:?} for table: {}",
            region_distribution,
            region_id.table_id()
        );
        if let Err(err) = table_metadata_manager
            .update_table_route(
                region_id.table_id(),
                RegionInfo {
                    engine: engine.to_string(),
                    region_storage_path: region_storage_path.to_string(),
                    region_options: region_options.clone(),
                    region_wal_options: region_wal_options.clone(),
                },
                table_route_value,
                region_routes,
                &region_options,
                &region_wal_options,
            )
            .await
            .context(error::TableMetadataManagerSnafu)
        {
            debug_assert!(ctx.remove_table_route_value());
            return error::RetryLaterSnafu {
                    reason: format!("Failed to update the table route during the upgrading candidate region, error: {err}")
                }.fail();
        };

        debug_assert!(ctx.remove_table_route_value());
        // Consumes the guard.
        ctx.volatile_ctx.opening_region_guard.take();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::region_keeper::MemoryRegionKeeper;
    use common_meta::rpc::router::{Region, RegionRoute, RegionStatus};
    use store_api::storage::RegionId;

    use crate::error::Error;
    use crate::procedure::region_migration::migration_end::RegionMigrationEnd;
    use crate::procedure::region_migration::test_util::{self, TestingEnv};
    use crate::procedure::region_migration::update_metadata::UpdateMetadata;
    use crate::procedure::region_migration::{ContextFactory, PersistentContext, State};

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    #[tokio::test]
    async fn test_table_route_is_not_found_error() {
        let state = UpdateMetadata::Upgrade;

        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let err = state
            .build_upgrade_candidate_region_metadata(&mut ctx)
            .await
            .unwrap_err();

        assert_matches!(err, Error::TableRouteNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_region_route_is_not_found() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024, vec![2]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 2)),
            leader_peer: Some(Peer::empty(4)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let err = state
            .build_upgrade_candidate_region_metadata(&mut ctx)
            .await
            .unwrap_err();

        assert_matches!(err, Error::RegionRouteNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_region_route_expected_leader() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(3)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let err = state
            .build_upgrade_candidate_region_metadata(&mut ctx)
            .await
            .unwrap_err();

        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("Unexpected region leader"));
    }

    #[tokio::test]
    async fn test_build_upgrade_candidate_region_metadata() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(1)),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_status: Some(RegionStatus::Downgraded),
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let new_region_routes = state
            .build_upgrade_candidate_region_metadata(&mut ctx)
            .await
            .unwrap();

        assert!(!new_region_routes[0].is_leader_downgraded());
        assert_eq!(new_region_routes[0].follower_peers, vec![Peer::empty(3)]);
        assert_eq!(new_region_routes[0].leader_peer.as_ref().unwrap().id, 2);
    }

    #[tokio::test]
    async fn test_failed_to_update_table_route_error() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let opening_keeper = MemoryRegionKeeper::default();

        let table_id = 1024;
        let table_info = new_test_table_info(table_id, vec![1]).into();
        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![Peer::empty(5), Peer::empty(3)],
                leader_status: Some(RegionStatus::Downgraded),
            },
            RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 2)),
                leader_peer: Some(Peer::empty(4)),
                leader_status: Some(RegionStatus::Downgraded),
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
                    // Removes the status.
                    Some(None)
                } else {
                    None
                }
            })
            .await
            .unwrap();

        // sets the old table route.
        ctx.volatile_ctx.table_route = Some(original_table_route);
        let guard = opening_keeper
            .register(2, RegionId::new(table_id, 1))
            .unwrap();
        ctx.volatile_ctx.opening_region_guard = Some(guard);

        let err = state.upgrade_candidate_region(&mut ctx).await.unwrap_err();

        assert!(ctx.volatile_ctx.table_route.is_none());
        assert!(ctx.volatile_ctx.opening_region_guard.is_some());
        assert_matches!(err, Error::RetryLater { .. });

        assert!(err.is_retryable());
        assert!(err.to_string().contains("Failed to update the table route"));
    }

    #[tokio::test]
    async fn test_check_metadata() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let leader_peer = persistent_context.from_peer.clone();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(leader_peer),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_status: None,
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let updated = state.check_metadata_updated(&mut ctx).await.unwrap();
        assert!(!updated);
    }

    #[tokio::test]
    async fn test_check_metadata_updated() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let candidate_peer = persistent_context.to_peer.clone();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(candidate_peer),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_status: None,
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let updated = state.check_metadata_updated(&mut ctx).await.unwrap();
        assert!(updated);
    }

    #[tokio::test]
    async fn test_check_metadata_intermediate_state() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let candidate_peer = persistent_context.to_peer.clone();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(candidate_peer),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_status: Some(RegionStatus::Downgraded),
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let err = state.check_metadata_updated(&mut ctx).await.unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert!(err.to_string().contains("intermediate state"));
    }

    #[tokio::test]
    async fn test_next_migration_end_state() {
        let mut state = Box::new(UpdateMetadata::Upgrade);
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let opening_keeper = MemoryRegionKeeper::default();

        let table_id = 1024;
        let table_info = new_test_table_info(table_id, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(table_id, 1)),
            leader_peer: Some(Peer::empty(1)),
            leader_status: Some(RegionStatus::Downgraded),
            ..Default::default()
        }];

        let guard = opening_keeper
            .register(2, RegionId::new(table_id, 1))
            .unwrap();
        ctx.volatile_ctx.opening_region_guard = Some(guard);

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_metadata_manager = env.table_metadata_manager();

        let (next, _) = state.next(&mut ctx).await.unwrap();

        let _ = next.as_any().downcast_ref::<RegionMigrationEnd>().unwrap();

        let table_route = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        let region_routes = table_route.region_routes().unwrap();

        assert!(ctx.volatile_ctx.table_route.is_none());
        assert!(ctx.volatile_ctx.opening_region_guard.is_none());
        assert_eq!(region_routes.len(), 1);
        assert!(!region_routes[0].is_leader_downgraded());
        assert!(region_routes[0].follower_peers.is_empty());
        assert_eq!(region_routes[0].leader_peer.as_ref().unwrap().id, 2);
    }
}
