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

use common_error::ext::BoxedError;
use common_meta::key::datanode_table::RegionInfo;
use common_meta::lock_key::TableLock;
use common_meta::rpc::router::{RegionRoute, region_distribution};
use common_procedure::ContextProviderRef;
use common_telemetry::{error, info, warn};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::region_migration::Context;
use crate::procedure::region_migration::update_metadata::UpdateMetadata;

impl UpdateMetadata {
    /// Returns new [Vec<RegionRoute>].
    fn build_upgrade_candidate_region_metadata(
        &self,
        ctx: &mut Context,
        region_ids: &[RegionId],
        mut region_routes: Vec<RegionRoute>,
    ) -> Result<Vec<RegionRoute>> {
        let old_leader_peer = &ctx.persistent_ctx.from_peer;
        let new_leader_peer = &ctx.persistent_ctx.to_peer;
        for region_id in region_ids {
            // Find the RegionRoute for this region_id.
            let region_route = region_routes
                .iter_mut()
                .find(|route| route.region.id == *region_id)
                .context(error::RegionRouteNotFoundSnafu {
                    region_id: *region_id,
                })?;

            // Remove any "downgraded leader" state.
            region_route.set_leader_state(None);

            // Check old leader matches expectation before upgrading to new leader.
            ensure!(
                region_route
                    .leader_peer
                    .take_if(|old_leader| old_leader.id == old_leader_peer.id)
                    .is_some(),
                error::UnexpectedSnafu {
                    violated: format!(
                        "Unexpected region leader: {:?} during the candidate-to-leader upgrade; expected: {:?}",
                        region_route.leader_peer, old_leader_peer
                    ),
                }
            );

            // Set new leader.
            region_route.leader_peer = Some(new_leader_peer.clone());

            // Remove new leader from followers (avoids duplicate leader/follower).
            let removed = region_route
                .follower_peers
                .extract_if(.., |peer| peer.id == new_leader_peer.id)
                .collect::<Vec<_>>();

            // Warn if more than one follower with the new leader id was present.
            if removed.len() > 1 {
                warn!(
                    "Removed duplicate followers: {removed:?} during candidate-to-leader upgrade for region: {region_id}"
                );
            }
        }

        info!(
            "Building metadata for upgrading candidate region to new leader: {:?} for regions: {:?}",
            new_leader_peer, region_ids,
        );

        Ok(region_routes)
    }

    /// Checks if metadata has been upgraded for a list of regions by verifying if their
    /// leader peers have been switched to a specified peer ID (`to_peer_id`) and that
    /// no region is in a leader downgrading state.
    ///
    /// Returns:
    /// - `Ok(true)` if all regions' leader is the target peer and no downgrading occurs.
    /// - `Ok(false)` if any region's leader is not the target peer.
    /// - Error if region route or leader peer cannot be found, or an unexpected state is detected.
    fn check_metadata_updated(
        &self,
        ctx: &mut Context,
        region_ids: &[RegionId],
        region_routes: &[RegionRoute],
    ) -> Result<bool> {
        // Iterate through each provided region ID
        for region_id in region_ids {
            // Find the route info for this region
            let region_route = region_routes
                .iter()
                .find(|route| route.region.id == *region_id)
                .context(error::RegionRouteNotFoundSnafu {
                    region_id: *region_id,
                })?;

            // Get the leader peer for the region, error if not found
            let leader_peer = region_route.leader_peer.as_ref().with_context(||error::UnexpectedSnafu {
                violated: format!(
                    "The leader peer of region {region_id} is not found during the metadata upgrade check"
                ),
            })?;

            // If the leader is not the expected peer, return false (i.e., not yet upgraded)
            if leader_peer.id != ctx.persistent_ctx.to_peer.id {
                return Ok(false);
            } else {
                // If leader matches but region is in leader downgrading state, error (unexpected state)
                ensure!(
                    !region_route.is_leader_downgrading(),
                    error::UnexpectedSnafu {
                        violated: format!(
                            "Unexpected intermediate state is found during the metadata upgrade check for region {region_id}"
                        ),
                    }
                );
            }
        }

        // All regions' leader match expected peer and are not downgrading; considered upgraded
        Ok(true)
    }

    /// Upgrades the candidate region.
    ///
    /// Abort(non-retry):
    /// - TableRoute or RegionRoute is not found.
    ///   Typically, it's impossible, there is no other DDL procedure executed concurrently for the current table.
    ///
    /// Retry:
    /// - Failed to update [TableRouteValue](common_meta::key::table_region::TableRegionValue).
    /// - Failed to retrieve the metadata of table.
    pub async fn upgrade_candidate_region(
        &self,
        ctx: &mut Context,
        ctx_provider: &ContextProviderRef,
    ) -> Result<()> {
        let table_metadata_manager = ctx.table_metadata_manager.clone();
        let table_regions = ctx.persistent_ctx.table_regions();
        let from_peer_id = ctx.persistent_ctx.from_peer.id;
        let to_peer_id = ctx.persistent_ctx.to_peer.id;

        for (table_id, region_ids) in table_regions {
            let table_lock = TableLock::Write(table_id).into();
            let _guard = ctx_provider.acquire_lock(&table_lock).await;

            let table_route_value = ctx.get_table_route_value(table_id).await?;
            let region_routes = table_route_value.region_routes().with_context(|_| {
                error::UnexpectedLogicalRouteTableSnafu {
                    err_msg: format!("TableRoute({table_id:?}) is a non-physical TableRouteValue."),
                }
            })?;
            if self.check_metadata_updated(ctx, &region_ids, region_routes)? {
                continue;
            }
            let datanode_table_value = ctx.get_from_peer_datanode_table_value(table_id).await?;
            let RegionInfo {
                region_storage_path,
                region_options,
                region_wal_options,
                engine,
            } = datanode_table_value.region_info.clone();
            let new_region_routes = self.build_upgrade_candidate_region_metadata(
                ctx,
                &region_ids,
                region_routes.clone(),
            )?;
            let region_distribution = region_distribution(region_routes);
            info!(
                "Trying to update region routes to {:?} for table: {}",
                region_distribution, table_id,
            );

            if let Err(err) = table_metadata_manager
                .update_table_route(
                    table_id,
                    RegionInfo {
                        engine: engine.clone(),
                        region_storage_path: region_storage_path.clone(),
                        region_options: region_options.clone(),
                        region_wal_options: region_wal_options.clone(),
                    },
                    &table_route_value,
                    new_region_routes,
                    &region_options,
                    &region_wal_options,
                )
                .await
                .context(error::TableMetadataManagerSnafu)
            {
                error!(err; "Failed to update the table route during the upgrading candidate region: {region_ids:?}, from_peer_id: {from_peer_id}");
                return Err(BoxedError::new(err)).context(error::RetryLaterWithSourceSnafu {
                    reason: format!("Failed to update the table route during the upgrading candidate region: {table_id}"),
                });
            };
            info!(
                "Upgrading candidate region table route success, table_id: {table_id}, regions: {region_ids:?}, to_peer_id: {to_peer_id}"
            );
        }

        ctx.deregister_failure_detectors().await;
        // Consumes the guard.
        ctx.volatile_ctx.opening_region_guards.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::region_keeper::MemoryRegionKeeper;
    use common_meta::rpc::router::{LeaderState, Region, RegionRoute};
    use common_time::util::current_time_millis;
    use store_api::storage::RegionId;

    use crate::error::Error;
    use crate::procedure::region_migration::close_downgraded_region::CloseDowngradedRegion;
    use crate::procedure::region_migration::test_util::{self, TestingEnv, new_procedure_context};
    use crate::procedure::region_migration::update_metadata::UpdateMetadata;
    use crate::procedure::region_migration::{ContextFactory, PersistentContext, State};

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    #[tokio::test]
    async fn test_table_route_is_not_found_error() {
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let ctx = env.context_factory().new_context(persistent_context);

        let err = ctx.get_table_route_value(1024).await.unwrap_err();

        assert_matches!(err, Error::TableRouteNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_region_route_is_not_found() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 2)),
            leader_peer: Some(Peer::empty(4)),
            ..Default::default()
        }];
        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_route_value = ctx.get_table_route_value(1024).await.unwrap();
        let region_routes = table_route_value
            .into_inner()
            .into_physical_table_route()
            .region_routes;
        let err = state
            .build_upgrade_candidate_region_metadata(
                &mut ctx,
                &[RegionId::new(1024, 1)],
                region_routes,
            )
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

        let table_info = new_test_table_info(1024).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(3)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_route_value = ctx.get_table_route_value(1024).await.unwrap();
        let region_routes = table_route_value
            .into_inner()
            .into_physical_table_route()
            .region_routes;
        let err = state
            .build_upgrade_candidate_region_metadata(
                &mut ctx,
                &[RegionId::new(1024, 1)],
                region_routes,
            )
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

        let table_info = new_test_table_info(1024).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(1)),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_state: Some(LeaderState::Downgrading),
            leader_down_since: Some(current_time_millis()),
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_route_value = ctx.get_table_route_value(1024).await.unwrap();
        let region_routes = table_route_value
            .into_inner()
            .into_physical_table_route()
            .region_routes;
        let new_region_routes = state
            .build_upgrade_candidate_region_metadata(
                &mut ctx,
                &[RegionId::new(1024, 1)],
                region_routes,
            )
            .unwrap();

        assert!(!new_region_routes[0].is_leader_downgrading());
        assert!(new_region_routes[0].leader_down_since.is_none());
        assert_eq!(new_region_routes[0].follower_peers, vec![Peer::empty(3)]);
        assert_eq!(new_region_routes[0].leader_peer.as_ref().unwrap().id, 2);
    }

    #[tokio::test]
    async fn test_check_metadata() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let leader_peer = persistent_context.from_peer.clone();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_info = new_test_table_info(1024).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(leader_peer),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_state: None,
            leader_down_since: None,
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;
        let table_routes = ctx.get_table_route_value(1024).await.unwrap();
        let region_routes = table_routes.region_routes().unwrap();
        let updated = state
            .check_metadata_updated(&mut ctx, &[RegionId::new(1024, 1)], region_routes)
            .unwrap();
        assert!(!updated);
    }

    #[tokio::test]
    async fn test_check_metadata_updated() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let candidate_peer = persistent_context.to_peer.clone();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_info = new_test_table_info(1024).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(candidate_peer),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_state: None,
            leader_down_since: None,
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_routes = ctx.get_table_route_value(1024).await.unwrap();
        let region_routes = table_routes.region_routes().unwrap();
        let updated = state
            .check_metadata_updated(&mut ctx, &[RegionId::new(1024, 1)], region_routes)
            .unwrap();
        assert!(updated);
    }

    #[tokio::test]
    async fn test_check_metadata_intermediate_state() {
        let state = UpdateMetadata::Upgrade;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let candidate_peer = persistent_context.to_peer.clone();

        let mut ctx = env.context_factory().new_context(persistent_context);
        let table_info = new_test_table_info(1024).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(candidate_peer),
            follower_peers: vec![Peer::empty(2), Peer::empty(3)],
            leader_state: Some(LeaderState::Downgrading),
            leader_down_since: None,
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_routes = ctx.get_table_route_value(1024).await.unwrap();
        let region_routes = table_routes.region_routes().unwrap();
        let err = state
            .check_metadata_updated(&mut ctx, &[RegionId::new(1024, 1)], region_routes)
            .unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert!(err.to_string().contains("intermediate state"));
    }

    #[tokio::test]
    async fn test_next_close_downgraded_region_state() {
        let mut state = Box::new(UpdateMetadata::Upgrade);
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let opening_keeper = MemoryRegionKeeper::default();

        let table_id = 1024;
        let table_info = new_test_table_info(table_id).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(table_id, 1)),
            leader_peer: Some(Peer::empty(1)),
            leader_state: Some(LeaderState::Downgrading),
            ..Default::default()
        }];

        let guard = opening_keeper
            .register(2, RegionId::new(table_id, 1))
            .unwrap();
        ctx.volatile_ctx.opening_region_guards.push(guard);

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let table_metadata_manager = env.table_metadata_manager();

        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();

        let _ = next
            .as_any()
            .downcast_ref::<CloseDowngradedRegion>()
            .unwrap();

        let table_route = table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();
        let region_routes = table_route.region_routes().unwrap();

        assert!(ctx.volatile_ctx.opening_region_guards.is_empty());
        assert_eq!(region_routes.len(), 1);
        assert!(!region_routes[0].is_leader_downgrading());
        assert!(region_routes[0].follower_peers.is_empty());
        assert_eq!(region_routes[0].leader_peer.as_ref().unwrap().id, 2);
    }
}
