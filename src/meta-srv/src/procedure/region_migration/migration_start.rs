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

use std::any::Any;

use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::procedure::region_migration::migration_abort::RegionMigrationAbort;
use crate::procedure::region_migration::migration_end::RegionMigrationEnd;
use crate::procedure::region_migration::open_candidate_region::OpenCandidateRegion;
use crate::procedure::region_migration::{Context, State};

/// The behaviors:
///
/// - If all regions have been migrated, transitions to [RegionMigrationEnd].
/// - If any of the region leaders is not the `from_peer`, transitions to [RegionMigrationAbort].
/// - Otherwise, continues with [OpenCandidateRegion] to initiate the candidate region.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegionMigrationStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for RegionMigrationStart {
    /// Yields next [State].
    ///
    /// Determines the next [State] for region migration:
    ///
    /// - If all regions have been migrated, transitions to [RegionMigrationEnd].
    /// - If any of the region leaders is not the `from_peer`, transitions to [RegionMigrationAbort].
    /// - Otherwise, continues with [OpenCandidateRegion] to initiate the candidate region.
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let mut region_routes = self.retrieve_region_routes(ctx).await?;
        let to_peer = &ctx.persistent_ctx.to_peer;
        let from_peer = &ctx.persistent_ctx.from_peer;
        let region_ids = &ctx.persistent_ctx.region_ids;

        self.filter_unmigrated_regions(&mut region_routes, to_peer);

        // No region to migrate, skip the migration.
        if region_routes.is_empty() {
            info!(
                "All regions have been migrated, regions: {:?}, to_peer: {:?}",
                region_ids, to_peer
            );
            return Ok((Box::new(RegionMigrationEnd), Status::done()));
        }

        // Updates the region ids to the unmigrated regions.
        if region_routes.len() != region_ids.len() {
            let unmigrated_region_ids = region_routes.iter().map(|route| route.region.id).collect();
            info!(
                "Some of the regions have been migrated, only migrate the following regions: {:?}, to_peer: {:?}",
                unmigrated_region_ids, to_peer
            );
            ctx.persistent_ctx.region_ids = unmigrated_region_ids;
        }

        // Checks if any of the region leaders is not the `from_peer`.
        for region_route in &region_routes {
            if self.invalid_leader_peer(region_route, from_peer)? {
                info!(
                    "Abort region migration, region:{}, unexpected leader peer: {:?}, expected: {:?}",
                    region_route.region.id, region_route.leader_peer, from_peer,
                );
                return Ok((
                    Box::new(RegionMigrationAbort::new(&format!(
                        "Invalid region leader peer: {:?}, expected: {:?}",
                        region_route.leader_peer.as_ref().unwrap(),
                        from_peer,
                    ))),
                    Status::done(),
                ));
            }
        }

        // If all checks pass, open the candidate region.
        Ok((Box::new(OpenCandidateRegion), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RegionMigrationStart {
    /// Retrieves region routes for multiple regions.
    ///
    /// Abort(non-retry):
    /// - TableRoute is not found.
    /// - RegionRoute is not found.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    async fn retrieve_region_routes(&self, ctx: &mut Context) -> Result<Vec<RegionRoute>> {
        let region_ids = &ctx.persistent_ctx.region_ids;
        let table_route_values = ctx.get_table_route_values().await?;
        let mut region_routes = Vec::with_capacity(region_ids.len());
        for region_id in region_ids {
            let table_id = region_id.table_id();
            let region_route = table_route_values
                .get(&table_id)
                .context(error::TableRouteNotFoundSnafu { table_id })?
                .region_routes()
                .with_context(|_| error::UnexpectedLogicalRouteTableSnafu {
                    err_msg: format!("TableRoute({table_id:?}) is a non-physical TableRouteValue."),
                })?
                .iter()
                .find(|route| route.region.id == *region_id)
                .cloned()
                .with_context(|| error::UnexpectedSnafu {
                    violated: format!(
                        "RegionRoute({}) is not found in TableRoute({})",
                        region_id, table_id
                    ),
                })?;
            region_routes.push(region_route);
        }

        Ok(region_routes)
    }

    /// Returns true if the region leader is not the `from_peer`.
    ///     
    /// Abort(non-retry):
    /// - Leader peer of RegionRoute is not found.
    fn invalid_leader_peer(&self, region_route: &RegionRoute, from_peer: &Peer) -> Result<bool> {
        let region_id = region_route.region.id;

        let is_invalid_leader_peer = region_route
            .leader_peer
            .as_ref()
            .with_context(|| error::UnexpectedSnafu {
                violated: format!("Leader peer is not found in TableRoute({})", region_id),
            })?
            .id
            != from_peer.id;
        Ok(is_invalid_leader_peer)
    }

    /// Filters out regions that unmigrated.
    fn filter_unmigrated_regions(&self, region_routes: &mut Vec<RegionRoute>, to_peer: &Peer) {
        region_routes
            .retain(|region_route| !self.has_migrated(region_route, to_peer).unwrap_or(false));
    }

    /// Checks whether the region has been migrated.
    /// Returns true if it's.
    ///     
    /// Abort(non-retry):
    /// - Leader peer of RegionRoute is not found.
    fn has_migrated(&self, region_route: &RegionRoute, to_peer: &Peer) -> Result<bool> {
        let region_id = region_route.region.id;

        let region_migrated = region_route
            .leader_peer
            .as_ref()
            .with_context(|| error::UnexpectedSnafu {
                violated: format!("Leader peer is not found in TableRoute({})", region_id),
            })?
            .id
            == to_peer.id;
        Ok(region_migrated)
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;

    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;
    use crate::procedure::region_migration::test_util::{self, TestingEnv, new_procedure_context};
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    #[tokio::test]
    async fn test_table_route_is_not_found_error() {
        let state = RegionMigrationStart;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let mut ctx = env.context_factory().new_context(persistent_context);
        let err = state.retrieve_region_routes(&mut ctx).await.unwrap_err();
        assert_matches!(err, Error::TableRouteNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_region_route_is_not_found_error() {
        let state = RegionMigrationStart;
        let persistent_context = new_persistent_context();
        let from_peer = persistent_context.from_peer.clone();

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024);
        let region_route = RegionRoute {
            region: Region::new_test(RegionId::new(1024, 3)),
            leader_peer: Some(from_peer.clone()),
            ..Default::default()
        };

        env.create_physical_table_metadata(table_info, vec![region_route])
            .await;
        let err = state.retrieve_region_routes(&mut ctx).await.unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_next_migration_end_state() {
        let mut state = Box::new(RegionMigrationStart);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let to_peer = persistent_context.to_peer.clone();
        let from_peer = persistent_context.from_peer.clone();
        let region_id = persistent_context.region_ids[0];

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024);
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(to_peer),
            follower_peers: vec![from_peer],
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();

        let _ = next.as_any().downcast_ref::<RegionMigrationEnd>().unwrap();
    }

    #[tokio::test]
    async fn test_next_open_candidate_region_state() {
        let mut state = Box::new(RegionMigrationStart);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let from_peer_id = persistent_context.from_peer.id;
        let region_id = persistent_context.region_ids[0];
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024);
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(from_peer_id)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();

        let _ = next.as_any().downcast_ref::<OpenCandidateRegion>().unwrap();
    }

    #[tokio::test]
    async fn test_next_migration_abort() {
        let mut state = Box::new(RegionMigrationStart);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_ids[0];
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024);
        let region_routes: Vec<RegionRoute> = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(1024)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;
        let procedure_ctx = new_procedure_context();
        let (next, _) = state.next(&mut ctx, &procedure_ctx).await.unwrap();

        let _ = next
            .as_any()
            .downcast_ref::<RegionMigrationAbort>()
            .unwrap();
    }
}
