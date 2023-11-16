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
use serde::{Deserialize, Serialize};
use snafu::{location, Location, OptionExt, ResultExt};
use store_api::storage::RegionId;

use super::downgrade_leader_region::DowngradeLeaderRegion;
use super::migration_end::RegionMigrationEnd;
use super::open_candidate_region::OpenCandidateRegion;
use crate::error::{self, Result};
use crate::procedure::region_migration::{Context, State};

#[derive(Debug, Serialize, Deserialize)]
pub struct RegionMigrationStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for RegionMigrationStart {
    /// Yields next [State].
    ///
    /// If the expected leader region has been opened on `to_peer`, go to the MigrationEnd state.
    ///
    /// If the candidate region has been opened on `to_peer`, go to the DowngradeLeader state.
    ///
    /// Otherwise go to the OpenCandidateRegion state.
    async fn next(&mut self, ctx: &mut Context) -> Result<Box<dyn State>> {
        let region_id = ctx.persistent_ctx.region_id;
        let to_peer = &ctx.persistent_ctx.to_peer;

        let region_route = self.retrieve_region_route(ctx, region_id).await?;

        if self.check_leader_region_on_peer(&region_route, to_peer)? {
            Ok(Box::new(RegionMigrationEnd))
        } else if self.check_candidate_region_on_peer(&region_route, to_peer) {
            Ok(Box::new(DowngradeLeaderRegion))
        } else {
            Ok(Box::new(OpenCandidateRegion))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RegionMigrationStart {
    /// Retrieves region route.
    ///
    /// Abort(non-retry):
    /// - TableRoute is not found.
    /// - RegionRoute is not found.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    async fn retrieve_region_route(
        &self,
        ctx: &Context,
        region_id: RegionId,
    ) -> Result<RegionRoute> {
        let table_id = region_id.table_id();
        let table_route = ctx
            .table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)
            .map_err(|e| error::Error::RetryLater {
                reason: e.to_string(),
                location: location!(),
            })?
            .context(error::TableRouteNotFoundSnafu { table_id })?;

        let region_route = table_route
            .region_routes
            .iter()
            .find(|route| route.region.id == region_id)
            .cloned()
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "RegionRoute({}) is not found in TableRoute({})",
                    region_id, table_id
                ),
            })?;

        Ok(region_route)
    }

    /// Checks whether the candidate region on region has been opened.
    /// Returns true if it's been opened.
    fn check_candidate_region_on_peer(&self, region_route: &RegionRoute, to_peer: &Peer) -> bool {
        let region_opened = region_route
            .follower_peers
            .iter()
            .any(|peer| peer.id == to_peer.id);

        region_opened
    }

    /// Checks whether the leader region on region has been opened.
    /// Returns true if it's been opened.
    ///     
    /// Abort(non-retry):
    /// - Leader peer of RegionRoute is not found.
    fn check_leader_region_on_peer(
        &self,
        region_route: &RegionRoute,
        to_peer: &Peer,
    ) -> Result<bool> {
        let region_id = region_route.region.id;

        let region_opened = region_route
            .leader_peer
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: format!("Leader peer is not found in TableRoute({})", region_id),
            })?
            .id
            == to_peer.id;

        Ok(region_opened)
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
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::procedure::region_migration::{ContextFactory, PersistentContext};

    fn new_persistent_context() -> PersistentContext {
        PersistentContext {
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            region_id: RegionId::new(1024, 1),
            cluster_id: 0,
        }
    }

    #[tokio::test]
    async fn test_table_route_is_not_found_error() {
        let state = RegionMigrationStart;
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context();
        let ctx = env.context_factory().new_context(persistent_context);

        let err = state
            .retrieve_region_route(&ctx, RegionId::new(1024, 1))
            .await
            .unwrap_err();

        assert_matches!(err, Error::TableRouteNotFound { .. });

        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_region_route_is_not_found_error() {
        let state = RegionMigrationStart;
        let persistent_context = new_persistent_context();
        let from_peer = persistent_context.from_peer.clone();

        let env = TestingEnv::new();
        let ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_route = RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(from_peer.clone()),
            ..Default::default()
        };

        env.table_metadata_manager()
            .create_table_metadata(table_info, vec![region_route])
            .await
            .unwrap();

        let err = state
            .retrieve_region_route(&ctx, RegionId::new(1024, 3))
            .await
            .unwrap_err();

        assert_matches!(err, Error::Unexpected { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_next_downgrade_leader_region_state() {
        let mut state = Box::new(RegionMigrationStart);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let to_peer = persistent_context.to_peer.clone();
        let region_id = persistent_context.region_id;

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(3)),
            follower_peers: vec![to_peer],
            ..Default::default()
        }];

        env.table_metadata_manager()
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        let next = state.next(&mut ctx).await.unwrap();

        let _ = next
            .as_any()
            .downcast_ref::<DowngradeLeaderRegion>()
            .unwrap();
    }

    #[tokio::test]
    async fn test_next_migration_end_state() {
        let mut state = Box::new(RegionMigrationStart);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let to_peer = persistent_context.to_peer.clone();
        let from_peer = persistent_context.from_peer.clone();
        let region_id = persistent_context.region_id;

        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(to_peer),
            follower_peers: vec![from_peer],
            ..Default::default()
        }];

        env.table_metadata_manager()
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        let next = state.next(&mut ctx).await.unwrap();

        let _ = next.as_any().downcast_ref::<RegionMigrationEnd>().unwrap();
    }

    #[tokio::test]
    async fn test_next_open_candidate_region_state() {
        let mut state = Box::new(RegionMigrationStart);
        // from_peer: 1
        // to_peer: 2
        let persistent_context = new_persistent_context();
        let region_id = persistent_context.region_id;
        let env = TestingEnv::new();
        let mut ctx = env.context_factory().new_context(persistent_context);

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(3)),
            ..Default::default()
        }];

        env.table_metadata_manager()
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        let next = state.next(&mut ctx).await.unwrap();

        let _ = next.as_any().downcast_ref::<OpenCandidateRegion>().unwrap();
    }
}
