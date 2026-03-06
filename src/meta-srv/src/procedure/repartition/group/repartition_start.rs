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
use std::collections::{HashMap, HashSet};

use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::debug;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{self, Result};
use crate::procedure::repartition::group::sync_region::SyncRegion;
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::group::{
    Context, GroupId, GroupPrepareResult, State, region_routes,
};
use crate::procedure::repartition::plan::RegionDescriptor;

#[derive(Debug, Serialize, Deserialize)]
pub struct RepartitionStart;

/// Ensures that the partition expression of the region route matches the partition expression of the region descriptor.
fn ensure_region_route_expr_match(
    region_route: &RegionRoute,
    region_descriptor: &RegionDescriptor,
) -> Result<RegionRoute> {
    let actual = region_route.region.partition_expr();
    let expected = region_descriptor
        .partition_expr
        .as_json_str()
        .context(error::SerializePartitionExprSnafu)?;
    ensure!(
        actual == expected,
        error::PartitionExprMismatchSnafu {
            region_id: region_route.region.id,
            expected,
            actual,
        }
    );
    Ok(region_route.clone())
}

impl RepartitionStart {
    /// Ensures that both source and target regions are present in the region routes.
    ///
    /// Both source and target regions must be present in the region routes (target regions should be allocated before repartitioning).
    fn ensure_route_present(
        group_id: GroupId,
        region_routes: &[RegionRoute],
        sources: &[RegionDescriptor],
        targets: &[RegionDescriptor],
    ) -> Result<GroupPrepareResult> {
        ensure!(
            !sources.is_empty(),
            error::UnexpectedSnafu {
                violated: "Sources are empty"
            }
        );

        let region_routes_map = region_routes
            .iter()
            .map(|r| (r.region.id, r))
            .collect::<HashMap<_, _>>();
        let source_region_routes = sources
            .iter()
            .map(|s| {
                region_routes_map
                    .get(&s.region_id)
                    .context(error::RepartitionSourceRegionMissingSnafu {
                        group_id,
                        region_id: s.region_id,
                    })
                    .and_then(|r| ensure_region_route_expr_match(r, s))
            })
            .collect::<Result<Vec<_>>>()?;
        let target_region_routes = targets
            .iter()
            .map(|t| {
                region_routes_map
                    .get(&t.region_id)
                    .context(error::RepartitionTargetRegionMissingSnafu {
                        group_id,
                        region_id: t.region_id,
                    })
                    .map(|r| (*r).clone())
            })
            .collect::<Result<Vec<_>>>()?;
        for target_region_route in &target_region_routes {
            ensure!(
                target_region_route.leader_peer.is_some(),
                error::UnexpectedSnafu {
                    violated: format!(
                        "Leader peer is not set for region: {}",
                        target_region_route.region.id
                    ),
                }
            );
        }
        let central_region = sources[0].region_id;
        let central_region_datanode = source_region_routes[0]
            .leader_peer
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "Leader peer is not set for central region: {}",
                    central_region
                ),
            })?
            .clone();

        Ok(GroupPrepareResult {
            source_routes: source_region_routes,
            target_routes: target_region_routes,
            central_region,
            central_region_datanode,
        })
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for RepartitionStart {
    /// Captures the group prepare result.
    ///
    /// Retry:
    /// - Failed to get the table route.
    ///
    /// Abort
    /// - Table route not found.
    /// - Table route is not physical.
    /// - Failed to ensure the route is present.
    /// - Failed to capture the group prepare result.
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if ctx.persistent_ctx.group_prepare_result.is_some() {
            return Ok((
                Box::new(UpdateMetadata::ApplyStaging),
                Status::executing(true),
            ));
        }
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;
        let table_route_value = ctx.get_table_route_value().await?.into_inner();
        let region_routes = region_routes(table_id, &table_route_value)?;
        let group_prepare_result = Self::ensure_route_present(
            group_id,
            region_routes,
            &ctx.persistent_ctx.sources,
            &ctx.persistent_ctx.targets,
        )?;
        ctx.persistent_ctx.group_prepare_result = Some(group_prepare_result);
        debug!(
            "Repartition group {}: captured {} sources, {} targets",
            group_id,
            ctx.persistent_ctx.sources.len(),
            ctx.persistent_ctx.targets.len()
        );

        if ctx.persistent_ctx.sync_region {
            let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
            let allocated_region_ids: HashSet<_> = ctx
                .persistent_ctx
                .allocated_region_ids
                .iter()
                .copied()
                .collect();
            let region_routes: Vec<_> = prepare_result
                .target_routes
                .iter()
                .filter(|route| allocated_region_ids.contains(&route.region.id))
                .cloned()
                .collect();
            if !region_routes.is_empty() {
                return Ok((
                    Box::new(SyncRegion { region_routes }),
                    Status::executing(true),
                ));
            }
        }

        Ok((
            Box::new(UpdateMetadata::ApplyStaging),
            Status::executing(true),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;
    use uuid::Uuid;

    use crate::error::Error;
    use crate::procedure::repartition::group::repartition_start::RepartitionStart;
    use crate::procedure::repartition::plan::RegionDescriptor;
    use crate::procedure::repartition::test_util::range_expr;

    #[test]
    fn test_ensure_route_present_missing_source_region() {
        let source_region = RegionDescriptor {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 100),
        };
        let target_region = RegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 2),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let err = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        )
        .unwrap_err();
        assert_matches!(err, Error::RepartitionSourceRegionMissing { .. });
    }

    #[test]
    fn test_ensure_route_present_partition_expr_mismatch() {
        let source_region = RegionDescriptor {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 100),
        };
        let target_region = RegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 1),
                partition_expr: range_expr("x", 0, 5).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let err = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        )
        .unwrap_err();
        assert_matches!(err, Error::PartitionExprMismatch { .. });
    }

    #[test]
    fn test_ensure_route_present_missing_target_region() {
        let source_region = RegionDescriptor {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 100),
        };
        let target_region = RegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 1),
                partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let err = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        )
        .unwrap_err();
        assert_matches!(err, Error::RepartitionTargetRegionMissing { .. });
    }

    #[test]
    fn test_ensure_route_present_legacy_partition_expr_source() {
        let source_region = RegionDescriptor {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 100),
        };
        let target_region = RegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let legacy_partition_expr = range_expr("x", 0, 100).as_json_str().unwrap();
        let legacy_region_json = serde_json::json!({
            "id": RegionId::new(1024, 1).as_u64(),
            "name": "",
            "partition": {
                "column_list": ["x"],
                "value_list": [legacy_partition_expr]
            },
            "partition_expr": "",
            "attrs": {}
        });

        let region_routes = vec![
            RegionRoute {
                region: serde_json::from_value(legacy_region_json).unwrap(),
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(1024, 2),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
        ];

        let result = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        );
        assert!(result.is_ok());
    }
}
