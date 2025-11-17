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

use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

use common_meta::key::TableMetadataManagerRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};

use crate::error::{self, Result};
use crate::procedure::region_migration::RegionMigrationTriggerReason;

/// A migration task describing how regions are intended to move between peers.
#[derive(Debug, Clone)]
pub struct RegionMigrationTask {
    /// Region ids involved in this migration.
    pub region_ids: Vec<RegionId>,
    /// Source peer where regions currently reside.
    pub from_peer: Peer,
    /// Destination peer to migrate regions to.
    pub to_peer: Peer,
    /// Timeout for migration.
    pub timeout: Duration,
    /// Reason why this migration was triggered.
    pub trigger_reason: RegionMigrationTriggerReason,
}

impl Display for RegionMigrationTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RegionMigrationTask {{ region_ids: {:?}, from_peer: {:?}, to_peer: {:?}, timeout: {:?}, trigger_reason: {:?} }}",
            self.region_ids, self.from_peer, self.to_peer, self.timeout, self.trigger_reason
        )
    }
}

impl RegionMigrationTask {
    /// Returns the table regions map.
    ///
    /// The key is the table id, the value is the region ids of the table.
    pub(crate) fn table_regions(&self) -> HashMap<TableId, Vec<RegionId>> {
        let mut table_regions = HashMap::new();
        for region_id in &self.region_ids {
            table_regions
                .entry(region_id.table_id())
                .or_insert_with(Vec::new)
                .push(*region_id);
        }
        table_regions
    }
}

/// Represents the result of analyzing a migration task.
#[derive(Debug, Clone, Default)]
pub(crate) struct RegionMigrationAnalysis {
    /// Regions already migrated to the `to_peer`.
    pub(crate) migrated: Vec<RegionId>,
    /// Regions where the leader peer has changed.
    pub(crate) leader_changed: Vec<RegionId>,
    /// Regions where `to_peer` is already a follower (conflict).
    pub(crate) peer_conflict: Vec<RegionId>,
    /// Regions whose table is not found.
    pub(crate) table_not_found: Vec<RegionId>,
    /// Regions still pending migration.
    pub(crate) pending: Vec<RegionId>,
}

fn leader_peer(region_route: &RegionRoute) -> Result<&Peer> {
    region_route
        .leader_peer
        .as_ref()
        .with_context(|| error::UnexpectedSnafu {
            violated: format!(
                "Region route leader peer is not found in region({})",
                region_route.region.id
            ),
        })
}

/// Returns true if the region has already been migrated to `to_peer`.
fn has_migrated(region_route: &RegionRoute, to_peer_id: u64) -> Result<bool> {
    if region_route.is_leader_downgrading() {
        return Ok(false);
    }

    let leader_peer = leader_peer(region_route)?;
    Ok(leader_peer.id == to_peer_id)
}

/// Returns true if the leader peer of the region has changed.
fn has_leader_changed(region_route: &RegionRoute, from_peer_id: u64) -> Result<bool> {
    let leader_peer = leader_peer(region_route)?;

    Ok(leader_peer.id != from_peer_id)
}

/// Returns true if `to_peer` is already a follower of the region (conflict).
fn has_peer_conflict(region_route: &RegionRoute, to_peer_id: u64) -> bool {
    region_route
        .follower_peers
        .iter()
        .map(|p| p.id)
        .contains(&to_peer_id)
}

/// Updates the verification result based on a single region route.
fn update_result_with_region_route(
    result: &mut RegionMigrationAnalysis,
    region_route: &RegionRoute,
    from_peer_id: u64,
    to_peer_id: u64,
) -> Result<()> {
    if has_migrated(region_route, to_peer_id)? {
        result.migrated.push(region_route.region.id);
        return Ok(());
    }
    if has_leader_changed(region_route, from_peer_id)? {
        result.leader_changed.push(region_route.region.id);
        return Ok(());
    }
    if has_peer_conflict(region_route, to_peer_id) {
        result.peer_conflict.push(region_route.region.id);
        return Ok(());
    }
    result.pending.push(region_route.region.id);
    Ok(())
}

/// Analyzes the migration task and categorizes regions by their current state.
///
/// Returns a [`RegionMigrationAnalysis`] describing the migration status.
pub async fn analyze_region_migration_task(
    task: &RegionMigrationTask,
    table_metadata_manager: &TableMetadataManagerRef,
) -> Result<RegionMigrationAnalysis> {
    if task.to_peer.id == task.from_peer.id {
        return error::InvalidArgumentsSnafu {
            err_msg: format!(
                "The `from_peer_id`({}) can't equal `to_peer_id`({})",
                task.from_peer.id, task.to_peer.id
            ),
        }
        .fail();
    }
    let table_regions = task.table_regions();
    let table_ids = table_regions.keys().cloned().collect::<Vec<_>>();
    let mut result = RegionMigrationAnalysis::default();

    let table_routes = table_metadata_manager
        .table_route_manager()
        .table_route_storage()
        .batch_get_with_raw_bytes(&table_ids)
        .await
        .context(error::TableMetadataManagerSnafu)?;

    for (table_id, table_route) in table_ids.into_iter().zip(table_routes) {
        let region_ids = table_regions.get(&table_id).unwrap();
        let Some(table_route) = table_route else {
            result.table_not_found.extend(region_ids);
            continue;
        };
        // Throws error if the table route is not a physical table route.
        let region_routes =
            table_route
                .region_routes()
                .context(error::UnexpectedLogicalRouteTableSnafu {
                    err_msg: format!("TableRoute({table_id:?}) is a non-physical TableRouteValue."),
                })?;
        for region_route in region_routes
            .iter()
            .filter(|r| region_ids.contains(&r.region.id))
        {
            update_result_with_region_route(
                &mut result,
                region_route,
                task.from_peer.id,
                task.to_peer.id,
            )?;
        }
    }

    Ok(result)
}
