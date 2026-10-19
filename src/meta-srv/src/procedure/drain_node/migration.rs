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

use std::collections::{BTreeMap, HashSet};

use common_meta::DatanodeId;
use common_meta::affinity::AffinityReference;
use common_meta::peer::Peer;
use common_procedure::ProcedureId;
use common_procedure::error::Error as ProcedureError;
use common_telemetry::{error, warn};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;

use super::DrainNodeMigrationContext;
use crate::discovery::utils::accept_ingest_workload;
use crate::error::{self, Result};
use crate::metasrv::SelectTarget;
use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;
use crate::procedure::region_migration::{
    DEFAULT_REGION_MIGRATION_TIMEOUT, RegionMigrationTriggerReason,
};
use crate::selector::SelectorOptions;

pub(crate) struct TargetGroup {
    target_peer: Peer,
    region_ids: Vec<RegionId>,
}

impl DrainNodeMigrationContext {
    pub(crate) async fn source_peer(&self, datanode_id: DatanodeId) -> Result<Peer> {
        self.selector_ctx
            .peer_discovery
            .active_datanodes(None)
            .await
            .context(error::ListActiveDatanodesSnafu)?
            .into_iter()
            .find(|node| node.peer.id == datanode_id)
            .map(|node| node.peer)
            .context(error::PeerUnavailableSnafu {
                peer_id: datanode_id,
            })
    }

    pub(crate) async fn leader_regions(&self, datanode_id: DatanodeId) -> Result<Vec<RegionId>> {
        let mut values = self
            .table_metadata_manager
            .datanode_table_manager()
            .tables(datanode_id);
        let mut leader_regions = Vec::new();
        let mut follower_regions = Vec::new();

        while let Some(value) = values
            .try_next()
            .await
            .context(error::TableMetadataManagerSnafu)?
        {
            leader_regions.extend(
                value
                    .regions
                    .into_iter()
                    .map(|region_number| RegionId::new(value.table_id, region_number)),
            );
            follower_regions.extend(
                value
                    .follower_regions
                    .into_iter()
                    .map(|region_number| RegionId::new(value.table_id, region_number)),
            );
        }

        leader_regions.sort_unstable();
        leader_regions.dedup();
        follower_regions.sort_unstable();
        follower_regions.dedup();
        ensure!(
            follower_regions.is_empty(),
            error::DrainNodeFollowerRegionUnsupportedSnafu {
                datanode_id,
                follower_regions,
            }
        );

        Ok(leader_regions)
    }

    pub(crate) async fn submit_migrations(
        &self,
        datanode_id: DatanodeId,
        region_ids: &[RegionId],
    ) -> Result<Vec<ProcedureId>> {
        let source_peer = self.source_peer(datanode_id).await?;
        let groups = self
            .select_migration_targets(datanode_id, region_ids)
            .await?;

        let mut procedure_ids = Vec::with_capacity(groups.len());
        for group in groups.into_values() {
            let task = RegionMigrationTaskBatch {
                timeout: DEFAULT_REGION_MIGRATION_TIMEOUT * group.region_ids.len() as u32,
                region_ids: group.region_ids,
                from_peer: source_peer.clone(),
                to_peer: group.target_peer,
                trigger_reason: RegionMigrationTriggerReason::Manual,
            };
            let result = self
                .region_migration_manager
                .submit_region_migration_task(task)
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error::Error::SubmitProcedure {
                    source: ProcedureError::TooManyRunningProcedures { .. },
                    ..
                }) => break,
                Err(err) if !procedure_ids.is_empty() => {
                    error!(err; "Drain-node batch stopped after partial submission");
                    break;
                }
                Err(err) => return Err(err),
            };
            if let Some(procedure_id) = result.procedure_id {
                procedure_ids.push(procedure_id);
            }
        }

        Ok(procedure_ids)
    }

    pub(crate) async fn select_migration_targets(
        &self,
        datanode_id: DatanodeId,
        region_ids: &[RegionId],
    ) -> Result<BTreeMap<DatanodeId, TargetGroup>> {
        let affinity = if let Some(resolver) = &self.affinity_resolver {
            match resolver
                .resolve(&[AffinityReference::Datanode(datanode_id)])
                .await
            {
                Ok(mut resolved) => {
                    ensure!(
                        resolved.len() == 1,
                        error::UnexpectedSnafu {
                            violated: "affinity resolution must preserve the number and order of references",
                        }
                    );
                    resolved.pop().flatten()
                }
                Err(err) => {
                    warn!(err; "Failed to resolve drain-node affinity for datanode {datanode_id}; selecting without affinity");
                    None
                }
            }
        } else {
            None
        };
        // TODO(Aetherance): Exclude unschedulable targets when node state is available.
        let excluded_nodes = HashSet::from([datanode_id]);
        let targets = self
            .selector
            .select(
                &self.selector_ctx,
                SelectorOptions {
                    affinity: affinity.as_ref(),
                    min_required_items: region_ids.len(),
                    allow_duplication: true,
                    exclude_peer_ids: excluded_nodes,
                    workload_filter: Some(accept_ingest_workload),
                },
            )
            .await?;
        ensure!(
            targets.len() == region_ids.len(),
            error::NoEnoughAvailableNodeSnafu {
                required: region_ids.len(),
                available: targets.len(),
                select_target: SelectTarget::Datanode,
            }
        );

        let mut groups = BTreeMap::<DatanodeId, TargetGroup>::new();
        for (region_id, target) in region_ids.iter().copied().zip(targets) {
            groups
                .entry(target.id)
                .or_insert_with(|| TargetGroup {
                    target_peer: target,
                    region_ids: Vec::new(),
                })
                .region_ids
                .push(region_id);
        }

        Ok(groups)
    }
}
