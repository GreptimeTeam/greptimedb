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
use std::sync::{Arc, Mutex};

use api::region::RegionResponse;
use api::v1::region::{
    ApplyStagedManifestRequest, PauseRequest, PublishRegionRuleRequest, RegionRequest,
    RegionRequestHeader, RemapManifestRequest, ResumeRequest, StageRegionRuleRequest,
    region_request,
};
use common_error::ext::BoxedError;
use common_meta::ddl::DdlContext;
use common_meta::key::TableMetadataManagerRef;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};

pub const REMAP_MANIFEST_STATS_EXTENSION: &str = "repartition.manifest.stats";

use super::group::{GroupRollbackRecord, RepartitionGroupProcedure};
use crate::procedure::repartition::plan::PlanGroupId;

/// Track the overall manifest stage for a repartition group.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ManifestStatus {
    #[default]
    NotStarted,
    Staged,
    Published,
    Discarded,
    Skipped,
    Failed,
}

/// Per-group status record that is collected by the top-level procedure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupManifestSummary {
    pub group_id: PlanGroupId,
    pub status: ManifestStatus,
    pub staged_region_count: u64,
    pub stats: Option<Value>,
    pub error: Option<String>,
}

/// Shared context that allows group procedures to interact with metadata and
/// datanodes. It also aggregates per-group manifest summaries.
#[derive(Clone)]
pub struct RepartitionContext {
    pub table_metadata_manager: TableMetadataManagerRef,
    pub node_manager: NodeManagerRef,
    manifest_records: Arc<Mutex<HashMap<PlanGroupId, GroupManifestSummary>>>,
    rollback_records: Arc<Mutex<HashMap<PlanGroupId, GroupRollbackRecord>>>,
}

impl RepartitionContext {
    pub fn new(context: &DdlContext) -> Self {
        Self {
            table_metadata_manager: context.table_metadata_manager.clone(),
            node_manager: context.node_manager.clone(),
            manifest_records: Arc::new(Mutex::new(HashMap::new())),
            rollback_records: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Send a pause request to the region leader so that local IO is quiesced.
    pub async fn pause_region_on_datanode(&self, peer: &Peer, region_id: RegionId) -> Result<()> {
        info!(
            "requesting pause to datanode {} for region {}",
            peer.id, region_id
        );
        let datanode = self.node_manager.datanode(peer).await;
        let request = RegionRequest {
            header: Some(RegionRequestHeader::default()),
            body: Some(region_request::Body::Pause(PauseRequest {
                region_id: region_id.as_u64(),
            })),
        };
        datanode
            .handle(request)
            .await
            .map_err(BoxedError::new)
            .context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "failed to pause region {} on datanode {}",
                    region_id, peer.id
                ),
            })?;
        Ok(())
    }

    /// Resume a previously paused region.
    pub async fn resume_region_on_datanode(&self, peer: &Peer, region_id: RegionId) -> Result<()> {
        info!(
            "requesting resume to datanode {} for region {}",
            peer.id, region_id
        );
        let datanode = self.node_manager.datanode(peer).await;
        let request = RegionRequest {
            header: Some(RegionRequestHeader::default()),
            body: Some(region_request::Body::Resume(ResumeRequest {
                region_id: region_id.as_u64(),
                rule_version: String::new(),
            })),
        };
        datanode
            .handle(request)
            .await
            .map_err(BoxedError::new)
            .context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "failed to resume region {} on datanode {}",
                    region_id, peer.id
                ),
            })?;
        Ok(())
    }

    /// Stage the provided rule version on the datanode.
    pub async fn stage_region_rule_on_datanode(
        &self,
        peer: &Peer,
        region_id: RegionId,
        rule_version: &str,
    ) -> Result<()> {
        info!(
            "requesting region rule staging to datanode {} for region {}",
            peer.id, region_id
        );
        let datanode = self.node_manager.datanode(peer).await;
        let request = RegionRequest {
            header: Some(RegionRequestHeader::default()),
            body: Some(region_request::Body::StageRegionRule(
                StageRegionRuleRequest {
                    region_id: region_id.as_u64(),
                    rule_version: rule_version.to_string(),
                },
            )),
        };
        datanode
            .handle(request)
            .await
            .map_err(BoxedError::new)
            .context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "failed to stage region rule for region {} on datanode {}",
                    region_id, peer.id
                ),
            })?;
        Ok(())
    }

    /// Publish the staged rule version to make it active.
    pub async fn publish_region_rule_on_datanode(
        &self,
        peer: &Peer,
        region_id: RegionId,
        rule_version: &str,
    ) -> Result<()> {
        info!(
            "requesting region rule publish to datanode {} for region {}",
            peer.id, region_id
        );
        let datanode = self.node_manager.datanode(peer).await;
        let request = RegionRequest {
            header: Some(RegionRequestHeader::default()),
            body: Some(region_request::Body::PublishRegionRule(
                PublishRegionRuleRequest {
                    region_id: region_id.as_u64(),
                    rule_version: rule_version.to_string(),
                },
            )),
        };
        datanode
            .handle(request)
            .await
            .map_err(BoxedError::new)
            .context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "failed to publish region rule for region {} on datanode {}",
                    region_id, peer.id
                ),
            })?;
        Ok(())
    }

    /// Drop the staged rule version during rollback.
    pub async fn clear_region_rule_stage_on_datanode(
        &self,
        peer: &Peer,
        region_id: RegionId,
    ) -> Result<()> {
        info!(
            "requesting region rule stage clear to datanode {} for region {}",
            peer.id, region_id
        );
        let datanode = self.node_manager.datanode(peer).await;
        let request = RegionRequest {
            header: Some(RegionRequestHeader::default()),
            body: Some(region_request::Body::StageRegionRule(
                StageRegionRuleRequest {
                    region_id: region_id.as_u64(),
                    rule_version: String::new(),
                },
            )),
        };
        datanode
            .handle(request)
            .await
            .map_err(BoxedError::new)
            .context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "failed to clear staged region rule for region {} on datanode {}",
                    region_id, peer.id
                ),
            })?;
        Ok(())
    }

    /// Instruct the datanode to remap manifests for this group.
    pub async fn remap_manifests_on_datanode(
        &self,
        peer: &Peer,
        manifest_request: RemapManifestRequest,
    ) -> Result<RegionResponse> {
        let table_id = manifest_request.table_id;
        let group_id = manifest_request.group_id.clone();
        info!(
            "requesting manifest remap to datanode {} for table {} in group {}",
            peer.id, table_id, group_id
        );
        let datanode = self.node_manager.datanode(peer).await;
        let region_request = RegionRequest {
            header: Some(RegionRequestHeader::default()),
            body: Some(region_request::Body::RemapManifest(manifest_request)),
        };
        let response = datanode
            .handle(region_request)
            .await
            .map_err(BoxedError::new)
            .context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "failed to remap manifests for group {} on datanode {}",
                    group_id, peer.id
                ),
            })?;
        Ok(response)
    }

    /// Publish or discard staged manifests.
    pub async fn apply_staged_manifests_on_datanode(
        &self,
        peer: &Peer,
        manifest_request: ApplyStagedManifestRequest,
    ) -> Result<RegionResponse> {
        let publish = manifest_request.publish;
        let table_id = manifest_request.table_id;
        let group_id = manifest_request.group_id.clone();
        info!(
            "requesting manifest {} on datanode {} for table {} in group {}",
            if publish { "publish" } else { "discard" },
            peer.id,
            table_id,
            group_id
        );
        let datanode = self.node_manager.datanode(peer).await;
        let region_request = RegionRequest {
            header: Some(RegionRequestHeader::default()),
            body: Some(region_request::Body::ApplyStagedManifest(manifest_request)),
        };
        let response = datanode
            .handle(region_request)
            .await
            .map_err(BoxedError::new)
            .context(error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "failed to {} staged manifests for group {} on datanode {}",
                    if publish { "publish" } else { "discard" },
                    group_id,
                    peer.id
                ),
            })?;
        Ok(response)
    }

    /// Store the latest manifest summary for a group.
    pub fn record_manifest_summary(&self, summary: GroupManifestSummary) {
        let mut records = self.manifest_records.lock().unwrap();
        records.insert(summary.group_id, summary);
    }

    pub fn register_group_success(&self, record: GroupRollbackRecord) {
        let mut records = self.rollback_records.lock().unwrap();
        let group_id = record.group_id;
        records.insert(group_id, record);
    }

    pub async fn rollback_registered_groups(&self) -> Result<()> {
        let records: Vec<GroupRollbackRecord> = {
            let mut map = self.rollback_records.lock().unwrap();
            map.drain().map(|(_, record)| record).collect()
        };

        let mut first_err: Option<error::Error> = None;
        for record in records {
            let group_id = record.group_id;
            if let Err(err) =
                RepartitionGroupProcedure::execute_rollback(self.clone(), record).await
            {
                error!(err; "repartition: rollback of group {:?} failed", group_id);
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }

        if let Some(err) = first_err {
            return Err(err);
        }

        Ok(())
    }

    pub fn clear_group_records(&self) {
        self.rollback_records.lock().unwrap().clear();
    }

    /// Collect all manifest summaries recorded so far.
    pub fn manifest_summaries(&self) -> Vec<GroupManifestSummary> {
        let records = self.manifest_records.lock().unwrap();
        records.values().cloned().collect()
    }
}
