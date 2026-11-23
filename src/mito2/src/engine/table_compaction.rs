// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! One-shot publication of already-admitted shared inputs. Task tracking is external.

pub(crate) mod output;
#[cfg(test)]
mod tests;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{FileId, RegionId};
use tokio::sync::oneshot;

use crate::engine::MitoEngine;
use crate::error::{
    Error, InvalidRequestSnafu, RecvSnafu, RegionBusySnafu, RegionNotFoundSnafu, Result,
    SerdeJsonSnafu,
};
use crate::manifest::action::RegionEdit;
use crate::region::options::RegionOptions;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{RegionEditKind, RegionEditRequest, WorkerRequest};
use crate::sst::file::{FileMeta, SHARED_LEVEL_SLOT};

/// A query-visible input snapshot. Callers may select a subset of `inputs` for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCompactionSnapshot {
    pub metadata: RegionMetadataRef,
    pub options: serde_json::Value,
    pub inputs: Vec<FileMeta>,
}

/// Transient publication preconditions, never stored in a manifest.
#[derive(Debug)]
pub(crate) struct PublicationContext {
    pub metadata: RegionMetadataRef,
    pub options: RegionOptions,
}

/// A result for this publication attempt, not a durable task receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationOutcome {
    /// This attempt installed the replacement, or its nonempty outputs are still fully visible
    /// with all of its inputs absent.
    Applied,
    /// The schema or file set no longer matches this publication. Re-snapshot and replan.
    StaleInputs,
}

/// Returns whether both snapshots describe the same table-compaction target range.
/// ALTER may change the schema while compaction runs. Publication verifies the
/// output footer against the captured schema and checks the input files separately.
pub fn is_table_compaction_metadata_compatible(
    captured: &RegionMetadata,
    current: &RegionMetadata,
) -> bool {
    captured.region_id == current.region_id
        && captured.partition_expr_version == current.partition_expr_version
        && captured.partition_expr == current.partition_expr
}

impl MitoEngine {
    /// Reads committed inputs while an ordinary edit or ALTER is in progress.
    pub fn table_compaction_snapshot(
        &self,
        region_id: RegionId,
    ) -> Result<TableCompactionSnapshot> {
        let region = self.committed_compaction_region(region_id)?;
        let current = region.version_control.current();
        Ok(TableCompactionSnapshot {
            metadata: current.version.metadata.clone(),
            options: serde_json::to_value(&current.version.options).context(SerdeJsonSnafu)?,
            inputs: current.version.ssts.levels()[SHARED_LEVEL_SLOT]
                .files()
                .map(|file| file.meta_ref().clone())
                .collect(),
        })
    }

    /// Reads committed shared inputs for admission checks, without requiring an idle worker.
    ///
    /// An empty selection reads only metadata and options. Lookups are bounded by the requested
    /// file ids, not the region's file count. Editing/Altering leave the committed version readable;
    /// actual admission still goes through the worker queue and its lifecycle checks.
    /// This is not a compaction planning snapshot or a reservation of the selected inputs.
    pub fn shared_ingest_snapshot(
        &self,
        region_id: RegionId,
        file_ids: &[FileId],
    ) -> Result<TableCompactionSnapshot> {
        let region = self.committed_compaction_region(region_id)?;
        let current = region.version_control.current();
        let shared = &current.version.ssts.levels()[SHARED_LEVEL_SLOT];
        Ok(TableCompactionSnapshot {
            metadata: current.version.metadata.clone(),
            options: serde_json::to_value(&current.version.options).context(SerdeJsonSnafu)?,
            inputs: file_ids
                .iter()
                .filter_map(|id| shared.files.get(id))
                .map(|file| file.meta_ref().clone())
                .collect(),
        })
    }

    fn committed_compaction_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(
            match region.state() {
                RegionRoleState::Leader(
                    RegionLeaderState::Writable | RegionLeaderState::Altering,
                ) => true,
                RegionRoleState::Leader(RegionLeaderState::Editing) =>
                    region.manifest_ctx.staging_partition_info().is_none(),
                _ => false,
            },
            RegionBusySnafu { region_id }
        );
        Ok(region)
    }

    /// Publishes one region independently, using compaction rather than import sequences.
    ///
    /// Duplicate submissions are allowed. A stale outcome does NOT prove these
    /// outputs were never published (an earlier ACK may have been lost). Never
    /// blindly delete outputs on stale/error: keep their protection until recovery
    /// or manifest-aware GC determines liveness. No input registration is performed.
    /// Input admission must be complete/quiescent; a compaction retry must not replay
    /// imports. The caller must produce a logically equivalent replacement under this snapshot.
    pub async fn publish_table_compaction(
        &self,
        region_id: RegionId,
        snapshot: TableCompactionSnapshot,
        outputs: Vec<FileMeta>,
    ) -> Result<PublicationOutcome> {
        ensure!(
            snapshot.metadata.region_id == region_id && !snapshot.inputs.is_empty(),
            InvalidRequestSnafu {
                region_id,
                reason: "Publication requires this region's nonempty input snapshot"
            }
        );
        let options = serde_json::from_value(snapshot.options).context(SerdeJsonSnafu)?;
        let edit = RegionEdit {
            files_to_remove: snapshot.inputs,
            files_to_add: outputs,
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            ..Default::default()
        };
        let (tx, rx) = oneshot::channel();
        let mut request = RegionEditRequest::new(region_id, edit, false, tx);
        request.kind = RegionEditKind::Compaction(Box::new(PublicationContext {
            metadata: snapshot.metadata,
            options,
        }));
        self.inner
            .workers
            .submit_to_worker(region_id, WorkerRequest::EditRegion(request))
            .await?;
        match rx.await.context(RecvSnafu)? {
            Ok(()) => Ok(PublicationOutcome::Applied),
            Err(Error::EditRegion { source, .. })
                if matches!(source.as_ref(), Error::StaleCompactionExecution { .. }) =>
            {
                Ok(PublicationOutcome::StaleInputs)
            }
            Err(error) => Err(error),
        }
    }
}
