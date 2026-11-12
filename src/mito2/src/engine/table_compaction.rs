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
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use crate::engine::MitoEngine;
use crate::error::{
    Error, InvalidRequestSnafu, RecvSnafu, RegionBusySnafu, RegionNotFoundSnafu, Result,
    SerdeJsonSnafu,
};
use crate::manifest::action::RegionEdit;
use crate::region::options::RegionOptions;
use crate::region::{RegionLeaderState, RegionRoleState};
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
    Applied,
    /// The captured inputs/schema/options no longer match. Re-snapshot and replan.
    StaleInputs,
}

impl MitoEngine {
    pub fn table_compaction_snapshot(
        &self,
        region_id: RegionId,
    ) -> Result<TableCompactionSnapshot> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(
            region.state() == RegionRoleState::Leader(RegionLeaderState::Writable),
            RegionBusySnafu { region_id }
        );
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
        // The public snapshot keeps JSON options; parse them once here so the
        // worker compares typed options and reads typed flags without reparsing.
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
