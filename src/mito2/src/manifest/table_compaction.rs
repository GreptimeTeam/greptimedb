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

//! Durable, target-local evidence of shared input admission and replacement.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use store_api::metadata::RegionMetadataRef;

use crate::sst::file::FileMeta;

/// Engine protocol capability required before admitting shared SSTs.
pub const TABLE_COMPACTION_PROTOCOL_VERSION: u32 = 1;
/// Bounds the number of receipts removed by one persisted cleanup operation.
pub const MAX_RECEIPTS_PER_CLEANUP: usize = 1024;

/// Immutable identity of the selected result. A retry must not change its generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCompactionIdentity {
    pub job_id: String,
    pub attempt: u32,
    pub plan_digest: String,
    pub result_digest: String,
    /// Admission generation assigned by the durable coordinator, not its leader epoch.
    pub request_epoch: u64,
}

/// Replacement prepared for a target, preserving its existing sequence domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCompactionPublication {
    pub identity: TableCompactionIdentity,
    pub metadata: RegionMetadataRef,
    pub options: serde_json::Value,
    pub inputs: Vec<FileMeta>,
    /// Empty outputs explicitly replace an empty target partition.
    pub outputs: Vec<FileMeta>,
}

/// Operations persisted together with their corresponding file-list changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableCompactionAction {
    Register {
        file: FileMeta,
        request_epoch: u64,
    },
    /// Trusted migration only: the caller proves bulk provenance and fences old compactions.
    Adopt {
        file: FileMeta,
        request_epoch: u64,
    },
    Prepare {
        publication: TableCompactionPublication,
        coordinator_epoch: u64,
    },
    Commit {
        identity: TableCompactionIdentity,
        coordinator_epoch: u64,
    },
    Abort {
        identity: TableCompactionIdentity,
        coordinator_epoch: u64,
    },
    /// The caller has durably closed generations below this floor. Active receipts remain.
    Cleanup {
        min_request_epoch: u64,
        max_receipts: usize,
        coordinator_epoch: u64,
    },
    /// Internal staging/copy operation, never accepted through the coordinator API.
    Remap {
        state: Box<TableCompactionState>,
    },
}

/// The assigned metadata survives removal, so retries cannot re-admit the same source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedSstReceipt {
    pub file: FileMeta,
    pub request_epoch: u64,
    pub retired: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableCompactionPhase {
    Prepared,
    Committed,
    Aborted,
}

/// A terminal receipt does not depend on the continued existence of its outputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCompactionReceipt {
    pub identity: TableCompactionIdentity,
    pub phase: TableCompactionPhase,
    pub preparation: Option<TableCompactionPublication>,
}

/// State shared by manifest checkpoints and worker-applied Version snapshots.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TableCompactionState {
    /// Existing RegionFileId string representation: source_region_id/file_id.
    pub registrations: BTreeMap<String, SharedSstReceipt>,
    pub publications: BTreeMap<String, TableCompactionReceipt>,
    pub coordinator_epoch: u64,
    /// Requests below this floor are rejected even after their receipts have been removed.
    pub min_request_epoch: u64,
}

impl TableCompactionState {
    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty()
            && self.publications.is_empty()
            && self.coordinator_epoch == 0
            && self.min_request_epoch == 0
    }

    pub(crate) fn retire_files<'a>(&mut self, files: impl IntoIterator<Item = &'a FileMeta>) {
        for file in files {
            if let Some(receipt) = self.registrations.get_mut(&file.file_id().to_string()) {
                receipt.retired = true;
            }
        }
    }

    /// Rebinds receipts to the target's post-remap admission sequences.
    pub(crate) fn reconcile_files(&mut self, files: &[FileMeta]) {
        for receipt in self.registrations.values_mut() {
            receipt.retired = true;
        }
        for file in files
            .iter()
            .filter(|file| file.level == crate::sst::file::LEVEL_SHARED)
        {
            if let Some(receipt) = self.registrations.get_mut(&file.file_id().to_string()) {
                receipt.file = file.clone();
                receipt.retired = false;
            }
        }
    }

    /// Combines source histories without losing terminal evidence or expiry fences.
    pub(crate) fn merge_from(
        &mut self,
        other: &Self,
        region_id: store_api::storage::RegionId,
    ) -> crate::error::Result<()> {
        for (key, receipt) in &other.registrations {
            if let Some(existing) = self.registrations.get(key) {
                let mut file = receipt.file.clone();
                file.sequence = existing.file.sequence;
                snafu::ensure!(
                    file == existing.file && receipt.request_epoch == existing.request_epoch,
                    crate::error::InvalidRequestSnafu {
                        region_id,
                        reason: "Conflicting remapped registration"
                    }
                );
            } else {
                self.registrations.insert(key.clone(), receipt.clone());
            }
        }
        for (key, receipt) in &other.publications {
            snafu::ensure!(
                receipt.phase != TableCompactionPhase::Prepared,
                crate::error::InvalidRequestSnafu {
                    region_id,
                    reason: "Cannot remap a prepared publication"
                }
            );
            if let Some(existing) = self.publications.get(key) {
                snafu::ensure!(
                    existing == receipt,
                    crate::error::InvalidRequestSnafu {
                        region_id,
                        reason: "Conflicting remapped publication",
                    }
                );
            } else {
                self.publications.insert(key.clone(), receipt.clone());
            }
        }
        self.coordinator_epoch = self.coordinator_epoch.max(other.coordinator_epoch);
        self.min_request_epoch = self.min_request_epoch.max(other.min_request_epoch);
        Ok(())
    }

    /// Replays a validated manifest operation; admission checks belong to the worker.
    pub(crate) fn apply(&mut self, action: TableCompactionAction) {
        match action {
            TableCompactionAction::Register {
                file,
                request_epoch,
            }
            | TableCompactionAction::Adopt {
                file,
                request_epoch,
            } => {
                self.registrations
                    .entry(file.file_id().to_string())
                    .or_insert(SharedSstReceipt {
                        file,
                        request_epoch,
                        retired: false,
                    });
            }
            TableCompactionAction::Prepare {
                publication,
                coordinator_epoch,
            } => {
                self.coordinator_epoch = self.coordinator_epoch.max(coordinator_epoch);
                self.publications
                    .entry(publication.identity.job_id.clone())
                    .or_insert_with(|| TableCompactionReceipt {
                        identity: publication.identity.clone(),
                        phase: TableCompactionPhase::Prepared,
                        preparation: Some(publication),
                    });
            }
            TableCompactionAction::Commit {
                identity,
                coordinator_epoch,
            } => {
                self.finish(identity, coordinator_epoch, TableCompactionPhase::Committed);
            }
            TableCompactionAction::Abort {
                identity,
                coordinator_epoch,
            } => {
                self.finish(identity, coordinator_epoch, TableCompactionPhase::Aborted);
            }
            TableCompactionAction::Cleanup {
                min_request_epoch,
                max_receipts,
                coordinator_epoch,
            } => {
                self.coordinator_epoch = self.coordinator_epoch.max(coordinator_epoch);
                self.min_request_epoch = self.min_request_epoch.max(min_request_epoch);
                self.clean_receipts(max_receipts);
            }
            TableCompactionAction::Remap { state } => *self = *state,
        }
    }

    fn finish(
        &mut self,
        identity: TableCompactionIdentity,
        epoch: u64,
        phase: TableCompactionPhase,
    ) {
        self.coordinator_epoch = self.coordinator_epoch.max(epoch);
        self.publications.insert(
            identity.job_id.clone(),
            TableCompactionReceipt {
                identity,
                phase,
                preparation: None,
            },
        );
    }

    fn clean_receipts(&mut self, mut remaining: usize) {
        self.registrations.retain(|_, receipt| {
            if remaining > 0 && receipt.retired && receipt.request_epoch < self.min_request_epoch {
                remaining -= 1;
                false
            } else {
                true
            }
        });
        self.publications.retain(|_, receipt| {
            if remaining > 0
                && receipt.phase != TableCompactionPhase::Prepared
                && receipt.identity.request_epoch < self.min_request_epoch
            {
                remaining -= 1;
                false
            } else {
                true
            }
        });
    }
}
