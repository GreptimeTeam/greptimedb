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

//! At-least-once requests, conditional file publication, no durable task state.

use std::collections::HashSet;

use snafu::ensure;

use crate::engine::table_compaction::{
    PublicationContext, is_table_compaction_metadata_compatible, output,
};
use crate::error::{InvalidRequestSnafu, RegionBusySnafu, Result, StaleCompactionExecutionSnafu};
use crate::manifest::action::{RegionEdit, RegionManifest, RegionMetaAction, RegionMetaActionList};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::RegionEditOutcome;
use crate::sst::file::{FileMeta, LEVEL_SHARED};

pub(crate) async fn publish(
    region: &MitoRegionRef,
    context: &PublicationContext,
    edit: RegionEdit,
) -> Result<RegionEditOutcome> {
    let snapshot = region.manifest_ctx.manifest().await;
    let replay = is_replay(region, context, &edit, &snapshot)?;
    validate_files(region, context, &edit)?;
    if !replay {
        check_sequence_policy(region, context, &edit)?;
        output::verify_and_notify(region, &context.metadata, &edit.files_to_add).await?;
    }

    let mut manager = region.manifest_ctx.manifest_manager.write().await;
    ensure!(
        matches!(
            region.state(),
            RegionRoleState::Leader(RegionLeaderState::Editing | RegionLeaderState::Downgrading)
        ) && !manager.is_stopped(),
        RegionBusySnafu {
            region_id: region.region_id
        }
    );
    if is_replay(region, context, &edit, &manager.manifest())? {
        region
            .manifest_ctx
            .ensure_leader_lease_or_downgrading(region.region_id)?;
        region.manifest_ctx.ensure_commit_deadline(&manager)?;
        return Ok(RegionEditOutcome::AlreadyApplied);
    }
    // A previously satisfied request skipped object verification.
    ensure!(
        !replay,
        StaleCompactionExecutionSnafu {
            region_id: region.region_id
        }
    );
    check_sequence_policy(region, context, &edit)?;
    let pending = match region
        .manifest_ctx
        .update_locked(
            &mut manager,
            RegionMetaActionList::with_action(RegionMetaAction::Edit(edit)),
            false,
        )
        .await
    {
        Ok(pending) => pending,
        Err(error) => {
            if error.may_have_persisted_manifest_update() {
                // Do not accept another publication against a potentially stale
                // in-memory file set. Completion removes this incarnation.
                manager.stop().await;
            }
            return Err(error);
        }
    };
    drop(manager);
    pending.fire().await;
    Ok(RegionEditOutcome::Written)
}

fn check_sequence_policy(
    region: &MitoRegionRef,
    context: &PublicationContext,
    edit: &RegionEdit,
) -> Result<()> {
    // An exact scan can skip an unmarked input below its lower bound, but
    // an unmarked replacement can inherit a higher sequence from another input.
    if !context.options.preserve_row_sequence && region.version().options.preserve_row_sequence {
        ensure!(
            edit.files_to_add
                .iter()
                .all(|file| file.preserve_row_sequence),
            StaleCompactionExecutionSnafu {
                region_id: region.region_id
            }
        );
    }
    Ok(())
}

fn is_replay(
    region: &MitoRegionRef,
    context: &PublicationContext,
    edit: &RegionEdit,
    manifest: &RegionManifest,
) -> Result<bool> {
    let region_id = region.region_id;
    ensure!(
        is_table_compaction_metadata_compatible(&context.metadata, &manifest.metadata)
            && !edit.files_to_remove.is_empty(),
        StaleCompactionExecutionSnafu { region_id }
    );
    let matches =
        |file: &FileMeta| manifest.files.get(&file.file_id).map(AsRef::as_ref) == Some(file);
    if edit.files_to_remove.iter().all(matches) {
        ensure!(
            edit.files_to_add
                .iter()
                .all(|file| !manifest.files.contains_key(&file.file_id)),
            InvalidRequestSnafu {
                region_id,
                reason: "Invalid region-local compaction output"
            }
        );
        return Ok(false);
    }

    // With no durable receipt, only a still-visible, nonempty replacement proves success.
    ensure!(
        edit.files_to_remove
            .iter()
            .all(|file| !manifest.files.contains_key(&file.file_id))
            && !edit.files_to_add.is_empty()
            && edit.files_to_add.iter().all(matches),
        StaleCompactionExecutionSnafu { region_id }
    );
    Ok(true)
}

fn validate_files(
    region: &MitoRegionRef,
    context: &PublicationContext,
    edit: &RegionEdit,
) -> Result<()> {
    let region_id = region.region_id;
    let mut inputs = HashSet::new();
    ensure!(
        edit.files_to_remove
            .iter()
            .all(|file| file.level == LEVEL_SHARED
                && file.region_id.table_id() == region_id.table_id()
                && inputs.insert(file.file_id)),
        InvalidRequestSnafu {
            region_id,
            reason: "Expected distinct, already-admitted shared inputs"
        }
    );
    let trusted = context.options.preserve_row_sequence
        && edit
            .files_to_remove
            .iter()
            .all(|file| file.is_effective_target_sequence_trusted(region_id));
    let partition =
        crate::region::parse_partition_expr(context.metadata.partition_expr.as_deref())?;
    let sequence = edit
        .files_to_remove
        .iter()
        .map(|file| file.sequence)
        .collect::<Option<Vec<_>>>()
        .and_then(|sequences| sequences.into_iter().max());
    let mut outputs = HashSet::new();
    ensure!(
        edit.files_to_add
            .iter()
            .all(|file| file.region_id == region_id
                && file.level == 0
                && outputs.insert(file.file_id)
                && file.partition_expr.as_deref() == partition.as_ref()
                && file.preserve_row_sequence == trusted
                && file.sequence == sequence),
        InvalidRequestSnafu {
            region_id,
            reason: "Invalid region-local compaction output"
        }
    );
    Ok(())
}
