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

use crate::engine::table_compaction::{PublicationContext, output};
use crate::error::{InvalidRequestSnafu, RegionBusySnafu, Result, StaleCompactionExecutionSnafu};
use crate::manifest::action::{RegionEdit, RegionManifest, RegionMetaAction, RegionMetaActionList};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::sst::file::LEVEL_SHARED;

pub(crate) async fn publish(
    region: &MitoRegionRef,
    context: &PublicationContext,
    edit: RegionEdit,
) -> Result<()> {
    let snapshot = region.manifest_ctx.manifest().await;
    // An old result can name outputs already compacted away. Reject stale inputs
    // before inspecting those objects; absence of an output is not a task receipt.
    ensure!(
        is_current(region, context, &edit, &snapshot),
        StaleCompactionExecutionSnafu {
            region_id: region.region_id
        }
    );
    validate_files(region, context, &edit, &snapshot)?;
    output::verify_and_notify(region, &context.metadata, &edit.files_to_add).await?;

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
    let manifest = manager.manifest();
    ensure!(
        is_current(region, context, &edit, &manifest),
        StaleCompactionExecutionSnafu {
            region_id: region.region_id
        }
    );
    validate_files(region, context, &edit, &manifest)?;
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
    Ok(())
}

fn is_current(
    region: &MitoRegionRef,
    context: &PublicationContext,
    edit: &RegionEdit,
    manifest: &RegionManifest,
) -> bool {
    let current = region.version_control.current();
    context.metadata == manifest.metadata
        && context.options == current.version.options
        && !edit.files_to_remove.is_empty()
        && edit
            .files_to_remove
            .iter()
            .all(|file| manifest.files.get(&file.file_id) == Some(file))
}

fn validate_files(
    region: &MitoRegionRef,
    context: &PublicationContext,
    edit: &RegionEdit,
    manifest: &RegionManifest,
) -> Result<()> {
    let region_id = region.region_id;
    let mut inputs = HashSet::new();
    ensure!(
        edit.files_to_remove
            .iter()
            .all(|file| file.level == LEVEL_SHARED
                && file.region_id.table_id() == region_id.table_id()
                && inputs.insert(file.file_id())),
        InvalidRequestSnafu {
            region_id,
            reason: "Expected distinct, already-admitted shared inputs"
        }
    );
    let trusted = context.options.preserve_row_sequence
        && edit.files_to_remove.iter().all(|file| {
            if file.region_id == region_id {
                file.preserve_row_sequence
            } else {
                file.sequence.is_some()
            }
        });
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
                && !manifest.files.contains_key(&file.file_id)
                && file.partition_expr == partition
                && file.preserve_row_sequence == trusted
                && file.sequence == sequence),
        InvalidRequestSnafu {
            region_id,
            reason: "Invalid region-local compaction output"
        }
    );
    Ok(())
}
