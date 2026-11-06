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

//! Table-specific admission policy for internal copy and staging operations.

use std::num::NonZeroU64;

use snafu::{OptionExt, ensure};

use crate::error::{InvalidRequestSnafu, Result};
use crate::manifest::action::RegionEdit;
use crate::manifest::table_compaction::TableCompactionAction;
use crate::region::MitoRegionRef;
use crate::sst::file::{FileMeta, LEVEL_SHARED, MAX_LEVEL};

/// Returns whether this edit was handled as an internal receipt remap.
pub(crate) fn prepare_remap(region: &MitoRegionRef, edit: &mut RegionEdit) -> Result<bool> {
    let Some(TableCompactionAction::Remap { state }) =
        edit.extensions.table_compaction.as_deref_mut()
    else {
        return Ok(false);
    };
    let region_id = region.region_id;
    let current = region.version_control.current();
    let existing = &current.version.extensions.table_compaction;
    ensure!(
        state.min_request_epoch >= existing.min_request_epoch
            && edit.files_to_add.iter().all(|file| file.level < MAX_LEVEL
                && (file.level != LEVEL_SHARED
                    || state
                        .registrations
                        .contains_key(&file.file_id().to_string()))),
        InvalidRequestSnafu {
            region_id,
            reason: "Invalid receipt staging promotion"
        }
    );
    let existing_sequence = |file: &FileMeta| {
        (file.level == LEVEL_SHARED)
            .then(|| existing.registrations.get(&file.file_id().to_string()))
            .flatten()
            .and_then(|receipt| receipt.file.sequence)
    };
    // In-place repartition must not re-admit a reference already known to this target.
    let sequence = if edit
        .files_to_add
        .iter()
        .any(|file| existing_sequence(file).is_none())
    {
        let sequence = current
            .last_sequence
            .checked_add(1)
            .context(InvalidRequestSnafu {
                region_id,
                reason: "Region sequence exhausted",
            })?;
        edit.committed_sequence = Some(sequence);
        NonZeroU64::new(sequence)
    } else {
        None
    };
    for file in &mut edit.files_to_add {
        file.sequence = existing_sequence(file).or(sequence);
        file.preserve_row_sequence = false;
    }
    state.merge_from(existing, region_id)?;
    let mut files = current
        .version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .map(|file| file.meta_ref().clone())
        .collect::<Vec<_>>();
    files.extend(edit.files_to_add.iter().cloned());
    state.reconcile_files(&files);
    Ok(true)
}
