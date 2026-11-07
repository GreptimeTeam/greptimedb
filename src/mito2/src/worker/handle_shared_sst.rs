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

use std::collections::HashSet;
use std::num::NonZeroU64;
use std::sync::Arc;

use common_telemetry::warn;
use common_time::Timestamp;
use futures::FutureExt;
use snafu::{OptionExt, ensure};
use store_api::logstore::LogStore;

use crate::engine::shared_sst::{
    ApplySharedCompaction, RegisterSharedSst, SharedSstFinished, SharedSstOperation,
    SharedSstRequest, SharedSstResponse, verify_outputs,
};
use crate::error::{
    InvalidRequestSnafu, RegionBusySnafu, RegionNotFoundSnafu, Result, UnexpectedSnafu,
};
use crate::manifest::action::{RegionEdit, RegionManifest, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::RegionManifestManager;
use crate::region::options::RegionOptions;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{BackgroundNotify, WorkerRequest, WorkerRequestWithTime};
use crate::sst::file::{LEVEL_SHARED, known_max_input_sequence};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) fn handle_shared_sst(&mut self, request: SharedSstRequest) {
        let SharedSstRequest {
            region_id,
            operation,
            sender,
        } = request;
        let Some(region) = self.regions.get_region(region_id) else {
            let _ = sender.send(RegionNotFoundSnafu { region_id }.fail());
            return;
        };
        if !region.is_writable() || self.flush_scheduler.has_pending_alter(region_id) {
            let _ = sender.send(RegionBusySnafu { region_id }.fail());
            return;
        }
        let current = region.version_control.current();
        let sequence = if let SharedSstOperation::Register(request) = &operation {
            // A retry of a live registration never reserves another sequence.
            if current.version.ssts.levels()[LEVEL_SHARED as usize]
                .files
                .contains_key(&request.file.file_id)
            {
                None
            } else {
                let Some(sequence) = current
                    .last_sequence
                    .checked_add(1)
                    .and_then(NonZeroU64::new)
                else {
                    let _ = sender.send(
                        InvalidRequestSnafu {
                            region_id,
                            reason: "Region sequence exhausted",
                        }
                        .fail(),
                    );
                    return;
                };
                Some(sequence)
            }
        } else {
            None
        };
        if let Err(error) = region.set_editing(RegionLeaderState::Writable) {
            let _ = sender.send(Err(error));
            return;
        }
        if let Some(sequence) = sequence {
            region.version_control.update_last_sequence(sequence.get());
        }
        let request_sender = self.sender.clone();
        common_runtime::spawn_global(async move {
            let result = std::panic::AssertUnwindSafe(execute(&region, &operation, sequence))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    UnexpectedSnafu {
                        reason: "Shared SST operation panicked",
                    }
                    .fail()
                });
            let notify = WorkerRequest::Background {
                region_id,
                notify: BackgroundNotify::SharedSst(Box::new(SharedSstFinished {
                    region,
                    sender,
                    result,
                })),
            };
            let _ = request_sender
                .send(WorkerRequestWithTime::new(notify))
                .await;
        });
    }

    pub(crate) async fn handle_shared_sst_finished(&mut self, finished: SharedSstFinished) {
        let SharedSstFinished {
            region: captured,
            sender,
            result,
        } = finished;
        let region_id = captured.region_id;
        let Some(region) = self
            .regions
            .get_region(region_id)
            .filter(|region| Arc::ptr_eq(region, &captured))
        else {
            let _ = sender.send(RegionNotFoundSnafu { region_id }.fail());
            return;
        };
        let mut needs_compaction = false;
        let response = result.map(|(edit, response)| {
            needs_compaction =
                matches!(response, SharedSstResponse::Applied) && !edit.files_to_add.is_empty();
            if !edit.files_to_add.is_empty() || !edit.files_to_remove.is_empty() {
                region
                    .version_control
                    .apply_edit(Some(edit), &[], region.file_purger.clone());
            }
            response
        });
        region.switch_state_to_writable(RegionLeaderState::Editing);
        let _ = sender.send(response);
        self.handle_region_stalled_requests(&region_id, false).await;
        self.dispatch_pending_region_requests(region_id).await;
        if needs_compaction && self.config.schedule_compaction_after_edit {
            self.schedule_compaction(&region).await;
        }
    }
}

async fn execute(
    region: &MitoRegionRef,
    operation: &SharedSstOperation,
    sequence: Option<NonZeroU64>,
) -> Result<(RegionEdit, SharedSstResponse)> {
    // Reject stale input plans before reading possibly already-reclaimed outputs.
    {
        let manager = region.manifest_ctx.manifest_manager.read().await;
        check_region(region, &manager)?;
        let (edit, response) = build_edit(
            &manager.manifest(),
            &region.version_control.current().version.options,
            operation,
            sequence,
        )?;
        if edit.files_to_add.is_empty() && edit.files_to_remove.is_empty() {
            return Ok((edit, response));
        }
    }
    if let SharedSstOperation::Apply(request) = operation {
        let footers = verify_outputs(&region.access_layer, request).await?;
        if let Some(hook) = region.manifest_ctx.hook() {
            let files = request
                .outputs
                .iter()
                .zip(&footers)
                .map(
                    |(file_meta, footer)| crate::engine::shared_sst::VerifiedSharedSst {
                        file_meta,
                        footer,
                    },
                )
                .collect::<Vec<_>>();
            hook.on_shared_sst_files_verified(region.region_id, &request.metadata, &files)
                .await;
        }
    }
    let mut manager = region.manifest_ctx.manifest_manager.write().await;
    check_region(region, &manager)?;
    let (edit, response) = build_edit(
        &manager.manifest(),
        &region.version_control.current().version.options,
        operation,
        sequence,
    )?;
    let pending = region
        .manifest_ctx
        .update_locked(
            &mut manager,
            RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone())),
            false,
        )
        .await?;
    drop(manager);
    // A hook failure must not turn a durable edit into an unapplied worker version.
    if std::panic::AssertUnwindSafe(pending.fire())
        .catch_unwind()
        .await
        .is_err()
    {
        warn!(
            "Manifest hook panicked after shared SST commit, region: {}",
            region.region_id
        );
    }
    Ok((edit, response))
}

fn check_region(region: &MitoRegionRef, manager: &RegionManifestManager) -> Result<()> {
    ensure!(
        !manager.is_stopped()
            && matches!(
                region.state(),
                RegionRoleState::Leader(
                    RegionLeaderState::Editing | RegionLeaderState::Downgrading
                )
            ),
        RegionBusySnafu {
            region_id: region.region_id
        }
    );
    Ok(())
}

fn build_edit(
    manifest: &RegionManifest,
    options: &RegionOptions,
    operation: &SharedSstOperation,
    sequence: Option<NonZeroU64>,
) -> Result<(RegionEdit, SharedSstResponse)> {
    match operation {
        SharedSstOperation::Register(request) => register(manifest, request, sequence),
        SharedSstOperation::Apply(request) => apply(manifest, options, request),
    }
}

fn register(
    manifest: &RegionManifest,
    request: &RegisterSharedSst,
    sequence: Option<NonZeroU64>,
) -> Result<(RegionEdit, SharedSstResponse)> {
    let region_id = manifest.metadata.region_id;
    let file = &request.file;
    ensure!(
        request.metadata.region_id == region_id
            && file.region_id.table_id() == region_id.table_id()
            && file.level == LEVEL_SHARED
            && file.sequence.is_none()
            && !file.preserve_row_sequence
            && file.partition_expr.is_none()
            && file.time_range.0 <= file.time_range.1,
        InvalidRequestSnafu {
            region_id,
            reason: "Invalid shared SST registration or changed schema"
        }
    );
    if let Some(current) = manifest.files.get(&file.file_id) {
        let mut expected = file.clone();
        expected.sequence = current.sequence;
        ensure!(
            expected == *current && current.sequence.is_some(),
            InvalidRequestSnafu {
                region_id,
                reason: "Conflicting shared SST registration"
            }
        );
        return Ok((
            RegionEdit::default(),
            SharedSstResponse::Registered(Box::new(current.clone())),
        ));
    }
    ensure!(
        request.manifest_version == manifest.manifest_version
            && request.metadata == manifest.metadata,
        InvalidRequestSnafu {
            region_id,
            reason: "Shared SST registration snapshot expired; do not re-admit an ambiguous request"
        }
    );
    let sequence = sequence.context(InvalidRequestSnafu {
        region_id,
        reason: "Shared SST sequence was not reserved",
    })?;
    let mut accepted = file.clone();
    accepted.sequence = Some(sequence);
    let edit = RegionEdit {
        files_to_add: vec![accepted.clone()],
        committed_sequence: Some(sequence.get()),
        timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
        ..Default::default()
    };
    Ok((edit, SharedSstResponse::Registered(Box::new(accepted))))
}

fn apply(
    manifest: &RegionManifest,
    options: &RegionOptions,
    request: &ApplySharedCompaction,
) -> Result<(RegionEdit, SharedSstResponse)> {
    let window = shared_window(request)?;
    let Some(ids) = validate_inputs(manifest, request, window)? else {
        return Ok((RegionEdit::default(), SharedSstResponse::InputsGone));
    };
    ensure!(
        manifest.metadata == request.metadata && options == &request.options,
        InvalidRequestSnafu {
            region_id: manifest.metadata.region_id,
            reason: "Shared compaction snapshot changed"
        }
    );
    validate_outputs(manifest, request, window, ids)?;
    Ok((
        RegionEdit {
            files_to_add: request.outputs.clone(),
            files_to_remove: request.inputs.clone(),
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            ..Default::default()
        },
        SharedSstResponse::Applied,
    ))
}

fn shared_window(request: &ApplySharedCompaction) -> Result<(Timestamp, Timestamp)> {
    let region_id = request.metadata.region_id;
    let end = request.window_start.checked_add(request.window_seconds);
    ensure!(
        request.window_seconds > 0
            && end.is_some()
            && request.window_start.rem_euclid(request.window_seconds) == 0,
        InvalidRequestSnafu {
            region_id,
            reason: "Invalid shared compaction target or window"
        }
    );
    let start = Timestamp::new_second(request.window_start);
    let end = Timestamp::new_second(end.context(InvalidRequestSnafu {
        region_id,
        reason: "Window overflow",
    })?);
    Ok((start, end))
}

fn validate_inputs(
    manifest: &RegionManifest,
    request: &ApplySharedCompaction,
    window: (Timestamp, Timestamp),
) -> Result<Option<HashSet<store_api::storage::FileId>>> {
    let region_id = manifest.metadata.region_id;
    ensure!(
        request.metadata.region_id == region_id && !request.inputs.is_empty(),
        InvalidRequestSnafu {
            region_id,
            reason: "Invalid shared compaction target or inputs"
        }
    );
    let (start, end) = window;
    let mut ids = HashSet::new();
    let mut present = 0;
    for file in &request.inputs {
        ensure!(
            file.level == LEVEL_SHARED
                && file.region_id.table_id() == region_id.table_id()
                && ids.insert(file.file_id)
                && file.time_range.0 >= start
                && file.time_range.1 < end,
            InvalidRequestSnafu {
                region_id,
                reason: "Invalid shared compaction input"
            }
        );
        if let Some(current) = manifest.files.get(&file.file_id) {
            ensure!(
                current == file,
                InvalidRequestSnafu {
                    region_id,
                    reason: "Shared input changed"
                }
            );
            present += 1;
        }
    }
    if present == 0 {
        return Ok(None);
    }
    ensure!(
        present == request.inputs.len(),
        InvalidRequestSnafu {
            region_id,
            reason: "Shared compaction inputs are only partially present"
        }
    );
    Ok(Some(ids))
}

fn validate_outputs(
    manifest: &RegionManifest,
    request: &ApplySharedCompaction,
    window: (Timestamp, Timestamp),
    mut ids: HashSet<store_api::storage::FileId>,
) -> Result<()> {
    let region_id = manifest.metadata.region_id;
    let options = &request.options;
    let (start, end) = window;
    let partition_expr =
        crate::region::parse_partition_expr(request.metadata.partition_expr.as_deref())?;
    let sequence = known_max_input_sequence(request.inputs.iter());
    let trusted = options.preserve_row_sequence
        && request
            .inputs
            .iter()
            .all(|file| file.is_effective_target_sequence_trusted(region_id));
    for file in &request.outputs {
        ensure!(
            file.region_id == region_id
                && file.level == 1
                && ids.insert(file.file_id)
                && !manifest.files.contains_key(&file.file_id)
                && file.time_range.0 >= start
                && file.time_range.1 < end
                && file.time_range.0 <= file.time_range.1
                && file.sequence == sequence
                && file.preserve_row_sequence == trusted
                && file.partition_expr == partition_expr,
            InvalidRequestSnafu {
                region_id,
                reason: "Invalid shared compaction output"
            }
        );
    }
    Ok(())
}
