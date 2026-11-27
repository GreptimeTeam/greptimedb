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

use std::collections::BTreeMap;
use std::time::Duration;

use client::OutputWithMetrics;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::adapter::SubstitutedRowRanges;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;

use crate::batching_mode::checkpoint::{
    FlowCheckpointDecision, FlowQueryFallbackReason, checkpoint_mode_label,
};
use crate::batching_mode::state::{CheckpointMode, DirtyTimeWindows, TaskState};
use crate::batching_mode::task::{BatchingTask, QueryCoverage};
use crate::error::UnexpectedSnafu;
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_CHECKPOINT_DECISION_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_MODE_CNT,
};
use crate::{Error, FlowId};

impl BatchingTask {
    /// Classify execution errors into checkpoint fallback reasons. A stale
    /// snapshot fence is special only for fenced repair chunks.
    pub(super) fn query_failure_reason(
        err: &Error,
        coverage: &QueryCoverage,
    ) -> FlowQueryFallbackReason {
        if err.status_code() == StatusCode::RequestOutdated {
            if matches!(coverage, QueryCoverage::FencedRepairChunk { .. }) {
                FlowQueryFallbackReason::SnapshotFenceExpired
            } else {
                FlowQueryFallbackReason::StaleCursor
            }
        } else if matches!(coverage, QueryCoverage::IncrementalDelta) {
            FlowQueryFallbackReason::IncrementalQueryFailure
        } else {
            FlowQueryFallbackReason::QueryFailure
        }
    }

    /// Apply the conservative state transition for a failed query. This never
    /// advances checkpoints; it only decides whether to abandon repair state or
    /// fall back from incremental mode.
    pub(super) fn apply_query_failure_to_state(
        state: &mut TaskState,
        elapsed: Duration,
        coverage: &QueryCoverage,
        reason: FlowQueryFallbackReason,
    ) -> Option<FlowCheckpointDecision> {
        state.after_query_exec(elapsed, false);
        let checkpoint_mode = state.checkpoint_mode();
        if matches!(coverage, QueryCoverage::FencedRepairChunk { .. })
            && matches!(reason, FlowQueryFallbackReason::SnapshotFenceExpired)
        {
            // `abandon_fenced_repair()` restores the not-yet-in-flight pending
            // windows from the stale repair back to live dirty windows. The
            // currently executing chunk is restored separately by the outer
            // failure path (`handle_executed_query_failure`), after this state
            // transition has removed `pending_fenced_repair`, so it also lands
            // in live dirty windows for the next scoped base repair.
            state.abandon_fenced_repair();
            return Some(FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: checkpoint_mode,
                reason,
            });
        }

        if checkpoint_mode == CheckpointMode::Incremental {
            state.mark_full_snapshot();
        }
        Some(FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: checkpoint_mode,
            reason,
        })
    }

    /// Apply checkpoint transitions for a successfully executed query using its
    /// terminal watermark proof and declared coverage.
    pub(super) fn apply_query_result_to_state(
        state: &mut TaskState,
        res: &OutputWithMetrics,
        elapsed: Duration,
        coverage: &QueryCoverage,
    ) -> FlowCheckpointDecision {
        state.after_query_exec(elapsed, true);
        let checkpoint_mode = state.checkpoint_mode();
        if let (Some(participating_regions), Some(watermark_map)) =
            (res.participating_regions(), res.region_watermark_map())
        {
            let participating_region_count = participating_regions.len();
            let watermark_count = watermark_map.len();
            match coverage {
                QueryCoverage::ScopedBaseRepair => {
                    if !state.can_advance_full_snapshot_checkpoints(
                        &participating_regions,
                        &watermark_map,
                    ) {
                        return FlowCheckpointDecision::FallbackToFullSnapshot {
                            previous_mode: checkpoint_mode,
                            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
                        };
                    }

                    if state.is_incremental_disabled() {
                        return FlowCheckpointDecision::FallbackToFullSnapshot {
                            previous_mode: CheckpointMode::FullSnapshot,
                            reason: FlowQueryFallbackReason::IncrementalDisabled,
                        };
                    }

                    if state.dirty_time_windows.is_empty() {
                        state.advance_checkpoints(watermark_map);
                        FlowCheckpointDecision::AdvancedFromFullSnapshot {
                            participating_regions: participating_region_count,
                            watermarks: watermark_count,
                        }
                    } else if let Some(repair) =
                        state.start_fenced_repair(watermark_map.into_iter().collect())
                    {
                        FlowCheckpointDecision::ContinuedFencedRepair {
                            pending_windows: repair.pending_windows().len(),
                            watermarks: repair.high().len(),
                        }
                    } else {
                        FlowCheckpointDecision::FallbackToFullSnapshot {
                            previous_mode: checkpoint_mode,
                            reason: FlowQueryFallbackReason::DirtyBacklogPending,
                        }
                    }
                }
                QueryCoverage::FencedRepairChunk { .. } => {
                    if !state
                        .fenced_repair_watermarks_match_high(&participating_regions, &watermark_map)
                    {
                        // A successful repair chunk whose terminal watermark
                        // differs from the frozen fence `H` cannot prove that
                        // continuing the same repair is safe. The pre-`H`
                        // repair work item already executed under the snapshot
                        // fence, so do not requeue it; restore only
                        // not-yet-in-flight pending windows, then start over
                        // with a fresh H. Later/post-`H` writes still rely on
                        // live dirty notifications.
                        state.abandon_fenced_repair();
                        return FlowCheckpointDecision::FallbackToFullSnapshot {
                            previous_mode: checkpoint_mode,
                            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
                        };
                    }

                    if state.fenced_repair_pending_is_empty() {
                        state.finish_fenced_repair();
                        if state.is_incremental_disabled() {
                            FlowCheckpointDecision::FallbackToFullSnapshot {
                                previous_mode: CheckpointMode::FullSnapshot,
                                reason: FlowQueryFallbackReason::IncrementalDisabled,
                            }
                        } else {
                            FlowCheckpointDecision::AdvancedFromFullSnapshot {
                                participating_regions: participating_region_count,
                                watermarks: watermark_count,
                            }
                        }
                    } else {
                        let repair = state
                            .pending_fenced_repair()
                            .expect("fenced repair exists after matching repair chunk watermark");
                        FlowCheckpointDecision::ContinuedFencedRepair {
                            pending_windows: repair.pending_windows().len(),
                            watermarks: repair.high().len(),
                        }
                    }
                }
                QueryCoverage::UnfilteredFull => {
                    if state.can_advance_full_snapshot_checkpoints(
                        &participating_regions,
                        &watermark_map,
                    ) {
                        state.advance_checkpoints(watermark_map);
                        if state.is_incremental_disabled() {
                            FlowCheckpointDecision::FallbackToFullSnapshot {
                                previous_mode: CheckpointMode::FullSnapshot,
                                reason: FlowQueryFallbackReason::IncrementalDisabled,
                            }
                        } else {
                            FlowCheckpointDecision::AdvancedFromFullSnapshot {
                                participating_regions: participating_region_count,
                                watermarks: watermark_count,
                            }
                        }
                    } else {
                        debug_assert_ne!(checkpoint_mode, CheckpointMode::Incremental);
                        FlowCheckpointDecision::FallbackToFullSnapshot {
                            previous_mode: checkpoint_mode,
                            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
                        }
                    }
                }
                QueryCoverage::IncrementalDelta => {
                    if state.can_advance_incremental_checkpoints_with_participation(
                        &participating_regions,
                        &watermark_map,
                    ) {
                        state.advance_incremental_checkpoints_with_participation(
                            &participating_regions,
                            watermark_map,
                        );
                        FlowCheckpointDecision::AdvancedIncremental {
                            participating_regions: participating_region_count,
                            watermarks: watermark_count,
                        }
                    } else {
                        state.mark_full_snapshot();
                        FlowCheckpointDecision::FallbackToFullSnapshot {
                            previous_mode: checkpoint_mode,
                            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
                        }
                    }
                }
                QueryCoverage::FencedIncrementalDelta { .. } => unreachable!(
                    "FencedIncrementalDelta is rejected by the default wrapper; only the raw execute_plan_unlocked dispatches it under a caller-owned guard"
                ),
            }
        } else {
            if matches!(coverage, QueryCoverage::FencedRepairChunk { .. }) {
                state.abandon_fenced_repair();
            }
            if matches!(checkpoint_mode, CheckpointMode::Incremental) {
                state.mark_full_snapshot();
            }
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: checkpoint_mode,
                reason: FlowQueryFallbackReason::MissingRegionWatermark,
            }
        }
    }

    /// Turns the substituted row ranges a physical-sequence delta reported into the
    /// complete set of half-open `[start, end)` windows a scoped recompute has to cover.
    ///
    /// The engine reports each range as an **inclusive** `[start, end]` pair of
    /// milliseconds ([`SubstitutedRowRanges`]), while a flow time window is half-open and
    /// [`DirtyTimeWindows::align_time_window`] treats an end that already sits on a window
    /// boundary as exclusive. The inclusive end is therefore widened by one millisecond
    /// before alignment, so a row whose timestamp lies exactly on a boundary - a point
    /// range `[t, t]` included - still selects the bucket that *contains* it instead of
    /// ending the window just before it.
    ///
    /// Fails when the ranges cannot be expressed as bounded windows: the caller must then
    /// fail the round instead of settling a delta that hid rows it cannot recompute.
    pub(super) fn substituted_repair_windows(
        &self,
        ranges: &[SubstitutedRowRanges],
    ) -> Result<Vec<(Timestamp, Timestamp)>, Error> {
        let Some(time_window_expr) = &self.config.time_window_expr else {
            return Err(UnexpectedSnafu {
                reason: format!(
                    "Flow {} scan reported substituted row ranges, but the flow has no time-window expression to turn them into recompute windows",
                    self.config.flow_id
                ),
            }
            .build());
        };

        let mut windows = Vec::new();
        for entry in ranges {
            for range in &entry.ranges {
                if range.end_millis < range.start_millis {
                    return Err(UnexpectedSnafu {
                        reason: format!(
                            "Flow {} scan reported the inverted substituted range [{}, {}] of region {}",
                            self.config.flow_id,
                            range.start_millis,
                            range.end_millis,
                            entry.region_id
                        ),
                    }
                    .build());
                }
                let start = Timestamp::new(range.start_millis, TimeUnit::Millisecond);
                // The reported end is inclusive; alignment expects a half-open end, so the
                // last millisecond of the range must still fall inside the aligned end.
                let end = Timestamp::new(range.end_millis.saturating_add(1), TimeUnit::Millisecond);
                let (window_start, window_end) =
                    DirtyTimeWindows::align_time_window(start, Some(end), time_window_expr)?;
                let Some(window_end) = window_end else {
                    return Err(UnexpectedSnafu {
                        reason: format!(
                            "Flow {} cannot align the substituted range [{}, {}] of region {} to a bounded time window",
                            self.config.flow_id,
                            range.start_millis,
                            range.end_millis,
                            entry.region_id
                        ),
                    }
                    .build());
                };
                windows.push((window_start, window_end));
            }
        }
        Ok(windows)
    }

    /// Refuses to settle an incremental delta whose scan hid substituted rows, and
    /// replaces that delta's additive contribution with a fenced full-window recompute.
    ///
    /// A physical-sequence delta hides every row at or below a file's substituted cut and
    /// reports the hidden rows' timestamp ranges in its terminal metrics
    /// ([`common_recordbatch::adapter::RecordBatchMetrics::substituted_row_ranges`]). Its
    /// additive DML has already run by the time those metrics are read, so re-marking the
    /// windows would only schedule work: the affected windows are recomputed as a fenced
    /// full-window read over exactly the reported ranges (which reads the hidden rows, as a
    /// repair read does not hide anything), under the delta's own proved high watermark
    /// `H`. The delta itself is **not** settled: the checkpoints keep the pre-round cursor
    /// until [`TaskState::finish_fenced_repair`] promotes `H` after the repair proved it.
    ///
    /// `Ok(None)` means the result substituted nothing, so the caller settles it as before.
    /// `Err` means the round cannot be expressed safely (no windows, or no fence to repair
    /// under), so the caller must fail it without advancing the cursor.
    pub(super) fn refuse_substituted_delta(
        &self,
        state: &mut TaskState,
        res: &OutputWithMetrics,
        elapsed: Duration,
        coverage: &QueryCoverage,
    ) -> Result<Option<FlowCheckpointDecision>, Error> {
        let substituted = res
            .metrics
            .get()
            .map(|metrics| metrics.substituted_row_ranges)
            .unwrap_or_default();
        if substituted.is_empty() {
            return Ok(None);
        }

        if !matches!(coverage, QueryCoverage::IncrementalDelta) {
            // Only the incremental delta asks a scan to read physical sequences and to hide
            // the rows a substitution produced. Any other coverage reports an empty range
            // set, so a non-empty one there means a caller-owned read this wrapper cannot
            // refuse on its behalf.
            warn!(
                "Flow {} observed substituted row ranges on a coverage that is not an incremental delta; leaving the checkpoint transition to the established path",
                self.config.flow_id
            );
            return Ok(None);
        }

        let windows = self.substituted_repair_windows(&substituted)?;
        if windows.is_empty() {
            return Err(UnexpectedSnafu {
                reason: format!(
                    "Flow {} delta substituted rows but reported no timestamp range to recompute",
                    self.config.flow_id
                ),
            }
            .build());
        }

        // The repair must run under the delta's own proved high watermark: that is the fence
        // the repair chunks re-prove, and the only bound the affected windows may be settled at.
        let participating = res.participating_regions().ok_or_else(|| {
            UnexpectedSnafu {
                reason: format!(
                    "Flow {} delta substituted rows but returned no terminal metrics to fence the repair with",
                    self.config.flow_id
                ),
            }
            .build()
        })?;
        let watermark_map = res.region_watermark_map().ok_or_else(|| {
            UnexpectedSnafu {
                reason: format!(
                    "Flow {} delta substituted rows but returned no per-region watermarks to fence the repair with",
                    self.config.flow_id
                ),
            }
            .build()
        })?;
        if !state
            .can_advance_incremental_checkpoints_with_participation(&participating, &watermark_map)
        {
            return Err(UnexpectedSnafu {
                reason: format!(
                    "Flow {} delta substituted rows but could not prove a high watermark over participating regions {participating:?}",
                    self.config.flow_id
                ),
            }
            .build());
        }

        state.after_query_exec(elapsed, true);
        let high: BTreeMap<u64, u64> = watermark_map.into_iter().collect();
        state.start_fenced_repair_windows(high, windows);
        let repair = state.pending_fenced_repair().ok_or_else(|| {
            UnexpectedSnafu {
                reason: format!(
                    "Flow {} failed to start a fenced repair for its substituted row ranges",
                    self.config.flow_id
                ),
            }
            .build()
        })?;
        Ok(Some(FlowCheckpointDecision::ContinuedFencedRepair {
            pending_windows: repair.pending_windows().len(),
            watermarks: repair.high().len(),
        }))
    }

    pub(super) fn record_checkpoint_decision(flow_id: FlowId, decision: FlowCheckpointDecision) {
        let flow_id = flow_id.to_string();
        METRIC_FLOW_BATCHING_ENGINE_CHECKPOINT_DECISION_CNT
            .with_label_values(&[
                flow_id.as_str(),
                decision.mode_label(),
                decision.decision_label(),
                decision.reason_label(),
            ])
            .inc();

        match decision {
            FlowCheckpointDecision::AdvancedFromFullSnapshot {
                participating_regions,
                watermarks,
            } => {
                info!(
                    "Flow {flow_id} switched to incremental mode after full snapshot, participating_regions={participating_regions}, watermarks={watermarks}"
                );
            }
            FlowCheckpointDecision::AdvancedIncremental {
                participating_regions,
                watermarks,
            } => {
                debug!(
                    "Flow {flow_id} advanced incremental checkpoints, participating_regions={participating_regions}, watermarks={watermarks}"
                );
            }
            FlowCheckpointDecision::ContinuedFencedRepair {
                pending_windows,
                watermarks,
            } => {
                debug!(
                    "Flow {flow_id} continued fenced repair, pending_windows={pending_windows}, watermarks={watermarks}"
                );
            }
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::FullSnapshot,
                reason: FlowQueryFallbackReason::IncrementalDisabled,
            } => {
                debug!(
                    "Flow {flow_id} remains in full snapshot mode, reason={}",
                    FlowQueryFallbackReason::IncrementalDisabled.as_label()
                );
            }
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode,
                reason,
            } => {
                warn!(
                    "Flow {flow_id} switched to full snapshot mode, previous_mode={}, reason={}",
                    checkpoint_mode_label(previous_mode),
                    reason.as_label()
                );
            }
        }
    }

    pub(super) fn record_query_mode(flow_id: FlowId, mode: CheckpointMode) {
        let flow_id = flow_id.to_string();
        METRIC_FLOW_BATCHING_ENGINE_QUERY_MODE_CNT
            .with_label_values(&[flow_id.as_str(), checkpoint_mode_label(mode)])
            .inc();
    }
}
