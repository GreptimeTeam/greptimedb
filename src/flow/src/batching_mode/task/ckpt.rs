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

use std::time::Duration;

use client::OutputWithMetrics;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};

use crate::batching_mode::checkpoint::{
    FlowCheckpointDecision, FlowQueryFallbackReason, checkpoint_mode_label,
};
use crate::batching_mode::state::{CheckpointMode, TaskState};
use crate::batching_mode::task::BatchingTask;
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_CHECKPOINT_DECISION_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_MODE_CNT,
};
use crate::{Error, FlowId};

impl BatchingTask {
    pub(super) fn query_failure_reason(err: &Error) -> FlowQueryFallbackReason {
        if err.status_code() == StatusCode::RequestOutdated {
            FlowQueryFallbackReason::StaleCursor
        } else {
            FlowQueryFallbackReason::IncrementalQueryFailure
        }
    }

    pub(super) fn apply_query_failure_to_state(
        state: &mut TaskState,
        elapsed: Duration,
        reason: FlowQueryFallbackReason,
    ) -> Option<FlowCheckpointDecision> {
        state.after_query_exec(elapsed, false);
        let checkpoint_mode = state.checkpoint_mode();
        if checkpoint_mode == CheckpointMode::Incremental {
            state.mark_full_snapshot();
            Some(FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: checkpoint_mode,
                reason,
            })
        } else {
            None
        }
    }

    pub(super) fn apply_query_result_to_state(
        state: &mut TaskState,
        res: &OutputWithMetrics,
        elapsed: Duration,
        can_advance_checkpoints: bool,
    ) -> FlowCheckpointDecision {
        state.after_query_exec(elapsed, true);
        let checkpoint_mode = state.checkpoint_mode();
        if !can_advance_checkpoints {
            state.mark_full_snapshot();
            return FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: checkpoint_mode,
                reason: FlowQueryFallbackReason::DirtyBacklogPending,
            };
        }

        if let (Some(participating_regions), Some(watermark_map)) =
            (res.participating_regions(), res.region_watermark_map())
        {
            let can_advance = match checkpoint_mode {
                CheckpointMode::FullSnapshot => state
                    .can_advance_full_snapshot_checkpoints(&participating_regions, &watermark_map),
                CheckpointMode::Incremental => state
                    .can_advance_incremental_checkpoints_with_participation(
                        &participating_regions,
                        &watermark_map,
                    ),
            };

            if can_advance {
                let participating_region_count = participating_regions.len();
                let watermark_count = watermark_map.len();
                match checkpoint_mode {
                    CheckpointMode::FullSnapshot => {
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
                    }
                    CheckpointMode::Incremental => {
                        state.advance_incremental_checkpoints_with_participation(
                            &participating_regions,
                            watermark_map,
                        );
                        FlowCheckpointDecision::AdvancedIncremental {
                            participating_regions: participating_region_count,
                            watermarks: watermark_count,
                        }
                    }
                }
            } else {
                state.mark_full_snapshot();
                FlowCheckpointDecision::FallbackToFullSnapshot {
                    previous_mode: checkpoint_mode,
                    reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
                }
            }
        } else {
            state.mark_full_snapshot();
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: checkpoint_mode,
                reason: FlowQueryFallbackReason::MissingRegionWatermark,
            }
        }
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
