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

use crate::batching_mode::state::CheckpointMode;

pub(super) const CHECKPOINT_DECISION_ADVANCE: &str = "advance";
pub(super) const CHECKPOINT_DECISION_FALLBACK: &str = "fallback";
pub(super) const CHECKPOINT_REASON_NONE: &str = "none";

/// Why the task fell back to full snapshot mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlowQueryFallbackReason {
    /// The query result did not include a region-watermark map at all.
    MissingRegionWatermark,
    /// Some participating regions could not prove safe advancement against
    /// both the returned watermarks and the checkpoint map.
    IncompleteRegionWatermark,
    /// A query failure while the task was checkpoint-ready; the Flow resets
    /// to full snapshot to avoid cascading errors.
    CheckpointReadyQueryFailure,
}

impl FlowQueryFallbackReason {
    pub(super) fn as_label(self) -> &'static str {
        match self {
            Self::MissingRegionWatermark => "missing_region_watermark",
            Self::IncompleteRegionWatermark => "incomplete_region_watermark",
            Self::CheckpointReadyQueryFailure => "checkpoint_ready_query_failure",
        }
    }
}

/// Decision produced by `BatchingTask::apply_query_result_to_state` after
/// each Flow query execution. Describes whether the task advanced its
/// checkpoint-readiness state or fell back to full snapshot, and why.
///
/// The task tracks checkpoint readiness internally so that subsequent queries
/// can prove that all participating regions have valid watermarks. This
/// decision only records readiness/fallback state; it does not by itself imply
/// that a query emitted incremental scan extensions (`FLOW_INCREMENTAL_MODE` /
/// `FLOW_INCREMENTAL_AFTER_SEQS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlowCheckpointDecision {
    /// FullSnapshot → Incremental (checkpoint-readiness) transition.
    ///
    /// The query exercised every participating region, all returned valid
    /// watermarks, and the checkpoint map was populated from scratch.
    /// The task is now checkpoint-ready. These checkpoints can be used by
    /// incremental scan extension generation when that execution mode is
    /// enabled.
    AdvancedFromFullSnapshot {
        participating_regions: usize,
        watermarks: usize,
    },
    /// Existing Incremental (checkpoint-ready) → Incremental (in-place advancement).
    ///
    /// A subset of participating regions advanced their watermarks. The
    /// task stays checkpoint-ready with an updated checkpoint map.
    AdvancedIncremental {
        participating_regions: usize,
        watermarks: usize,
    },
    /// Any mode → FullSnapshot.
    ///
    /// Watermark information was incomplete, a participating region was
    /// absent from the existing checkpoint map, or the query itself
    /// failed. The task resets to full snapshot semantics for the next
    /// execution.
    FallbackToFullSnapshot {
        previous_mode: CheckpointMode,
        reason: FlowQueryFallbackReason,
    },
}

impl FlowCheckpointDecision {
    pub(super) fn mode_label(self) -> &'static str {
        match self {
            Self::AdvancedFromFullSnapshot { .. } => {
                checkpoint_mode_label(CheckpointMode::FullSnapshot)
            }
            Self::AdvancedIncremental { .. } => checkpoint_mode_label(CheckpointMode::Incremental),
            Self::FallbackToFullSnapshot { previous_mode, .. } => {
                checkpoint_mode_label(previous_mode)
            }
        }
    }

    pub(super) fn decision_label(self) -> &'static str {
        match self {
            Self::AdvancedFromFullSnapshot { .. } | Self::AdvancedIncremental { .. } => {
                CHECKPOINT_DECISION_ADVANCE
            }
            Self::FallbackToFullSnapshot { .. } => CHECKPOINT_DECISION_FALLBACK,
        }
    }

    pub(super) fn reason_label(self) -> &'static str {
        match self {
            Self::FallbackToFullSnapshot { reason, .. } => reason.as_label(),
            _ => CHECKPOINT_REASON_NONE,
        }
    }
}

pub(super) fn checkpoint_mode_label(mode: CheckpointMode) -> &'static str {
    match mode {
        CheckpointMode::FullSnapshot => "full_snapshot",
        CheckpointMode::Incremental => "incremental",
    }
}
