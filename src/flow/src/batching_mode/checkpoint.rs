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
    /// The query only covered part of the dirty backlog, so global checkpoints
    /// cannot advance yet. Incremental SQL drains all dirty windows before
    /// checkpoint advancement; this primarily protects scoped full-snapshot
    /// runs capped by the per-query dirty-window limit.
    DirtyBacklogPending,
    /// The datanode detected a stale incremental cursor and the Flow
    /// must recompute from scratch.
    StaleCursor,
    /// A non-stale-cursor query failure; the Flow resets to full snapshot
    /// to avoid cascading errors.
    IncrementalQueryFailure,
    /// Incremental mode has been permanently disabled for this Flow
    /// (e.g. because the query shape is not incrementally safe).
    IncrementalDisabled,
}

impl FlowQueryFallbackReason {
    pub(super) fn as_label(self) -> &'static str {
        match self {
            Self::MissingRegionWatermark => "missing_region_watermark",
            Self::IncompleteRegionWatermark => "incomplete_region_watermark",
            Self::DirtyBacklogPending => "dirty_backlog_pending",
            Self::StaleCursor => "stale_cursor",
            Self::IncrementalQueryFailure => "incremental_query_failure",
            Self::IncrementalDisabled => "incremental_disabled",
        }
    }
}

/// Decision produced by `BatchingTask::apply_query_result_to_state` after
/// each Flow query execution. Describes whether the task advanced its
/// checkpoint state or fell back to full snapshot, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlowCheckpointDecision {
    /// FullSnapshot → Incremental transition.
    ///
    /// The query exercised every participating region, all returned valid
    /// watermarks, and the checkpoint map was populated from scratch.
    /// Subsequent executions will use incremental after-seqs.
    AdvancedFromFullSnapshot {
        participating_regions: usize,
        watermarks: usize,
    },
    /// Existing Incremental → Incremental (in-place advancement).
    ///
    /// A subset of participating regions advanced their watermarks. The
    /// task stays in incremental mode with an updated checkpoint map.
    AdvancedIncremental {
        participating_regions: usize,
        watermarks: usize,
    },
    /// Any mode → FullSnapshot.
    ///
    /// Watermark information was incomplete, a participating region was
    /// absent from the existing checkpoint map, the task has permanently
    /// disabled incremental mode, or the query itself failed. The task
    /// resets to full snapshot semantics for the next execution.
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
