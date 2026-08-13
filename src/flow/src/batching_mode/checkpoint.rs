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

use common_time::timestamp::TimeUnit;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS;
use crate::batching_mode::state::CheckpointMode;
use crate::error::UnexpectedSnafu;

pub(super) const CHECKPOINT_DECISION_ADVANCE: &str = "advance";
pub(super) const CHECKPOINT_DECISION_FALLBACK: &str = "fallback";
pub(super) const CHECKPOINT_DECISION_CONTINUE_REPAIR: &str = "continue_repair";
pub(super) const CHECKPOINT_REASON_NONE: &str = "none";

/// Version of the private on-disk checkpoint record format. Bump when the
/// serialized shape changes; old versions are rejected on load (backfill).
pub(super) const CHECKPOINT_RECORD_FORMAT_VERSION: u32 = 1;

/// The private, versioned checkpoint record stored in the sink table's BINARY
/// state column of the singleton sentinel row.
///
/// `serde_json` is used on purpose: it is an existing flow dependency, and the
/// `BTreeMap` key order makes the encoding deterministic for a given value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CheckpointRecord {
    /// Record format version; must equal [`CHECKPOINT_RECORD_FORMAT_VERSION`].
    pub format_version: u32,
    /// Epoch of the persisted checkpoint. Rows stamped with a larger epoch are
    /// newer than this record and invalidate it (crash between state write and
    /// checkpoint write).
    pub epoch: u64,
    /// Region id -> last consumed watermark sequence map. Must be non-empty to
    /// be trusted.
    pub checkpoints: BTreeMap<u64, u64>,
}

/// Encode a checkpoint record deterministically.
pub(super) fn encode_checkpoint_record(record: &CheckpointRecord) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(record).map_err(|err| {
        UnexpectedSnafu {
            reason: err.to_string(),
        }
        .build()
    })
}

/// Decode a checkpoint record, rejecting unknown format versions.
pub(super) fn decode_checkpoint_record(bytes: &[u8]) -> Result<Option<CheckpointRecord>, Error> {
    let record: CheckpointRecord = match serde_json::from_slice(bytes) {
        Ok(record) => record,
        Err(_) => return Ok(None),
    };
    if record.format_version != CHECKPOINT_RECORD_FORMAT_VERSION {
        return Ok(None);
    }
    Ok(Some(record))
}

/// Convert the millisecond sentinel window timestamp to the sink window
/// column's native time unit.
///
/// A nanosecond sentinel at year 9999 would overflow `i64`, so the nanosecond
/// representation is clamped to the largest representable value; every
/// practical source timestamp is far below it. Second/microsecond conversions
/// are exact.
pub(super) fn checkpoint_sentinel_ts_in_unit(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS / 1000,
        TimeUnit::Millisecond => CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS,
        TimeUnit::Microsecond => CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS * 1000,
        TimeUnit::Nanosecond => i64::MAX,
    }
}

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
    /// A fenced repair chunk tried to use a snapshot upper bound that the
    /// storage engine can no longer enforce, so the current repair high must
    /// be abandoned and rebound by a fresh scoped full snapshot repair.
    SnapshotFenceExpired,
    /// A non-stale-cursor query failure; the Flow resets to full snapshot
    /// to avoid cascading errors.
    IncrementalQueryFailure,
    /// A non-incremental query failed while the task was already in full
    /// snapshot or scoped repair mode.
    QueryFailure,
    /// Incremental mode has been permanently disabled for this Flow
    /// (e.g. because the query shape is not incrementally safe).
    IncrementalDisabled,
    /// The sink state rows were written but the singleton checkpoint row could
    /// not be persisted (write failure or ambiguous result). The runtime
    /// resets to full snapshot so a later cycle rebuilds and re-persists the
    /// checkpoint instead of claiming persistence it does not have.
    CheckpointPersistFailure,
}

impl FlowQueryFallbackReason {
    pub(super) fn as_label(self) -> &'static str {
        match self {
            Self::MissingRegionWatermark => "missing_region_watermark",
            Self::IncompleteRegionWatermark => "incomplete_region_watermark",
            Self::DirtyBacklogPending => "dirty_backlog_pending",
            Self::StaleCursor => "stale_cursor",
            Self::SnapshotFenceExpired => "snapshot_fence_expired",
            Self::IncrementalQueryFailure => "incremental_query_failure",
            Self::QueryFailure => "query_failure",
            Self::IncrementalDisabled => "incremental_disabled",
            Self::CheckpointPersistFailure => "checkpoint_persist_failure",
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
    /// FullSnapshot stayed in full snapshot mode because a scoped base repair
    /// found additional dirty windows that may be concurrent with the returned
    /// high watermark. These windows must be repaired under the fixed high
    /// before checkpoints can advance.
    ContinuedFencedRepair {
        pending_windows: usize,
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
            // Fenced repair is intentionally a FullSnapshot sub-state, not a
            // third top-level checkpoint mode, so metrics keep the
            // `full_snapshot` mode label while the decision label carries
            // `continue_repair`.
            Self::ContinuedFencedRepair { .. } => {
                checkpoint_mode_label(CheckpointMode::FullSnapshot)
            }
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
            Self::ContinuedFencedRepair { .. } => CHECKPOINT_DECISION_CONTINUE_REPAIR,
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

#[cfg(test)]
mod tests {
    use common_time::timestamp::TimeUnit;

    use super::*;
    use crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS;

    #[test]
    fn test_checkpoint_record_roundtrip_and_version_rejection() {
        let record = CheckpointRecord {
            format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
            epoch: 42,
            checkpoints: BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]),
        };
        let encoded = encode_checkpoint_record(&record).unwrap();
        assert_eq!(
            record,
            decode_checkpoint_record(&encoded)
                .unwrap()
                .expect("roundtrip")
        );

        // Deterministic encoding: same value -> same bytes.
        assert_eq!(encoded, encode_checkpoint_record(&record).unwrap());

        // Garbage bytes are not a decodable record.
        assert!(decode_checkpoint_record(b"not-a-record").unwrap().is_none());

        // Unknown format versions are rejected (future compat guard).
        let mut json = serde_json::to_value(&record).unwrap();
        json["format_version"] = serde_json::json!(2);
        let bytes = serde_json::to_vec(&json).unwrap();
        assert!(decode_checkpoint_record(&bytes).unwrap().is_none());
    }

    #[test]
    fn test_checkpoint_sentinel_ts_in_unit_conversion() {
        let sentinel = CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS;
        assert_eq!(
            sentinel / 1000,
            checkpoint_sentinel_ts_in_unit(TimeUnit::Second)
        );
        assert_eq!(
            sentinel,
            checkpoint_sentinel_ts_in_unit(TimeUnit::Millisecond)
        );
        assert_eq!(
            sentinel * 1000,
            checkpoint_sentinel_ts_in_unit(TimeUnit::Microsecond)
        );
        // A year-9999 nanosecond sentinel would overflow i64, so the
        // nanosecond representation is clamped to the largest representable
        // value. The exact clamped value is a storage detail; the assertion
        // pins that it stays representable and far above any real timestamp.
        assert_eq!(
            i64::MAX,
            checkpoint_sentinel_ts_in_unit(TimeUnit::Nanosecond)
        );
    }
}
