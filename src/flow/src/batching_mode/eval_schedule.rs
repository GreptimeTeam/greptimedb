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

//! Helpers for stable `EVAL INTERVAL` scheduled times.

pub use common_meta::key::flow::flow_info::{FlowMissedTickPolicy, FlowScheduleConfig};
use snafu::ensure;

use crate::error::{InvalidQuerySnafu, Result, UnexpectedSnafu};

/// Schedule for an `EVAL INTERVAL` flow.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalSchedule {
    /// Interval between scheduled times in seconds.
    pub interval_secs: i64,
    /// Anchor timestamp as seconds since Unix epoch.
    pub anchor_secs: i64,
    /// First scheduled time as seconds since Unix epoch.
    pub start_secs: i64,
    /// Policy for handling missed scheduled times.
    pub missed_tick_policy: FlowMissedTickPolicy,
    /// Maximum number of due scheduled times to catch up.
    pub max_runs: u32,
    /// Maximum age of a due scheduled time to keep for catch-up.
    pub max_lag_secs: i64,
}

impl EvalSchedule {
    pub fn from_config(
        eval_interval_secs: Option<i64>,
        config: Option<&FlowScheduleConfig>,
    ) -> Result<Option<Self>> {
        let Some(interval_secs) = eval_interval_secs else {
            return Ok(None);
        };
        ensure!(
            interval_secs > 0,
            InvalidQuerySnafu {
                reason: format!(
                    "Invalid eval_interval_secs: must be positive, got {interval_secs}"
                )
            }
        );

        Ok(Some(match config {
            Some(c) => {
                ensure!(
                    c.catchup_max_runs > 0,
                    InvalidQuerySnafu {
                        reason:
                            "Invalid FlowScheduleConfig.catchup_max_runs: must be positive, got 0"
                                .to_string()
                    }
                );
                ensure!(
                    c.catchup_max_lag_secs > 0,
                    InvalidQuerySnafu {
                        reason: format!(
                            "Invalid FlowScheduleConfig.catchup_max_lag_secs: must be positive, got {}",
                            c.catchup_max_lag_secs
                        )
                    }
                );
                // The anchor defines the epoch phase `anchor + k * interval`; it
                // must be a valid offset within one interval.
                ensure!(
                    c.anchor_secs >= 0 && c.anchor_secs < interval_secs,
                    InvalidQuerySnafu {
                        reason: format!(
                            "Invalid FlowScheduleConfig.anchor_secs: must be in [0, {interval_secs}), got {}",
                            c.anchor_secs
                        )
                    }
                );
                // The start must be phase-consistent with the anchor (on an
                // `anchor + k * interval` boundary) and not before the anchor.
                ensure!(
                    c.start_secs >= c.anchor_secs
                        && (c.start_secs - c.anchor_secs) % interval_secs == 0,
                    InvalidQuerySnafu {
                        reason: format!(
                            "Invalid FlowScheduleConfig.start_secs: must be on an anchor + k * interval boundary and >= anchor, got start={}, anchor={}, interval={}",
                            c.start_secs, c.anchor_secs, interval_secs
                        )
                    }
                );

                Self {
                    interval_secs,
                    anchor_secs: c.anchor_secs,
                    start_secs: c.start_secs,
                    missed_tick_policy: c.missed_tick_policy,
                    max_runs: c.catchup_max_runs,
                    max_lag_secs: c.catchup_max_lag_secs,
                }
            }
            None => {
                let c = FlowScheduleConfig::default_with_start(0, interval_secs);
                Self {
                    interval_secs,
                    anchor_secs: c.anchor_secs,
                    start_secs: c.start_secs,
                    missed_tick_policy: c.missed_tick_policy,
                    max_runs: c.catchup_max_runs,
                    max_lag_secs: c.catchup_max_lag_secs,
                }
            }
        }))
    }

    /// Returns the next scheduled time strictly after `cursor_secs`, on the
    /// `anchor + k * interval` lattice.
    ///
    /// Fallible: a non-positive interval or a next boundary that does not fit
    /// in `i64` yields an explicit error instead of a saturated non-phase
    /// value such as `i64::MAX`.
    pub fn next_scheduled_time_after(&self, cursor_secs: i64) -> Result<i64> {
        next_in_sequence(cursor_secs, self.start_secs, self.interval_secs)
    }
}

/// The smallest `start + k * interval` value that is strictly after `cursor`
/// (`start` itself lies on the `anchor + k * interval` lattice, so every
/// result is phase-consistent with the anchor). All arithmetic happens in
/// `i128`: `cursor - start` cannot overflow and the result is either exactly
/// on the lattice or an explicit error.
fn next_in_sequence(cursor: i64, start: i64, interval: i64) -> Result<i64> {
    ensure!(
        interval > 0,
        InvalidQuerySnafu {
            reason: format!("Invalid eval interval: must be positive, got {interval}")
        }
    );
    let interval = i128::from(interval);
    let start = i128::from(start);
    let cursor = i128::from(cursor);

    let next = if cursor < start {
        start
    } else {
        let k = (cursor - start) / interval;
        start + (k + 1) * interval
    };

    i64::try_from(next).map_err(|_| {
        UnexpectedSnafu {
            reason: format!(
                "Cannot advance the eval schedule past cursor {cursor}: the next scheduled time {next} does not fit in i64 (start={start}, interval={interval})"
            ),
        }
        .build()
    })
}

fn first_due_in_sequence(cursor: i64, start: i64, interval: i64) -> Result<i64> {
    if cursor < start {
        Ok(start)
    } else {
        next_in_sequence(cursor, start, interval)
    }
}

/// Scheduled times selected for execution in one scheduler pass.
///
/// A scheduled time is the logical evaluation timestamp for one flow run. When
/// executing a timestamp from `scheduled_times_secs`, SQL/TQL `now()` is bound
/// to that timestamp instead of the wall-clock execution time.
#[derive(Debug, Clone, PartialEq)]
pub struct DueScheduledTimes {
    /// Scheduled times to execute, ordered oldest to newest.
    pub scheduled_times_secs: Vec<i64>,
    /// Number of due scheduled times skipped by lag or max-runs limits.
    pub skipped: u64,
}

/// Select due scheduled times `<= wall_now_secs` without materializing all missed ticks.
///
/// Fallible: a non-positive interval or a scheduled time that does not fit in
/// `i64` yields an explicit error instead of silently producing saturated
/// non-phase timestamps.
pub fn select_due_scheduled_times(
    schedule: &EvalSchedule,
    cursor_secs: i64,
    wall_now_secs: i64,
) -> Result<DueScheduledTimes> {
    let interval = schedule.interval_secs;
    ensure!(
        interval > 0,
        InvalidQuerySnafu {
            reason: format!("Invalid eval interval: must be positive, got {interval}")
        }
    );

    let first_due = first_due_in_sequence(cursor_secs, schedule.start_secs, interval)?;
    if first_due > wall_now_secs {
        return Ok(DueScheduledTimes {
            scheduled_times_secs: vec![],
            skipped: 0,
        });
    }

    // Count and select due scheduled times in i128 so every value stays
    // exactly on the `anchor + k * interval` lattice; a value beyond `i64` is
    // an explicit error, never a saturated non-phase timestamp.
    let first_due = i128::from(first_due);
    let wall_now = i128::from(wall_now_secs);
    let interval = i128::from(interval);

    let total_count = (wall_now - first_due) / interval + 1;
    // `first_due >= 0` and `wall_now <= i64::MAX`, so this always fits in u64.
    let total_count = u64::try_from(total_count).map_err(|_| {
        UnexpectedSnafu {
            reason: format!(
                "Cannot count due eval scheduled times up to {wall_now}: {total_count} does not fit in u64"
            ),
        }
        .build()
    })?;

    match schedule.missed_tick_policy {
        FlowMissedTickPolicy::Skip => {
            // Keep only the latest due scheduled time; it is still on-lattice
            // and `<= wall_now`.
            let last = i64::try_from(first_due + i128::from(total_count - 1) * interval)
                .map_err(|_| {
                    UnexpectedSnafu {
                        reason: format!(
                            "Cannot compute the latest due eval scheduled time (first_due={first_due}, interval={interval}, count={total_count}): result does not fit in i64"
                        ),
                    }
                    .build()
                })?;
            Ok(DueScheduledTimes {
                scheduled_times_secs: vec![last],
                skipped: total_count - 1,
            })
        }
        FlowMissedTickPolicy::BoundedCatchUp => {
            // The cutoff is computed in i128: `wall_now - max_lag` may
            // legitimately underflow i64 (a cutoff before the Unix epoch) and
            // must not saturate to a wrong value.
            let cutoff = wall_now - i128::from(schedule.max_lag_secs);
            let skipped_by_cutoff = if first_due >= cutoff {
                0
            } else {
                // ceil((cutoff - first_due) / interval), capped at u64::MAX
                // before the `.min(total_count)` below.
                let skipped = (cutoff - first_due + interval - 1) / interval;
                u64::try_from(skipped).unwrap_or(u64::MAX)
            }
            .min(total_count);

            let remaining = total_count - skipped_by_cutoff;
            if remaining == 0 {
                return Ok(DueScheduledTimes {
                    scheduled_times_secs: vec![],
                    skipped: total_count,
                });
            }

            // max_lag decides which missed scheduled times are recent enough to
            // run; max_runs caps how many of those times execute back-to-back
            // in one scheduler pass.
            let keep_count = remaining.min(u64::from(schedule.max_runs));
            let keep_start = skipped_by_cutoff + remaining - keep_count;
            let mut scheduled_times_secs = Vec::with_capacity(keep_count as usize);
            for i in 0..keep_count {
                let t = i64::try_from(
                    first_due + (i128::from(keep_start) + i128::from(i)) * interval,
                )
                .map_err(|_| {
                    UnexpectedSnafu {
                        reason: format!(
                            "Cannot compute a due eval scheduled time (first_due={first_due}, interval={interval}, index={i}): result does not fit in i64"
                        ),
                    }
                    .build()
                })?;
                scheduled_times_secs.push(t);
            }

            Ok(DueScheduledTimes {
                scheduled_times_secs,
                skipped: total_count - keep_count,
            })
        }
    }
}

/// Ceils `time` to the next `anchor + k * interval` boundary.
///
/// Fallible: if the next boundary does not fit in `i64`, an explicit error is
/// returned instead of clamping to a non-phase value such as `i64::MAX`.
pub fn ceil_to_boundary(time: i64, anchor: i64, interval: i64) -> Result<i64> {
    if interval <= 0 {
        return Ok(time);
    }
    if time <= anchor {
        return Ok(anchor);
    }

    let diff = i128::from(time) - i128::from(anchor);
    let interval = i128::from(interval);
    let k = (diff + interval - 1) / interval;
    let boundary = i128::from(anchor) + k * interval;

    i64::try_from(boundary).map_err(|_| {
        crate::error::UnexpectedSnafu {
            reason: format!(
                "Cannot align time {time} to the next `anchor + k * interval` boundary (anchor={anchor}, interval={interval}): result {boundary} does not fit in i64"
            ),
        }
        .build()
    })
}

#[cfg(test)]
mod test {
    use super::*;

    fn schedule(
        start: i64,
        policy: FlowMissedTickPolicy,
        max_runs: u32,
        max_lag_secs: i64,
    ) -> EvalSchedule {
        EvalSchedule {
            interval_secs: 60,
            anchor_secs: 0,
            start_secs: start,
            missed_tick_policy: policy,
            max_runs,
            max_lag_secs,
        }
    }

    fn config(policy: FlowMissedTickPolicy) -> FlowScheduleConfig {
        FlowScheduleConfig {
            anchor_secs: 10,
            // phase-consistent: 310 = anchor(10) + 1 * interval(300)
            start_secs: 310,
            missed_tick_policy: policy,
            catchup_max_runs: 4,
            catchup_max_lag_secs: 600,
        }
    }

    #[test]
    fn ceil_to_boundary_handles_anchor_and_interval_edges() {
        assert_eq!(ceil_to_boundary(-10, 0, 60).unwrap(), 0);
        assert_eq!(ceil_to_boundary(0, 0, 60).unwrap(), 0);
        assert_eq!(ceil_to_boundary(1, 0, 60).unwrap(), 60);
        assert_eq!(ceil_to_boundary(60, 0, 60).unwrap(), 60);
        assert_eq!(ceil_to_boundary(101, 100, 60).unwrap(), 160);
        assert_eq!(ceil_to_boundary(50, 0, 0).unwrap(), 50);
        // Never clamp to the non-phase i64::MAX: the next boundary does not fit.
        assert!(ceil_to_boundary(i64::MAX, 0, 60).is_err());
        assert!(ceil_to_boundary(i64::MAX - 1, i64::MIN, 60).is_err());
    }

    #[test]
    fn from_config_maps_typed_config_and_defaults() {
        assert!(EvalSchedule::from_config(None, None).unwrap().is_none());
        assert!(EvalSchedule::from_config(Some(0), None).is_err());

        let from_typed =
            EvalSchedule::from_config(Some(300), Some(&config(FlowMissedTickPolicy::Skip)))
                .unwrap()
                .unwrap();
        assert_eq!(from_typed.interval_secs, 300);
        assert_eq!(from_typed.anchor_secs, 10);
        assert_eq!(from_typed.start_secs, 310);
        assert_eq!(from_typed.missed_tick_policy, FlowMissedTickPolicy::Skip);
        assert_eq!(from_typed.max_runs, 4);
        assert_eq!(from_typed.max_lag_secs, 600);

        let defaulted = EvalSchedule::from_config(Some(300), None).unwrap().unwrap();
        assert_eq!(defaulted.start_secs, 0);
        assert_eq!(defaulted.max_runs, 3);
        assert_eq!(defaulted.max_lag_secs, 900);
    }

    #[test]
    fn from_config_rejects_invalid_catchup_limits() {
        let mut c = config(FlowMissedTickPolicy::BoundedCatchUp);
        c.catchup_max_runs = 0;
        assert!(EvalSchedule::from_config(Some(300), Some(&c)).is_err());

        let mut c = config(FlowMissedTickPolicy::BoundedCatchUp);
        c.catchup_max_lag_secs = 0;
        assert!(EvalSchedule::from_config(Some(300), Some(&c)).is_err());
    }

    #[test]
    fn nonzero_anchor_due_selection_follows_phase() {
        // anchor=120 (i.e. `EVAL OFFSET '2 minutes'`), interval=3600:
        // boundaries at :02 every hour. start=3720 (120 + 3600).
        let s = EvalSchedule {
            interval_secs: 3600,
            anchor_secs: 120,
            start_secs: 3720,
            missed_tick_policy: FlowMissedTickPolicy::BoundedCatchUp,
            max_runs: 3,
            max_lag_secs: 3600,
        };
        assert_eq!(
            select_due_scheduled_times(&s, 0, 100)
                .unwrap()
                .scheduled_times_secs,
            Vec::<i64>::new()
        );
        // From 3720 on, every selected time must be on the :02 phase.
        let due = select_due_scheduled_times(&s, 0, 3720).unwrap();
        assert_eq!(due.scheduled_times_secs, vec![3720]);
        let due = select_due_scheduled_times(&s, 3720, 7320).unwrap();
        assert_eq!(due.scheduled_times_secs, vec![7320]);
        for t in &due.scheduled_times_secs {
            assert_eq!((t - 120) % 3600, 0);
        }
        assert_eq!(s.next_scheduled_time_after(3720).unwrap(), 7320);
        assert_eq!(s.next_scheduled_time_after(7300).unwrap(), 7320);
    }

    #[test]
    fn next_scheduled_time_after_respects_start_sequence() {
        let s = schedule(50, FlowMissedTickPolicy::BoundedCatchUp, 3, 300);
        assert_eq!(s.next_scheduled_time_after(0).unwrap(), 50);
        assert_eq!(s.next_scheduled_time_after(50).unwrap(), 110);
        assert_eq!(s.next_scheduled_time_after(100).unwrap(), 110);
    }

    #[test]
    fn near_i64_max_advancement_is_exact_or_explicit_error() {
        // anchor=0, interval=60: the next boundary after i64::MAX - 60 is
        // 9223372036854775800, still in range and exactly on the lattice.
        let s = schedule(0, FlowMissedTickPolicy::Skip, 5, 3600);
        let cursor = i64::MAX - 60;
        let next = s.next_scheduled_time_after(cursor).unwrap();
        assert_eq!(next, 9223372036854775800);
        assert_eq!(next % 60, 0);

        // Advancing past the last representable boundary is an explicit error,
        // never a saturated non-phase value like i64::MAX.
        let err = s
            .next_scheduled_time_after(9223372036854775800)
            .unwrap_err();
        assert!(err.to_string().contains("does not fit in i64"));

        // A non-positive interval is an explicit error, not a saturating
        // `cursor + 1` result.
        let invalid = EvalSchedule {
            interval_secs: 0,
            anchor_secs: 0,
            start_secs: 0,
            missed_tick_policy: FlowMissedTickPolicy::Skip,
            max_runs: 3,
            max_lag_secs: 900,
        };
        assert!(invalid.next_scheduled_time_after(0).is_err());
    }

    #[test]
    fn due_scheduled_time_selection_handles_empty_and_start_boundary() {
        let s = schedule(120, FlowMissedTickPolicy::BoundedCatchUp, 10, 3600);
        assert_eq!(
            select_due_scheduled_times(&s, 0, 100)
                .unwrap()
                .scheduled_times_secs,
            Vec::<i64>::new()
        );
        assert_eq!(
            select_due_scheduled_times(&s, 0, 300)
                .unwrap()
                .scheduled_times_secs,
            vec![120, 180, 240, 300]
        );
    }

    #[test]
    fn bounded_catch_up_applies_lag_and_max_runs() {
        let s = schedule(0, FlowMissedTickPolicy::BoundedCatchUp, 2, 180);
        let due = select_due_scheduled_times(&s, 0, 600).unwrap();
        assert_eq!(due.scheduled_times_secs, vec![540, 600]);
        assert_eq!(due.skipped, 8);
    }

    #[test]
    fn bounded_catch_up_can_skip_all_due_scheduled_times() {
        let s = schedule(0, FlowMissedTickPolicy::BoundedCatchUp, 3, 30);
        let due = select_due_scheduled_times(&s, 0, 100).unwrap();
        assert!(due.scheduled_times_secs.is_empty());
        assert_eq!(due.skipped, 1);
    }

    #[test]
    fn skip_policy_keeps_only_latest_due_scheduled_time() {
        let s = schedule(0, FlowMissedTickPolicy::Skip, 5, 3600);
        let due = select_due_scheduled_times(&s, 0, 300).unwrap();
        assert_eq!(due.scheduled_times_secs, vec![300]);
        assert_eq!(due.skipped, 4);
    }

    #[test]
    fn huge_missed_gap_allocates_only_kept_scheduled_times() {
        let s = schedule(0, FlowMissedTickPolicy::BoundedCatchUp, 5, 3600);
        let due = select_due_scheduled_times(&s, 0, 86400).unwrap();
        assert_eq!(
            due.scheduled_times_secs,
            vec![86160, 86220, 86280, 86340, 86400]
        );
        assert_eq!(due.skipped, 1435);
    }
}
