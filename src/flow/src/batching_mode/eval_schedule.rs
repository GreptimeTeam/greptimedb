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

//! Pure helpers for `EVAL INTERVAL` schedule semantics.
//!
//! This module provides boundary math, due-runtime selection for bounded
//! catch-up, and a typed `EvalSchedule` that can be built from
//! `FlowScheduleConfig`.

pub use common_meta::key::flow::flow_info::{FlowMissedTickPolicy, FlowScheduleConfig};

const DEFAULT_MAX_RUNS: u32 = 3;
const MIN_LAG_SECONDS: i64 = 300; // 5 minutes

/// The schedule for an `EVAL INTERVAL` flow, derived from the flow's interval
/// and persisted options.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalSchedule {
    /// Interval between scheduled runtimes in seconds.
    pub interval_secs: i64,
    /// Anchor timestamp as seconds since Unix epoch (default: 0 = Unix epoch).
    pub anchor_secs: i64,
    /// The first scheduled runtime (seconds since epoch) for this flow.
    /// Must be `>= anchor_secs` and aligned to interval boundaries.
    pub start_secs: i64,
    /// Policy for handling runtimes that were missed.
    pub missed_tick_policy: MissedTickPolicy,
    /// Maximum number of due runtimes to catch up when using bounded catch-up.
    pub max_runs: u32,
    /// Maximum age (in seconds) of a due runtime to still consider for catch-up.
    pub max_lag_secs: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissedTickPolicy {
    /// Keep the most recent `max_runs` due runtimes within `max_lag`, drop older ones.
    BoundedCatchUp,
    /// Skip all missed runtimes; only execute the single most recent due runtime.
    Skip,
}

impl EvalSchedule {
    /// Build an `EvalSchedule` from a typed `FlowScheduleConfig`.
    ///
    /// This is the preferred code path (post-Phase-B). It avoids string
    /// parsing and uses the typed struct directly.
    pub fn from_config(
        eval_interval_secs: Option<i64>,
        config: Option<&crate::batching_mode::eval_schedule::FlowScheduleConfig>,
    ) -> Result<Option<Self>, String> {
        let interval_secs = match eval_interval_secs {
            None => return Ok(None),
            Some(s) if s <= 0 => {
                return Err(format!(
                    "Invalid eval_interval_secs: must be positive, got {s}"
                ));
            }
            Some(s) => s,
        };

        let sched = match config {
            Some(c) => {
                if c.catchup_max_runs == 0 {
                    return Err(
                        "Invalid FlowScheduleConfig.catchup_max_runs: must be positive, got 0"
                            .to_string(),
                    );
                }
                if c.catchup_max_lag_secs <= 0 {
                    return Err(format!(
                        "Invalid FlowScheduleConfig.catchup_max_lag_secs: must be positive, got {}",
                        c.catchup_max_lag_secs
                    ));
                }
                Self {
                    interval_secs,
                    anchor_secs: c.anchor_secs,
                    start_secs: c.start_secs,
                    missed_tick_policy: match c.missed_tick_policy {
                        crate::batching_mode::eval_schedule::FlowMissedTickPolicy::BoundedCatchUp => {
                            MissedTickPolicy::BoundedCatchUp
                        }
                        crate::batching_mode::eval_schedule::FlowMissedTickPolicy::Skip => {
                            MissedTickPolicy::Skip
                        }
                    },
                    max_runs: c.catchup_max_runs,
                    max_lag_secs: c.catchup_max_lag_secs,
                }
            }
            None => {
                // Fallback: no typed config → use defaults.
                Self {
                    interval_secs,
                    anchor_secs: 0,
                    start_secs: ceil_to_boundary(0, 0, interval_secs),
                    missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
                    max_runs: DEFAULT_MAX_RUNS,
                    max_lag_secs: std::cmp::max(MIN_LAG_SECONDS, 3 * interval_secs),
                }
            }
        };

        Ok(Some(sched))
    }

    /// Compute the next scheduled runtime strictly after `cursor_secs`.
    ///
    /// The runtime sequence is `start_secs + n * interval` (n ≥ 0). If
    /// `cursor_secs < start_secs`, the first runtime (`start_secs`) is returned.
    /// Otherwise returns the next boundary in the sequence strictly after
    /// `cursor_secs`.
    ///
    /// `cursor_secs` is typically the last completed runtime.
    pub fn next_runtime_after(&self, cursor_secs: i64) -> i64 {
        next_in_sequence(cursor_secs, self.start_secs, self.interval_secs)
    }
}

/// Returns the next runtime in the sequence `start + n * interval` that is
/// strictly after `cursor`. If `cursor < start`, returns `start`.
fn next_in_sequence(cursor: i64, start: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return cursor.saturating_add(1).max(start);
    }
    if cursor < start {
        return start;
    }
    let diff = cursor - start;
    // k = floor(diff / interval) — how many full intervals have elapsed since start.
    let k = diff / interval;
    start.saturating_add((k + 1).saturating_mul(interval))
}

/// Returns the first runtime in the sequence that is `> cursor` but also `>= start`.
fn first_due_in_sequence(cursor: i64, start: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return cursor.saturating_add(1).max(start);
    }
    if cursor < start {
        return start;
    }
    next_in_sequence(cursor, start, interval)
}

/// Result of due-runtime selection under bounded catch-up policy.
#[derive(Debug, Clone, PartialEq)]
pub struct DueRuntimes {
    /// Runtimes to execute, ordered oldest → newest.
    pub runtimes: Vec<i64>,
    /// Number of runtimes that were skipped (too old or exceeded max_runs).
    pub skipped: u64,
}

/// Select due runtimes that are `<= wall_now_secs`, applying the bounded
/// catch-up policy.
///
/// Never returns runtimes before `schedule.start_secs`, even if `cursor_secs`
/// is older than start.
///
/// Uses arithmetic to compute skipped/kept counts without materializing every
/// missed runtime. Only allocates up to `max_runs` runtimes when bounded
/// catch-up is in effect.
///
/// Returns `None` if the schedule is invalid (interval <= 0).
pub fn select_due_runtimes(
    schedule: &EvalSchedule,
    cursor_secs: i64,
    wall_now_secs: i64,
) -> Option<DueRuntimes> {
    if schedule.interval_secs <= 0 {
        return None;
    }

    let first_due = first_due_in_sequence(cursor_secs, schedule.start_secs, schedule.interval_secs);

    if first_due > wall_now_secs {
        return Some(DueRuntimes {
            runtimes: vec![],
            skipped: 0,
        });
    }

    // Total number of due runtimes from first_due ..= wall_now_secs.
    let total_count = ((wall_now_secs - first_due) / schedule.interval_secs) as u64 + 1;

    match schedule.missed_tick_policy {
        MissedTickPolicy::Skip => {
            let last = first_due + (total_count as i64 - 1) * schedule.interval_secs;
            Some(DueRuntimes {
                runtimes: vec![last],
                skipped: total_count.saturating_sub(1),
            })
        }
        MissedTickPolicy::BoundedCatchUp => {
            let cutoff = wall_now_secs.saturating_sub(schedule.max_lag_secs);

            // How many runtimes are strictly before the cutoff?
            let skipped_by_cutoff: u64 = if first_due >= cutoff {
                0
            } else {
                // ceil((cutoff - first_due) / interval), capped to total_count
                let count = ((cutoff - first_due + schedule.interval_secs - 1)
                    / schedule.interval_secs) as u64;
                count.min(total_count)
            };

            let remaining = total_count.saturating_sub(skipped_by_cutoff);

            if remaining == 0 {
                return Some(DueRuntimes {
                    runtimes: vec![],
                    skipped: total_count,
                });
            }

            let (keep_count, overflow) = if remaining > schedule.max_runs as u64 {
                (
                    schedule.max_runs as u64,
                    remaining - schedule.max_runs as u64,
                )
            } else {
                (remaining, 0)
            };

            // First kept runtime is at index `skipped_by_cutoff + overflow` (0-based from first_due).
            let keep_start = skipped_by_cutoff + overflow;
            let mut runtimes = Vec::with_capacity(keep_count as usize);
            for i in 0..keep_count {
                runtimes.push(first_due + (keep_start as i64 + i as i64) * schedule.interval_secs);
            }

            Some(DueRuntimes {
                runtimes,
                skipped: total_count - keep_count,
            })
        }
    }
}

/// Computes the ceiling of `time` to the next schedule boundary aligned with
/// `anchor + k * interval`. All values are Unix timestamps in seconds.
///
/// If `time <= anchor`, returns `anchor`.
/// If `interval <= 0`, returns `time`.
pub fn ceil_to_boundary(time: i64, anchor: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return time;
    }
    if time <= anchor {
        return anchor;
    }
    let diff = time - anchor;
    // ceil division: (diff + interval - 1) / interval
    let k = (diff + interval - 1) / interval;
    anchor.saturating_add(k.saturating_mul(interval))
}

#[cfg(test)]
mod test {
    use super::*;

    // --- ceil_to_boundary tests ---

    #[test]
    fn ceil_at_anchor_returns_anchor() {
        assert_eq!(ceil_to_boundary(0, 0, 60), 0);
    }

    #[test]
    fn ceil_before_anchor_returns_anchor() {
        assert_eq!(ceil_to_boundary(-10, 0, 60), 0);
    }

    #[test]
    fn ceil_just_after_anchor_returns_first_boundary() {
        assert_eq!(ceil_to_boundary(1, 0, 60), 60);
    }

    #[test]
    fn ceil_exactly_on_boundary_returns_same() {
        assert_eq!(ceil_to_boundary(60, 0, 60), 60);
        assert_eq!(ceil_to_boundary(120, 0, 60), 120);
    }

    #[test]
    fn ceil_one_second_over_boundary_returns_next() {
        assert_eq!(ceil_to_boundary(61, 0, 60), 120);
    }

    #[test]
    fn ceil_with_non_zero_anchor() {
        assert_eq!(ceil_to_boundary(100, 100, 60), 100);
        assert_eq!(ceil_to_boundary(101, 100, 60), 160);
        assert_eq!(ceil_to_boundary(160, 100, 60), 160);
        assert_eq!(ceil_to_boundary(161, 100, 60), 220);
    }

    #[test]
    fn ceil_zero_interval_returns_time() {
        assert_eq!(ceil_to_boundary(50, 0, 0), 50);
        assert_eq!(ceil_to_boundary(50, 0, -1), 50);
    }

    #[test]
    fn ceil_large_values() {
        let anchor = 0;
        let interval = 300;
        let t = 1234567890;
        let ceiled = ceil_to_boundary(t, anchor, interval);
        assert!(ceiled >= t);
        assert_eq!((ceiled - anchor) % interval, 0);
    }

    #[test]
    fn ceil_negative_time_with_positive_anchor() {
        assert_eq!(ceil_to_boundary(-100, 100, 60), 100);
    }

    // --- EvalSchedule::from_config tests ---

    fn typed_config(policy: FlowMissedTickPolicy) -> FlowScheduleConfig {
        FlowScheduleConfig {
            anchor_secs: 10,
            start_secs: 70,
            missed_tick_policy: policy,
            catchup_max_runs: 4,
            catchup_max_lag_secs: 600,
        }
    }

    #[test]
    fn from_config_none_interval_returns_none() {
        assert!(
            EvalSchedule::from_config(None, Some(&typed_config(FlowMissedTickPolicy::Skip)))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn from_config_invalid_interval_returns_err() {
        let err = EvalSchedule::from_config(
            Some(0),
            Some(&typed_config(FlowMissedTickPolicy::BoundedCatchUp)),
        )
        .unwrap_err();
        assert!(err.contains("must be positive"));
    }

    #[test]
    fn from_config_maps_typed_fields_and_policy() {
        let config = typed_config(FlowMissedTickPolicy::Skip);
        let sched = EvalSchedule::from_config(Some(300), Some(&config))
            .unwrap()
            .unwrap();
        assert_eq!(sched.interval_secs, 300);
        assert_eq!(sched.anchor_secs, 10);
        assert_eq!(sched.start_secs, 70);
        assert_eq!(sched.missed_tick_policy, MissedTickPolicy::Skip);
        assert_eq!(sched.max_runs, 4);
        assert_eq!(sched.max_lag_secs, 600);
    }

    #[test]
    fn from_config_uses_default_when_config_missing() {
        let sched = EvalSchedule::from_config(Some(300), None).unwrap().unwrap();
        assert_eq!(sched.interval_secs, 300);
        assert_eq!(sched.anchor_secs, 0);
        assert_eq!(sched.start_secs, 0);
        assert_eq!(sched.missed_tick_policy, MissedTickPolicy::BoundedCatchUp);
        assert_eq!(sched.max_runs, 3);
        assert_eq!(sched.max_lag_secs, 900);
    }

    #[test]
    fn from_config_rejects_invalid_catchup_limits() {
        let mut config = typed_config(FlowMissedTickPolicy::BoundedCatchUp);
        config.catchup_max_runs = 0;
        let err = EvalSchedule::from_config(Some(300), Some(&config)).unwrap_err();
        assert!(err.contains("catchup_max_runs"));

        let mut config = typed_config(FlowMissedTickPolicy::BoundedCatchUp);
        config.catchup_max_lag_secs = 0;
        let err = EvalSchedule::from_config(Some(300), Some(&config)).unwrap_err();
        assert!(err.contains("catchup_max_lag_secs"));

        let mut config = typed_config(FlowMissedTickPolicy::BoundedCatchUp);
        config.catchup_max_lag_secs = -1;
        let err = EvalSchedule::from_config(Some(300), Some(&config)).unwrap_err();
        assert!(err.contains("catchup_max_lag_secs"));
    }

    // --- select_due_runtimes tests ---

    fn make_schedule(
        interval: i64,
        start: i64,
        policy: MissedTickPolicy,
        max_runs: u32,
        max_lag: i64,
    ) -> EvalSchedule {
        EvalSchedule {
            interval_secs: interval,
            anchor_secs: 0,
            start_secs: start,
            missed_tick_policy: policy,
            max_runs,
            max_lag_secs: max_lag,
        }
    }

    #[test]
    fn no_due_runtimes_when_cursor_ahead() {
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 3, 300);
        let result = select_due_runtimes(&sched, 100, 100).unwrap();
        assert!(result.runtimes.is_empty());
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn single_due_runtime() {
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 3, 300);
        let result = select_due_runtimes(&sched, 50, 100).unwrap();
        assert_eq!(result.runtimes, vec![60]);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn multiple_due_runtimes_within_bounds() {
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 5, 3600);
        let result = select_due_runtimes(&sched, 0, 200).unwrap();
        assert_eq!(result.runtimes, vec![60, 120, 180]);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn bounded_catch_up_drops_too_old_runtimes() {
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 5, 120);
        let result = select_due_runtimes(&sched, 0, 300).unwrap();
        assert_eq!(result.runtimes, vec![180, 240, 300]);
        assert_eq!(result.skipped, 2);
    }

    #[test]
    fn bounded_catch_up_exceeds_max_runs() {
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 2, 3600);
        let result = select_due_runtimes(&sched, 0, 300).unwrap();
        assert_eq!(result.runtimes, vec![240, 300]);
        assert_eq!(result.skipped, 3);
    }

    #[test]
    fn bounded_catch_up_both_cutoff_and_max_runs() {
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 2, 180);
        let result = select_due_runtimes(&sched, 0, 600).unwrap();
        assert_eq!(result.runtimes, vec![540, 600]);
        assert_eq!(result.skipped, 8);
    }

    #[test]
    fn skip_policy_keeps_only_latest() {
        let sched = make_schedule(60, 0, MissedTickPolicy::Skip, 5, 3600);
        let result = select_due_runtimes(&sched, 0, 300).unwrap();
        assert_eq!(result.runtimes, vec![300]);
        assert_eq!(result.skipped, 4);
    }

    #[test]
    fn skip_policy_single_due_no_skip() {
        let sched = make_schedule(60, 0, MissedTickPolicy::Skip, 5, 3600);
        let result = select_due_runtimes(&sched, 50, 100).unwrap();
        assert_eq!(result.runtimes, vec![60]);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn invalid_interval_returns_none() {
        let sched = make_schedule(0, 0, MissedTickPolicy::BoundedCatchUp, 3, 300);
        assert!(select_due_runtimes(&sched, 0, 100).is_none());
    }

    #[test]
    fn respects_start_secs_cursor_before_start() {
        // start=120, cursor=0, wall=300 → shouldn't return runtimes before 120
        let sched = make_schedule(60, 120, MissedTickPolicy::BoundedCatchUp, 10, 3600);
        let result = select_due_runtimes(&sched, 0, 300).unwrap();
        // due should start at 120: 120, 180, 240, 300
        assert_eq!(result.runtimes, vec![120, 180, 240, 300]);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn respects_start_secs_with_catchup() {
        // start=120, cursor=0, wall=500, max_runs=2
        let sched = make_schedule(60, 120, MissedTickPolicy::BoundedCatchUp, 2, 3600);
        let result = select_due_runtimes(&sched, 0, 500).unwrap();
        // due: 120,180,240,300,360,420,480 → keep most recent 2: 420,480
        assert_eq!(result.runtimes, vec![420, 480]);
        assert_eq!(result.skipped, 5);
    }

    #[test]
    fn respects_start_secs_no_due_before_start() {
        // start=500, cursor=0, wall=400 → nothing due yet
        let sched = make_schedule(60, 500, MissedTickPolicy::BoundedCatchUp, 3, 3600);
        let result = select_due_runtimes(&sched, 0, 400).unwrap();
        assert!(result.runtimes.is_empty());
        assert_eq!(result.skipped, 0);
    }

    // --- next_runtime_after tests ---

    #[test]
    fn next_runtime_after_basic() {
        let sched = EvalSchedule {
            interval_secs: 60,
            anchor_secs: 0,
            start_secs: 0,
            missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
            max_runs: 3,
            max_lag_secs: 300,
        };
        assert_eq!(sched.next_runtime_after(0), 60);
        assert_eq!(sched.next_runtime_after(50), 60);
        assert_eq!(sched.next_runtime_after(60), 120);
        assert_eq!(sched.next_runtime_after(119), 120);
    }

    #[test]
    fn next_runtime_after_respects_start() {
        let sched = EvalSchedule {
            interval_secs: 60,
            anchor_secs: 0,
            start_secs: 300,
            missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
            max_runs: 3,
            max_lag_secs: 300,
        };
        // cursor at 0, start=300 → first due should be 300 (next boundary >= start)
        assert_eq!(sched.next_runtime_after(0), 300);
        // cursor at 310 → next after 310 is 360
        assert_eq!(sched.next_runtime_after(310), 360);
        // cursor at 360 → next is 420
        assert_eq!(sched.next_runtime_after(360), 420);
    }

    #[test]
    fn next_runtime_after_non_zero_anchor() {
        let sched = EvalSchedule {
            interval_secs: 300,
            anchor_secs: 100,
            start_secs: 100,
            missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
            max_runs: 3,
            max_lag_secs: 900,
        };
        assert_eq!(sched.next_runtime_after(100), 400);
        assert_eq!(sched.next_runtime_after(399), 400);
        assert_eq!(sched.next_runtime_after(400), 700);
    }

    // --- start ≠ anchor (explicit eval_interval_start) ---

    #[test]
    fn next_runtime_after_start_not_aligned_to_anchor() {
        // anchor=0, explicit start=50, interval=60
        let sched = EvalSchedule {
            interval_secs: 60,
            anchor_secs: 0,
            start_secs: 50,
            missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
            max_runs: 3,
            max_lag_secs: 3600,
        };
        // cursor < start → return start
        assert_eq!(sched.next_runtime_after(0), 50);
        assert_eq!(sched.next_runtime_after(40), 50);
        // cursor == start → next is 110
        assert_eq!(sched.next_runtime_after(50), 110);
        // cursor in between → next boundary from start sequence
        assert_eq!(sched.next_runtime_after(100), 110);
        assert_eq!(sched.next_runtime_after(110), 170);
    }

    #[test]
    fn due_selection_with_start_not_aligned_to_anchor() {
        let sched = EvalSchedule {
            interval_secs: 60,
            anchor_secs: 0,
            start_secs: 50,
            missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
            max_runs: 10,
            max_lag_secs: 3600,
        };
        // cursor=0 (before start), wall=200 → runtimes: 50, 110, 170
        let result = select_due_runtimes(&sched, 0, 200).unwrap();
        assert_eq!(result.runtimes, vec![50, 110, 170]);
        assert_eq!(result.skipped, 0);
    }

    #[test]
    fn respects_start_with_cursor_before_and_wall_partial() {
        let sched = EvalSchedule {
            interval_secs: 60,
            anchor_secs: 0,
            start_secs: 50,
            missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
            max_runs: 10,
            max_lag_secs: 3600,
        };
        // cursor=0, wall=100 → only start(50) is due, not 110
        let result = select_due_runtimes(&sched, 0, 100).unwrap();
        assert_eq!(result.runtimes, vec![50]);
        assert_eq!(result.skipped, 0);
    }

    // --- bounded catch-up all-skipped and huge-missed tests ---

    #[test]
    fn bounded_catch_up_all_skipped_by_cutoff() {
        // All 5 due runtimes are older than the max-lag cutoff → 0 runtimes, all skipped.
        // cursor=0, wall=360, max_lag=120 → cutoff=240
        // runtimes: 60,120,180,240,300,360
        // before cutoff (<240): 60,120,180 → 3 skipped
        // remaining: 240,300,360 → within max_runs=3, keep all
        // Actually wait, let me design a proper all-skipped scenario:
        // max_lag=60, wall=360 → cutoff=300
        // runtimes: 60,120,180,240,300,360
        // before cutoff (<300): 60,120,180,240 → 4 skipped
        // remaining: 300,360 → max_runs=2 → keep both, 0 overflow
        // Not all-skipped. Let me make all-skipped:
        // max_lag=60 → cutoff = wall-lag = 360-60 = 300
        // runtimes: 60,...,360 → before cutoff: 60,120,180,240 → 4
        // remaining: 300,360 → not empty.
        //
        // For all-skipped, use max_runs=0? No, max_runs must be > 0.
        // Try: max_lag=60, max_runs=2, wall=240
        // runtimes: 60,120,180,240
        // cutoff=180, before_cutoff: 60,120 → 2
        // remaining: 180,240 → max_runs=2 → keep both, 0 overflow
        //
        // Better: max_lag=0 → actually max_lag must be positive.
        // Let me use: interval=60, max_lag=60, wall=240
        // runtimes: 60,120,180,240
        // cutoff=180, before_cutoff: 60,120 (2 rt < 180)
        // remaining: 180,240 → max_runs=2 → keep both
        //
        // For true all-skipped: need all remaining to be > max_runs AND overflow pushes them out.
        // Or just have all runtimes < cutoff.
        // Use: cursor close to wall, small max_lag.
        // cursor=0, wall=120, max_lag=60, interval=60
        // runtimes: 60,120
        // cutoff=60, before_cutoff: (<60) → none! 60 is not < 60. So 0 skipped_by_cutoff.
        // remaining: 60,120, max_runs=2 → keep both.
        //
        // Hmm, for all-skipped we need skipped_by_cutoff to eat everything.
        // Actually, let me reconsider. The scenario is:
        // max_lag_secs=30, wall=360, interval=60
        // runtimes: 60,120,180,240,300,360
        // cutoff = 360-30 = 330
        // before_cutoff (<330): 60,120,180,240,300 → 5
        // remaining: 360 → just 1, but if max_runs=1, keep 1
        //
        // To get ALL skipped, we need: cutoff > last_runtime, so skipped_by_cutoff = total_count
        // max_lag_secs=30, wall=100, interval=60
        // runtimes: 60, (120 > 100 not included) → total=1
        // cutoff=100-30=70, before_cutoff: 60 (<70) → 1 → remaining=0 → ALL SKIPPED!
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 3, 30);
        // cursor=0, wall=100, max_lag=30, interval=60
        // first_due=60, total_count=1 (60 <= 100, 120 > 100)
        // cutoff=100-30=70
        // skipped_by_cutoff = ceil((70-60)/60) = ceil(10/60) = 1
        // remaining=0 → all skipped, 0 runtimes
        let result = select_due_runtimes(&sched, 0, 100).unwrap();
        assert!(result.runtimes.is_empty());
        assert_eq!(result.skipped, 1);
    }

    #[test]
    fn bounded_catch_up_all_skipped_and_then_exceeded_max_runs() {
        // Large missed interval: cursor=0, wall=6000, interval=60 → 100 runtimes
        // max_lag=30, so cutoff=5970 → skipped_by_cutoff = ceil((5970-60)/60)=ceil(5910/60)=99
        // remaining=1 (runtime 6000), max_runs=3 → keep 1.
        // But if max_lag=60, cutoff=5940 → skipped_by_cutoff=ceil(5880/60)=98
        // remaining=2 (5940, 6000). max_runs=1 → keep 1, overflow=1.
        // Total skipped=98+1=99.
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 1, 60);
        let result = select_due_runtimes(&sched, 0, 6000).unwrap();
        // first_due=60, total_count = (6000-60)/60 + 1 = 5940/60 + 1 = 99 + 1 = 100
        // cutoff = 6000-60 = 5940
        // skipped_by_cutoff = ceil((5940-60)/60) = ceil(5880/60) = 98
        // remaining = 2 → max_runs=1 → keep_count=1, overflow=1
        // kept: runtime at index 98+1=99 → 60+99*60=6000
        assert_eq!(result.runtimes, vec![6000]);
        assert_eq!(result.skipped, 99); // total 100 - 1 kept
    }

    #[test]
    fn huge_missed_bounded_catch_up_only_allocates_max_runs() {
        // A huge gap: cursor=0, wall=86400, interval=60, max_runs=5.
        // Thousands of due runtimes, but we only allocate 5.
        // start=0, first_due=60.
        // total = (86400-60)/60 + 1 = 1440.
        // cutoff = 86400-3600 = 82800.
        // skipped_by_cutoff = ceil((82800-60)/60) = 82740/60 = 1379.
        // remaining = 1440-1379 = 61.
        // max_runs=5 → keep_count=5, overflow=56.
        // keep_start = 1379+56 = 1435.
        // kept: indices 1435..=1439: 60*(1436)=86160, ..., 60*(1440)=86400.
        let sched = make_schedule(60, 0, MissedTickPolicy::BoundedCatchUp, 5, 3600);
        let result = select_due_runtimes(&sched, 0, 86400).unwrap();
        assert_eq!(result.runtimes.len(), 5);
        assert_eq!(result.runtimes[0], 86160);
        assert_eq!(result.runtimes[4], 86400);
        assert_eq!(result.skipped, 1435);
    }

    #[test]
    fn huge_missed_skip_policy_only_allocates_one() {
        // Skip policy with huge miss: should only allocate 1 runtime.
        // start=0, cursor=0, wall=100000, interval=60
        // first_due=60, total=1666, last=60+1665*60=99960
        let sched = make_schedule(60, 0, MissedTickPolicy::Skip, 5, 3600);
        let result = select_due_runtimes(&sched, 0, 100000).unwrap();
        assert_eq!(result.runtimes.len(), 1);
        assert_eq!(result.runtimes[0], 99960);
        assert_eq!(result.skipped, 1665);
    }
}
