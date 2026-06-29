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

use crate::error::{InvalidQuerySnafu, Result};

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

    /// Returns the next scheduled time strictly after `cursor_secs`.
    pub fn next_scheduled_time_after(&self, cursor_secs: i64) -> i64 {
        next_in_sequence(cursor_secs, self.start_secs, self.interval_secs)
    }
}

fn next_in_sequence(cursor: i64, start: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return cursor.saturating_add(1).max(start);
    }
    if cursor < start {
        return start;
    }

    let k = (cursor - start) / interval;
    start.saturating_add((k + 1).saturating_mul(interval))
}

fn first_due_in_sequence(cursor: i64, start: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return cursor.saturating_add(1).max(start);
    }
    if cursor < start {
        start
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
pub fn select_due_scheduled_times(
    schedule: &EvalSchedule,
    cursor_secs: i64,
    wall_now_secs: i64,
) -> Option<DueScheduledTimes> {
    if schedule.interval_secs <= 0 {
        return None;
    }

    let first_due = first_due_in_sequence(cursor_secs, schedule.start_secs, schedule.interval_secs);
    if first_due > wall_now_secs {
        return Some(DueScheduledTimes {
            scheduled_times_secs: vec![],
            skipped: 0,
        });
    }

    let total_count = ((wall_now_secs - first_due) / schedule.interval_secs) as u64 + 1;
    match schedule.missed_tick_policy {
        FlowMissedTickPolicy::Skip => {
            let last = first_due + (total_count as i64 - 1) * schedule.interval_secs;
            Some(DueScheduledTimes {
                scheduled_times_secs: vec![last],
                skipped: total_count.saturating_sub(1),
            })
        }
        FlowMissedTickPolicy::BoundedCatchUp => {
            let cutoff = wall_now_secs.saturating_sub(schedule.max_lag_secs);
            let skipped_by_cutoff = if first_due >= cutoff {
                0
            } else {
                ((cutoff - first_due + schedule.interval_secs - 1) / schedule.interval_secs) as u64
            }
            .min(total_count);

            let remaining = total_count.saturating_sub(skipped_by_cutoff);
            if remaining == 0 {
                return Some(DueScheduledTimes {
                    scheduled_times_secs: vec![],
                    skipped: total_count,
                });
            }

            // max_lag_secs decides which missed scheduled times are recent enough to
            // run; max_runs caps how many of those times we execute
            // back-to-back in one scheduler pass.
            let keep_count = remaining.min(schedule.max_runs as u64);
            let keep_start = skipped_by_cutoff + remaining.saturating_sub(keep_count);
            let scheduled_times_secs = (0..keep_count)
                .map(|i| first_due + (keep_start as i64 + i as i64) * schedule.interval_secs)
                .collect::<Vec<_>>();

            Some(DueScheduledTimes {
                scheduled_times_secs,
                skipped: total_count - keep_count,
            })
        }
    }
}

/// Ceils `time` to the next `anchor + k * interval` boundary.
pub fn ceil_to_boundary(time: i64, anchor: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return time;
    }
    if time <= anchor {
        return anchor;
    }

    let diff = i128::from(time) - i128::from(anchor);
    let interval = i128::from(interval);
    let k = (diff + interval - 1) / interval;
    let boundary = i128::from(anchor) + k * interval;

    boundary.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
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
            start_secs: 70,
            missed_tick_policy: policy,
            catchup_max_runs: 4,
            catchup_max_lag_secs: 600,
        }
    }

    #[test]
    fn ceil_to_boundary_handles_anchor_and_interval_edges() {
        assert_eq!(ceil_to_boundary(-10, 0, 60), 0);
        assert_eq!(ceil_to_boundary(0, 0, 60), 0);
        assert_eq!(ceil_to_boundary(1, 0, 60), 60);
        assert_eq!(ceil_to_boundary(60, 0, 60), 60);
        assert_eq!(ceil_to_boundary(101, 100, 60), 160);
        assert_eq!(ceil_to_boundary(50, 0, 0), 50);
        assert_eq!(ceil_to_boundary(i64::MAX, 0, 60), i64::MAX);
        assert_eq!(ceil_to_boundary(i64::MAX - 1, i64::MIN, 60), i64::MAX);
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
        assert_eq!(from_typed.start_secs, 70);
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
    fn next_scheduled_time_after_respects_start_sequence() {
        let s = schedule(50, FlowMissedTickPolicy::BoundedCatchUp, 3, 300);
        assert_eq!(s.next_scheduled_time_after(0), 50);
        assert_eq!(s.next_scheduled_time_after(50), 110);
        assert_eq!(s.next_scheduled_time_after(100), 110);
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
