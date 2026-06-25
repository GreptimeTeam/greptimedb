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

//! Helpers for stable `EVAL INTERVAL` scheduled runtimes.

pub use common_meta::key::flow::flow_info::{FlowMissedTickPolicy, FlowScheduleConfig};

const DEFAULT_MAX_RUNS: u32 = 3;
const MIN_LAG_SECONDS: i64 = 300;

/// Runtime schedule for an `EVAL INTERVAL` flow.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalSchedule {
    /// Interval between scheduled runtimes in seconds.
    pub interval_secs: i64,
    /// Anchor timestamp as seconds since Unix epoch.
    pub anchor_secs: i64,
    /// First scheduled runtime as seconds since Unix epoch.
    pub start_secs: i64,
    /// Policy for handling missed runtimes.
    pub missed_tick_policy: MissedTickPolicy,
    /// Maximum number of due runtimes to catch up.
    pub max_runs: u32,
    /// Maximum due-runtime lag to keep for catch-up.
    pub max_lag_secs: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissedTickPolicy {
    /// Keep recent due runtimes within `max_lag_secs`, capped by `max_runs`.
    BoundedCatchUp,
    /// Execute only the latest due runtime.
    Skip,
}

impl EvalSchedule {
    pub fn from_config(
        eval_interval_secs: Option<i64>,
        config: Option<&FlowScheduleConfig>,
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

        Ok(Some(match config {
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
                        FlowMissedTickPolicy::BoundedCatchUp => MissedTickPolicy::BoundedCatchUp,
                        FlowMissedTickPolicy::Skip => MissedTickPolicy::Skip,
                    },
                    max_runs: c.catchup_max_runs,
                    max_lag_secs: c.catchup_max_lag_secs,
                }
            }
            None => Self {
                interval_secs,
                anchor_secs: 0,
                start_secs: 0,
                missed_tick_policy: MissedTickPolicy::BoundedCatchUp,
                max_runs: DEFAULT_MAX_RUNS,
                max_lag_secs: std::cmp::max(MIN_LAG_SECONDS, 3 * interval_secs),
            },
        }))
    }

    /// Returns the next scheduled runtime strictly after `cursor_secs`.
    pub fn next_runtime_after(&self, cursor_secs: i64) -> i64 {
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

#[derive(Debug, Clone, PartialEq)]
pub struct DueRuntimes {
    /// Runtimes to execute, ordered oldest to newest.
    pub runtimes: Vec<i64>,
    /// Number of due runtimes skipped by lag or max-runs limits.
    pub skipped: u64,
}

/// Select due runtimes `<= wall_now_secs` without materializing all missed ticks.
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
            let skipped_by_cutoff = if first_due >= cutoff {
                0
            } else {
                ((cutoff - first_due + schedule.interval_secs - 1) / schedule.interval_secs) as u64
            }
            .min(total_count);

            let remaining = total_count.saturating_sub(skipped_by_cutoff);
            if remaining == 0 {
                return Some(DueRuntimes {
                    runtimes: vec![],
                    skipped: total_count,
                });
            }

            let keep_count = remaining.min(schedule.max_runs as u64);
            let keep_start = skipped_by_cutoff + remaining.saturating_sub(keep_count);
            let runtimes = (0..keep_count)
                .map(|i| first_due + (keep_start as i64 + i as i64) * schedule.interval_secs)
                .collect::<Vec<_>>();

            Some(DueRuntimes {
                runtimes,
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

    let k = (time - anchor + interval - 1) / interval;
    anchor.saturating_add(k.saturating_mul(interval))
}

#[cfg(test)]
mod test {
    use super::*;

    fn schedule(
        start: i64,
        policy: MissedTickPolicy,
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
        assert_eq!(from_typed.missed_tick_policy, MissedTickPolicy::Skip);
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
    fn next_runtime_after_respects_start_sequence() {
        let s = schedule(50, MissedTickPolicy::BoundedCatchUp, 3, 300);
        assert_eq!(s.next_runtime_after(0), 50);
        assert_eq!(s.next_runtime_after(50), 110);
        assert_eq!(s.next_runtime_after(100), 110);
    }

    #[test]
    fn due_runtime_selection_handles_empty_and_start_boundary() {
        let s = schedule(120, MissedTickPolicy::BoundedCatchUp, 10, 3600);
        assert_eq!(
            select_due_runtimes(&s, 0, 100).unwrap().runtimes,
            Vec::<i64>::new()
        );
        assert_eq!(
            select_due_runtimes(&s, 0, 300).unwrap().runtimes,
            vec![120, 180, 240, 300]
        );
    }

    #[test]
    fn bounded_catch_up_applies_lag_and_max_runs() {
        let s = schedule(0, MissedTickPolicy::BoundedCatchUp, 2, 180);
        let due = select_due_runtimes(&s, 0, 600).unwrap();
        assert_eq!(due.runtimes, vec![540, 600]);
        assert_eq!(due.skipped, 8);
    }

    #[test]
    fn bounded_catch_up_can_skip_all_due_runtimes() {
        let s = schedule(0, MissedTickPolicy::BoundedCatchUp, 3, 30);
        let due = select_due_runtimes(&s, 0, 100).unwrap();
        assert!(due.runtimes.is_empty());
        assert_eq!(due.skipped, 1);
    }

    #[test]
    fn skip_policy_keeps_only_latest_due_runtime() {
        let s = schedule(0, MissedTickPolicy::Skip, 5, 3600);
        let due = select_due_runtimes(&s, 0, 300).unwrap();
        assert_eq!(due.runtimes, vec![300]);
        assert_eq!(due.skipped, 4);
    }

    #[test]
    fn huge_missed_gap_allocates_only_kept_runtimes() {
        let s = schedule(0, MissedTickPolicy::BoundedCatchUp, 5, 3600);
        let due = select_due_runtimes(&s, 0, 86400).unwrap();
        assert_eq!(due.runtimes, vec![86160, 86220, 86280, 86340, 86400]);
        assert_eq!(due.skipped, 1435);
    }
}
