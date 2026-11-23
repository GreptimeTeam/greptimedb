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

//! Bounded-label progress metrics. Stage durations are wall time, not CPU time.
//! Read stages include decode/cache work; store_page_bytes counts fetched Parquet
//! pages, not all object-store traffic (for example, footer requests).

use std::time::Instant;

use prometheus::{
    Histogram, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, register_histogram_vec,
    register_int_counter_vec, register_int_gauge_vec,
};

lazy_static::lazy_static! {
    static ref STAGE_SECONDS: HistogramVec = register_histogram_vec!(
        "greptime_mito_table_compaction_stage_seconds",
        "Table compaction stage wall seconds; overlapping stages are not additive",
        &["stage"],
        vec![0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 60.0, 300.0]
    ).unwrap();
    static ref STAGE_ACTIVE: IntGaugeVec = register_int_gauge_vec!(
        "greptime_mito_table_compaction_stage_active",
        "Active table compaction stage operations",
        &["stage"]
    ).unwrap();
    static ref PROGRESS: IntCounterVec = register_int_counter_vec!(
        "greptime_mito_table_compaction_progress_total",
        "Table compaction work by unit; counters include work from failed attempts",
        &["unit"]
    ).unwrap();
}

/// Balances active-stage accounting on success, error and future cancellation.
pub(crate) struct StageGuard {
    started: Instant,
    elapsed: Histogram,
    active: IntGauge,
}

impl StageGuard {
    pub(crate) fn start(stage: &'static str) -> Self {
        let active = STAGE_ACTIVE.with_label_values(&[stage]);
        active.inc();
        Self {
            started: Instant::now(),
            elapsed: STAGE_SECONDS.with_label_values(&[stage]),
            active,
        }
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        self.elapsed.observe(self.started.elapsed().as_secs_f64());
        self.active.dec();
    }
}

pub(crate) fn observe_stage(stage: &'static str, duration: std::time::Duration) {
    STAGE_SECONDS
        .with_label_values(&[stage])
        .observe(duration.as_secs_f64());
}

pub(crate) fn progress(unit: &'static str, count: u64) {
    PROGRESS.with_label_values(&[unit]).inc_by(count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_guard_balances_on_early_return() {
        fn work() -> Result<(), ()> {
            let _stage = StageGuard::start("test_early_return");
            Err(())
        }
        assert!(work().is_err());
        assert_eq!(
            STAGE_ACTIVE.with_label_values(&["test_early_return"]).get(),
            0
        );
        assert_eq!(
            STAGE_SECONDS
                .with_label_values(&["test_early_return"])
                .get_sample_count(),
            1
        );
    }
}
