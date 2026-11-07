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

#[cfg(test)]
mod tests;

use std::time::{Duration, Instant};

use crate::metrics::{INDEX_CREATE_BYTES_TOTAL, INDEX_CREATE_ELAPSED, INDEX_CREATE_ROWS_TOTAL};

pub type ByteCount = u64;
pub type RowCount = usize;

/// Stage of the index creation process.
enum Stage {
    Update,
    Merge,
    Finish,
    Cleanup,
}

/// Statistics for index creation. Flush metrics when dropped.
pub struct Statistics {
    /// Index type.
    index_type: &'static str,
    /// Accumulated elapsed time for the index update stage.
    update_elapsed: Duration,
    /// Whether update and total elapsed time can be attributed to this index.
    update_timing_available: bool,
    /// Accumulated time spent merging existing index states.
    merge_elapsed: Duration,
    /// Accumulated elapsed time for the index finish stage.
    finish_elapsed: Duration,
    /// Accumulated elapsed time for the cleanup stage.
    cleanup_eplased: Duration,
    /// Number of rows in the index.
    row_count: RowCount,
    /// Number of bytes in the index.
    byte_count: ByteCount,
}

impl Statistics {
    /// Creates index statistics whose accumulated metrics are published on drop.
    pub fn new(index_type: &'static str) -> Self {
        Self {
            index_type,
            update_elapsed: Duration::default(),
            update_timing_available: true,
            merge_elapsed: Duration::default(),
            finish_elapsed: Duration::default(),
            cleanup_eplased: Duration::default(),
            row_count: 0,
            byte_count: 0,
        }
    }

    /// Creates counters for interleaved work whose update time cannot be attributed
    /// to this index. Update and total histograms remain absent until `record_update`.
    pub fn new_without_update_timing(index_type: &'static str) -> Self {
        let mut stats = Self::new(index_type);
        stats.update_timing_available = false;
        stats
    }

    /// Counts rows without taking a clock sample or claiming an update duration.
    pub fn inc_row_count(&mut self, rows: RowCount) {
        self.row_count += rows;
    }

    /// Starts timing the update stage, returning a `TimerGuard` to automatically record duration.
    #[must_use]
    pub fn record_update(&mut self) -> TimerGuard<'_> {
        self.update_timing_available = true;
        TimerGuard::new(self, Stage::Update)
    }

    /// Starts timing merges of existing index states.
    ///
    /// Merged input rows are represented rows, not newly scanned output rows. Callers
    /// count actual output rows separately through the update or finish guard.
    #[must_use]
    pub fn record_merge(&mut self) -> TimerGuard<'_> {
        TimerGuard::new(self, Stage::Merge)
    }

    /// Starts timing the finish stage, returning a `TimerGuard` to automatically record duration.
    #[must_use]
    pub fn record_finish(&mut self) -> TimerGuard<'_> {
        TimerGuard::new(self, Stage::Finish)
    }

    /// Starts timing the cleanup stage, returning a `TimerGuard` to automatically record duration.
    #[must_use]
    pub fn record_cleanup(&mut self) -> TimerGuard<'_> {
        TimerGuard::new(self, Stage::Cleanup)
    }

    /// Returns row count.
    pub fn row_count(&self) -> RowCount {
        self.row_count
    }

    /// Returns byte count.
    pub fn byte_count(&self) -> ByteCount {
        self.byte_count
    }
}

impl Drop for Statistics {
    fn drop(&mut self) {
        if self.update_timing_available {
            INDEX_CREATE_ELAPSED
                .with_label_values(&["update", self.index_type])
                .observe(self.update_elapsed.as_secs_f64());
        }
        if !self.merge_elapsed.is_zero() {
            INDEX_CREATE_ELAPSED
                .with_label_values(&["merge", self.index_type])
                .observe(self.merge_elapsed.as_secs_f64());
        }
        INDEX_CREATE_ELAPSED
            .with_label_values(&["finish", self.index_type])
            .observe(self.finish_elapsed.as_secs_f64());
        INDEX_CREATE_ELAPSED
            .with_label_values(&["cleanup", self.index_type])
            .observe(self.cleanup_eplased.as_secs_f64());
        if self.update_timing_available {
            INDEX_CREATE_ELAPSED
                .with_label_values(&["total", self.index_type])
                .observe(
                    (self.update_elapsed
                        + self.merge_elapsed
                        + self.finish_elapsed
                        + self.cleanup_eplased)
                        .as_secs_f64(),
                );
        }

        INDEX_CREATE_ROWS_TOTAL
            .with_label_values(&[self.index_type])
            .inc_by(self.row_count as _);
        INDEX_CREATE_BYTES_TOTAL
            .with_label_values(&[self.index_type])
            .inc_by(self.byte_count as _);
    }
}

/// `TimerGuard` is a RAII struct that ensures elapsed time
/// is recorded when it goes out of scope.
pub struct TimerGuard<'a> {
    stats: &'a mut Statistics,
    stage: Stage,
    timer: Instant,
}

impl<'a> TimerGuard<'a> {
    /// Creates a new `TimerGuard`,
    fn new(stats: &'a mut Statistics, stage: Stage) -> Self {
        Self {
            stats,
            stage,
            timer: Instant::now(),
        }
    }

    /// Increases the row count of the index creation statistics.
    pub fn inc_row_count(&mut self, n: usize) {
        self.stats.row_count += n;
    }

    /// Increases the byte count of the index creation statistics.
    pub fn inc_byte_count(&mut self, n: u64) {
        self.stats.byte_count += n;
    }
}

impl Drop for TimerGuard<'_> {
    fn drop(&mut self) {
        match self.stage {
            Stage::Update => {
                self.stats.update_elapsed += self.timer.elapsed();
            }
            Stage::Merge => {
                self.stats.merge_elapsed += self.timer.elapsed();
            }
            Stage::Finish => {
                self.stats.finish_elapsed += self.timer.elapsed();
            }
            Stage::Cleanup => {
                self.stats.cleanup_eplased += self.timer.elapsed();
            }
        }
    }
}
