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

use std::time::{Duration, Instant};

use crate::metrics::{INDEX_CREATE_BYTES_TOTAL, INDEX_CREATE_ELAPSED, INDEX_CREATE_ROWS_TOTAL};

pub(crate) type ByteCount = u64;
pub(crate) type RowCount = usize;

/// Stage of the index creation process.
enum Stage {
    Update,
    Finish,
    Cleanup,
}

/// Statistics for index creation. Flush metrics when dropped.
pub(crate) struct Statistics {
    /// Index type.
    index_type: &'static str,
    /// Accumulated elapsed time for the index update stage.
    update_elapsed: Duration,
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
    pub fn new(index_type: &'static str) -> Self {
        Self {
            index_type,
            update_elapsed: Duration::default(),
            finish_elapsed: Duration::default(),
            cleanup_eplased: Duration::default(),
            row_count: 0,
            byte_count: 0,
        }
    }

    /// Starts timing the update stage, returning a `TimerGuard` to automatically record duration.
    #[must_use]
    pub fn record_update(&mut self) -> TimerGuard<'_> {
        TimerGuard::new(self, Stage::Update)
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
        INDEX_CREATE_ELAPSED
            .with_label_values(&["update", self.index_type])
            .observe(self.update_elapsed.as_secs_f64());
        INDEX_CREATE_ELAPSED
            .with_label_values(&["finish", self.index_type])
            .observe(self.finish_elapsed.as_secs_f64());
        INDEX_CREATE_ELAPSED
            .with_label_values(&["cleanup", self.index_type])
            .observe(self.cleanup_eplased.as_secs_f64());
        INDEX_CREATE_ELAPSED
            .with_label_values(&["total", self.index_type])
            .observe(
                (self.update_elapsed + self.finish_elapsed + self.cleanup_eplased).as_secs_f64(),
            );

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
pub(crate) struct TimerGuard<'a> {
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
            Stage::Finish => {
                self.stats.finish_elapsed += self.timer.elapsed();
            }
            Stage::Cleanup => {
                self.stats.cleanup_eplased += self.timer.elapsed();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_basic() {
        let mut stats = Statistics::new("test");
        {
            let mut guard = stats.record_update();
            guard.inc_byte_count(100);
            guard.inc_row_count(10);

            let now = Instant::now();
            while now.elapsed().is_zero() {
                // busy loop
            }
        }
        {
            let _guard = stats.record_finish();
            let now = Instant::now();
            while now.elapsed().is_zero() {
                // busy loop
            }
        }
        {
            let _guard = stats.record_cleanup();
            let now = Instant::now();
            while now.elapsed().is_zero() {
                // busy loop
            }
        }
        assert_eq!(stats.row_count(), 10);
        assert_eq!(stats.byte_count(), 100);
        assert!(stats.update_elapsed > Duration::default());
        assert!(stats.finish_elapsed > Duration::default());
        assert!(stats.cleanup_eplased > Duration::default());
    }
}
