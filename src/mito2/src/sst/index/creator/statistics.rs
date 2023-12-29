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

use crate::metrics::{INDEX_CREATE_BYTES_TOTAL, INDEX_CREATE_COST_TIME, INDEX_CREATE_ROWS_TOTAL};

enum Stage {
    Update,
    Finish,
    Cleanup,
}

#[derive(Default)]
pub(crate) struct Statistics {
    update_cost: Duration,
    finish_cost: Duration,
    cleanup_cost: Duration,
    row_count: usize,
    byte_count: usize,
}

impl Statistics {
    pub fn record_update(&mut self) -> TimerGuard<'_> {
        TimerGuard::new(self, Stage::Update)
    }

    pub fn record_finish(&mut self) -> TimerGuard<'_> {
        TimerGuard::new(self, Stage::Finish)
    }

    pub fn record_cleanup(&mut self) -> TimerGuard<'_> {
        TimerGuard::new(self, Stage::Cleanup)
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn byte_count(&self) -> usize {
        self.byte_count
    }

    fn flush(&self) {
        INDEX_CREATE_COST_TIME
            .with_label_values(&["update"])
            .observe(self.update_cost.as_secs_f64());
        INDEX_CREATE_COST_TIME
            .with_label_values(&["finish"])
            .observe(self.finish_cost.as_secs_f64());
        INDEX_CREATE_COST_TIME
            .with_label_values(&["cleanup"])
            .observe(self.cleanup_cost.as_secs_f64());
        INDEX_CREATE_COST_TIME
            .with_label_values(&["total"])
            .observe((self.update_cost + self.finish_cost + self.cleanup_cost).as_secs_f64());

        INDEX_CREATE_ROWS_TOTAL.inc_by(self.row_count as _);
        INDEX_CREATE_BYTES_TOTAL.inc_by(self.byte_count as _);
    }
}

impl Drop for Statistics {
    fn drop(&mut self) {
        self.flush();
    }
}

pub(crate) struct TimerGuard<'a> {
    stats: &'a mut Statistics,
    stage: Stage,
    timer: Instant,
}

impl<'a> TimerGuard<'a> {
    fn new(stats: &'a mut Statistics, stage: Stage) -> Self {
        Self {
            stats,
            stage,
            timer: Instant::now(),
        }
    }

    pub fn inc_row_count(&mut self, n: usize) {
        self.stats.row_count += n;
    }

    pub fn inc_byte_count(&mut self, n: usize) {
        self.stats.byte_count += n;
    }
}

impl Drop for TimerGuard<'_> {
    fn drop(&mut self) {
        match self.stage {
            Stage::Update => {
                self.stats.update_cost += self.timer.elapsed();
            }
            Stage::Finish => {
                self.stats.finish_cost += self.timer.elapsed();
            }
            Stage::Cleanup => {
                self.stats.cleanup_cost += self.timer.elapsed();
            }
        }
    }
}
