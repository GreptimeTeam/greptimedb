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

use std::time::Duration;

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, ScopedTimerGuard, Time, Timestamp,
};

/// This metrics struct is used to record and hold metrics like memory usage
/// of result batch in [`crate::table::scan::StreamWithMetricWrapper`]
/// during query execution.
#[derive(Debug, Clone)]
pub struct StreamMetrics {
    /// Timestamp when the stream finished
    end_time: Timestamp,
    /// Number of rows in output
    output_rows: Count,
    /// Number of bytes in output
    output_bytes: Count,
    /// Elapsed time used to `poll` the stream
    poll_elapsed: Time,
    /// CPU time used to compute scan output.
    elapsed_compute: Time,
    /// Elapsed time used to `.await`ing the stream
    await_elapsed: Time,
}

impl StreamMetrics {
    /// Create a new [`StreamMetrics`] structure, and set `start_time` to now.
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let start_time = MetricBuilder::new(metrics).start_timestamp(partition);
        start_time.record();

        Self {
            end_time: MetricBuilder::new(metrics).end_timestamp(partition),
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            output_bytes: MetricBuilder::new(metrics).output_bytes(partition),
            poll_elapsed: MetricBuilder::new(metrics).subset_time("elapsed_poll", partition),
            elapsed_compute: MetricBuilder::new(metrics).elapsed_compute(partition),
            await_elapsed: MetricBuilder::new(metrics).subset_time("elapsed_await", partition),
        }
    }

    pub fn record_output(&self, num_rows: usize) {
        self.output_rows.add(num_rows);
    }

    pub fn record_output_bytes(&self, num_bytes: usize) {
        self.output_bytes.add(num_bytes);
    }

    /// Record the end time of the query
    pub fn try_done(&self) {
        if self.end_time.value().is_none() {
            self.end_time.record()
        }
    }

    /// Return a timer guard that records the time elapsed in poll
    pub fn poll_timer(&self) -> ScopedTimerGuard<'_> {
        self.poll_elapsed.timer()
    }

    pub fn record_elapsed_compute(&self, duration: Duration) {
        self.elapsed_compute.add_duration(duration);
    }

    pub fn record_await_duration(&self, duration: Duration) {
        self.await_elapsed.add_duration(duration);
    }
}

impl Drop for StreamMetrics {
    fn drop(&mut self) {
        self.try_done()
    }
}
