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

// Sample coverage of a region scan that carries a sample.
//
// The figures are registered on the scan node's `ExecutionPlanMetricsSet` by the region
// scanner, and only when the scan actually samples: an unsampled scan never shows these
// names, not even with a `0` value. `EXPLAIN ANALYZE` therefore reports them with the
// rest of the node's metrics, in the JSON `metrics` object and in the text
// `metrics=[...]` list. All of them count rows or series, never blocks.
//
// - `sample_rows_total`: rows of the candidate SST files a classic-table primary-key
//   sample could order by key, summed from their manifest row counts. Its premise: a
//   candidate is a file the key predicate does not exclude, so files the predicate
//   prunes contribute no row and are not counted; files without encoded-key statistics
//   are not counted either (they show up in `sample_rows_unaccounted`); and a file whose
//   key range overlaps another candidate is still counted although the step function the
//   interval is chosen from leaves it out. The figure therefore bounds the rows the
//   interval was inverted against, and can exceed it when candidate ranges overlap.
// - `sample_rows_in_kept_files`: upper bound of the rows the sample keeps, at file
//   granularity: the manifest `num_rows` of every file whose key range the interval
//   overlaps, summed. A file the interval covers only in part still contributes all of
//   its rows, so whenever a boundary of the interval falls inside a file the figure
//   over-counts. It never under-counts the rows the sample keeps from the key order,
//   because every row inside the interval lies in a file the interval overlaps; the rows
//   of files without key statistics are not part of it and are counted by
//   `sample_rows_unaccounted` instead. For the exact number of kept rows read the same
//   node's existing `output_rows`, which with no further filter is the number of rows the
//   scan returns.
// - `sample_rows_unaccounted`: rows held by in-range files without encoded-key
//   statistics. Such a file cannot be placed on the key axis, so the sample never prunes
//   the file and the interval does not account for its rows; its row groups are still
//   pruned by their own key statistics, so this too is an upper bound of the rows the
//   file contributes. Absent when there is no such file.
// - `sample_series_kept`: metric series the series sample kept, i.e. the series the scan
//   reads.
// - `sample_series_total`: metric series the sample decided on, counted before its
//   `tsid` bound is applied: the series the series-index artifacts hold for the scanned
//   ranges, or the candidates the scan discovered. Absent when the artifacts cannot
//   state how many series they hold.

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
    /// Elapsed time used to `.await`ing the stream
    await_elapsed: Time,
    /// Time spent evaluating and applying decoded dynamic filters.
    dynamic_filter_cost: Time,
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
            await_elapsed: MetricBuilder::new(metrics).subset_time("elapsed_await", partition),
            dynamic_filter_cost: MetricBuilder::new(metrics)
                .subset_time("dynamic_filter_cost", partition),
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

    pub fn dynamic_filter_timer(&self) -> ScopedTimerGuard<'_> {
        self.dynamic_filter_cost.timer()
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
