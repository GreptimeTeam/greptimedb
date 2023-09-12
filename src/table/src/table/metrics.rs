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

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Timestamp,
};

/// This metrics struct is used to record and hold memory usage
/// of result batch in [`crate::table::scan::StreamWithMetricWrapper`]
/// during query execution, indicating size of the dataset.
#[derive(Debug, Clone)]
pub struct MemoryUsageMetrics {
    end_time: Timestamp,
    // used memory in bytes
    mem_used: Gauge,
    // number of rows in output
    output_rows: Count,
}

impl MemoryUsageMetrics {
    /// Create a new MemoryUsageMetrics structure, and set `start_time` to now
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let start_time = MetricBuilder::new(metrics).start_timestamp(partition);
        start_time.record();

        Self {
            end_time: MetricBuilder::new(metrics).end_timestamp(partition),
            mem_used: MetricBuilder::new(metrics).mem_used(partition),
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
        }
    }

    pub fn record_mem_usage(&self, mem_used: usize) {
        self.mem_used.add(mem_used);
    }

    pub fn record_output(&self, num_rows: usize) {
        self.output_rows.add(num_rows);
    }

    /// Record the end time of the query
    pub fn try_done(&self) {
        if self.end_time.value().is_none() {
            self.end_time.record()
        }
    }
}

impl Drop for MemoryUsageMetrics {
    fn drop(&mut self) {
        self.try_done()
    }
}
