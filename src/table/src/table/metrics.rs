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
    ExecutionPlanMetricsSet, Gauge, MetricBuilder, Timestamp,
};

#[derive(Debug, Clone)]
pub struct MemoryUsageMetrics {
    end_time: Timestamp,
    mem_used: Gauge,
}

impl MemoryUsageMetrics {
    /// Create a new BaselineMetric structure, and set  `start_time` to now
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let start_time = MetricBuilder::new(metrics).start_timestamp(partition);
        start_time.record();

        Self {
            end_time: MetricBuilder::new(metrics).end_timestamp(partition),
            mem_used: MetricBuilder::new(metrics).gauge("mem_used", partition),
        }
    }

    pub fn record_mem_usage(&self, mem_used: usize) {
        self.mem_used.add(mem_used);
    }

    /// If not previously recorded `done()`, record
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
