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

use common_telemetry::logging::SlowQueryOptions;
use common_telemetry::slow;
use rand::random;

use crate::parser::QueryStatement;

/// StatementStatistics is used to collect statistics for a statement.
#[derive(Default, Clone, Debug)]
pub struct StatementStatistics {
    /// slow_query is used to configure slow query log.
    pub slow_query: SlowQueryOptions,
}

impl StatementStatistics {
    pub fn new(slow_query_options: SlowQueryOptions) -> Self {
        Self {
            slow_query: slow_query_options,
        }
    }

    pub fn start_slow_query_timer(&self, stmt: QueryStatement) -> Option<SlowQueryTimer> {
        if self.slow_query.enable {
            Some(SlowQueryTimer {
                start: std::time::Instant::now(),
                stmt,
                threshold: self.slow_query.threshold,
                sample_ratio: self.slow_query.sample_ratio,
            })
        } else {
            None
        }
    }
}

/// SlowQueryTimer is used to log slow query when it's dropped.
pub struct SlowQueryTimer {
    start: std::time::Instant,
    stmt: QueryStatement,
    threshold: Option<Duration>,
    sample_ratio: Option<f64>,
}

impl SlowQueryTimer {
    fn log_slow_query(&self, elapsed: Duration, threshold: Duration) {
        match &self.stmt {
            QueryStatement::Sql(stmt) => {
                slow!(
                    cost = elapsed.as_millis() as u64,
                    threshold = threshold.as_millis() as u64,
                    sql = stmt.to_string()
                );
            }
            QueryStatement::Promql(stmt) => {
                slow!(
                    cost = elapsed.as_millis() as u64,
                    threshold = threshold.as_millis() as u64,
                    promql = stmt.to_string()
                );
            }
        }
    }
}

impl Drop for SlowQueryTimer {
    fn drop(&mut self) {
        if let Some(threshold) = self.threshold {
            let elapsed = self.start.elapsed();
            if elapsed > threshold {
                if let Some(ratio) = self.sample_ratio {
                    // Generate a random number in [0, 1) and compare it with sample_ratio.
                    if ratio >= 1.0 || random::<f64>() <= ratio {
                        self.log_slow_query(elapsed, threshold);
                    }
                } else {
                    // If sample_ratio is not set, log all slow queries.
                    self.log_slow_query(elapsed, threshold);
                }
            }
        }
    }
}
