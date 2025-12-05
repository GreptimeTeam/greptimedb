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

use std::sync::Arc;
use std::time::Duration;

use common_query::{Output, OutputData};
use common_recordbatch::{RecordBatches, SendableRecordBatchStream, util};
use common_telemetry::info;
use frontend::instance::Instance;
use futures::StreamExt;

use crate::tests::test_util::execute_sql;

/// Executor for simulating long-running queries that hold file references
/// for extended periods to test garbage collection behavior
pub struct DelayedQueryExecutor {
    /// Delay duration to inject between batch processing
    /// Delay between processing each record batch (to simulate slow processing)
    batch_delay: Duration,
    /// Minimum total query execution time (ensures stream stays alive this long)
    min_duration: Duration,
}

impl DelayedQueryExecutor {
    /// Create a new delayed query executor
    pub fn new(batch_delay: Duration, min_duration: Duration) -> Self {
        Self {
            batch_delay,
            min_duration,
        }
    }

    /// Execute query with delays to simulate long-running query
    /// Returns pretty-printed results like check_output_stream
    pub async fn execute_query(&self, instance: &Arc<Instance>, sql: &str) -> String {
        info!("Starting delayed query execution: {}", sql);
        let start_time = std::time::Instant::now();

        // Execute SQL and get output - this ensures query planning completes and stream is created
        let output = execute_sql(instance, sql).await;

        // Process output with delays to extend query duration
        let recordbatches = match output.data {
            OutputData::Stream(mut stream) => {
                info!("Query returned stream, processing with delays");
                let schema = stream.schema();

                // Calculate the deadline for minimum duration
                let deadline = start_time + self.min_duration;
                let mut batches = Vec::new();
                let mut last_batch_time = start_time;

                // Check if we need to extend the stream life to meet min_duration
                // do it before processing each batch so if stream only have one batch we still extend its life
                let now = std::time::Instant::now();
                if now < deadline && batches.len() > 0 {
                    // If we have time remaining and have processed batches, add delay
                    let time_remaining = deadline - now;
                    if time_remaining > self.batch_delay {
                        info!(
                            "Extending stream life, remaining time: {:?}",
                            time_remaining
                        );
                        tokio::time::sleep(self.batch_delay).await;
                    }
                }

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.unwrap();

                    // Add delay between batches to simulate slow processing
                    if batch.num_rows() > 0 {
                        info!("Processing batch with delay {:?}", self.batch_delay);
                        tokio::time::sleep(self.batch_delay).await;
                        last_batch_time = std::time::Instant::now();
                    }

                    batches.push(batch);
                }

                // Ensure stream stays alive for at least min_duration
                let now = std::time::Instant::now();
                if now < deadline {
                    let remaining = deadline - now;
                    info!(
                        "Stream ended early, waiting additional {:?} to ensure min_duration",
                        remaining
                    );
                    tokio::time::sleep(remaining).await;
                }

                // Convert collected batches to RecordBatches for pretty printing
                RecordBatches::try_new(schema, batches).unwrap()
            }
            OutputData::RecordBatches(recordbatches) => {
                info!("Query returned direct record batches, applying delays");

                // For direct batches, we can still add delays between processing
                for batch in recordbatches.iter() {
                    if batch.num_rows() > 0 {
                        tokio::time::sleep(self.batch_delay).await;
                    }
                }

                recordbatches
            }
            OutputData::AffectedRows(rows) => {
                info!("Query returned affected rows: {}", rows);
                // For affected rows, we still ensure minimum duration
                return format!("Affected rows: {}", rows);
            }
        };

        // Ensure minimum query duration by waiting if necessary
        let elapsed = start_time.elapsed();
        if elapsed < self.min_duration {
            let remaining = self.min_duration - elapsed;
            info!(
                "Query completed too fast, waiting additional {:?}",
                remaining
            );
            tokio::time::sleep(remaining).await;
        }

        info!("Query execution completed after {:?}", start_time.elapsed());

        // Return pretty-printed results like check_output_stream
        recordbatches.pretty_print().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use meta_srv::gc::GcSchedulerOptions;
    use mito2::gc::GcConfig;

    use super::*;
    use crate::cluster::GreptimeDbClusterBuilder;
    use crate::test_util::{StorageType, TempDirGuard, get_test_store_config};
    use crate::tests::test_util::{MockInstance, MockInstanceBuilder, TestContext};

    /// Test helper function to create a mock instance for testing
    async fn create_test_instance() -> (TestContext, TempDirGuard) {
        let test_name = uuid::Uuid::new_v4().to_string();
        let (store_config, guard) = get_test_store_config(&StorageType::File);
        let builder = GreptimeDbClusterBuilder::new(&test_name)
            .await
            .with_metasrv_gc_config(GcSchedulerOptions {
                enable: true,
                ..Default::default()
            })
            .with_datanode_gc_config(GcConfig {
                enable: true,
                // set lingering time to zero for test speedup
                lingering_time: Some(Duration::ZERO),
                ..Default::default()
            })
            .with_store_config(store_config);
        (
            TestContext::new(MockInstanceBuilder::Distributed(builder)).await,
            guard,
        )
    }

    #[tokio::test]
    async fn test_delayed_query_executor_basic() {
        let (test_context, _guard) = create_test_instance().await;
        let instance = test_context.frontend();
        let executor = DelayedQueryExecutor::new(
            Duration::from_millis(10),  // Small delay for testing
            Duration::from_millis(100), // Minimum duration
        );
        let now = std::time::Instant::now();

        // Test with a simple query - this should be replaced with actual test data
        let result = executor.execute_query(&instance, "SELECT 1").await;

        assert!(now.elapsed() >= Duration::from_millis(100));

        // The query should complete successfully and return a string
        assert!(!result.is_empty());
        info!("Query result: {}", result);
    }
}
