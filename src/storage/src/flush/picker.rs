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

use async_trait::async_trait;
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::logging;
use common_time::util;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::storage::{FlushContext, FlushReason, Region};

use crate::config::DEFAULT_AUTO_FLUSH_INTERVAL;
use crate::engine::RegionMap;
use crate::error::{Error, Result, StartPickTaskSnafu, StopPickTaskSnafu};
use crate::region::RegionImpl;

/// Config for [FlushPicker].
pub struct PickerConfig {
    /// Interval to auto flush a region if it has not flushed yet.
    pub auto_flush_interval: Duration,
}

impl PickerConfig {
    /// Returns the interval to pick regions.
    fn picker_schedule_interval(&self) -> Duration {
        self.auto_flush_interval / 2
    }

    /// Returns the auto flush interval in millis.
    fn auto_flush_interval_millis(&self) -> i64 {
        self.auto_flush_interval
            .as_millis()
            .try_into()
            .unwrap_or(DEFAULT_AUTO_FLUSH_INTERVAL.into())
    }
}

impl Default for PickerConfig {
    fn default() -> Self {
        PickerConfig {
            auto_flush_interval: Duration::from_millis(DEFAULT_AUTO_FLUSH_INTERVAL.into()),
        }
    }
}

/// Flush task picker.
pub struct FlushPicker {
    /// Background repeated pick task.
    pick_task: RepeatedTask<Error>,
}

impl FlushPicker {
    /// Returns a new FlushPicker.
    pub fn new<S: LogStore>(regions: Arc<RegionMap<S>>, config: PickerConfig) -> Result<Self> {
        let task_fn = PickTaskFunction {
            regions,
            auto_flush_interval_millis: config.auto_flush_interval_millis(),
        };
        let pick_task = RepeatedTask::new(config.picker_schedule_interval(), Box::new(task_fn));
        // Start the background task.
        pick_task
            .start(common_runtime::bg_runtime())
            .context(StartPickTaskSnafu)?;

        Ok(Self { pick_task })
    }

    /// Stop the picker.
    pub async fn stop(&self) -> Result<()> {
        self.pick_task.stop().await.context(StopPickTaskSnafu)
    }
}

/// Task function to pick regions to flush.
struct PickTaskFunction<S: LogStore> {
    /// Regions of the engine.
    regions: Arc<RegionMap<S>>,
    /// Interval to flush a region automatically.
    auto_flush_interval_millis: i64,
}

impl<S: LogStore> PickTaskFunction<S> {
    /// Auto flush regions based on last flush time.
    async fn auto_flush_regions(&self, regions: &[RegionImpl<S>], earliest_flush_millis: i64) {
        for region in regions {
            if region.last_flush_millis() < earliest_flush_millis {
                logging::debug!(
                    "Auto flush region {} due to last flush time ({} < {})",
                    region.id(),
                    region.last_flush_millis(),
                    earliest_flush_millis,
                );

                Self::flush_region(region).await;
            }
        }
    }

    /// Try to flush region.
    async fn flush_region(region: &RegionImpl<S>) {
        let ctx = FlushContext {
            wait: false,
            reason: FlushReason::Periodically,
        };
        if let Err(e) = region.flush(&ctx).await {
            logging::error!(e; "Failed to flush region {}", region.id());
        }
    }
}

#[async_trait]
impl<S: LogStore> TaskFunction<Error> for PickTaskFunction<S> {
    async fn call(&mut self) -> Result<()> {
        // Get all regions.
        let regions = self.regions.list_regions();
        let now = util::current_time_millis();
        // Flush regions by interval.
        if let Some(earliest_flush_millis) = now.checked_sub(self.auto_flush_interval_millis) {
            self.auto_flush_regions(&regions, earliest_flush_millis)
                .await;
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "FlushPicker-pick-task"
    }
}
