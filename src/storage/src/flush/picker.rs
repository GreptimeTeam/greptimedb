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

use common_telemetry::logging;
use common_time::util;
use store_api::logstore::LogStore;
use store_api::storage::{FlushContext, FlushReason, Region};

use crate::config::DEFAULT_AUTO_FLUSH_INTERVAL;
use crate::region::RegionImpl;

/// Config for [FlushPicker].
pub struct PickerConfig {
    /// Interval to auto flush a region if it has not flushed yet.
    pub auto_flush_interval: Duration,
}

impl PickerConfig {
    /// Returns the interval to pick regions.
    pub(crate) fn picker_schedule_interval(&self) -> Duration {
        self.auto_flush_interval / 2
    }

    /// Returns the auto flush interval in millis.
    pub(crate) fn auto_flush_interval_millis(&self) -> i64 {
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
    /// Interval to flush a region automatically.
    auto_flush_interval_millis: i64,
}

impl FlushPicker {
    /// Returns a new FlushPicker.
    pub fn new(config: PickerConfig) -> FlushPicker {
        FlushPicker {
            auto_flush_interval_millis: config.auto_flush_interval_millis(),
        }
    }

    /// Pick regions and flush them by interval.
    ///
    /// Returns the number of flushed regions.
    pub async fn pick_by_interval<S: LogStore>(&self, regions: &[RegionImpl<S>]) -> usize {
        let now = util::current_time_millis();
        // Flush regions by interval.
        if let Some(earliest_flush_millis) = now.checked_sub(self.auto_flush_interval_millis) {
            flush_regions_by_interval(regions, earliest_flush_millis).await
        } else {
            0
        }
    }
}

/// Auto flush regions based on last flush time.
///
/// Returns the number of flushed regions.
async fn flush_regions_by_interval<S: LogStore>(
    regions: &[RegionImpl<S>],
    earliest_flush_millis: i64,
) -> usize {
    let mut flushed = 0;
    for region in regions {
        if region.last_flush_millis() < earliest_flush_millis {
            logging::debug!(
                "Auto flush region {} due to last flush time ({} < {})",
                region.id(),
                region.last_flush_millis(),
                earliest_flush_millis,
            );

            flushed += 1;
            flush_region(region).await;
        }
    }

    flushed
}

/// Try to flush region.
async fn flush_region<S: LogStore>(region: &RegionImpl<S>) {
    let ctx = FlushContext {
        wait: false,
        reason: FlushReason::Periodically,
    };
    if let Err(e) = region.flush(&ctx).await {
        logging::error!(e; "Failed to flush region {}", region.id());
    }
}
