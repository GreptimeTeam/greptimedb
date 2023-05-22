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

use async_trait::async_trait;
use common_telemetry::logging;
use common_time::util;
use store_api::logstore::LogStore;
use store_api::storage::{FlushContext, FlushReason, Region};

use crate::config::{DEFAULT_AUTO_FLUSH_INTERVAL, DEFAULT_PICKER_SCHEDULE_INTERVAL};
use crate::region::RegionImpl;

/// Config for [FlushPicker].
pub struct PickerConfig {
    /// Interval to schedule the picker.
    pub schedule_interval: Duration,
    /// Interval to auto flush a region if it has not flushed yet.
    pub auto_flush_interval: Duration,
}

impl PickerConfig {
    /// Returns the auto flush interval in millis or a default value
    /// if overflow occurs.
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
            schedule_interval: Duration::from_millis(DEFAULT_PICKER_SCHEDULE_INTERVAL.into()),
            auto_flush_interval: Duration::from_millis(DEFAULT_AUTO_FLUSH_INTERVAL.into()),
        }
    }
}

/// Flush task picker.
#[derive(Debug, Clone)]
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

    /// Picks regions and flushes them by interval.
    ///
    /// Returns the number of flushed regions.
    pub async fn pick_by_interval<T: FlushItem>(&self, regions: &[T]) -> usize {
        let now = util::current_time_millis();
        // Flush regions by interval.
        if let Some(earliest_flush_millis) = now.checked_sub(self.auto_flush_interval_millis) {
            flush_regions_by_interval(regions, earliest_flush_millis).await
        } else {
            0
        }
    }

    /// Picks and flushes regions when the write buffer is full.
    pub async fn pick_by_write_buffer_full<T: FlushItem>(&self, regions: &[T]) {
        // In such case, we pick the oldest region to flush. If this is not enough,
        // the next time the region writer will trigger the picker again. Then we
        // can pick another region to flush. The total memory will go down eventually.
        let target = regions
            .iter()
            .filter(|region| region.mutable_memtable_usage() > 0)
            .min_by_key(|region| region.last_flush_time());
        if let Some(region) = target {
            logging::debug!(
                "Request flush for region {} due to global buffer is full",
                region.item_id()
            );

            region.request_flush(FlushReason::GlobalBufferFull).await;
        }
    }
}

/// Item for picker to flush.
#[async_trait]
pub trait FlushItem {
    /// Id of the item.
    fn item_id(&self) -> u64;

    /// Last flush time in millis.
    fn last_flush_time(&self) -> i64;

    /// Mutable memtable usage.
    fn mutable_memtable_usage(&self) -> usize;

    /// Requests the item to schedule a flush for specific `reason`.
    ///
    /// The flush job itself should run in background.
    async fn request_flush(&self, reason: FlushReason);
}

#[async_trait]
impl<S: LogStore> FlushItem for RegionImpl<S> {
    fn item_id(&self) -> u64 {
        self.id()
    }

    fn last_flush_time(&self) -> i64 {
        self.last_flush_millis()
    }

    fn mutable_memtable_usage(&self) -> usize {
        let current = self.version_control().current();
        let memtables = current.memtables();
        memtables.mutable_bytes_allocated()
    }

    async fn request_flush(&self, reason: FlushReason) {
        let ctx = FlushContext {
            wait: false,
            reason,
        };

        if let Err(e) = self.flush(&ctx).await {
            logging::error!(e; "Failed to flush region {}", self.id());
        }
    }
}

/// Auto flush regions based on last flush time.
///
/// Returns the number of flushed regions.
async fn flush_regions_by_interval<T: FlushItem>(
    regions: &[T],
    earliest_flush_millis: i64,
) -> usize {
    let mut flushed = 0;
    for region in regions {
        if region.last_flush_time() < earliest_flush_millis {
            logging::debug!(
                "Auto flush region {} due to last flush time ({} < {})",
                region.item_id(),
                region.last_flush_time(),
                earliest_flush_millis,
            );

            flushed += 1;
            region.request_flush(FlushReason::Periodically).await;
        }
    }

    flushed
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockItem {
        id: u64,
        last_flush_time: i64,
        usage: usize,
        flush_reason: Mutex<Option<FlushReason>>,
    }

    impl MockItem {
        fn new(id: u64, last_flush_time: i64, usage: usize) -> MockItem {
            MockItem {
                id,
                last_flush_time,
                usage,
                flush_reason: Mutex::new(None),
            }
        }

        fn flush_reason(&self) -> Option<FlushReason> {
            *self.flush_reason.lock().unwrap()
        }
    }

    #[async_trait]
    impl FlushItem for MockItem {
        fn item_id(&self) -> u64 {
            self.id
        }

        fn last_flush_time(&self) -> i64 {
            self.last_flush_time
        }

        fn mutable_memtable_usage(&self) -> usize {
            self.usage
        }

        async fn request_flush(&self, reason: FlushReason) {
            let mut flush_reason = self.flush_reason.lock().unwrap();
            *flush_reason = Some(reason);
        }
    }

    #[tokio::test]
    async fn test_pick_by_interval() {
        let regions = [
            MockItem::new(0, util::current_time_millis(), 1),
            MockItem::new(1, util::current_time_millis() - 60 * 1000, 1),
        ];
        let picker = FlushPicker::new(PickerConfig {
            // schedule_interval is unused in this test.
            schedule_interval: Duration::from_millis(10),
            auto_flush_interval: Duration::from_millis(30 * 1000),
        });
        let flushed = picker.pick_by_interval(&regions).await;
        assert_eq!(1, flushed);
        assert!(regions[0].flush_reason().is_none());
        assert_eq!(Some(FlushReason::Periodically), regions[1].flush_reason());
    }

    #[tokio::test]
    async fn test_pick_by_buffer_full() {
        let regions = [
            MockItem::new(0, util::current_time_millis(), 10),
            MockItem::new(1, util::current_time_millis() - 60 * 1000, 0),
            MockItem::new(1, util::current_time_millis() - 60 * 1000, 10),
        ];
        let picker = FlushPicker::new(PickerConfig {
            schedule_interval: Duration::from_millis(10),
            auto_flush_interval: Duration::from_millis(30 * 1000),
        });
        picker.pick_by_write_buffer_full(&regions).await;
        assert!(regions[0].flush_reason().is_none());
        assert!(regions[1].flush_reason().is_none());
        assert_eq!(
            Some(FlushReason::GlobalBufferFull),
            regions[2].flush_reason()
        );

        // No target.
        let regions = [MockItem::new(1, util::current_time_millis(), 0)];
        picker.pick_by_write_buffer_full(&regions).await;
        assert!(regions[0].flush_reason().is_none());
    }
}
