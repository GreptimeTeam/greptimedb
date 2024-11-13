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

//! Internal metrics of the memtable.

use std::sync::atomic::{AtomicI64, Ordering};

/// Metrics of writing memtables.
pub(crate) struct WriteMetrics {
    /// Size allocated by keys.
    pub(crate) key_bytes: usize,
    /// Size allocated by values.
    pub(crate) value_bytes: usize,
    /// Minimum timestamp.
    pub(crate) min_ts: i64,
    /// Maximum timestamp
    pub(crate) max_ts: i64,
}

impl WriteMetrics {
    /// Update the min/max timestamp range according to current write metric.
    pub(crate) fn update_timestamp_range(&self, prev_max_ts: &AtomicI64, prev_min_ts: &AtomicI64) {
        loop {
            let current_min = prev_min_ts.load(Ordering::Relaxed);
            if self.min_ts >= current_min {
                break;
            }

            let Err(updated) = prev_min_ts.compare_exchange(
                current_min,
                self.min_ts,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) else {
                break;
            };

            if updated == self.min_ts {
                break;
            }
        }

        loop {
            let current_max = prev_max_ts.load(Ordering::Relaxed);
            if self.max_ts <= current_max {
                break;
            }

            let Err(updated) = prev_max_ts.compare_exchange(
                current_max,
                self.max_ts,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) else {
                break;
            };

            if updated == self.max_ts {
                break;
            }
        }
    }
}

impl Default for WriteMetrics {
    fn default() -> Self {
        Self {
            key_bytes: 0,
            value_bytes: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }
}
