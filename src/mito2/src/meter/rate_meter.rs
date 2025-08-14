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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// `RateMeter` tracks a cumulative value and computes the rate per interval.
#[derive(Default, Debug, Clone)]
pub struct RateMeter {
    inner: Arc<RateMeterInner>,
}

#[derive(Default, Debug)]
struct RateMeterInner {
    /// Accumulated value since last rate calculation.
    value: AtomicU64,

    /// The last computed rate (per second).
    last_rate: AtomicU64,

    /// Optional: total accumulated value, never reset.
    total: AtomicU64,
}

impl RateMeter {
    /// Creates a new `RateMeter` with an initial value.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RateMeterInner::default()),
        }
    }

    /// Increments the accumulated value by `v`.
    pub fn inc_by(&self, v: u64) {
        self.inner.value.fetch_add(v, Ordering::Relaxed);
        self.inner.total.fetch_add(v, Ordering::Relaxed);
    }

    /// Returns the current accumulated value since last rate calculation.
    pub fn get_value(&self) -> u64 {
        self.inner.value.load(Ordering::Relaxed)
    }

    /// Returns the total accumulated value since creation.
    pub fn get_total(&self) -> u64 {
        self.inner.total.load(Ordering::Relaxed)
    }

    /// Returns the last computed rate (per second).
    pub fn get_rate(&self) -> u64 {
        self.inner.last_rate.load(Ordering::Relaxed)
    }

    /// Updates the current rate based on the accumulated value over the given interval.
    ///
    /// `interval` should represent the duration since the last `update_rate` call.
    /// This method resets the internal accumulated counter (`value`) to 0.
    pub fn update_rate(&self, interval: Duration) {
        let current_value = self.inner.value.swap(0, Ordering::Relaxed);

        let interval_secs = interval.as_secs();
        if interval_secs > 0 {
            let rate = current_value / interval_secs;
            self.inner.last_rate.store(rate, Ordering::Relaxed);
        }
    }

    /// Resets the meter: clears both current value and last rate.
    /// Total accumulated value remains unchanged.
    pub fn reset(&self) {
        self.inner.value.store(0, Ordering::Relaxed);
        self.inner.last_rate.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_inc_and_get_value_and_total() {
        let meter = RateMeter::new();
        assert_eq!(meter.get_value(), 0);
        assert_eq!(meter.get_total(), 0);

        meter.inc_by(10);
        assert_eq!(meter.get_value(), 10);
        assert_eq!(meter.get_total(), 10);

        meter.inc_by(5);
        assert_eq!(meter.get_value(), 15);
        assert_eq!(meter.get_total(), 15);
    }

    #[test]
    fn test_update_rate_and_get_rate() {
        let meter = RateMeter::new();
        meter.inc_by(100);
        meter.update_rate(Duration::from_secs(2));
        assert_eq!(meter.get_rate(), 50);
        // After update, value should be reset
        assert_eq!(meter.get_value(), 0);

        // If interval is zero, rate should not be updated
        meter.inc_by(30);
        meter.update_rate(Duration::from_secs(0));
        // Should still be 50
        assert_eq!(meter.get_rate(), 50);
    }

    #[test]
    fn test_reset() {
        let meter = RateMeter::new();
        meter.inc_by(100);
        meter.update_rate(Duration::from_secs(1));
        assert_eq!(meter.get_rate(), 100);
        meter.reset();
        assert_eq!(meter.get_value(), 0);
        assert_eq!(meter.get_rate(), 0);
        // Total should remain unchanged
        assert_eq!(meter.get_total(), 100);
    }

    #[test]
    fn test_total_accumulates() {
        let meter = RateMeter::new();
        meter.inc_by(10);
        meter.update_rate(Duration::from_secs(1));
        meter.inc_by(20);
        meter.update_rate(Duration::from_secs(2));
        assert_eq!(meter.get_total(), 30);
        assert_eq!(meter.get_rate(), 10);
    }

    #[test]
    fn test_clone_and_shared_state() {
        let meter = RateMeter::new();
        let meter2 = meter.clone();
        meter.inc_by(10);
        meter2.inc_by(5);
        assert_eq!(meter.get_value(), 15);
        assert_eq!(meter2.get_value(), 15);
        assert_eq!(meter.get_total(), 15);
        assert_eq!(meter2.get_total(), 15);

        meter.update_rate(Duration::from_secs(1));
        assert_eq!(meter2.get_rate(), 15);
    }
}
