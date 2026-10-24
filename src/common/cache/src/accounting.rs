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

use std::sync::atomic::{AtomicU32, Ordering};

use prometheus::IntGauge;

/// Releases cache weight once, either on eviction or when an unadmitted value is dropped.
/// This tracks cache accounting rather than memory still held by consumers.
pub struct CacheAccounting {
    bytes: AtomicU32,
    gauge: IntGauge,
}

impl CacheAccounting {
    /// Charges the value's weight to an existing metric.
    pub fn new(bytes: u32, gauge: IntGauge) -> Self {
        gauge.add(bytes.into());
        Self {
            bytes: AtomicU32::new(bytes),
            gauge,
        }
    }

    /// Releases the charge even when consumers still hold the cached value.
    pub fn release(&self) {
        self.gauge.sub(self.bytes.swap(0, Ordering::Relaxed).into());
    }
}

impl Drop for CacheAccounting {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_releases_once_on_eviction_or_unadmitted_drop() {
        let gauge = IntGauge::new("test_cache_accounting_bytes", "test").unwrap();
        let value = Arc::new(CacheAccounting::new(42, gauge.clone()));
        let consumer = value.clone();
        assert_eq!(42, gauge.get());
        value.release();
        assert_eq!(0, gauge.get());
        consumer.release();
        drop(value);
        drop(consumer);
        assert_eq!(0, gauge.get());

        let pending_insertion = CacheAccounting::new(17, gauge.clone());
        assert_eq!(17, gauge.get());
        drop(pending_insertion);
        assert_eq!(0, gauge.get());
    }
}
