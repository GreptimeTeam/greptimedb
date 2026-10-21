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

use std::sync::{Arc, Weak};

use common_cache::accounting::CacheAccounting;
use dashmap::DashMap;
use moka::sync::Cache;
use tokio::sync::OnceCell;

use crate::cache::{RANGE_RESULT_TYPE, removal_cause_str};
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION};
use crate::read::range_cache::shared_range::SharedRange;
use crate::read::range_cache::{RangeScanCacheKey, RangeScanCacheValue};

// Active candidate merges may own reservations in different query pools.
// Completed results remain keyed only by their data identity.
type RangeLoads = DashMap<(RangeScanCacheKey, usize), Weak<RangeLoad>>;

/// Completed results retain the original synchronous cache policy. Active scans
/// are coordinated separately and live only while readers or finalization own them.
#[derive(Clone)]
pub(crate) struct RangeResultCache {
    cache: Cache<RangeScanCacheKey, Arc<CachedRangeResult>>,
    loads: Arc<RangeLoads>,
}

/// A reader-owned active scan registration, scoped to one query memory pool.
/// The registry holds a weak reference and removal checks this instance identity.
pub(crate) struct RangeLoad {
    pub(crate) result: OnceCell<(Arc<SharedRange>, usize)>,
    key: (RangeScanCacheKey, usize),
    registry: Weak<RangeLoads>,
}

impl Drop for RangeLoad {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.remove_if(&self.key, |_, entry| std::ptr::eq(entry.as_ptr(), self));
        }
    }
}

impl RangeResultCache {
    /// Creates a completed-result cache with a byte-weighted capacity.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(capacity)
                .weigher(|key: &RangeScanCacheKey, result: &Arc<CachedRangeResult>| {
                    result_weight(key, &result.value)
                })
                .eviction_listener(|_, result: Arc<CachedRangeResult>, cause| {
                    result.accounting.release();
                    CACHE_EVICTION
                        .with_label_values(&[RANGE_RESULT_TYPE, removal_cause_str(cause)])
                        .inc();
                })
                .build(),
            loads: Arc::new(DashMap::new()),
        }
    }

    /// Looks up a completed range result.
    pub(crate) fn get(&self, key: &RangeScanCacheKey) -> Option<Arc<RangeScanCacheValue>> {
        self.cache.get(key).map(|result| result.value.clone())
    }

    /// Publishes a completed range result with cache memory accounting.
    pub(crate) fn insert(&self, key: RangeScanCacheKey, value: Arc<RangeScanCacheValue>) {
        let result = CachedRangeResult::new(&key, value);
        self.cache.insert(key, result);
    }

    /// Reuses or creates the active registration for a range and query memory pool.
    pub(crate) fn get_or_create_range_load(
        &self,
        key: &RangeScanCacheKey,
        memory_pool_id: usize,
    ) -> Arc<RangeLoad> {
        let key = (key.clone(), memory_pool_id);
        let mut slot = self.loads.entry(key.clone()).or_default();
        if let Some(load) = slot.upgrade() {
            return load;
        }
        let load = Arc::new(RangeLoad {
            result: OnceCell::new(),
            key: key.clone(),
            registry: Arc::downgrade(&self.loads),
        });
        *slot = Arc::downgrade(&load);
        load
    }
}

struct CachedRangeResult {
    value: Arc<RangeScanCacheValue>,
    accounting: CacheAccounting,
}

impl CachedRangeResult {
    fn new(key: &RangeScanCacheKey, value: Arc<RangeScanCacheValue>) -> Arc<Self> {
        Arc::new(Self {
            accounting: CacheAccounting::new(
                result_weight(key, &value),
                CACHE_BYTES.with_label_values(&[RANGE_RESULT_TYPE]),
            ),
            value,
        })
    }
}

fn result_weight(key: &RangeScanCacheKey, value: &RangeScanCacheValue) -> u32 {
    u32::try_from(key.estimated_size().saturating_add(value.estimated_size())).unwrap_or(u32::MAX)
}
