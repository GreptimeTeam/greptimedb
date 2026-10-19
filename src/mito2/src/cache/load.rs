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

//! Coordinates unfinished cache loads without retaining completed values.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};

use prometheus::{HistogramTimer, IntGauge};
use tokio::sync::watch;

use crate::metrics::{
    CACHE_LOAD_EVENTS, CACHE_LOAD_WAIT_ELAPSED, CACHE_WORK_ACTIVE, CACHE_WORK_BYTES,
    CACHE_WORK_ELAPSED,
};

/// Accounts live work through success, error, queue drops, and cancellation.
/// Bytes describe referenced buffers, not deduplicated physical allocations.
pub(crate) struct CacheWork {
    cache_type: &'static str,
    active: IntGauge,
    bytes_gauge: IntGauge,
    bytes: usize,
    _timer: HistogramTimer,
}

impl CacheWork {
    pub(crate) fn new(cache_type: &'static str, stage: &'static str) -> Self {
        let active = CACHE_WORK_ACTIVE.with_label_values(&[cache_type, stage]);
        active.inc();
        Self {
            cache_type,
            active,
            bytes_gauge: CACHE_WORK_BYTES.with_label_values(&[cache_type, stage]),
            bytes: 0,
            _timer: CACHE_WORK_ELAPSED
                .with_label_values(&[cache_type, stage])
                .start_timer(),
        }
    }

    pub(crate) fn set_bytes(&mut self, bytes: usize) {
        self.bytes_gauge.add(bytes as i64 - self.bytes as i64);
        self.bytes = bytes;
    }

    pub(crate) fn transition(&mut self, stage: &'static str) {
        let mut next = Self::new(self.cache_type, stage);
        next.set_bytes(self.bytes);
        *self = next;
    }
}

impl Drop for CacheWork {
    fn drop(&mut self) {
        self.active.dec();
        self.bytes_gauge.sub(self.bytes as i64);
    }
}

/// Tracks loaders independently of completed cache entries and their eviction.
#[derive(Debug)]
pub(crate) struct CacheLoads<K, V> {
    entries: Mutex<HashMap<K, Weak<watch::Sender<Option<V>>>>>,
}

impl<K, V> Default for CacheLoads<K, V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> CacheLoads<K, V> {
    /// Joins a pending load or takes responsibility for publishing its result.
    pub(crate) fn start(self: &Arc<Self>, key: K, cache_type: &'static str) -> CacheLoad<K, V> {
        let mut entries = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(sender) = entries.get(&key).and_then(Weak::upgrade) {
            drop(entries);
            CACHE_LOAD_EVENTS
                .with_label_values(&[cache_type, "wait"])
                .inc();
            return CacheLoad::Wait(CacheLoadWaiter {
                receiver: sender.subscribe(),
                cache_type,
                _work: CacheWork::new(cache_type, "waiter"),
            });
        }
        let (sender, _) = watch::channel(None);
        let sender = Arc::new(sender);
        entries.insert(key.clone(), Arc::downgrade(&sender));
        drop(entries);
        CACHE_LOAD_EVENTS
            .with_label_values(&[cache_type, "load"])
            .inc();
        CacheLoad::Load(CacheLoadGuard {
            loads: self.clone(),
            key,
            sender,
            cache_type,
            completed: false,
            _work: CacheWork::new(cache_type, "producer"),
        })
    }
}

/// A cache load either produces the value or awaits the current producer.
pub(crate) enum CacheLoad<K: Eq + Hash, V> {
    Load(CacheLoadGuard<K, V>),
    Wait(CacheLoadWaiter<V>),
}

/// Removes pending registration and wakes waiters when dropped or completed.
pub(crate) struct CacheLoadGuard<K: Eq + Hash, V> {
    loads: Arc<CacheLoads<K, V>>,
    key: K,
    sender: Arc<watch::Sender<Option<V>>>,
    cache_type: &'static str,
    completed: bool,
    _work: CacheWork,
}

impl<K: Eq + Hash, V> CacheLoadGuard<K, V> {
    /// Publishes the result directly, including when the cache cannot retain it.
    pub(crate) fn finish(mut self, value: V) {
        self.sender.send_replace(Some(value));
        self.completed = true;
        CACHE_LOAD_EVENTS
            .with_label_values(&[self.cache_type, "complete"])
            .inc();
    }
}

impl<K: Eq + Hash, V> Drop for CacheLoadGuard<K, V> {
    fn drop(&mut self) {
        let mut entries = self
            .loads
            .entries
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if entries
            .get(&self.key)
            .is_some_and(|entry| entry.ptr_eq(&Arc::downgrade(&self.sender)))
        {
            entries.remove(&self.key);
        }
        drop(entries);
        if !self.completed {
            CACHE_LOAD_EVENTS
                .with_label_values(&[self.cache_type, "abandon"])
                .inc();
        }
    }
}

/// Waits without keeping the producer alive after cancellation.
pub(crate) struct CacheLoadWaiter<V> {
    receiver: watch::Receiver<Option<V>>,
    _work: CacheWork,
    cache_type: &'static str,
}

impl<V: Clone> CacheLoadWaiter<V> {
    /// Returns None when the producer was cancelled or could not cache a result.
    pub(crate) async fn wait(mut self) -> Option<V> {
        let _timer = CACHE_LOAD_WAIT_ELAPSED
            .with_label_values(&[self.cache_type])
            .start_timer();
        match self.receiver.wait_for(Option::is_some).await {
            Ok(value) => {
                CACHE_LOAD_EVENTS
                    .with_label_values(&[self.cache_type, "shared"])
                    .inc();
                value.clone()
            }
            Err(_) => {
                CACHE_LOAD_EVENTS
                    .with_label_values(&[self.cache_type, "fallback"])
                    .inc();
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_metrics_follow_transfers_and_drop() {
        let bytes = |stage| {
            CACHE_WORK_BYTES
                .with_label_values(&["test_work", stage])
                .get()
        };
        let active = |stage| {
            CACHE_WORK_ACTIVE
                .with_label_values(&["test_work", stage])
                .get()
        };
        let mut work = CacheWork::new("test_work", "buffer");
        work.set_bytes(123);
        assert_eq!((active("buffer"), bytes("buffer")), (1, 123));
        work.transition("queue");
        assert_eq!((active("buffer"), bytes("buffer")), (0, 0));
        assert_eq!((active("queue"), bytes("queue")), (1, 123));
        work.set_bytes(45);
        drop(work);
        assert_eq!((active("queue"), bytes("queue")), (0, 0));
    }

    #[tokio::test]
    async fn completed_result_survives_registration_removal() {
        let loads = Arc::new(CacheLoads::<u64, Arc<String>>::default());
        let CacheLoad::Load(guard) = loads.start(1, "test") else {
            panic!("expected loader")
        };
        let CacheLoad::Wait(waiter) = loads.start(1, "test") else {
            panic!("expected waiter")
        };
        let value = Arc::new("result".to_string());
        guard.finish(value.clone());
        assert!(loads.entries.lock().unwrap().is_empty());
        assert!(Arc::ptr_eq(&waiter.wait().await.unwrap(), &value));
    }

    #[tokio::test]
    async fn cancelled_load_wakes_waiters_and_can_retry() {
        let loads = Arc::new(CacheLoads::<u64, u64>::default());
        let CacheLoad::Load(guard) = loads.start(1, "test") else {
            panic!("expected loader")
        };
        let CacheLoad::Wait(waiter) = loads.start(1, "test") else {
            panic!("expected waiter")
        };
        drop(guard);
        assert_eq!(waiter.wait().await, None);
        assert!(matches!(loads.start(1, "test"), CacheLoad::Load(_)));
    }

    #[tokio::test]
    async fn different_keys_do_not_wait_and_cancelled_waiter_does_not_cancel_load() {
        let loads = Arc::new(CacheLoads::<u64, u64>::default());
        let CacheLoad::Load(first) = loads.start(1, "test") else {
            panic!("expected loader")
        };
        let CacheLoad::Load(second) = loads.start(2, "test") else {
            panic!("expected loader")
        };
        let CacheLoad::Wait(waiter) = loads.start(1, "test") else {
            panic!("expected waiter")
        };
        drop(waiter);
        first.finish(10);
        second.finish(20);
        assert!(loads.entries.lock().unwrap().is_empty());
    }
}
