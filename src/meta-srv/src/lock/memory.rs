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

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::error::Result;
use crate::lock::{DistLock, Key, Opts};

#[derive(Default)]
pub(crate) struct MemLock {
    mutexes: DashMap<Key, Arc<Mutex<()>>>,
    guards: DashMap<Key, OwnedMutexGuard<()>>,
}

#[async_trait]
impl DistLock for MemLock {
    async fn lock(&self, key: Vec<u8>, _opts: Opts) -> Result<Key> {
        let mutex = self
            .mutexes
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let guard = mutex.lock_owned().await;

        let _ = self.guards.insert(key.clone(), guard);
        Ok(key)
    }

    async fn unlock(&self, key: Vec<u8>) -> Result<()> {
        // drop the guard, so that the mutex can be unlocked,
        // effectively make the `mutex.lock_owned` in `lock` method to proceed
        let _ = self.guards.remove(&key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    use rand::seq::SliceRandom;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_mem_lock_concurrently() {
        let lock = Arc::new(MemLock::default());

        let keys = (0..10)
            .map(|i| format!("my-lock-{i}").into_bytes())
            .collect::<Vec<Key>>();
        let counters: [(Key, AtomicU32); 10] = keys
            .iter()
            .map(|x| (x.clone(), AtomicU32::new(0)))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let counters = Arc::new(HashMap::from(counters));

        let tasks = (0..100)
            .map(|_| {
                let mut keys = keys.clone();
                keys.shuffle(&mut rand::thread_rng());

                let lock_clone = lock.clone();
                let counters_clone = counters.clone();
                tokio::spawn(async move {
                    // every key counter will be added by 1 for 10 times
                    for i in 0..100 {
                        let key = &keys[i % keys.len()];
                        assert!(lock_clone
                            .lock(key.clone(), Opts { expire_secs: None })
                            .await
                            .is_ok());

                        // Intentionally create a critical section:
                        // if our MemLock is flawed, the resulting counter is wrong.
                        //
                        // Note that AtomicU32 is only used to enable the updates from multiple tasks,
                        // does not make any guarantee about the correctness of the result.

                        let counter = counters_clone.get(key).unwrap();
                        let v = counter.load(Ordering::Relaxed);
                        counter.store(v + 1, Ordering::Relaxed);

                        lock_clone.unlock(key.clone()).await.unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        let _ = futures::future::join_all(tasks).await;

        assert!(counters.values().all(|x| x.load(Ordering::Relaxed) == 1000));
    }
}
