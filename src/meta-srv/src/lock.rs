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

pub mod etcd;
pub(crate) mod memory;

use std::sync::Arc;

use common_telemetry::error;

use crate::error::Result;

pub type Key = Vec<u8>;

pub const DEFAULT_EXPIRE_TIME_SECS: u64 = 10;

pub struct Opts {
    // If the expiration time is exceeded and currently holds the lock, the lock is
    // automatically released.
    pub expire_secs: Option<u64>,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            expire_secs: Some(DEFAULT_EXPIRE_TIME_SECS),
        }
    }
}

#[async_trait::async_trait]
pub trait DistLock: Send + Sync {
    // Lock acquires a distributed shared lock on a given named lock. On success, it
    // will return a unique key that exists so long as the lock is held by the caller.
    async fn lock(&self, name: Vec<u8>, opts: Opts) -> Result<Key>;

    // Unlock takes a key returned by Lock and releases the hold on lock.
    async fn unlock(&self, key: Vec<u8>) -> Result<()>;
}

pub type DistLockRef = Arc<dyn DistLock>;

pub struct DistLockGuard<'a> {
    lock: &'a DistLockRef,
    name: Vec<u8>,
    key: Option<Key>,
}

impl<'a> DistLockGuard<'a> {
    pub fn new(lock: &'a DistLockRef, name: Vec<u8>) -> Self {
        Self {
            lock,
            name,
            key: None,
        }
    }

    pub async fn lock(&mut self) -> Result<()> {
        if self.key.is_some() {
            return Ok(());
        }
        let key = self
            .lock
            .lock(
                self.name.clone(),
                Opts {
                    expire_secs: Some(2),
                },
            )
            .await?;
        self.key = Some(key);
        Ok(())
    }
}

impl Drop for DistLockGuard<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let lock = self.lock.clone();
            let name = self.name.clone();
            let _handle = common_runtime::spawn_global(async move {
                if let Err(e) = lock.unlock(key).await {
                    error!(e; "Failed to unlock '{}'", String::from_utf8_lossy(&name));
                }
            });
        }
    }
}
