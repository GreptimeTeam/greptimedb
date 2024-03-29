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

use etcd_client::{Client, LockOptions};
use snafu::ResultExt;

use super::{DistLock, DistLockRef, Opts, DEFAULT_EXPIRE_TIME_SECS};
use crate::error;
use crate::error::Result;

/// A implementation of distributed lock based on etcd. The Clone of EtcdLock is cheap.
#[derive(Clone)]
pub struct EtcdLock {
    client: Client,
    store_key_prefix: String,
}

impl EtcdLock {
    pub async fn with_endpoints<E, S>(endpoints: S, store_key_prefix: String) -> Result<DistLockRef>
    where
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let client = Client::connect(endpoints, None)
            .await
            .context(error::ConnectEtcdSnafu)?;

        Self::with_etcd_client(client, store_key_prefix)
    }

    pub fn with_etcd_client(client: Client, store_key_prefix: String) -> Result<DistLockRef> {
        Ok(Arc::new(EtcdLock {
            client,
            store_key_prefix,
        }))
    }

    fn lock_key(&self, key: Vec<u8>) -> Vec<u8> {
        if self.store_key_prefix.is_empty() {
            key
        } else {
            let mut prefix = self.store_key_prefix.as_bytes().to_vec();
            prefix.extend_from_slice(&key);
            prefix
        }
    }
}

#[async_trait::async_trait]
impl DistLock for EtcdLock {
    async fn lock(&self, key: Vec<u8>, opts: Opts) -> Result<Vec<u8>> {
        let expire = opts.expire_secs.unwrap_or(DEFAULT_EXPIRE_TIME_SECS) as i64;

        let mut client = self.client.clone();

        let resp = client
            .lease_grant(expire, None)
            .await
            .context(error::LeaseGrantSnafu)?;

        let lease_id = resp.id();
        let lock_opts = LockOptions::new().with_lease(lease_id);

        let resp = client
            .lock(self.lock_key(key), Some(lock_opts))
            .await
            .context(error::LockSnafu)?;

        Ok(resp.key().to_vec())
    }

    async fn unlock(&self, key: Vec<u8>) -> Result<()> {
        let mut client = self.client.clone();
        let _ = client
            .unlock(self.lock_key(key))
            .await
            .context(error::UnlockSnafu)?;
        Ok(())
    }
}
