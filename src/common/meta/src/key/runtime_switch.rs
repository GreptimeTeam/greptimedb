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
use std::time::Duration;

use common_error::ext::BoxedError;
use common_procedure::local::PauseAware;
use moka::future::Cache;
use snafu::ResultExt;

use crate::error::{GetCacheSnafu, Result};
use crate::key::{LEGACY_MAINTENANCE_KEY, MAINTENANCE_KEY, PAUSE_PROCEDURE_KEY, RECOVERY_MODE_KEY};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{BatchDeleteRequest, PutRequest};

pub type RuntimeSwitchManagerRef = Arc<RuntimeSwitchManager>;

/// The runtime switch manager.
///
/// Used to enable or disable runtime switches.
#[derive(Clone)]
pub struct RuntimeSwitchManager {
    kv_backend: KvBackendRef,
    cache: Cache<Vec<u8>, Option<Vec<u8>>>,
}

#[async_trait::async_trait]
impl PauseAware for RuntimeSwitchManager {
    async fn is_paused(&self) -> std::result::Result<bool, BoxedError> {
        self.is_procedure_paused().await.map_err(BoxedError::new)
    }
}

const CACHE_TTL: Duration = Duration::from_secs(10);
const MAX_CAPACITY: u64 = 32;

impl RuntimeSwitchManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        let cache = Cache::builder()
            .time_to_live(CACHE_TTL)
            .max_capacity(MAX_CAPACITY)
            .build();
        Self { kv_backend, cache }
    }

    async fn put_key(&self, key: &str) -> Result<()> {
        let req = PutRequest {
            key: Vec::from(key),
            value: vec![],
            prev_kv: false,
        };
        self.kv_backend.put(req).await?;
        self.cache.invalidate(key.as_bytes()).await;
        Ok(())
    }

    async fn delete_keys(&self, keys: &[&str]) -> Result<()> {
        let req = BatchDeleteRequest::new()
            .with_keys(keys.iter().map(|x| x.as_bytes().to_vec()).collect());
        self.kv_backend.batch_delete(req).await?;
        for key in keys {
            self.cache.invalidate(key.as_bytes()).await;
        }
        Ok(())
    }

    /// Returns true if the key exists.
    async fn exists(&self, key: &str) -> Result<bool> {
        let key = key.as_bytes().to_vec();
        let kv_backend = self.kv_backend.clone();
        let value = self
            .cache
            .try_get_with(key.clone(), async move {
                kv_backend.get(&key).await.map(|v| v.map(|v| v.value))
            })
            .await
            .context(GetCacheSnafu)?;

        Ok(value.is_some())
    }

    /// Enables maintenance mode.
    pub async fn set_maintenance_mode(&self) -> Result<()> {
        self.put_key(MAINTENANCE_KEY).await
    }

    /// Unsets maintenance mode.
    pub async fn unset_maintenance_mode(&self) -> Result<()> {
        self.delete_keys(&[MAINTENANCE_KEY, LEGACY_MAINTENANCE_KEY])
            .await
    }

    /// Returns true if maintenance mode is enabled.
    pub async fn maintenance_mode(&self) -> Result<bool> {
        let exists = self.exists(MAINTENANCE_KEY).await?;
        if exists {
            return Ok(true);
        }

        let exists = self.exists(LEGACY_MAINTENANCE_KEY).await?;
        if exists {
            return Ok(true);
        }

        Ok(false)
    }

    // Pauses handling of incoming procedure requests.
    pub async fn pasue_procedure(&self) -> Result<()> {
        self.put_key(PAUSE_PROCEDURE_KEY).await
    }

    /// Resumes processing of incoming procedure requests.
    pub async fn resume_procedure(&self) -> Result<()> {
        self.delete_keys(&[PAUSE_PROCEDURE_KEY]).await
    }

    /// Returns true if the system is currently pausing incoming procedure requests.
    pub async fn is_procedure_paused(&self) -> Result<bool> {
        self.exists(PAUSE_PROCEDURE_KEY).await
    }

    /// Enables recovery mode.
    pub async fn set_recovery_mode(&self) -> Result<()> {
        self.put_key(RECOVERY_MODE_KEY).await
    }

    /// Unsets recovery mode.
    pub async fn unset_recovery_mode(&self) -> Result<()> {
        self.delete_keys(&[RECOVERY_MODE_KEY]).await
    }

    /// Returns true if the system is currently in recovery mode.
    pub async fn recovery_mode(&self) -> Result<bool> {
        self.exists(RECOVERY_MODE_KEY).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::key::runtime_switch::RuntimeSwitchManager;
    use crate::key::{LEGACY_MAINTENANCE_KEY, MAINTENANCE_KEY};
    use crate::kv_backend::KvBackend;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::rpc::store::PutRequest;

    #[tokio::test]
    async fn test_runtime_switch_manager_basic() {
        let runtime_switch_manager =
            Arc::new(RuntimeSwitchManager::new(Arc::new(MemoryKvBackend::new())));
        runtime_switch_manager
            .put_key(MAINTENANCE_KEY)
            .await
            .unwrap();
        let v = runtime_switch_manager
            .cache
            .get(MAINTENANCE_KEY.as_bytes())
            .await;
        assert!(v.is_none());
        runtime_switch_manager
            .exists(MAINTENANCE_KEY)
            .await
            .unwrap();
        let v = runtime_switch_manager
            .cache
            .get(MAINTENANCE_KEY.as_bytes())
            .await;
        assert!(v.is_some());
        runtime_switch_manager
            .delete_keys(&[MAINTENANCE_KEY])
            .await
            .unwrap();
        let v = runtime_switch_manager
            .cache
            .get(MAINTENANCE_KEY.as_bytes())
            .await;
        assert!(v.is_none());
    }

    #[tokio::test]
    async fn test_runtime_switch_manager() {
        let runtime_switch_manager =
            Arc::new(RuntimeSwitchManager::new(Arc::new(MemoryKvBackend::new())));
        assert!(!runtime_switch_manager.maintenance_mode().await.unwrap());
        runtime_switch_manager.set_maintenance_mode().await.unwrap();
        assert!(runtime_switch_manager.maintenance_mode().await.unwrap());
        runtime_switch_manager
            .unset_maintenance_mode()
            .await
            .unwrap();
        assert!(!runtime_switch_manager.maintenance_mode().await.unwrap());
    }

    #[tokio::test]
    async fn test_runtime_switch_manager_with_legacy_key() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        kv_backend
            .put(PutRequest {
                key: Vec::from(LEGACY_MAINTENANCE_KEY),
                value: vec![],
                prev_kv: false,
            })
            .await
            .unwrap();
        let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend));
        assert!(runtime_switch_manager.maintenance_mode().await.unwrap());
        runtime_switch_manager
            .unset_maintenance_mode()
            .await
            .unwrap();
        assert!(!runtime_switch_manager.maintenance_mode().await.unwrap());
        runtime_switch_manager.set_maintenance_mode().await.unwrap();
        assert!(runtime_switch_manager.maintenance_mode().await.unwrap());
    }

    #[tokio::test]
    async fn test_pasue_procedure() {
        let runtime_switch_manager =
            Arc::new(RuntimeSwitchManager::new(Arc::new(MemoryKvBackend::new())));
        runtime_switch_manager.pasue_procedure().await.unwrap();
        assert!(runtime_switch_manager.is_procedure_paused().await.unwrap());
        runtime_switch_manager.resume_procedure().await.unwrap();
        assert!(!runtime_switch_manager.is_procedure_paused().await.unwrap());
    }

    #[tokio::test]
    async fn test_recovery_mode() {
        let runtime_switch_manager =
            Arc::new(RuntimeSwitchManager::new(Arc::new(MemoryKvBackend::new())));
        assert!(!runtime_switch_manager.recovery_mode().await.unwrap());
        runtime_switch_manager.set_recovery_mode().await.unwrap();
        assert!(runtime_switch_manager.recovery_mode().await.unwrap());
        runtime_switch_manager.unset_recovery_mode().await.unwrap();
        assert!(!runtime_switch_manager.recovery_mode().await.unwrap());
    }
}
