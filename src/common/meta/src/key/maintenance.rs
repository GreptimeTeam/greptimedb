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

use crate::error::Result;
use crate::key::MAINTENANCE_KEY;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::PutRequest;

pub type MaintenanceModeManagerRef = Arc<MaintenanceModeManager>;

/// The maintenance mode manager.
///
/// Used to enable or disable maintenance mode.
#[derive(Clone)]
pub struct MaintenanceModeManager {
    kv_backend: KvBackendRef,
}

impl MaintenanceModeManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Enables maintenance mode.
    pub async fn set_maintenance_mode(&self) -> Result<()> {
        let req = PutRequest {
            key: Vec::from(MAINTENANCE_KEY),
            value: vec![],
            prev_kv: false,
        };
        self.kv_backend.put(req).await?;
        Ok(())
    }

    /// Unsets maintenance mode.
    pub async fn unset_maintenance_mode(&self) -> Result<()> {
        self.kv_backend
            .delete(MAINTENANCE_KEY.as_bytes(), false)
            .await?;
        Ok(())
    }

    /// Returns true if maintenance mode is enabled.
    pub async fn maintenance_mode(&self) -> Result<bool> {
        self.kv_backend.exists(MAINTENANCE_KEY.as_bytes()).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::key::maintenance::MaintenanceModeManager;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_maintenance_mode_manager() {
        let maintenance_mode_manager = Arc::new(MaintenanceModeManager::new(Arc::new(
            MemoryKvBackend::new(),
        )));
        assert!(!maintenance_mode_manager.maintenance_mode().await.unwrap());
        maintenance_mode_manager
            .set_maintenance_mode()
            .await
            .unwrap();
        assert!(maintenance_mode_manager.maintenance_mode().await.unwrap());
        maintenance_mode_manager
            .unset_maintenance_mode()
            .await
            .unwrap();
        assert!(!maintenance_mode_manager.maintenance_mode().await.unwrap());
    }
}
