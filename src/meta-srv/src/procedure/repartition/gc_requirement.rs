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

use common_meta::key::REPARTITION_GC_REQUIRED_KEY;
use common_meta::key::table_repart::TableRepartManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::PutRequest;
use common_procedure::ProcedureManagerRef;
use snafu::{ResultExt, ensure};

use crate::error::{self, Result};
use crate::procedure::repartition::RepartitionProcedure;
use crate::procedure::repartition::group::RepartitionGroupProcedure;

const REPARTITION_GC_REQUIREMENT_VALUE: &[u8] = b"repartition";

pub type RepartitionGcRequirementManagerRef = Arc<RepartitionGcRequirementManager>;

/// Persists and enforces the cluster-level GC requirement introduced by repartition.
pub struct RepartitionGcRequirementManager {
    kv_backend: KvBackendRef,
}

impl RepartitionGcRequirementManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Persists the requirement before a repartition procedure can be submitted.
    pub async fn require_gc(&self) -> Result<()> {
        self.kv_backend
            .put(
                PutRequest::new()
                    .with_key(REPARTITION_GC_REQUIRED_KEY.as_bytes().to_vec())
                    .with_value(REPARTITION_GC_REQUIREMENT_VALUE.to_vec()),
            )
            .await
            .context(error::RepartitionGcRequirementSnafu)?;
        Ok(())
    }

    pub async fn is_gc_required(&self) -> Result<bool> {
        self.kv_backend
            .get(REPARTITION_GC_REQUIRED_KEY.as_bytes())
            .await
            .map(|value| value.is_some())
            .context(error::RepartitionGcRequirementSnafu)
    }

    /// Backfills the requirement from durable state written by older versions.
    ///
    /// An unfinished procedure covers the crash window before repartition mappings
    /// are written. A non-empty mapping covers completed repartitions whose
    /// manifests can still contain cross-region file references.
    pub async fn reconcile_legacy_state(
        &self,
        procedure_manager: &ProcedureManagerRef,
    ) -> Result<bool> {
        if self.is_gc_required().await? {
            return Ok(true);
        }

        let table_reparts = TableRepartManager::new(self.kv_backend.clone())
            .table_reparts()
            .await
            .context(error::RepartitionGcRequirementSnafu)?;
        let has_cross_region_references = table_reparts
            .iter()
            .any(|(_, value)| !value.src_to_dst.is_empty());
        let has_unfinished_repartition = procedure_manager
            .has_unfinished_procedure(&[
                RepartitionProcedure::TYPE_NAME,
                RepartitionGroupProcedure::TYPE_NAME,
            ])
            .await
            .context(error::InspectRepartitionProceduresSnafu)?;

        if has_cross_region_references || has_unfinished_repartition {
            self.require_gc().await?;
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn ensure_gc_enabled(
        &self,
        gc_enabled: bool,
        procedure_manager: &ProcedureManagerRef,
    ) -> Result<()> {
        let required = self.reconcile_legacy_state(procedure_manager).await?;
        ensure!(!required || gc_enabled, error::RepartitionGcRequiredSnafu);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use async_trait::async_trait;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::state_store::KvStateStore;
    use common_procedure::local::{LocalManager, ManagerConfig};
    use common_procedure::{
        Context, LockKey, Procedure, ProcedureId, ProcedureManager, ProcedureWithId, Status,
    };
    use store_api::storage::RegionId;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;

    fn local_procedure_manager(kv_backend: KvBackendRef) -> Arc<LocalManager> {
        let state_store = Arc::new(KvStateStore::new(kv_backend));
        Arc::new(LocalManager::new(
            ManagerConfig::default(),
            state_store.clone(),
            state_store,
            None,
            None,
        ))
    }

    fn procedure_manager(kv_backend: KvBackendRef) -> ProcedureManagerRef {
        local_procedure_manager(kv_backend)
    }

    #[derive(Debug)]
    struct LegacyRepartitionProcedure {
        persisted: bool,
        block_after_persist: bool,
        persisted_tx: Option<oneshot::Sender<()>>,
    }

    #[async_trait]
    impl Procedure for LegacyRepartitionProcedure {
        fn type_name(&self) -> &str {
            RepartitionProcedure::TYPE_NAME
        }

        async fn execute(&mut self, _ctx: &Context) -> common_procedure::error::Result<Status> {
            if !self.persisted {
                self.persisted = true;
                return Ok(Status::executing(true));
            }

            if self.block_after_persist {
                if let Some(tx) = self.persisted_tx.take() {
                    let _ = tx.send(());
                }
                return std::future::pending().await;
            }

            Ok(Status::done())
        }

        fn dump(&self) -> common_procedure::error::Result<String> {
            Ok("{}".to_string())
        }

        fn lock_key(&self) -> LockKey {
            LockKey::default()
        }
    }

    #[tokio::test]
    async fn test_completed_repartition_requirement_rejects_gc_disabled_restart() {
        let kv_backend: KvBackendRef = Arc::new(MemoryKvBackend::new());
        let manager = RepartitionGcRequirementManager::new(kv_backend.clone());
        let procedure_manager = procedure_manager(kv_backend.clone());

        manager.require_gc().await.unwrap();
        let marker = kv_backend
            .get(REPARTITION_GC_REQUIRED_KEY.as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(REPARTITION_GC_REQUIREMENT_VALUE, marker.value.as_slice());

        let err = manager
            .ensure_gc_enabled(false, &procedure_manager)
            .await
            .unwrap_err();
        assert!(matches!(err, error::Error::RepartitionGcRequired { .. }));
        assert!(manager.is_gc_required().await.unwrap());
        manager
            .ensure_gc_enabled(true, &procedure_manager)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_reconcile_legacy_repartition_mapping() {
        let kv_backend: KvBackendRef = Arc::new(MemoryKvBackend::new());
        let manager = RepartitionGcRequirementManager::new(kv_backend.clone());
        let procedure_manager = procedure_manager(kv_backend.clone());
        let table_id = 1024;
        let source = RegionId::new(table_id, 1);
        let destination = RegionId::new(table_id, 2);
        TableRepartManager::new(kv_backend)
            .update_mappings(table_id, &HashMap::from([(source, vec![destination])]))
            .await
            .unwrap();

        assert!(
            manager
                .reconcile_legacy_state(&procedure_manager)
                .await
                .unwrap()
        );
        assert!(manager.is_gc_required().await.unwrap());
    }

    #[tokio::test]
    async fn test_legacy_unfinished_repartition_fails_closed_and_recovers() {
        let kv_backend: KvBackendRef = Arc::new(MemoryKvBackend::new());
        let first_manager = local_procedure_manager(kv_backend.clone());
        first_manager.start().await.unwrap();

        let procedure_id = ProcedureId::random();
        let (persisted_tx, persisted_rx) = oneshot::channel();
        first_manager
            .submit(ProcedureWithId {
                id: procedure_id,
                procedure: Box::new(LegacyRepartitionProcedure {
                    persisted: false,
                    block_after_persist: true,
                    persisted_tx: Some(persisted_tx),
                }),
            })
            .await
            .unwrap();
        timeout(Duration::from_secs(10), persisted_rx)
            .await
            .unwrap()
            .unwrap();
        first_manager.stop().await.unwrap();

        let requirement_manager = RepartitionGcRequirementManager::new(kv_backend.clone());
        let disabled_manager = local_procedure_manager(kv_backend.clone());
        let disabled_manager_ref: ProcedureManagerRef = disabled_manager.clone();
        let err = requirement_manager
            .ensure_gc_enabled(false, &disabled_manager_ref)
            .await
            .unwrap_err();
        assert!(matches!(err, error::Error::RepartitionGcRequired { .. }));
        assert!(requirement_manager.is_gc_required().await.unwrap());

        assert!(
            disabled_manager
                .has_unfinished_procedure(&[RepartitionProcedure::TYPE_NAME])
                .await
                .unwrap()
        );
        assert!(
            disabled_manager
                .procedure_state(procedure_id)
                .await
                .unwrap()
                .is_none()
        );

        let enabled_manager = local_procedure_manager(kv_backend);
        enabled_manager
            .register_loader(
                RepartitionProcedure::TYPE_NAME,
                Box::new(|_| {
                    Ok(Box::new(LegacyRepartitionProcedure {
                        persisted: true,
                        block_after_persist: false,
                        persisted_tx: None,
                    }) as _)
                }),
            )
            .unwrap();
        enabled_manager.start().await.unwrap();
        let mut watcher = enabled_manager.procedure_watcher(procedure_id).unwrap();
        timeout(Duration::from_secs(10), async {
            while !watcher.borrow().is_done() {
                watcher.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(
            !enabled_manager
                .has_unfinished_procedure(&[RepartitionProcedure::TYPE_NAME])
                .await
                .unwrap()
        );
        enabled_manager.stop().await.unwrap();
    }
}
