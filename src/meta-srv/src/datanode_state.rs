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

use std::collections::HashSet;
use std::sync::Arc;

use common_meta::DatanodeId;
use common_meta::error::{Result, UnexpectedSnafu};
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use common_meta::rpc::store::RangeRequest;

const DATANODE_STATE_PREFIX: &str = "__datanode_state/";
const UNSCHEDULABLE_VALUE: &[u8] = b"unschedulable";

pub type DatanodeStateManagerRef = Arc<DatanodeStateManager>;

/// Reads and updates datanode scheduling eligibility in the shared KV backend.
pub struct DatanodeStateManager {
    kv_backend: KvBackendRef,
}

impl DatanodeStateManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Changes scheduling eligibility if the current state is the opposite value.
    /// Returns whether the transition was applied.
    pub async fn set_schedulable(
        &self,
        datanode_id: DatanodeId,
        schedulable: bool,
    ) -> Result<bool> {
        let key = state_key(datanode_id);
        let txn = if schedulable {
            Txn::new()
                .when(vec![Compare::with_value(
                    key.clone(),
                    CompareOp::Equal,
                    UNSCHEDULABLE_VALUE.to_vec(),
                )])
                .and_then(vec![TxnOp::Delete(key)])
        } else {
            Txn::put_if_not_exists(key, UNSCHEDULABLE_VALUE.to_vec())
        };
        Ok(self.kv_backend.txn(txn).await?.succeeded)
    }

    /// Returns every datanode that cannot receive new regions in one prefix range request.
    pub async fn unschedulable_ids(&self) -> Result<HashSet<DatanodeId>> {
        let response = self
            .kv_backend
            .range(RangeRequest::new().with_prefix(DATANODE_STATE_PREFIX))
            .await?;
        let mut ids = HashSet::new();
        for kv in response.kvs {
            let id = decode_id(&kv.key)?;
            if kv.value.as_slice() != UNSCHEDULABLE_VALUE {
                return Err(UnexpectedSnafu {
                    err_msg: format!("invalid datanode scheduling state for id: {id}"),
                }
                .build());
            }
            ids.insert(id);
        }
        Ok(ids)
    }
}

fn state_key(datanode_id: DatanodeId) -> Vec<u8> {
    format!("{DATANODE_STATE_PREFIX}{datanode_id}").into_bytes()
}

fn decode_id(key: &[u8]) -> Result<DatanodeId> {
    key.strip_prefix(DATANODE_STATE_PREFIX.as_bytes())
        .and_then(|id| std::str::from_utf8(id).ok())
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| {
            UnexpectedSnafu {
                err_msg: format!(
                    "invalid datanode state key: {}",
                    String::from_utf8_lossy(key)
                ),
            }
            .build()
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::kv_backend::KvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;

    use super::{DatanodeStateManager, state_key};

    #[tokio::test]
    async fn test_scheduling_state_survives_manager_recreation() {
        let backend = Arc::new(MemoryKvBackend::new());
        let manager = DatanodeStateManager::new(backend.clone());
        assert!(manager.unschedulable_ids().await.unwrap().is_empty());

        assert!(manager.set_schedulable(7, false).await.unwrap());
        assert!(!manager.set_schedulable(7, false).await.unwrap());
        let value = backend.get(&state_key(7)).await.unwrap().unwrap().value;
        assert_eq!(value.as_slice(), b"unschedulable");
        let recreated = DatanodeStateManager::new(backend.clone());
        assert_eq!(recreated.unschedulable_ids().await.unwrap(), [7].into());
        assert!(recreated.set_schedulable(8, false).await.unwrap());
        assert_eq!(recreated.unschedulable_ids().await.unwrap(), [7, 8].into());

        assert!(recreated.set_schedulable(7, true).await.unwrap());
        assert!(!recreated.set_schedulable(7, true).await.unwrap());
        assert_eq!(manager.unschedulable_ids().await.unwrap(), [8].into());
        assert!(backend.get(&state_key(7)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_only_one_concurrent_transition_succeeds() {
        let backend = Arc::new(MemoryKvBackend::new());
        let manager = DatanodeStateManager::new(backend);

        let (first, second) = tokio::join!(
            manager.set_schedulable(7, false),
            manager.set_schedulable(7, false)
        );
        assert_ne!(first.unwrap(), second.unwrap());
        assert_eq!(manager.unschedulable_ids().await.unwrap(), [7].into());
    }
}
