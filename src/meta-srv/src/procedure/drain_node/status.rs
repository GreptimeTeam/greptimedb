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
use common_meta::key::{DRAIN_NODE_STATUS_PREFIX, drain_node_status_key};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::{PutRequest, RangeRequest};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};

pub type DrainNodeStatusManagerRef = Arc<DrainNodeStatusManager>;

/// Persists node placement exclusion across metasrv restarts and leader changes.
pub struct DrainNodeStatusManager {
    kv_backend: KvBackendRef,
}

impl DrainNodeStatusManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Begins draining by preventing new table and region placements on the node.
    pub async fn mark_draining(&self, datanode_id: DatanodeId) -> Result<()> {
        self.kv_backend
            .put(
                PutRequest::new()
                    .with_key(drain_node_status_key(datanode_id))
                    .with_value(b"draining".to_vec()),
            )
            .await
            .context(error::DrainNodeStatusSnafu)?;
        Ok(())
    }

    /// Records completion while retaining the node's placement exclusion marker.
    pub async fn mark_drained(&self, datanode_id: DatanodeId) -> Result<()> {
        self.kv_backend
            .put(
                PutRequest::new()
                    .with_key(drain_node_status_key(datanode_id))
                    .with_value(b"drained".to_vec()),
            )
            .await
            .context(error::DrainNodeStatusSnafu)?;
        Ok(())
    }

    /// Returns all datanodes excluded from new placements.
    pub async fn excluded_ids(&self) -> Result<HashSet<DatanodeId>> {
        let response = self
            .kv_backend
            .range(
                RangeRequest::new()
                    .with_prefix(DRAIN_NODE_STATUS_PREFIX)
                    .with_keys_only(),
            )
            .await
            .context(error::DrainNodeStatusSnafu)?;
        response
            .kvs
            .into_iter()
            .map(|kv| {
                kv.key
                    .strip_prefix(DRAIN_NODE_STATUS_PREFIX.as_bytes())
                    .and_then(|id| std::str::from_utf8(id).ok())
                    .and_then(|id| id.parse().ok())
                    .context(error::UnexpectedSnafu {
                        violated: format!("invalid drain-node status key: {:?}", kv.key),
                    })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::key::drain_node_status_key;
    use common_meta::kv_backend::KvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;

    use super::DrainNodeStatusManager;

    #[tokio::test]
    async fn test_mark_draining_persists_exclusion() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let manager = DrainNodeStatusManager::new(kv_backend.clone());
        manager.mark_draining(7).await.unwrap();

        assert_eq!(
            kv_backend
                .get(&drain_node_status_key(7))
                .await
                .unwrap()
                .unwrap()
                .value,
            b"draining"
        );
        assert!(
            DrainNodeStatusManager::new(kv_backend.clone())
                .excluded_ids()
                .await
                .unwrap()
                .contains(&7)
        );

        manager.mark_drained(7).await.unwrap();
        assert_eq!(
            kv_backend
                .get(&drain_node_status_key(7))
                .await
                .unwrap()
                .unwrap()
                .value,
            b"drained"
        );
        assert!(manager.excluded_ids().await.unwrap().contains(&7));
    }
}
