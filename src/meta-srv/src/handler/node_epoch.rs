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

use std::cmp::Ordering;
use std::time::Duration;

use common_meta::key::MetadataValue;
use common_meta::key::datanode_epoch::{DatanodeEpochKey, DatanodeEpochValue};
use common_meta::kv_backend::KvBackend;
use common_meta::rpc::store::CompareAndPutRequest;
use common_telemetry::{info, warn};
use common_time::util::current_time_millis;
use snafu::ResultExt;

use crate::error::{self, Result};

/// How far ahead of the leader's clock an epoch can be recorded. Recording an epoch from a
/// clock that jumped further ahead would fence every later process with the node id.
const MAX_NODE_EPOCH_AHEAD: Duration = Duration::from_secs(5 * 60);

/// Checks the `node_epoch` of a datanode request against the highest epoch recorded for its
/// node id, raising the record first when the request's is higher.
///
/// Returns false if the request comes from a process that a newer process with the same node
/// id has replaced, or if its epoch is too far ahead of this metasrv's clock to be recorded.
pub(crate) async fn check_node_epoch(
    kv_backend: &dyn KvBackend<Error = common_meta::error::Error>,
    node_id: u64,
    node_epoch: u64,
) -> Result<bool> {
    let key = DatanodeEpochKey::new(node_id).to_bytes();
    let mut current = kv_backend.get(&key).await.context(error::KvBackendSnafu)?;
    loop {
        let (expect, highest) = match current {
            Some(kv) => {
                let value = DatanodeEpochValue::try_from_raw_value(&kv.value)
                    .context(error::KvBackendSnafu)?;
                (kv.value, value.node_epoch)
            }
            None => (vec![], 0),
        };
        match node_epoch.cmp(&highest) {
            Ordering::Less => {
                warn!(
                    "Fenced datanode {} with epoch {}: a newer process with epoch {} has been accepted",
                    node_id, node_epoch, highest
                );
                return Ok(false);
            }
            Ordering::Equal => return Ok(true),
            Ordering::Greater => {}
        }

        let limit = (current_time_millis().max(0) as u64)
            .saturating_add(MAX_NODE_EPOCH_AHEAD.as_millis() as u64);
        if node_epoch > limit {
            warn!(
                "Fenced datanode {} with epoch {}: it is more than {:?} ahead of this metasrv's clock",
                node_id, node_epoch, MAX_NODE_EPOCH_AHEAD
            );
            return Ok(false);
        }

        let value = DatanodeEpochValue { node_epoch }
            .try_as_raw_value()
            .context(error::KvBackendSnafu)?;
        let resp = kv_backend
            .compare_and_put(CompareAndPutRequest {
                key: key.clone(),
                expect,
                value,
            })
            .await
            .context(error::KvBackendSnafu)?;
        if resp.success {
            info!(
                "Raised the epoch of datanode {} from {} to {}",
                node_id, highest, node_epoch
            );
            return Ok(true);
        }
        current = resp.prev_kv;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::{KvBackendRef, ResettableKvBackend};

    use super::*;
    use crate::service::store::cached_kv::LeaderCachedKvBackend;

    #[tokio::test]
    async fn test_record_is_reloaded_on_leader_change() {
        let store: KvBackendRef = Arc::new(MemoryKvBackend::new());
        let cached = LeaderCachedKvBackend::with_always_leader(store.clone());
        let now = current_time_millis() as u64;
        let (e1, e2) = (now - 1000, now);
        assert!(check_node_epoch(&cached, 1, e1).await.unwrap());
        assert!(check_node_epoch(&cached, 1, e2).await.unwrap());

        // The cache is reset and loaded again when this metasrv becomes the leader. The record
        // is then served from the loaded cache, even without the store.
        cached.reset();
        cached.load().await.unwrap();
        store
            .delete(&DatanodeEpochKey::new(1).to_bytes(), false)
            .await
            .unwrap();
        assert!(!check_node_epoch(&cached, 1, e1).await.unwrap());
        assert!(check_node_epoch(&cached, 1, e2).await.unwrap());
    }

    #[tokio::test]
    async fn test_epoch_far_ahead_is_not_recorded() {
        let kv_backend = MemoryKvBackend::new();
        let now = current_time_millis() as u64;
        let ahead = now + Duration::from_secs(10 * 60).as_millis() as u64;
        assert!(!check_node_epoch(&kv_backend, 1, ahead).await.unwrap());
        let key = DatanodeEpochKey::new(1).to_bytes();
        assert!(kv_backend.get(&key).await.unwrap().is_none());

        let slightly_ahead = now + Duration::from_secs(60).as_millis() as u64;
        assert!(
            check_node_epoch(&kv_backend, 1, slightly_ahead)
                .await
                .unwrap()
        );
    }
}
