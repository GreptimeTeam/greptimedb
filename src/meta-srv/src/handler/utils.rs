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

use std::future::Future;
use std::sync::Arc;

use api::v1::meta::{Peer, Role};
use common_meta::instruction::CacheIdent;
use common_meta::key::MetadataValue;
use common_meta::key::node_address::NodeAddressValue;
use common_meta::rpc::store::PutRequest;
use common_telemetry::{error, info};
use dashmap::DashMap;
use snafu::ResultExt;
use tokio::sync::Mutex;

use crate::Result;
use crate::error::{InvalidNodeInfoFormatSnafu, KvBackendSnafu};
use crate::metasrv::Context;

/// Serializes address changes per node and acknowledges only successful updates.
/// Node information refreshed on every heartbeat has a separate lifecycle.
#[derive(Debug, Default)]
pub(crate) struct NodeAddressUpdater {
    states: DashMap<(Role, u64), Arc<Mutex<NodeAddressEpochs>>>,
}

#[derive(Debug, Default)]
struct NodeAddressEpochs {
    /// Rejects stale heartbeats even after a newer epoch failed to save.
    seen_epoch: Option<u64>,
    /// Suppresses retries only after persistence succeeds.
    saved_epoch: Option<u64>,
}

/// The outcome of a successful address save.
pub(crate) struct SavedAddress<T> {
    pub(crate) output: T,
    /// An earlier save of this epoch failed or was cancelled, but its write
    /// may still have reached the store.
    pub(crate) retried: bool,
}

impl NodeAddressUpdater {
    /// Saves a new or previously failed epoch. Returns `None` when the
    /// heartbeat is stale or already saved.
    /// Cache invalidation is separate and does not affect acknowledgement.
    pub(crate) async fn update_if_needed<T, F, Fut>(
        &self,
        node: (Role, u64),
        epoch: u64,
        save: F,
    ) -> Result<Option<SavedAddress<T>>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        // Never retain a DashMap guard across await, even while waiting for a
        // different node: its entry may live in the same shard.
        let state = {
            let entry = self.states.entry(node).or_default();
            Arc::clone(entry.value())
        };
        let mut state = state.lock().await;
        if state.seen_epoch.is_some_and(|seen| epoch < seen) || state.saved_epoch == Some(epoch) {
            return Ok(None);
        }

        // Seeing an epoch rejects stale heartbeats, but only a successful save
        // suppresses retries of that same epoch. Cancellation also leaves it retryable.
        let retried = state.seen_epoch == Some(epoch);
        state.seen_epoch = Some(epoch);
        let output = save().await?;
        state.saved_epoch = Some(epoch);
        Ok(Some(SavedAddress { output, retried }))
    }
}

/// Persists an address through the leader cache and returns the one it
/// replaced, treating an undecodable value as absent.
pub(crate) async fn save_node_address(
    ctx: &Context,
    key: Vec<u8>,
    peer: Peer,
) -> Result<Option<NodeAddressValue>> {
    let address = NodeAddressValue::new(peer);
    let value = address
        .try_as_raw_value()
        .context(InvalidNodeInfoFormatSnafu)?;
    let resp = ctx
        .leader_cached_kv_backend
        .put(PutRequest {
            key,
            value,
            prev_kv: true,
        })
        .await
        .context(KvBackendSnafu)?;
    info!("Successfully updated node address: {:?}", address.peer);
    Ok(resp
        .prev_kv
        .and_then(|kv| NodeAddressValue::try_from_raw_value(&kv.value).ok()))
}

/// Preserves the existing best-effort notification behavior. Notification
/// failures do not undo a persisted address or cause same-epoch rewrites.
pub(crate) async fn invalidate_address_caches(
    ctx: &Context,
    node_id: u64,
    cache_idents: &[CacheIdent],
) {
    if let Err(e) = ctx
        .cache_invalidator
        .invalidate(&Default::default(), cache_idents)
        .await
    {
        error!(e; "Failed to invalidate address caches for node {}: {:?}", node_id, cache_idents);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::Notify;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_failed_save_retries_same_epoch_and_rejects_stale_heartbeats() {
        let updater = NodeAddressUpdater::default();
        let key = (Role::Datanode, 1);
        assert!(
            updater
                .update_if_needed::<(), _, _>(key, 2, || async {
                    crate::error::UnexpectedSnafu {
                        violated: "injected save failure",
                    }
                    .fail()
                })
                .await
                .is_err()
        );
        assert!(
            updater
                .update_if_needed::<(), _, _>(key, 1, || async {
                    panic!("stale heartbeat must not save");
                })
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            updater
                .update_if_needed(key, 2, || async { Ok(()) })
                .await
                .unwrap()
                .is_some_and(|saved| saved.retried)
        );
        assert!(
            updater
                .update_if_needed::<(), _, _>(key, 2, || async {
                    panic!("successful epoch must not save again");
                })
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            updater
                .update_if_needed(key, 3, || async { Ok(()) })
                .await
                .unwrap()
                .is_some_and(|saved| !saved.retried)
        );
    }

    #[tokio::test]
    async fn test_cancelled_save_can_be_retried() {
        let updater = NodeAddressUpdater::default();
        let key = (Role::Datanode, 1);
        let entered = Notify::new();
        {
            let first = updater.update_if_needed(key, 0, || async {
                entered.notify_one();
                std::future::pending::<Result<()>>().await
            });
            tokio::pin!(first);
            tokio::select! {
                _ = entered.notified() => {},
                _ = &mut first => panic!("save must remain pending"),
            }
        }
        assert!(
            timeout(
                Duration::from_secs(2),
                updater.update_if_needed(key, 0, || async { Ok(()) })
            )
            .await
            .unwrap()
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn test_same_node_serialized_other_nodes_progress() {
        // A watchdog outside the runtime detects synchronous DashMap deadlocks;
        // a Tokio timeout alone cannot fire if the only worker is blocked.
        let (progress_tx, progress_rx) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let worker = std::thread::spawn({
            let release = release.clone();
            let finish = finish.clone();
            move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let updater = Arc::new(NodeAddressUpdater::default());
                        let entered = Arc::new(Notify::new());

                        let first = tokio::spawn({
                            let updater = updater.clone();
                            let entered = entered.clone();
                            let release = release.clone();
                            async move {
                                updater
                                    .update_if_needed((Role::Datanode, 1), 1, || async {
                                        entered.notify_one();
                                        release.notified().await;
                                        Ok(())
                                    })
                                    .await
                            }
                        });
                        entered.notified().await;
                        let second = tokio::spawn({
                            let updater = updater.clone();
                            async move {
                                updater
                                    .update_if_needed((Role::Datanode, 1), 2, || async { Ok(()) })
                                    .await
                            }
                        });
                        tokio::task::yield_now().await;
                        assert!(!second.is_finished());
                        // Exercise shared DashMap shards on a single-thread runtime while the
                        // first node is suspended in external I/O.
                        timeout(Duration::from_secs(2), async {
                            for id in 0_u64..128 {
                                assert!(
                                    updater
                                        .update_if_needed((Role::Datanode, id + 2), 1, || async {
                                            Ok(())
                                        })
                                        .await
                                        .unwrap()
                                        .is_some()
                                );
                            }
                        })
                        .await
                        .unwrap();
                        progress_tx.send(()).unwrap();
                        finish.notified().await;
                        timeout(Duration::from_secs(2), async {
                            assert!(first.await.unwrap().unwrap().is_some());
                            assert!(second.await.unwrap().unwrap().is_some());
                        })
                        .await
                        .unwrap();
                        assert!(
                            updater
                                .update_if_needed::<(), _, _>((Role::Datanode, 1), 1, || async {
                                    panic!("old epoch must not overwrite a newer one");
                                })
                                .await
                                .unwrap()
                                .is_none()
                        );
                    });
            }
        });
        let progress = progress_rx.recv_timeout(Duration::from_secs(5));
        release.notify_one();
        finish.notify_one();
        progress.expect("other node updates must progress while one node save is blocked");
        worker.join().unwrap();
    }
}
