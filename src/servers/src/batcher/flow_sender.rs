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

use api::v1::flow::{DirtyWindowRequest, DirtyWindowRequests};
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::NodeManagerRef;
use common_telemetry::error;
use store_api::storage::TableId;

/// One source table's timestamps, expressed in its native time unit.
pub(in crate::batcher) struct FlowNotification {
    pub table_id: TableId,
    pub timestamps: Vec<i64>,
}

/// Resolves Flow targets and delivers dirty-window notifications best-effort.
/// Queue admission, delivery concurrency and write completion belong to callers.
#[derive(Clone)]
pub(in crate::batcher) struct FlowSender {
    cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
}

impl FlowSender {
    /// Binds delivery dependencies without creating a queue or starting a task.
    pub fn new(cache: TableFlownodeSetCacheRef, node_manager: NodeManagerRef) -> Self {
        Self {
            cache,
            node_manager,
        }
    }

    /// Sends once per distinct peer, without retrying or failing the completed write.
    /// Peer RPCs stay within the caller's notification concurrency budget.
    pub async fn send(&self, notification: FlowNotification) {
        let table_id = notification.table_id;
        let flownodes = match self.cache.get(table_id).await {
            Ok(Some(flownodes)) => flownodes,
            Ok(None) => return,
            Err(e) => {
                error!(e; "Failed to get flownodes for table id: {}", table_id);
                return;
            }
        };
        let peers = flownodes.values().cloned().collect::<HashSet<_>>();

        for peer in peers {
            if let Err(e) = self
                .node_manager
                .flownode(&peer)
                .await
                .handle_mark_window_dirty(DirtyWindowRequests {
                    requests: vec![DirtyWindowRequest {
                        table_id,
                        timestamps: notification.timestamps.clone(),
                        time_ranges: Vec::new(),
                    }],
                })
                .await
            {
                error!(
                    e;
                    "Failed to mark timestamps as dirty, table_id: {}, peer_id: {}, peer_addr: {}",
                    table_id,
                    peer.id,
                    peer.addr
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use api::v1::flow::{DirtyWindowRequest, DirtyWindowRequests, FlowRequest, FlowResponse};
    use api::v1::region::InsertRequests;
    use async_trait::async_trait;
    use common_meta::error::{Result as MetaResult, UnexpectedSnafu};
    use common_meta::node_manager::{
        DatanodeManager, DatanodeRef, Flownode, FlownodeManager, FlownodeRef,
    };
    use common_meta::peer::Peer;

    use crate::batcher::flow_sender::{FlowNotification, FlowSender};
    use crate::batcher::test_util::mock_table_flownode_cache;

    #[derive(Default)]
    struct RecordingNodeManager {
        requests: Arc<Mutex<Vec<(u64, DirtyWindowRequests)>>>,
        fail_first: bool,
    }

    struct RecordingFlownode {
        peer_id: u64,
        requests: Arc<Mutex<Vec<(u64, DirtyWindowRequests)>>>,
        fail_first: bool,
    }

    #[async_trait]
    impl Flownode for RecordingFlownode {
        async fn handle(&self, _: FlowRequest) -> MetaResult<FlowResponse> {
            unreachable!("notifications must use the dirty-window RPC")
        }

        async fn handle_inserts(&self, _: InsertRequests) -> MetaResult<FlowResponse> {
            unreachable!("notifications must not mirror row inserts")
        }

        async fn handle_mark_window_dirty(
            &self,
            request: DirtyWindowRequests,
        ) -> MetaResult<FlowResponse> {
            let mut requests = self.requests.lock().unwrap();
            requests.push((self.peer_id, request));
            if self.fail_first && requests.len() == 1 {
                return UnexpectedSnafu {
                    err_msg: "injected first notification failure",
                }
                .fail();
            }
            Ok(FlowResponse::default())
        }
    }

    #[async_trait]
    impl DatanodeManager for RecordingNodeManager {
        async fn datanode(&self, _: &Peer) -> DatanodeRef {
            unreachable!("flow notifications must not contact datanodes")
        }
    }

    #[async_trait]
    impl FlownodeManager for RecordingNodeManager {
        async fn flownode(&self, peer: &Peer) -> FlownodeRef {
            Arc::new(RecordingFlownode {
                peer_id: peer.id,
                requests: self.requests.clone(),
                fail_first: self.fail_first,
            })
        }
    }

    #[tokio::test]
    async fn test_deduplicated_delivery_preserves_payload_and_continues_after_error() {
        // HashSet peer order is unspecified, so fail the first attempted RPC rather
        // than a chosen peer. The second peer must still receive the notification.
        for fail_first in [false, true] {
            let first = Peer {
                id: 1,
                addr: "flow-1".to_string(),
            };
            let second = Peer {
                id: 2,
                addr: "flow-2".to_string(),
            };
            let cache =
                mock_table_flownode_cache(42, vec![(0, first.clone()), (1, first), (2, second)])
                    .await;
            let manager = Arc::new(RecordingNodeManager {
                fail_first,
                ..Default::default()
            });
            let sender = FlowSender::new(cache, manager.clone());
            let timestamps = vec![-1, 42, 1_700_000_000_000_000_001, 42];
            sender
                .send(FlowNotification {
                    table_id: 42,
                    timestamps: timestamps.clone(),
                })
                .await;
            let expected = DirtyWindowRequests {
                requests: vec![DirtyWindowRequest {
                    table_id: 42,
                    timestamps,
                    time_ranges: vec![],
                }],
            };
            let mut actual = manager.requests.lock().unwrap().clone();
            actual.sort_by_key(|(peer, _)| *peer);
            assert_eq!(actual, vec![(1, expected.clone()), (2, expected)]);
        }
    }

    #[tokio::test]
    async fn test_missing_targets_do_not_send() {
        let cache = mock_table_flownode_cache(42, vec![]).await;
        let manager = Arc::new(RecordingNodeManager::default());
        let sender = FlowSender::new(cache, manager.clone());
        sender
            .send(FlowNotification {
                table_id: 42,
                timestamps: vec![42],
            })
            .await;
        assert!(manager.requests.lock().unwrap().is_empty());
    }
}
