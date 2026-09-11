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
use std::num::NonZeroUsize;

use api::v1::flow::{DirtyWindowRequest, DirtyWindowRequests};
use common_batcher::notifier::{Notifier, run_notifier};
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::NodeManagerRef;
use common_runtime::spawn_global;
use common_telemetry::{error, warn};
use datatypes::timestamp::append_timestamps;
use table::metadata::TableId;
use tokio::sync::mpsc;

use crate::batcher::logical_table::batch_convert::TableBatch;
use crate::metrics::FLOW_NOTIFICATION_DROPPED;

pub(in crate::batcher::logical_table) const MAX_CONCURRENT_FLOW_NOTIFICATIONS: NonZeroUsize =
    NonZeroUsize::new(8).unwrap();

pub(in crate::batcher::logical_table) fn extract_timestamps(table_batch: &TableBatch) -> Vec<i64> {
    let mut timestamps = Vec::with_capacity(table_batch.row_count);
    for batch in &table_batch.batches {
        let timestamp_column = batch.batch.column(batch.timestamp_index);
        let Some(()) = append_timestamps(timestamp_column, &mut timestamps) else {
            error!(
                "Failed to extract timestamps from record batch, table_id: {}, timestamp_index: {}",
                table_batch.table_id, batch.timestamp_index
            );
            continue;
        };
    }
    timestamps
}

pub(in crate::batcher::logical_table) struct FlowNotification {
    pub(in crate::batcher::logical_table) table_id: TableId,
    pub(in crate::batcher::logical_table) timestamps: Vec<i64>,
}

pub(in crate::batcher::logical_table) fn try_enqueue_flow_notification(
    tx: &Notifier<FlowNotification>,
    notification: FlowNotification,
) -> bool {
    match tx.try_notify(notification) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(notification)) => {
            FLOW_NOTIFICATION_DROPPED.with_label_values(&["full"]).inc();
            warn!(
                "Dropping flow notification because queue is full, table_id: {}, queue_capacity: {}",
                notification.table_id,
                tx.max_capacity()
            );
            false
        }
        Err(mpsc::error::TrySendError::Closed(notification)) => {
            FLOW_NOTIFICATION_DROPPED
                .with_label_values(&["closed"])
                .inc();
            error!(
                "Dropping flow notification because queue is closed, table_id: {}, queue_capacity: {}",
                notification.table_id,
                tx.max_capacity()
            );
            false
        }
    }
}

pub(in crate::batcher::logical_table) fn enqueue_flow_notifications(
    table_batches: Vec<TableBatch>,
    tx: &Notifier<FlowNotification>,
) {
    for table_batch in table_batches {
        let timestamps = extract_timestamps(&table_batch);
        if timestamps.is_empty() {
            continue;
        }
        try_enqueue_flow_notification(
            tx,
            FlowNotification {
                table_id: table_batch.table_id,
                timestamps,
            },
        );
    }
}

pub(in crate::batcher::logical_table) async fn handle_flow_notification(
    notification: FlowNotification,
    table_flownode_set_cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
) {
    let table_id = notification.table_id;
    let flownodes = match table_flownode_set_cache.get(table_id).await {
        Ok(Some(flownodes)) => flownodes,
        Ok(None) => return,
        Err(e) => {
            error!(e; "Failed to get flownodes for table id: {}", table_id);
            return;
        }
    };
    let peers = flownodes.values().cloned().collect::<HashSet<_>>();

    for peer in peers {
        if let Err(e) = node_manager
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

pub(in crate::batcher::logical_table) fn start_flow_notification_worker(
    notification_rx: mpsc::Receiver<FlowNotification>,
    table_flownode_set_cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
) {
    spawn_global(async move {
        run_notifier(
            notification_rx,
            MAX_CONCURRENT_FLOW_NOTIFICATIONS,
            |notification| {
                let table_flownode_set_cache = table_flownode_set_cache.clone();
                let node_manager = node_manager.clone();
                handle_flow_notification(notification, table_flownode_set_cache, node_manager)
            },
        )
        .await;
    });
}

#[cfg(test)]
pub(in crate::batcher::logical_table) fn notify_flow_dirty_windows_after_flush(
    table_batches: Vec<TableBatch>,
    table_flownode_set_cache: TableFlownodeSetCacheRef,
    node_manager: NodeManagerRef,
) {
    let (tx, rx) = Notifier::try_new(table_batches.len().max(1)).unwrap();
    start_flow_notification_worker(rx, table_flownode_set_cache, node_manager);
    enqueue_flow_notifications(table_batches, &tx);
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use api::v1::meta::Peer;
    use async_trait::async_trait;
    use common_batcher::notifier::Notifier;
    use common_meta::cache::new_table_flownode_set_cache;
    use common_meta::error::Result as MetaResult;
    use common_meta::instruction::{CacheIdent, CreateFlow};
    use common_meta::kv_backend::{KvBackend, TxnService};
    use common_meta::node_manager::NodeManagerRef;
    use common_meta::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
        PutResponse, RangeRequest, RangeResponse,
    };
    use moka::future::CacheBuilder;
    use tokio::sync::{Notify, mpsc, oneshot};

    use crate::batcher::logical_table::batch_convert::TableBatch;
    use crate::batcher::logical_table::flow_notifier::{
        notify_flow_dirty_windows_after_flush, try_enqueue_flow_notification,
    };
    use crate::batcher::logical_table::test_util::{
        FlowNotificationMockNodeManager, RecordingFlownode, mock_aligned_tag_batch,
        mock_table_flownode_cache,
    };
    use crate::metrics::FLOW_NOTIFICATION_DROPPED;

    #[test]
    fn test_flow_notification_queue_drops_when_full() {
        let (tx, mut rx) = Notifier::try_new(1).unwrap();
        let notification =
            |table_id| crate::batcher::logical_table::flow_notifier::FlowNotification {
                table_id,
                timestamps: vec![table_id as i64],
            };
        let dropped = FLOW_NOTIFICATION_DROPPED.with_label_values(&["full"]);
        let dropped_before = dropped.get();

        assert!(try_enqueue_flow_notification(&tx, notification(1)));
        assert!(!try_enqueue_flow_notification(&tx, notification(2)));

        assert_eq!(1, rx.try_recv().unwrap().table_id);
        assert_eq!(dropped_before + 1, dropped.get());

        let closed = FLOW_NOTIFICATION_DROPPED.with_label_values(&["closed"]);
        let closed_before = closed.get();
        drop(rx);
        assert!(!try_enqueue_flow_notification(&tx, notification(3)));
        assert_eq!(closed_before + 1, closed.get());
    }

    struct BlockingRangeKvBackend {
        range_started: Mutex<Option<oneshot::Sender<()>>>,
        range_release: Arc<Notify>,
    }

    impl TxnService for BlockingRangeKvBackend {
        type Error = common_meta::error::Error;
    }

    #[async_trait]
    impl KvBackend for BlockingRangeKvBackend {
        fn name(&self) -> &str {
            "blocking_range"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, _req: RangeRequest) -> MetaResult<RangeResponse> {
            let range_started = self.range_started.lock().unwrap().take();
            if let Some(range_started) = range_started {
                let _ = range_started.send(());
                self.range_release.notified().await;
            }
            Ok(RangeResponse {
                kvs: Vec::new(),
                more: false,
            })
        }

        async fn put(&self, _req: PutRequest) -> MetaResult<PutResponse> {
            unimplemented!()
        }

        async fn batch_put(&self, _req: BatchPutRequest) -> MetaResult<BatchPutResponse> {
            unimplemented!()
        }

        async fn batch_get(&self, _req: BatchGetRequest) -> MetaResult<BatchGetResponse> {
            unimplemented!()
        }

        async fn delete_range(&self, _req: DeleteRangeRequest) -> MetaResult<DeleteRangeResponse> {
            unimplemented!()
        }

        async fn batch_delete(&self, _req: BatchDeleteRequest) -> MetaResult<BatchDeleteResponse> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_flow_notifications_do_not_block_on_previous_table_cache_lookup() {
        let blocked_table_id = 41;
        let cached_table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let (range_started_tx, range_started_rx) = oneshot::channel();
        let range_release = Arc::new(Notify::new());
        let cache = Arc::new(new_table_flownode_set_cache(
            "test".to_string(),
            CacheBuilder::new(2).build(),
            Arc::new(BlockingRangeKvBackend {
                range_started: Mutex::new(Some(range_started_tx)),
                range_release: range_release.clone(),
            }),
        ));
        cache
            .invalidate(&[CacheIdent::CreateFlow(CreateFlow {
                flow_id: 1,
                source_table_ids: vec![cached_table_id],
                partition_to_peer_mapping: vec![(0, peer)],
            })])
            .await
            .unwrap();
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let table_batches = vec![
            TableBatch {
                table_name: "blocked".to_string(),
                table_id: blocked_table_id,
                batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
                row_count: 1,
            },
            TableBatch {
                table_name: "cached".to_string(),
                table_id: cached_table_id,
                batches: vec![mock_aligned_tag_batch("tag1", "host-2", 2000, 2.0)],
                row_count: 1,
            },
        ];

        notify_flow_dirty_windows_after_flush(table_batches, cache, node_manager);

        tokio::time::timeout(Duration::from_secs(1), range_started_rx)
            .await
            .unwrap()
            .unwrap();
        let requests = tokio::time::timeout(Duration::from_secs(1), requests_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached_table_id, requests.requests[0].table_id);
        range_release.notify_one();
    }

    #[tokio::test]
    async fn test_successful_flush_notifies_flownode_with_logical_table_timestamps() {
        let table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let cache = mock_table_flownode_cache(table_id, peer).await;
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let table_batches = vec![TableBatch {
            table_name: "cpu".to_string(),
            table_id,
            batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];

        notify_flow_dirty_windows_after_flush(table_batches, cache, node_manager);

        let requests = tokio::time::timeout(Duration::from_secs(1), requests_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![api::v1::flow::DirtyWindowRequest {
                table_id,
                timestamps: vec![1000],
                time_ranges: Vec::new(),
            }],
            requests.requests
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), requests_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_successful_flush_coalesces_logical_batches_per_flownode() {
        let table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let cache = mock_table_flownode_cache(table_id, peer).await;
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let table_batches = vec![TableBatch {
            table_name: "cpu".to_string(),
            table_id,
            batches: vec![
                mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0),
                mock_aligned_tag_batch("tag1", "host-1", 2000, 2.0),
            ],
            row_count: 2,
        }];

        notify_flow_dirty_windows_after_flush(table_batches, cache, node_manager);

        let requests = tokio::time::timeout(Duration::from_secs(1), requests_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![api::v1::flow::DirtyWindowRequest {
                table_id,
                timestamps: vec![1000, 2000],
                time_ranges: Vec::new(),
            }],
            requests.requests
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), requests_rx.recv())
                .await
                .is_err()
        );
    }
}
