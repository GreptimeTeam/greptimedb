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
use std::time::Instant;

use catalog::CatalogManagerRef;
use common_batcher::flush_limiter::FlushLimiter;
use common_meta::node_manager::NodeManagerRef;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::{debug, warn};
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::OptionExt;

use crate::batcher::flow_notifier::FlowNotifier;
use crate::batcher::logical_table::PHYSICAL_TABLE_KEY;
use crate::batcher::logical_table::batch_convert::{
    TableBatch, concat_modified_batches, transform_logical_batches_to_physical,
};
use crate::batcher::logical_table::flow_notifier::enqueue_flow_notifications;
use crate::batcher::logical_table::pending_worker::FlushWaiter;
use crate::batcher::logical_table::region_write::{
    CatalogManagerPhysicalFlushAdapter, NodeManagerPhysicalFlushAdapter,
    PartitionManagerPhysicalFlushAdapter, PhysicalFlushCatalogProvider, PhysicalFlushNodeRequester,
    PhysicalFlushPartitionProvider, encode_region_write_requests, flush_region_writes_concurrently,
    plan_region_batches, resolve_region_targets,
};
use crate::error;
use crate::error::Result;
use crate::metrics::{
    FLUSH_DROPPED_ROWS, FLUSH_ELAPSED, FLUSH_FAILURES, FLUSH_ROWS, FLUSH_TOTAL,
    PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED,
};

pub(in crate::batcher::logical_table) struct Batch {
    pub(in crate::batcher::logical_table) table_batches: Vec<TableBatch>,
    pub(in crate::batcher::logical_table) total_row_count: usize,
    pub(in crate::batcher::logical_table) db_string: String,
    pub(in crate::batcher::logical_table) ctx: QueryContextRef,
    pub(in crate::batcher::logical_table) waiters: Vec<FlushWaiter>,
}

pub(in crate::batcher::logical_table) async fn spawn_flush(
    flush: Batch,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flow_notification_tx: FlowNotifier,
    flush_limiter: FlushLimiter,
) {
    match flush_limiter.acquire().await {
        Ok(permit) => {
            tokio::spawn(async move {
                let _permit = permit;
                flush_batch_with_managers(
                    flush,
                    partition_manager,
                    node_manager,
                    catalog_manager,
                    flow_notification_tx,
                )
                .await;
            });
        }
        Err(err) => {
            warn!(err; "Flush semaphore closed, flushing inline");
            flush_batch_with_managers(
                flush,
                partition_manager,
                node_manager,
                catalog_manager,
                flow_notification_tx,
            )
            .await;
        }
    }
}

pub(in crate::batcher::logical_table) async fn flush_batch_with_managers(
    flush: Batch,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flow_notification_tx: FlowNotifier,
) {
    let partition_provider = PartitionManagerPhysicalFlushAdapter { partition_manager };
    let node_requester = NodeManagerPhysicalFlushAdapter {
        node_manager: node_manager.clone(),
    };
    let catalog_provider = CatalogManagerPhysicalFlushAdapter { catalog_manager };
    flush_batch(
        flush,
        &partition_provider,
        &node_requester,
        &catalog_provider,
        flow_notification_tx,
    )
    .await;
}

pub(in crate::batcher::logical_table) async fn flush_batch(
    flush: Batch,
    partition_manager: &(impl PhysicalFlushPartitionProvider + ?Sized),
    node_manager: &(impl PhysicalFlushNodeRequester + ?Sized),
    catalog_manager: &(impl PhysicalFlushCatalogProvider + ?Sized),
    flow_notification_tx: FlowNotifier,
) {
    let Batch {
        table_batches,
        total_row_count,
        db_string,
        ctx,
        waiters,
    } = flush;
    let start = Instant::now();

    // Physical-table-level flush: transform all logical table batches
    // into physical format and write them together.
    let physical_table_name = ctx
        .extension(PHYSICAL_TABLE_KEY)
        .unwrap_or(GREPTIME_PHYSICAL_TABLE)
        .to_string();
    let result = flush_batch_physical(
        &table_batches,
        &physical_table_name,
        &ctx,
        partition_manager,
        node_manager,
        catalog_manager,
    )
    .await;

    let elapsed = start.elapsed().as_secs_f64();
    FLUSH_ELAPSED.observe(elapsed);

    debug!(
        "Pending rows batch flushed, total rows: {}, elapsed time: {}s",
        total_row_count, elapsed
    );

    match result {
        Ok(affected_rows) => {
            FLUSH_TOTAL.inc();
            FLUSH_ROWS.observe(total_row_count as f64);
            operator::metrics::DIST_INGEST_ROW_COUNT
                .with_label_values(&[db_string.as_str()])
                .inc_by(affected_rows as u64);

            notify_waiters(waiters, Ok(()));
            enqueue_flow_notifications(table_batches, &flow_notification_tx);
        }
        Err(err) => {
            FLUSH_FAILURES.inc();
            FLUSH_DROPPED_ROWS.inc_by(total_row_count as u64);
            notify_waiters(waiters, Err(err));
        }
    }
}

/// Flushes a batch of logical table rows by transforming them into the physical table format
/// and writing them to the appropriate datanode regions.
///
/// This function performs the end-to-end physical flush pipeline:
/// 1. Resolves the physical table metadata and column ID mapping.
/// 2. Fetches the physical table's partition rule.
/// 3. Transforms each logical table batch into the physical (sparse primary key) format.
/// 4. Concatenates all transformed batches into a single combined batch.
/// 5. Splits the combined batch by partition rule and sends region write requests
///    concurrently to the target datanodes.
pub async fn flush_batch_physical(
    table_batches: &[TableBatch],
    physical_table_name: &str,
    ctx: &QueryContextRef,
    partition_manager: &(impl PhysicalFlushPartitionProvider + ?Sized),
    node_manager: &(impl PhysicalFlushNodeRequester + ?Sized),
    catalog_manager: &(impl PhysicalFlushCatalogProvider + ?Sized),
) -> Result<usize> {
    // 1. Resolve the physical table and get column ID mapping
    let physical_table = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_resolve_table"])
            .start_timer();
        catalog_manager
            .physical_table(
                ctx.current_catalog(),
                &ctx.current_schema(),
                physical_table_name,
                ctx.as_ref(),
            )
            .await?
            .with_context(|| error::InternalSnafu {
                err_msg: format!(
                    "Physical table '{}' not found during pending flush",
                    physical_table_name
                ),
            })?
    };

    let physical_table_info = physical_table.table_info;
    let name_to_ids = physical_table
        .col_name_to_ids
        .with_context(|| error::InternalSnafu {
            err_msg: format!(
                "Physical table '{}' has no column IDs for pending flush",
                physical_table_name
            ),
        })?;

    // 2. Get the physical table's partition rule (one lookup instead of N)
    let partition_rule = {
        let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
            .with_label_values(&["flush_physical_fetch_partition_rule"])
            .start_timer();
        partition_manager
            .find_table_partition_rule(physical_table_info.as_ref())
            .await?
    };
    let partition_columns = partition_rule.partition_columns();
    let partition_columns_set: HashSet<&str> =
        partition_columns.iter().map(String::as_str).collect();

    // 3. Transform each logical table batch into physical format
    let modified_batches =
        transform_logical_batches_to_physical(table_batches, &name_to_ids, &partition_columns_set)?;

    // 4. Concatenate all modified batches (all share the same physical schema)
    let combined_batch = concat_modified_batches(&modified_batches)?;

    // 5. Split by physical partition rule and send to regions
    let physical_table_id = physical_table_info.table_id();
    let planned_batches = plan_region_batches(
        combined_batch,
        physical_table_id,
        partition_rule.as_ref(),
        partition_columns,
    )?;

    let resolved_batches = resolve_region_targets(planned_batches, partition_manager).await?;
    let region_writes = encode_region_write_requests(resolved_batches)?;
    flush_region_writes_concurrently(node_manager, region_writes).await
}

pub(in crate::batcher::logical_table) fn notify_waiters(
    waiters: Vec<FlushWaiter>,
    result: Result<()>,
) {
    let shared_result = result.map_err(Arc::new);
    for waiter in waiters {
        let _ = waiter.response_tx.send(match &shared_result {
            Ok(()) => Ok(()),
            Err(error) => Err(Arc::clone(error)),
        });
        // waiter._permit is dropped here, releasing the inflight semaphore slot
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use api::region::RegionResponse;
    use api::v1::meta::Peer;
    use api::v1::region::RegionRequest;
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use catalog::error::Result as CatalogResult;
    use common_batcher::request_limiter::RequestLimiter;
    use common_meta::cache::TableFlownodeSetCacheRef;
    use common_meta::node_manager::NodeManagerRef;
    use datatypes::schema::{ColumnSchema as DtColumnSchema, Schema as DtSchema};
    use partition::error::Result as PartitionResult;
    use partition::partition::{PartitionRule, PartitionRuleRef, RegionMask};
    use store_api::storage::RegionId;
    use table::metadata::TableId;
    use table::test_util::table_info::test_table_info;
    use tokio::sync::{mpsc, oneshot};

    use crate::batcher::flow_notifier::{FlowNotifier, start_flow_notification_worker};
    use crate::batcher::logical_table::batch::{
        Batch, flush_batch, flush_batch_physical, notify_waiters,
    };
    use crate::batcher::logical_table::batch_convert::TableBatch;
    use crate::batcher::logical_table::pending_worker::FlushWaiter;
    use crate::batcher::logical_table::region_write::{
        PhysicalFlushCatalogProvider, PhysicalFlushNodeRequester, PhysicalFlushPartitionProvider,
        PhysicalTableMetadata,
    };
    use crate::batcher::logical_table::test_util::{
        FlowNotificationMockNodeManager, RecordingFlownode, mock_aligned_tag_batch,
    };
    use crate::batcher::test_util::mock_table_flownode_cache;
    use crate::error;
    use crate::error::Error;
    use crate::metrics::FLOW_NOTIFICATION_DROPPED;

    #[tokio::test]
    async fn test_flush_batch_notifies_flownode_after_successful_physical_write() {
        let table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let cache = mock_table_flownode_cache(table_id, vec![(0, peer.clone()), (1, peer)]).await;
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let flow_node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let flow_notification_tx = mock_flow_notification_sender(cache, flow_node_manager.clone());
        let ctx = session::context::QueryContext::arc();
        let table_batches = vec![TableBatch {
            table_name: "cpu".to_string(),
            table_id,
            batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];
        let writes = Arc::new(AtomicUsize::new(0));

        flush_batch(
            Batch {
                table_batches,
                total_row_count: 1,
                db_string: ctx.get_db_string(),
                ctx,
                waiters: Vec::new(),
            },
            &MockFlushPartitionProvider {
                partition_rule_calls: Arc::new(AtomicUsize::new(0)),
                region_leader_calls: Arc::new(AtomicUsize::new(0)),
            },
            &MockFlushNodeRequester {
                writes: writes.clone(),
                fail: false,
            },
            &MockFlushCatalogProvider {
                table: Some(mock_physical_table_metadata(1024)),
            },
            flow_notification_tx,
        )
        .await;

        assert_eq!(1, writes.load(Ordering::SeqCst));
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
    }

    #[tokio::test]
    async fn test_flush_batch_does_not_notify_flownode_after_physical_write_error() {
        let table_id = 42;
        let peer = Peer {
            id: 7,
            addr: "flow-7".to_string(),
        };
        let cache = mock_table_flownode_cache(table_id, vec![(0, peer.clone()), (1, peer)]).await;
        let (requests_tx, mut requests_rx) = mpsc::unbounded_channel();
        let _requests_tx = requests_tx.clone();
        let flow_node_manager: NodeManagerRef = Arc::new(FlowNotificationMockNodeManager {
            flownode: Arc::new(RecordingFlownode { requests_tx }),
        });
        let flow_notification_tx = mock_flow_notification_sender(cache, flow_node_manager.clone());
        let ctx = session::context::QueryContext::arc();
        let table_batches = vec![TableBatch {
            table_name: "cpu".to_string(),
            table_id,
            batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];
        let writes = Arc::new(AtomicUsize::new(0));

        flush_batch(
            Batch {
                table_batches,
                total_row_count: 1,
                db_string: ctx.get_db_string(),
                ctx,
                waiters: Vec::new(),
            },
            &MockFlushPartitionProvider {
                partition_rule_calls: Arc::new(AtomicUsize::new(0)),
                region_leader_calls: Arc::new(AtomicUsize::new(0)),
            },
            &MockFlushNodeRequester {
                writes: writes.clone(),
                fail: true,
            },
            &MockFlushCatalogProvider {
                table: Some(mock_physical_table_metadata(1024)),
            },
            flow_notification_tx,
        )
        .await;

        assert_eq!(1, writes.load(Ordering::SeqCst));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), requests_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_cancelled_waiter_retains_request_slot_until_notification() {
        let limiter = RequestLimiter::try_new(1).unwrap();
        let (response_tx, response_rx) = oneshot::channel();
        let waiter = FlushWaiter {
            response_tx,
            _permit: limiter.acquire().await.unwrap(),
        };
        drop(response_rx);
        let next = limiter.acquire();
        tokio::pin!(next);
        assert!(poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx).is_pending())).await);
        notify_waiters(vec![waiter], Ok(()));
        let _permit = next.await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_batch_physical_uses_mockable_trait_dependencies() {
        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 11,
            batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];
        let partition_calls = Arc::new(AtomicUsize::new(0));
        let leader_calls = Arc::new(AtomicUsize::new(0));
        let node = MockFlushNodeRequester::default();
        let ctx = session::context::QueryContext::arc();

        flush_batch_physical(
            &table_batches,
            "phy",
            &ctx,
            &MockFlushPartitionProvider {
                partition_rule_calls: partition_calls.clone(),
                region_leader_calls: leader_calls.clone(),
            },
            &node,
            &MockFlushCatalogProvider {
                table: Some(mock_physical_table_metadata(1024)),
            },
        )
        .await
        .unwrap();

        assert_eq!(1, partition_calls.load(Ordering::SeqCst));
        assert_eq!(1, leader_calls.load(Ordering::SeqCst));
        assert_eq!(1, node.writes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_flush_batch_physical_returns_actual_affected_rows() {
        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 11,
            batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];
        let ctx = session::context::QueryContext::arc();

        let affected_rows = flush_batch_physical(
            &table_batches,
            "phy",
            &ctx,
            &MockFlushPartitionProvider {
                partition_rule_calls: Arc::new(AtomicUsize::new(0)),
                region_leader_calls: Arc::new(AtomicUsize::new(0)),
            },
            &AffectedRowsFlushNodeRequester { affected_rows: 7 },
            &MockFlushCatalogProvider {
                table: Some(mock_physical_table_metadata(1024)),
            },
        )
        .await
        .unwrap();

        assert_eq!(7, affected_rows);
    }

    #[tokio::test]
    async fn test_flush_batch_physical_stops_before_partition_and_node_when_table_missing() {
        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 11,
            batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
            row_count: 1,
        }];
        let partition_calls = Arc::new(AtomicUsize::new(0));
        let leader_calls = Arc::new(AtomicUsize::new(0));
        let node = MockFlushNodeRequester::default();
        let ctx = session::context::QueryContext::arc();

        let err = flush_batch_physical(
            &table_batches,
            "missing_phy",
            &ctx,
            &MockFlushPartitionProvider {
                partition_rule_calls: partition_calls.clone(),
                region_leader_calls: leader_calls.clone(),
            },
            &node,
            &MockFlushCatalogProvider { table: None },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Physical table 'missing_phy' not found")
        );
        assert_eq!(0, partition_calls.load(Ordering::SeqCst));
        assert_eq!(0, leader_calls.load(Ordering::SeqCst));
        assert_eq!(0, node.writes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_flush_batch_physical_aborts_immediately_on_transform_error() {
        let table_batches = vec![
            TableBatch {
                table_name: "broken".to_string(),
                table_id: 11,
                batches: vec![mock_aligned_tag_batch("unknown_tag", "host-1", 1000, 1.0)],
                row_count: 1,
            },
            TableBatch {
                table_name: "healthy".to_string(),
                table_id: 12,
                batches: vec![mock_aligned_tag_batch("tag1", "host-2", 2000, 2.0)],
                row_count: 1,
            },
        ];
        let partition_calls = Arc::new(AtomicUsize::new(0));
        let leader_calls = Arc::new(AtomicUsize::new(0));
        let node = MockFlushNodeRequester::default();
        let ctx = session::context::QueryContext::arc();

        let err = flush_batch_physical(
            &table_batches,
            "phy",
            &ctx,
            &MockFlushPartitionProvider {
                partition_rule_calls: partition_calls.clone(),
                region_leader_calls: leader_calls.clone(),
            },
            &node,
            &MockFlushCatalogProvider {
                table: Some(mock_physical_table_metadata(1024)),
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("unknown_tag"));
        assert_eq!(1, partition_calls.load(Ordering::SeqCst));
        assert_eq!(0, leader_calls.load(Ordering::SeqCst));
        assert_eq!(0, node.writes.load(Ordering::SeqCst));
    }

    fn mock_physical_table_metadata(table_id: TableId) -> PhysicalTableMetadata {
        let schema = Arc::new(
            DtSchema::try_new(vec![
                DtColumnSchema::new(
                    "__primary_key",
                    datatypes::prelude::ConcreteDataType::binary_datatype(),
                    false,
                ),
                DtColumnSchema::new(
                    "greptime_timestamp",
                    datatypes::prelude::ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                DtColumnSchema::new(
                    "greptime_value",
                    datatypes::prelude::ConcreteDataType::float64_datatype(),
                    true,
                ),
                DtColumnSchema::new(
                    "tag1",
                    datatypes::prelude::ConcreteDataType::string_datatype(),
                    true,
                ),
            ])
            .unwrap(),
        );
        let mut table_info = test_table_info(table_id, "phy", "public", "greptime", schema);
        table_info.meta.column_ids = vec![0, 1, 2, 3];

        PhysicalTableMetadata {
            table_info: Arc::new(table_info),
            col_name_to_ids: Some(HashMap::from([("tag1".to_string(), 3)])),
        }
    }

    struct MockFlushCatalogProvider {
        table: Option<PhysicalTableMetadata>,
    }

    #[async_trait]
    impl PhysicalFlushCatalogProvider for MockFlushCatalogProvider {
        async fn physical_table(
            &self,
            _catalog: &str,
            _schema: &str,
            _table_name: &str,
            _query_ctx: &session::context::QueryContext,
        ) -> CatalogResult<Option<PhysicalTableMetadata>> {
            Ok(self.table.clone())
        }
    }

    struct SingleRegionPartitionRule;

    impl PartitionRule for SingleRegionPartitionRule {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn partition_columns(&self) -> &[String] {
            &[]
        }

        fn find_region(
            &self,
            _values: &[datatypes::prelude::Value],
        ) -> partition::error::Result<store_api::storage::RegionNumber> {
            unimplemented!()
        }

        fn split_record_batch(
            &self,
            record_batch: &RecordBatch,
        ) -> partition::error::Result<HashMap<store_api::storage::RegionNumber, RegionMask>>
        {
            Ok(HashMap::from([(
                1,
                RegionMask::new(
                    arrow::array::BooleanArray::from(vec![true; record_batch.num_rows()]),
                    record_batch.num_rows(),
                ),
            )]))
        }
    }

    struct MockFlushPartitionProvider {
        partition_rule_calls: Arc<AtomicUsize>,
        region_leader_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PhysicalFlushPartitionProvider for MockFlushPartitionProvider {
        async fn find_table_partition_rule(
            &self,
            _table_info: &table::metadata::TableInfo,
        ) -> PartitionResult<PartitionRuleRef> {
            self.partition_rule_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(SingleRegionPartitionRule))
        }

        async fn find_region_leader(&self, _region_id: RegionId) -> error::Result<Peer> {
            self.region_leader_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Peer {
                id: 1,
                addr: "node-1".to_string(),
            })
        }
    }

    #[derive(Default)]
    struct MockFlushNodeRequester {
        writes: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait]
    impl PhysicalFlushNodeRequester for MockFlushNodeRequester {
        async fn handle(
            &self,
            _peer: &Peer,
            _request: RegionRequest,
        ) -> error::Result<RegionResponse> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(Error::Internal {
                    err_msg: "physical write failed".to_string(),
                });
            }
            Ok(RegionResponse::new(0))
        }
    }

    fn mock_flow_notification_sender(
        cache: TableFlownodeSetCacheRef,
        node_manager: NodeManagerRef,
    ) -> FlowNotifier {
        let (tx, rx) = FlowNotifier::try_new(
            NonZeroUsize::new(16).unwrap(),
            FLOW_NOTIFICATION_DROPPED.clone(),
        )
        .unwrap();
        start_flow_notification_worker(rx, cache, node_manager);
        tx
    }

    #[derive(Default)]
    struct AffectedRowsFlushNodeRequester {
        affected_rows: usize,
    }

    #[async_trait]
    impl PhysicalFlushNodeRequester for AffectedRowsFlushNodeRequester {
        async fn handle(
            &self,
            _peer: &Peer,
            _request: RegionRequest,
        ) -> error::Result<RegionResponse> {
            Ok(RegionResponse::new(self.affected_rows))
        }
    }
}
