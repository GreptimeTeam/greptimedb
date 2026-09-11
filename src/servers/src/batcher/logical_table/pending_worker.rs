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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use catalog::CatalogManagerRef;
use common_batcher::flush_limiter::FlushLimiter;
use common_batcher::flush_policy::FlushTrigger;
use common_batcher::flush_policy::timing::TimingFlushPolicy;
use common_batcher::notifier::Notifier;
use common_batcher::pending_worker::PendingWorker as PendingCore;
use common_batcher::worker_registry::WorkerRegistry;
use common_meta::node_manager::NodeManagerRef;
use common_telemetry::debug;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use table::metadata::TableId;
use tokio::sync::{OwnedSemaphorePermit, broadcast, mpsc, oneshot};

use crate::batcher::logical_table::batch::{Batch, flush_batch_with_managers, spawn_flush};
use crate::batcher::logical_table::batch_convert::{RecordBatchWithTsIdx, TableBatch};
use crate::batcher::logical_table::{BatchKey, FlowNotification};
use crate::error::Error;
use crate::metrics::{PENDING_BATCHES, PENDING_ROWS, PENDING_WORKERS};

pub(in crate::batcher::logical_table) struct PendingBatch {
    pub(in crate::batcher::logical_table) tables: HashMap<TableId, TableBatch>,
    pub(in crate::batcher::logical_table) db_string: String,
    pub(in crate::batcher::logical_table) ctx: QueryContextRef,
}

pub(in crate::batcher::logical_table) struct FlushWaiter {
    pub(in crate::batcher::logical_table) response_tx:
        oneshot::Sender<std::result::Result<(), Arc<Error>>>,
    pub(in crate::batcher::logical_table) _permit: Arc<OwnedSemaphorePermit>,
}

#[derive(Clone)]
pub(in crate::batcher::logical_table) struct PendingWorker {
    pub(in crate::batcher::logical_table) tx: mpsc::Sender<WorkerCommand>,
}

pub(in crate::batcher::logical_table) enum WorkerCommand {
    Submit {
        table_batches: Vec<(String, u32, RecordBatchWithTsIdx)>,
        total_rows: usize,
        ctx: QueryContextRef,
        response_tx: oneshot::Sender<std::result::Result<(), Arc<Error>>>,
        _permit: Arc<OwnedSemaphorePermit>,
    },
    #[cfg(test)]
    Ack { ack_tx: oneshot::Sender<()> },
}

impl PendingBatch {
    pub(in crate::batcher::logical_table) fn new(ctx: QueryContextRef) -> Self {
        let db_string = ctx.get_db_string();
        Self {
            tables: HashMap::new(),
            db_string,
            ctx,
        }
    }

    pub(in crate::batcher::logical_table) fn add_table_batch(
        &mut self,
        table_name: String,
        table_id: TableId,
        record_batch: RecordBatchWithTsIdx,
    ) {
        let entry = self.tables.entry(table_id).or_insert_with(|| TableBatch {
            table_name,
            table_id,
            batches: Vec::new(),
            row_count: 0,
        });
        entry.row_count += record_batch.batch.num_rows();
        entry.batches.push(record_batch);
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::batcher::logical_table) fn start_worker(
    key: BatchKey,
    worker_tx: mpsc::Sender<WorkerCommand>,
    workers: Arc<WorkerRegistry<BatchKey, WorkerCommand>>,
    mut rx: mpsc::Receiver<WorkerCommand>,
    shutdown: broadcast::Sender<()>,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flow_notification_tx: Notifier<FlowNotification>,
    worker_idle_timeout: Duration,
    flush_policy: TimingFlushPolicy,
    flush_limiter: FlushLimiter,
) {
    tokio::spawn(async move {
        // The business batch and pending flush are populated and drained together.
        let mut batch = None;
        let mut pending_flush = PendingCore::new(flush_policy);
        let mut shutdown_rx = shutdown.subscribe();
        let idle_deadline = tokio::time::Instant::now() + worker_idle_timeout;
        let idle_timer = tokio::time::sleep_until(idle_deadline);
        tokio::pin!(idle_timer);

        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    match cmd {
                        Some(WorkerCommand::Submit { table_batches, total_rows, ctx, response_tx, _permit }) => {
                            let submitted_at = tokio::time::Instant::now();
                            idle_timer.as_mut().reset(submitted_at + worker_idle_timeout);

                            pending_flush.submit(
                                FlushWaiter { response_tx, _permit },
                                total_rows,
                            );
                            let pending_batch = batch.get_or_insert_with(||{
                                PENDING_BATCHES.inc();
                                PendingBatch::new(ctx)
                            });

                            for (table_name, table_id, record_batch) in table_batches {
                                pending_batch.add_table_batch(table_name, table_id, record_batch);
                            }

                            PENDING_ROWS.add(total_rows as i64);

                            if let Some(flush) = drain_batch(&mut batch, &mut pending_flush, Some(FlushTrigger::Submission)) {
                                spawn_flush(
                                    flush,
                                    partition_manager.clone(),
                                    node_manager.clone(),
                                    catalog_manager.clone(),
                                    flow_notification_tx.clone(),
                                    flush_limiter.clone(),
                                ).await;
                            }
                        }
                        None => {
                            if let Some(flush) = drain_batch(&mut batch, &mut pending_flush, None) {
                                flush_batch_with_managers(
                                    flush,
                                    partition_manager.clone(),
                                    node_manager.clone(),
                                    catalog_manager.clone(),
                                    flow_notification_tx.clone(),
                                ).await;
                            }
                            break;
                        }
                        #[cfg(test)]
                        Some(WorkerCommand::Ack { ack_tx }) => {
                            let _ = ack_tx.send(());
                        }
                    }
                }
                _ = &mut idle_timer => {
                    if !should_close_worker_on_idle_timeout(
                        pending_flush.total_rows(),
                        rx.len(),
                    ) {
                        idle_timer
                            .as_mut()
                            .reset(tokio::time::Instant::now() + worker_idle_timeout);
                        continue;
                    }

                    debug!(
                        "Closing idle pending rows worker due to timeout: catalog={}, schema={}, physical_table={}",
                        key.catalog,
                        key.schema,
                        key.physical_table
                    );
                    break;
                }
                _ = pending_flush.wait_flush() => {
                    if let Some(flush) = drain_batch(&mut batch, &mut pending_flush, Some(FlushTrigger::Deadline)) {
                        spawn_flush(
                            flush,
                            partition_manager.clone(),
                            node_manager.clone(),
                            catalog_manager.clone(),
                            flow_notification_tx.clone(),
                            flush_limiter.clone(),
                        ).await;
                    }
                }
                _ = shutdown_rx.recv() => {
                    if let Some(flush) = drain_batch(&mut batch, &mut pending_flush, None) {
                        flush_batch_with_managers(
                            flush,
                            partition_manager.clone(),
                            node_manager.clone(),
                            catalog_manager.clone(),
                            flow_notification_tx.clone(),
                        ).await;
                    }
                    break;
                }
            }
        }

        remove_worker_if_same_channel(workers.as_ref(), &key, &worker_tx).await;
    });
}

pub(in crate::batcher::logical_table) async fn remove_worker_if_same_channel(
    workers: &WorkerRegistry<BatchKey, WorkerCommand>,
    key: &BatchKey,
    worker_tx: &mpsc::Sender<WorkerCommand>,
) -> bool {
    if workers.remove_if_same(key, worker_tx).await {
        PENDING_WORKERS.set(workers.len().await as i64);
        true
    } else {
        false
    }
}

pub(in crate::batcher::logical_table) fn should_close_worker_on_idle_timeout(
    total_row_count: usize,
    queued_requests: usize,
) -> bool {
    total_row_count == 0 && queued_requests == 0
}

/// Transfers the ready batch, or drains unconditionally when no trigger is given.
/// Execution and flush permits remain owned by the caller.
pub(in crate::batcher::logical_table) fn drain_batch(
    batch: &mut Option<PendingBatch>,
    pending_flush: &mut PendingCore<FlushWaiter, TimingFlushPolicy>,
    trigger: Option<FlushTrigger>,
) -> Option<Batch> {
    let total_row_count = pending_flush.total_rows();
    let waiters = match trigger {
        Some(trigger) => pending_flush.take_ready(trigger)?,
        None => pending_flush.take_pending()?,
    };
    let batch = batch.take()?;

    if total_row_count == 0 {
        return None;
    }

    let table_batches = batch.tables.into_values().collect();

    PENDING_ROWS.sub(total_row_count as i64);
    PENDING_BATCHES.dec();

    Some(Batch {
        table_batches,
        total_row_count,
        db_string: batch.db_string,
        ctx: batch.ctx,
        waiters,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use catalog::memory::MemoryCatalogManager;
    use common_batcher::flush_limiter::FlushLimiter;
    use common_batcher::flush_policy::FlushTrigger;
    use common_batcher::flush_policy::timing::TimingFlushPolicy;
    use common_batcher::notifier::Notifier;
    use common_batcher::pending_worker::PendingWorker as PendingCore;
    use common_batcher::worker_registry::WorkerRegistry;
    use common_meta::cache::new_table_route_cache;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::node_manager::NodeManagerRef;
    use moka::future::CacheBuilder;
    use partition::cache::new_partition_info_cache;
    use partition::manager::PartitionRuleManager;
    use tokio::sync::{Semaphore, broadcast, mpsc, oneshot};
    use tokio::time::advance;

    use crate::batcher::logical_table::BatchKey;
    use crate::batcher::logical_table::batch_convert::TableBatch;
    use crate::batcher::logical_table::pending_worker::{
        FlushWaiter, PendingBatch, WorkerCommand, drain_batch, remove_worker_if_same_channel,
        should_close_worker_on_idle_timeout, start_worker,
    };
    use crate::batcher::logical_table::test_util::{
        ConcurrentMockNodeManager, mock_aligned_tag_batch,
    };
    use crate::error::Error;

    #[tokio::test]
    async fn test_drain_batch_takes_initialized_pending_batch_from_option() {
        let ctx = session::context::QueryContext::arc();
        let (response_tx, _response_rx) = oneshot::channel();
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let mut pending_flush =
            PendingCore::new(TimingFlushPolicy::try_new(Duration::from_secs(10), 1).unwrap());
        pending_flush.submit(
            FlushWaiter {
                response_tx,
                _permit: Arc::new(permit),
            },
            1,
        );
        let mut batch = Some(PendingBatch {
            tables: HashMap::from([(
                42,
                TableBatch {
                    table_name: "cpu".to_string(),
                    table_id: 42,
                    batches: vec![mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0)],
                    row_count: 1,
                },
            )]),
            db_string: ctx.get_db_string(),
            ctx: ctx.clone(),
        });

        let flush = drain_batch(
            &mut batch,
            &mut pending_flush,
            Some(FlushTrigger::Submission),
        )
        .unwrap();

        assert!(batch.is_none());
        assert!(pending_flush.is_empty());
        assert_eq!(0, pending_flush.total_rows());
        assert_eq!(1, flush.waiters.len());
        assert_eq!(1, flush.total_row_count);
        assert_eq!(1, flush.table_batches.len());
        assert_eq!(ctx.get_db_string(), flush.db_string);
        assert_eq!(ctx.current_catalog(), flush.ctx.current_catalog());
    }

    #[tokio::test]
    async fn test_drain_batch_preserves_unready_state_and_clears_zero_rows() {
        for total_rows in [0, 1] {
            let mut pending_flush =
                PendingCore::new(TimingFlushPolicy::try_new(Duration::from_secs(10), 2).unwrap());
            let mut batch = Some(PendingBatch::new(session::context::QueryContext::arc()));
            let semaphore = Arc::new(Semaphore::new(1));
            let (response_tx, mut response_rx) = oneshot::channel();
            pending_flush.submit(
                FlushWaiter {
                    response_tx,
                    _permit: Arc::new(semaphore.clone().acquire_owned().await.unwrap()),
                },
                total_rows,
            );
            assert!(
                drain_batch(
                    &mut batch,
                    &mut pending_flush,
                    Some(FlushTrigger::Submission)
                )
                .is_none()
            );
            assert!(batch.is_some());
            assert!(!pending_flush.is_empty());
            assert_eq!(total_rows, pending_flush.total_rows());
            assert_eq!(0, semaphore.available_permits());

            let drained = drain_batch(&mut batch, &mut pending_flush, None);
            assert!(batch.is_none());
            assert!(pending_flush.is_empty());
            assert_eq!(0, pending_flush.total_rows());
            if total_rows == 0 {
                assert!(drained.is_none());
                assert_eq!(1, semaphore.available_permits());
                assert!(matches!(
                    response_rx.try_recv(),
                    Err(oneshot::error::TryRecvError::Closed)
                ));
            } else {
                let drained = drained.unwrap();
                assert_eq!(1, drained.total_row_count);
                assert_eq!(1, drained.waiters.len());
                assert_eq!(0, semaphore.available_permits());
                drop(drained);
                assert_eq!(1, semaphore.available_permits());
            }
        }
    }

    #[test]
    fn test_pending_batch_keeps_same_name_batches_with_distinct_table_ids() {
        let ctx = session::context::QueryContext::arc();
        let mut pending_batch = PendingBatch::new(ctx);

        pending_batch.add_table_batch(
            "cpu".to_string(),
            42,
            mock_aligned_tag_batch("tag1", "host-1", 1000, 1.0),
        );
        pending_batch.add_table_batch(
            "cpu".to_string(),
            43,
            mock_aligned_tag_batch("tag1", "host-1", 2000, 2.0),
        );

        assert_eq!(2, pending_batch.tables.len());
        assert_eq!(42, pending_batch.tables[&42].table_id);
        assert_eq!(43, pending_batch.tables[&43].table_id);
        assert_eq!("cpu", pending_batch.tables[&42].table_name);
        assert_eq!("cpu", pending_batch.tables[&43].table_name);
    }

    #[tokio::test]
    async fn test_remove_worker_if_same_channel_removes_matching_entry() {
        let workers = WorkerRegistry::new();
        let key = BatchKey {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            physical_table: "phy".to_string(),
        };

        let (tx, _rx) = mpsc::channel::<WorkerCommand>(1);
        workers.get_or_insert_with(key.clone(), || tx.clone()).await;

        assert!(remove_worker_if_same_channel(&workers, &key, &tx).await);
        assert!(workers.is_empty().await);
    }

    #[tokio::test]
    async fn test_remove_worker_if_same_channel_keeps_newer_entry() {
        let workers = WorkerRegistry::new();
        let key = BatchKey {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            physical_table: "phy".to_string(),
        };

        let (stale_tx, _stale_rx) = mpsc::channel::<WorkerCommand>(1);
        let (fresh_tx, _fresh_rx) = mpsc::channel::<WorkerCommand>(1);
        workers
            .get_or_insert_with(key.clone(), || fresh_tx.clone())
            .await;

        assert!(!remove_worker_if_same_channel(&workers, &key, &stale_tx).await);
        assert!(workers.get(&key).await.is_some());
        assert!(workers.get(&key).await.unwrap().same_channel(&fresh_tx));
    }

    #[test]
    fn test_worker_idle_timeout_close_decision() {
        assert!(should_close_worker_on_idle_timeout(0, 0));
        assert!(!should_close_worker_on_idle_timeout(1, 0));
        assert!(!should_close_worker_on_idle_timeout(0, 1));
    }

    const WORKER_TEST_TIMEOUT: Duration = Duration::from_secs(30);

    async fn submit_mock_worker_batch(
        worker_tx: &mpsc::Sender<WorkerCommand>,
        total_rows: usize,
        timestamp: i64,
    ) -> oneshot::Receiver<std::result::Result<(), Arc<Error>>> {
        let (response_tx, response_rx) = oneshot::channel();
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        worker_tx
            .send(WorkerCommand::Submit {
                table_batches: vec![(
                    "cpu".to_string(),
                    42,
                    mock_aligned_tag_batch("tag1", "host-1", timestamp, 1.0),
                )],
                total_rows,
                ctx: session::context::QueryContext::arc(),
                response_tx,
                _permit: Arc::new(permit),
            })
            .await
            .unwrap();

        // The channel is FIFO, so the ack proves the worker has dequeued and
        // processed the submission (anchoring the flush deadline) before the
        // caller advances virtual time.
        let (ack_tx, ack_rx) = oneshot::channel();
        worker_tx.send(WorkerCommand::Ack { ack_tx }).await.unwrap();
        ack_rx
            .await
            .expect("worker exited before acking the submitted batch");

        response_rx
    }

    async fn receive_mock_flush_result(
        response_rx: oneshot::Receiver<std::result::Result<(), Arc<Error>>>,
        context: &str,
    ) -> std::result::Result<(), Arc<Error>> {
        // Under paused time the timeout auto-advances the clock and fires
        // deterministically if the flush never completes.
        tokio::time::timeout(WORKER_TEST_TIMEOUT, response_rx)
            .await
            .unwrap_or_else(|_| panic!("{context}"))
            .expect("flush result channel closed without a result")
    }

    fn assert_missing_physical_table(result: std::result::Result<(), Arc<Error>>) {
        let err = result.expect_err("the empty catalog should make the flush fail");
        assert!(
            matches!(
                err.as_ref(),
                Error::Internal { err_msg }
                    if err_msg.contains("not found during pending flush")
            ),
            "unexpected flush error: {err}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_worker_preserves_first_deadline_and_inline_shutdown() {
        let flush_interval = Duration::from_secs(10);
        let worker_idle_timeout = Duration::from_secs(30);
        let key = BatchKey {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            physical_table: "phy".to_string(),
        };
        let workers = Arc::new(WorkerRegistry::new());
        let (worker_tx, worker_rx) = mpsc::channel(1);
        workers
            .get_or_insert_with(key.clone(), || worker_tx.clone())
            .await;

        let backend = Arc::new(MemoryKvBackend::default());
        let table_route_cache = Arc::new(new_table_route_cache(
            "pending-rows-flush-deadline-routes".to_string(),
            CacheBuilder::new(1).build(),
            backend.clone(),
        ));
        let partition_info_cache = Arc::new(new_partition_info_cache(
            "pending-rows-flush-deadline-partitions".to_string(),
            CacheBuilder::new(1).build(),
            table_route_cache.clone(),
        ));
        let partition_manager = Arc::new(PartitionRuleManager::new(
            backend,
            table_route_cache,
            partition_info_cache,
        ));
        let node_manager: NodeManagerRef = Arc::new(ConcurrentMockNodeManager {
            datanodes: Arc::new(HashMap::new()),
        });
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        let (flow_notification_tx, _flow_notification_rx) = Notifier::try_new(1).unwrap();
        let (shutdown, _) = broadcast::channel(1);

        let flush_limiter = FlushLimiter::try_new(1).unwrap();
        start_worker(
            key.clone(),
            worker_tx.clone(),
            workers.clone(),
            worker_rx,
            shutdown.clone(),
            partition_manager,
            node_manager,
            catalog_manager,
            flow_notification_tx,
            worker_idle_timeout,
            TimingFlushPolicy::try_new(flush_interval, 3).unwrap(),
            flush_limiter.clone(),
        );

        // Start the worker, then size-flush a batch halfway to the first
        // worker-aligned interval boundary. This arms the reusable timer and
        // drains the batch before that deadline is reached.
        tokio::task::yield_now().await;
        advance(flush_interval / 2).await;
        let size_flush_rx = submit_mock_worker_batch(&worker_tx, 3, 1000).await;
        let size_flush_result =
            receive_mock_flush_result(size_flush_rx, "row threshold did not flush the first batch")
                .await;
        assert_missing_physical_table(size_flush_result);

        // Submit a low-volume batch before the first batch's old timer would
        // expire. It must receive a fresh full interval.
        advance(flush_interval / 5).await;
        let mut timed_flush_rx = submit_mock_worker_batch(&worker_tx, 1, 2000).await;

        let first_submission = tokio::time::Instant::now();
        advance(flush_interval / 2).await;
        let later_flush_rx = submit_mock_worker_batch(&worker_tx, 1, 3000).await;
        advance(flush_interval / 2 - Duration::from_millis(1)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            timed_flush_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        advance(Duration::from_millis(1)).await;
        let timed_flush_result = receive_mock_flush_result(
            timed_flush_rx,
            "batch was not flushed one interval after its creation",
        )
        .await;
        assert_missing_physical_table(timed_flush_result);

        assert_eq!(
            first_submission + flush_interval,
            tokio::time::Instant::now()
        );
        assert_missing_physical_table(
            receive_mock_flush_result(
                later_flush_rx,
                "later submission was not included in the timed flush",
            )
            .await,
        );

        // Shutdown retains the existing inline path even with no flush permits.
        let _held_permit = flush_limiter.acquire().await.unwrap();
        let shutdown_flush_rx = submit_mock_worker_batch(&worker_tx, 1, 4000).await;
        let shutdown_at = tokio::time::Instant::now();
        let _ = shutdown.send(());
        assert_missing_physical_table(
            receive_mock_flush_result(
                shutdown_flush_rx,
                "shutdown incorrectly waited for a flush permit",
            )
            .await,
        );
        assert_eq!(shutdown_at, tokio::time::Instant::now());
        for _ in 0..10 {
            if workers.is_empty().await {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            workers.is_empty().await,
            "worker did not exit after shutdown"
        );
    }
}
