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
use std::sync::Arc;

use api::v1::meta::{HeartbeatRequest, Role};
use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue, Stat};
use common_meta::instruction::CacheIdent;
use common_meta::key::node_address::{NodeAddressKey, NodeAddressValue};
use common_meta::key::{MetadataKey, MetadataValue};
use common_meta::peer::Peer;
use common_meta::rpc::store::PutRequest;
use common_telemetry::{error, info, warn};
use dashmap::DashMap;
use snafu::ResultExt;
use tokio::sync::Mutex;

use crate::error::{self, Result};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

#[derive(Debug, Default)]
struct EpochStats {
    stats: Vec<Stat>,
    epoch: Option<u64>,
}

impl EpochStats {
    #[inline]
    fn drain_all(&mut self) -> Vec<Stat> {
        self.stats.drain(..).collect()
    }

    #[inline]
    fn clear_stats(&mut self) {
        self.stats.clear();
    }

    #[inline]
    fn push_stat(&mut self, stat: Stat) {
        self.stats.push(stat);
    }

    #[inline]
    fn len(&self) -> usize {
        self.stats.len()
    }

    #[inline]
    fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    #[inline]
    fn set_epoch(&mut self, epoch: u64) {
        self.epoch = Some(epoch);
    }
}

const DEFAULT_FLUSH_STATS_FACTOR: usize = 3;

pub struct CollectStatsHandler {
    stats_cache: DashMap<DatanodeStatKey, Arc<Mutex<EpochStats>>>,
    flush_stats_factor: usize,
}

impl Default for CollectStatsHandler {
    fn default() -> Self {
        Self::new(None)
    }
}

impl CollectStatsHandler {
    pub fn new(flush_stats_factor: Option<usize>) -> Self {
        Self {
            flush_stats_factor: flush_stats_factor.unwrap_or(DEFAULT_FLUSH_STATS_FACTOR),
            stats_cache: DashMap::default(),
        }
    }
}

#[async_trait::async_trait]
impl HeartbeatHandler for CollectStatsHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(current_stat) = acc.stat.take() else {
            return Ok(HandleControl::Continue);
        };

        let key = current_stat.stat_key();
        let state = {
            let entry = self
                .stats_cache
                .entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(EpochStats::default())));
            Arc::clone(entry.value())
        };
        let mut epoch_stats = state.lock().await;

        let key: Vec<u8> = key.into();

        let refresh = if let Some(epoch) = epoch_stats.epoch() {
            match current_stat.node_epoch.cmp(&epoch) {
                Ordering::Greater => {
                    // This node may have been redeployed.
                    epoch_stats.clear_stats();
                    epoch_stats.set_epoch(current_stat.node_epoch);
                    epoch_stats.push_stat(current_stat);
                    true
                }
                Ordering::Equal => {
                    epoch_stats.push_stat(current_stat);
                    false
                }
                Ordering::Less => {
                    warn!("Ignore stale heartbeat: {:?}", current_stat);
                    false
                }
            }
        } else {
            epoch_stats.set_epoch(current_stat.node_epoch);
            epoch_stats.push_stat(current_stat);
            // If the epoch is empty, it indicates that the current node sending the heartbeat
            // for the first time to the current meta leader, so it is necessary to save
            // the data to the KV store as soon as possible.
            true
        };

        // Need to refresh the [datanode -> address] mapping
        if refresh {
            // Safety: `epoch_stats.stats` is not empty
            let last = epoch_stats.stats.last().unwrap();
            rewrite_node_address(ctx, last).await;
        }

        if !refresh && epoch_stats.len() < self.flush_stats_factor {
            return Ok(HandleControl::Continue);
        }

        let value: Vec<u8> = DatanodeStatValue {
            stats: epoch_stats.drain_all(),
        }
        .try_into()
        .context(error::InvalidDatanodeStatFormatSnafu {})?;
        let put = PutRequest {
            key,
            value,
            prev_kv: false,
        };

        let _ = ctx
            .in_memory
            .put(put)
            .await
            .context(error::KvBackendSnafu)?;

        Ok(HandleControl::Continue)
    }
}

async fn rewrite_node_address(ctx: &Context, stat: &Stat) {
    let peer = Peer {
        id: stat.id,
        addr: stat.addr.clone(),
    };
    let key = NodeAddressKey::with_datanode(peer.id).to_bytes();
    if let Ok(value) = NodeAddressValue::new(peer.clone()).try_as_raw_value() {
        let put = PutRequest {
            key,
            value,
            prev_kv: false,
        };

        match ctx.leader_cached_kv_backend.put(put).await {
            Ok(_) => {
                info!(
                    "Successfully updated datanode `NodeAddressValue`: {:?}",
                    peer
                );
                // broadcast invalidating cache
                let cache_idents = stat
                    .table_ids()
                    .into_iter()
                    .map(CacheIdent::TableId)
                    .collect::<Vec<_>>();
                if let Err(e) = ctx
                    .cache_invalidator
                    .invalidate(&Default::default(), &cache_idents)
                    .await
                {
                    error!(e; "Failed to invalidate {} `NodeAddressKey` cache, peer: {:?}", cache_idents.len(), peer);
                }
            }
            Err(e) => {
                error!(e; "Failed to update datanode `NodeAddressValue`: {:?}", peer);
            }
        }
    } else {
        warn!(
            "Failed to serialize datanode `NodeAddressValue`: {:?}",
            peer
        );
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex as StdMutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use common_meta::datanode::DatanodeStatKey;
    use common_meta::error::{Error as MetaError, Result as MetaResult};
    use common_meta::kv_backend::{KvBackend, KvBackendRef, ResettableKvBackend, TxnService};
    use common_meta::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutResponse,
        RangeRequest, RangeResponse,
    };
    use tokio::sync::Semaphore;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::handler::test_utils::TestEnv;
    use crate::service::store::cached_kv::LeaderCachedKvBackend;

    struct ControlledKvBackend {
        recorded_puts: StdMutex<Vec<PutRequest>>,
        put_entered: Semaphore,
        put_release: Semaphore,
        block_next_put: AtomicBool,
        delay_puts: AtomicBool,
        active_puts: AtomicUsize,
        max_active_puts: AtomicUsize,
    }

    impl ControlledKvBackend {
        fn new() -> Self {
            Self {
                recorded_puts: StdMutex::new(Vec::new()),
                put_entered: Semaphore::new(0),
                put_release: Semaphore::new(0),
                block_next_put: AtomicBool::new(false),
                delay_puts: AtomicBool::new(false),
                active_puts: AtomicUsize::new(0),
                max_active_puts: AtomicUsize::new(0),
            }
        }

        fn block_next_put(&self) {
            self.block_next_put.store(true, AtomicOrdering::Relaxed);
        }

        async fn wait_for_blocked_put(&self) {
            self.put_entered.acquire().await.unwrap().forget();
        }

        fn release_one_put(&self) {
            self.put_release.add_permits(1);
        }

        fn set_delay_puts(&self, delay: bool) {
            self.delay_puts.store(delay, AtomicOrdering::Relaxed);
        }

        fn clear_recorded_puts(&self) {
            self.recorded_puts.lock().unwrap().clear();
        }

        fn recorded_puts(&self) -> Vec<PutRequest> {
            self.recorded_puts.lock().unwrap().clone()
        }

        fn max_active_puts(&self) -> usize {
            self.max_active_puts.load(AtomicOrdering::Relaxed)
        }

        fn start_put(&self, req: &PutRequest) -> PutGuard<'_> {
            self.recorded_puts.lock().unwrap().push(req.clone());
            let active = self.active_puts.fetch_add(1, AtomicOrdering::Relaxed) + 1;
            self.max_active_puts
                .fetch_max(active, AtomicOrdering::Relaxed);
            PutGuard { backend: self }
        }
    }

    struct PutGuard<'a> {
        backend: &'a ControlledKvBackend,
    }

    impl Drop for PutGuard<'_> {
        fn drop(&mut self) {
            self.backend
                .active_puts
                .fetch_sub(1, AtomicOrdering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl TxnService for ControlledKvBackend {
        type Error = MetaError;
    }

    #[async_trait::async_trait]
    impl KvBackend for ControlledKvBackend {
        fn name(&self) -> &str {
            "controlled"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, _req: RangeRequest) -> MetaResult<RangeResponse> {
            unimplemented!()
        }

        async fn put(&self, req: PutRequest) -> MetaResult<PutResponse> {
            let _guard = self.start_put(&req);
            if self.block_next_put.swap(false, AtomicOrdering::Relaxed) {
                self.put_entered.add_permits(1);
                self.put_release.acquire().await.unwrap().forget();
            }
            if self.delay_puts.load(AtomicOrdering::Relaxed) {
                sleep(Duration::from_millis(10)).await;
            }
            Ok(PutResponse::default())
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

    impl ResettableKvBackend for ControlledKvBackend {
        fn reset(&self) {
            self.clear_recorded_puts();
        }

        fn as_kv_backend_ref(self: Arc<Self>) -> KvBackendRef {
            self
        }
    }

    fn stat(node_id: u64, epoch: u64, marker: u64, addr: &str) -> Stat {
        Stat {
            timestamp_millis: marker as i64,
            id: node_id,
            addr: addr.to_string(),
            region_num: marker,
            node_epoch: epoch,
            ..Default::default()
        }
    }

    async fn handle_stat(
        handler: Arc<CollectStatsHandler>,
        mut ctx: Context,
        stat: Stat,
    ) -> Result<HandleControl> {
        let mut acc = HeartbeatAccumulator {
            stat: Some(stat),
            ..Default::default()
        };
        handler
            .handle(&HeartbeatRequest::default(), &mut ctx, &mut acc)
            .await
    }

    fn use_controlled_address_backend(ctx: &mut Context) -> Arc<ControlledKvBackend> {
        let backend = Arc::new(ControlledKvBackend::new());
        ctx.leader_cached_kv_backend =
            Arc::new(LeaderCachedKvBackend::with_always_leader(backend.clone()));
        backend
    }

    #[tokio::test]
    async fn test_handle_datanode_stats() {
        let env = TestEnv::new();
        let ctx = env.ctx();

        let handler = CollectStatsHandler::default();
        handle_request_many_times(ctx.clone(), &handler, 1).await;

        let key = DatanodeStatKey { node_id: 101 };
        let key: Vec<u8> = key.into();
        let res = ctx.in_memory.get(&key).await.unwrap();
        let kv = res.unwrap();
        let key: DatanodeStatKey = kv.key.clone().try_into().unwrap();
        assert_eq!(101, key.node_id);
        let val: DatanodeStatValue = kv.value.try_into().unwrap();
        // first new stat must be set in kv store immediately
        assert_eq!(1, val.stats.len());
        assert_eq!(1, val.stats[0].region_num);

        handle_request_many_times(ctx.clone(), &handler, 10).await;

        let key: Vec<u8> = key.into();
        let res = ctx.in_memory.get(&key).await.unwrap();
        let kv = res.unwrap();
        let val: DatanodeStatValue = kv.value.try_into().unwrap();
        assert_eq!(handler.flush_stats_factor, val.stats.len());
    }

    #[test]
    fn test_same_datanode_wait_keeps_current_thread_runtime_responsive() {
        let (backend_tx, backend_rx) = mpsc::sync_channel(1);
        let (timer_tx, timer_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);

        let worker = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(async move {
                    let env = TestEnv::new();
                    let mut ctx = env.ctx();
                    let address_backend = use_controlled_address_backend(&mut ctx);
                    address_backend.block_next_put();
                    backend_tx.send(address_backend.clone()).unwrap();

                    let handler = Arc::new(CollectStatsHandler::default());
                    let first = tokio::spawn(handle_stat(
                        handler.clone(),
                        ctx.clone(),
                        stat(101, 1, 1, "dn-101-v1"),
                    ));
                    address_backend.wait_for_blocked_put().await;

                    let second =
                        tokio::spawn(handle_stat(handler, ctx, stat(101, 1, 2, "dn-101-v1")));
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(10)).await;
                        timer_tx.send(()).unwrap();
                    });
                    tokio::task::yield_now().await;

                    first.await.unwrap().unwrap();
                    second.await.unwrap().unwrap();
                    done_tx.send(()).unwrap();
                });
        });

        let address_backend = backend_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let timer_result = timer_rx.recv_timeout(Duration::from_secs(1));
        address_backend.release_one_put();
        timer_result.expect("the current-thread runtime must remain responsive");
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("both heartbeat handlers must complete after releasing the write");
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_flush_persists_every_stat_once() {
        let env = TestEnv::new();
        let mut ctx = env.ctx();
        let stats_backend = Arc::new(ControlledKvBackend::new());
        ctx.in_memory = stats_backend.clone();
        stats_backend.set_delay_puts(true);

        let flush_stats_factor = 3;
        let handler = Arc::new(CollectStatsHandler::new(Some(flush_stats_factor)));
        handle_stat(handler.clone(), ctx.clone(), stat(101, 1, 0, "dn-101"))
            .await
            .unwrap();
        stats_backend.clear_recorded_puts();

        let mut tasks = Vec::with_capacity(2 * flush_stats_factor);
        for marker in 1..=(2 * flush_stats_factor) {
            tasks.push(tokio::spawn(handle_stat(
                handler.clone(),
                ctx.clone(),
                stat(101, 1, marker as u64, "dn-101"),
            )));
        }
        for task in tasks {
            timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }

        let puts = stats_backend.recorded_puts();
        assert_eq!(2, puts.len());
        let mut markers = puts
            .into_iter()
            .flat_map(|put| {
                let value: DatanodeStatValue = put.value.try_into().unwrap();
                value
                    .stats
                    .into_iter()
                    .map(|stat| stat.region_num)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        markers.sort_unstable();
        assert_eq!((1..=6).collect::<Vec<_>>(), markers);
        assert_eq!(1, stats_backend.max_active_puts());
    }

    async fn handle_request_many_times(
        mut ctx: Context,
        handler: &CollectStatsHandler,
        loop_times: i32,
    ) {
        let req = HeartbeatRequest::default();
        for i in 1..=loop_times {
            let mut acc = HeartbeatAccumulator {
                stat: Some(Stat {
                    id: 101,
                    region_num: i as _,
                    ..Default::default()
                }),
                ..Default::default()
            };
            handler.handle(&req, &mut ctx, &mut acc).await.unwrap();
        }
    }
}
