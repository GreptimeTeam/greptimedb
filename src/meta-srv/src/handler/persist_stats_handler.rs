// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;

use api::v1::meta::{HeartbeatRequest, Role};
use common_meta::rpc::store::PutRequest;
use common_telemetry::warn;
use dashmap::DashMap;

use crate::error::Result;
use crate::handler::node_stat::Stat;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::keys::{StatKey, StatValue};
use crate::metasrv::Context;

const MAX_CACHED_STATS_PER_KEY: usize = 10;

#[derive(Default)]
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
    fn clear(&mut self) {
        self.stats.clear();
    }

    #[inline]
    fn push(&mut self, stat: Stat) {
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

#[derive(Default)]
pub struct PersistStatsHandler {
    stats_cache: DashMap<StatKey, EpochStats>,
}

#[async_trait::async_trait]
impl HeartbeatHandler for PersistStatsHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        let Some(current_stat) = acc.stat.take() else {
            return Ok(());
        };

        let key = current_stat.stat_key();
        let mut entry = self.stats_cache.entry(key).or_default();

        let key: Vec<u8> = key.into();
        let epoch_stats = entry.value_mut();

        let refresh = if let Some(epoch) = epoch_stats.epoch() {
            match current_stat.node_epoch.cmp(&epoch) {
                Ordering::Greater => {
                    // This node may have been redeployed.
                    epoch_stats.set_epoch(current_stat.node_epoch);
                    epoch_stats.clear();
                    true
                }
                Ordering::Less => {
                    warn!("Ignore stale heartbeat: {:?}", current_stat);
                    false
                }
                Ordering::Equal => false,
            }
        } else {
            epoch_stats.set_epoch(current_stat.node_epoch);
            // If the epoch is empty, it indicates that the current node sending the heartbeat
            // for the first time to the current meta leader, so it is necessary to persist
            // the data to the KV store as soon as possible.
            true
        };

        epoch_stats.push(current_stat);

        if !refresh && epoch_stats.len() < MAX_CACHED_STATS_PER_KEY {
            return Ok(());
        }

        let value: Vec<u8> = StatValue {
            stats: epoch_stats.drain_all(),
        }
        .try_into()?;
        let put = PutRequest {
            key,
            value,
            ..Default::default()
        };

        let _ = ctx.in_memory.put(put).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use common_meta::key::TableMetadataManager;

    use super::*;
    use crate::cluster::MetaPeerClientBuilder;
    use crate::handler::{HeartbeatMailbox, Pushers};
    use crate::keys::StatKey;
    use crate::sequence::Sequence;
    use crate::service::store::cached_kv::LeaderCachedKvStore;
    use crate::service::store::kv::KvBackendAdapter;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_handle_datanode_stats() {
        let in_memory = Arc::new(MemStore::new());
        let kv_store = Arc::new(MemStore::new());
        let leader_cached_kv_store =
            Arc::new(LeaderCachedKvStore::with_always_leader(kv_store.clone()));
        let seq = Sequence::new("test_seq", 0, 10, kv_store.clone());
        let mailbox = HeartbeatMailbox::create(Pushers::default(), seq);
        let meta_peer_client = MetaPeerClientBuilder::default()
            .election(None)
            .in_memory(in_memory.clone())
            .build()
            .map(Arc::new)
            // Safety: all required fields set at initialization
            .unwrap();
        let ctx = Context {
            server_addr: "127.0.0.1:0000".to_string(),
            in_memory,
            kv_store: kv_store.clone(),
            leader_cached_kv_store,
            meta_peer_client,
            mailbox,
            election: None,
            skip_all: Arc::new(AtomicBool::new(false)),
            is_infancy: false,
            table_metadata_manager: Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(
                kv_store.clone(),
            ))),
        };

        let handler = PersistStatsHandler::default();
        handle_request_many_times(ctx.clone(), &handler, 1).await;

        let key = StatKey {
            cluster_id: 3,
            node_id: 101,
        };
        let key: Vec<u8> = key.try_into().unwrap();
        let res = ctx.in_memory.get(&key).await.unwrap();
        let kv = res.unwrap();
        let key: StatKey = kv.key.clone().try_into().unwrap();
        assert_eq!(3, key.cluster_id);
        assert_eq!(101, key.node_id);
        let val: StatValue = kv.value.try_into().unwrap();
        // first new stat must be set in kv store immediately
        assert_eq!(1, val.stats.len());
        assert_eq!(Some(1), val.stats[0].region_num);

        handle_request_many_times(ctx.clone(), &handler, 10).await;

        let key: Vec<u8> = key.try_into().unwrap();
        let res = ctx.in_memory.get(&key).await.unwrap();
        let kv = res.unwrap();
        let val: StatValue = kv.value.try_into().unwrap();
        // refresh every 10 stats
        assert_eq!(10, val.stats.len());
    }

    async fn handle_request_many_times(
        mut ctx: Context,
        handler: &PersistStatsHandler,
        loop_times: i32,
    ) {
        let req = HeartbeatRequest::default();
        for i in 1..=loop_times {
            let mut acc = HeartbeatAccumulator {
                stat: Some(Stat {
                    cluster_id: 3,
                    id: 101,
                    region_num: Some(i as _),
                    ..Default::default()
                }),
                ..Default::default()
            };
            handler.handle(&req, &mut ctx, &mut acc).await.unwrap();
        }
    }
}
