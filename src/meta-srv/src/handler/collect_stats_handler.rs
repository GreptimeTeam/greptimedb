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

use api::v1::meta::{HeartbeatRequest, Role};
use common_meta::instruction::CacheIdent;
use common_meta::key::node_address::{NodeAddressKey, NodeAddressValue};
use common_meta::key::{MetadataKey, MetadataValue};
use common_meta::peer::Peer;
use common_meta::rpc::store::PutRequest;
use common_telemetry::{error, warn};
use dashmap::DashMap;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::handler::node_stat::Stat;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::key::{DatanodeStatKey, DatanodeStatValue};
use crate::metasrv::Context;

const MAX_CACHED_STATS_PER_KEY: usize = 10;

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

#[derive(Default)]
pub struct CollectStatsHandler {
    stats_cache: DashMap<DatanodeStatKey, EpochStats>,
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
        let mut entry = self.stats_cache.entry(key).or_default();

        let key: Vec<u8> = key.into();
        let epoch_stats = entry.value_mut();

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

        if !refresh && epoch_stats.len() < MAX_CACHED_STATS_PER_KEY {
            return Ok(HandleControl::Continue);
        }

        let value: Vec<u8> = DatanodeStatValue {
            stats: epoch_stats.drain_all(),
        }
        .try_into()?;
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

async fn rewrite_node_address(ctx: &mut Context, stat: &Stat) {
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
                error!(e; "Failed to update NodeAddressValue: {:?}", peer);
            }
        }
    } else {
        warn!("Failed to serialize NodeAddressValue: {:?}", peer);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::cache_invalidator::DummyCacheInvalidator;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::sequence::SequenceBuilder;

    use super::*;
    use crate::cluster::MetaPeerClientBuilder;
    use crate::handler::{HeartbeatMailbox, Pushers};
    use crate::key::DatanodeStatKey;
    use crate::service::store::cached_kv::LeaderCachedKvBackend;

    #[tokio::test]
    async fn test_handle_datanode_stats() {
        let in_memory = Arc::new(MemoryKvBackend::new());
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let leader_cached_kv_backend = Arc::new(LeaderCachedKvBackend::with_always_leader(
            kv_backend.clone(),
        ));
        let seq = SequenceBuilder::new("test_seq", kv_backend.clone()).build();
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
            kv_backend: kv_backend.clone(),
            leader_cached_kv_backend,
            meta_peer_client,
            mailbox,
            election: None,
            is_infancy: false,
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend.clone())),
            cache_invalidator: Arc::new(DummyCacheInvalidator),
        };

        let handler = CollectStatsHandler::default();
        handle_request_many_times(ctx.clone(), &handler, 1).await;

        let key = DatanodeStatKey {
            cluster_id: 3,
            node_id: 101,
        };
        let key: Vec<u8> = key.into();
        let res = ctx.in_memory.get(&key).await.unwrap();
        let kv = res.unwrap();
        let key: DatanodeStatKey = kv.key.clone().try_into().unwrap();
        assert_eq!(3, key.cluster_id);
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
        // refresh every 10 stats
        assert_eq!(10, val.stats.len());
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
                    cluster_id: 3,
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
