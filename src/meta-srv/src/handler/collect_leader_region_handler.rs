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

use api::v1::meta::{HeartbeatRequest, Role};
use common_meta::region_registry::{LeaderRegion, LeaderRegionManifestInfo};
use store_api::region_engine::RegionRole;

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct CollectLeaderRegionHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CollectLeaderRegionHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(current_stat) = acc.stat.as_ref() else {
            return Ok(HandleControl::Continue);
        };

        let mut key_values = Vec::with_capacity(current_stat.region_stats.len());
        for stat in current_stat.region_stats.iter() {
            if stat.role != RegionRole::Leader {
                continue;
            }

            let manifest = LeaderRegionManifestInfo::from_region_stat(stat);
            let value = LeaderRegion {
                datanode_id: current_stat.id,
                manifest,
            };
            key_values.push((stat.id, value));
        }
        ctx.leader_region_registry.batch_put(key_values);

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::cache_invalidator::DummyCacheInvalidator;
    use common_meta::datanode::{RegionManifestInfo, RegionStat, Stat};
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::region_registry::{LeaderRegionManifestInfo, LeaderRegionRegistry};
    use common_meta::sequence::SequenceBuilder;
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use super::*;
    use crate::cluster::MetaPeerClientBuilder;
    use crate::handler::{HeartbeatMailbox, Pushers};
    use crate::service::store::cached_kv::LeaderCachedKvBackend;

    fn mock_ctx() -> Context {
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
        Context {
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
            leader_region_registry: Arc::new(LeaderRegionRegistry::new()),
        }
    }

    fn new_region_stat(id: RegionId, manifest_version: u64, role: RegionRole) -> RegionStat {
        RegionStat {
            id,
            region_manifest: RegionManifestInfo::Mito {
                manifest_version,
                flushed_entry_id: 0,
            },
            rcus: 0,
            wcus: 0,
            approximate_bytes: 0,
            engine: "mito".to_string(),
            role,
            num_rows: 0,
            memtable_size: 0,
            manifest_size: 0,
            sst_size: 0,
            index_size: 0,
            data_topic_latest_entry_id: 0,
            metadata_topic_latest_entry_id: 0,
        }
    }

    #[tokio::test]
    async fn test_handle_collect_leader_region() {
        let mut ctx = mock_ctx();

        let mut acc = HeartbeatAccumulator {
            stat: Some(Stat {
                id: 1,
                region_stats: vec![
                    new_region_stat(RegionId::new(1, 1), 1, RegionRole::Leader),
                    new_region_stat(RegionId::new(1, 2), 2, RegionRole::Follower),
                ],
                addr: "127.0.0.1:0000".to_string(),
                region_num: 2,
                ..Default::default()
            }),
            ..Default::default()
        };

        let handler = CollectLeaderRegionHandler;
        let control = handler
            .handle(&HeartbeatRequest::default(), &mut ctx, &mut acc)
            .await
            .unwrap();

        assert_eq!(control, HandleControl::Continue);
        let regions = ctx
            .leader_region_registry
            .batch_get(vec![RegionId::new(1, 1), RegionId::new(1, 2)].into_iter());
        assert_eq!(regions.len(), 1);
        assert_eq!(
            regions.get(&RegionId::new(1, 1)),
            Some(&LeaderRegion {
                datanode_id: 1,
                manifest: LeaderRegionManifestInfo::Mito {
                    manifest_version: 1,
                    flushed_entry_id: 0,
                    topic_latest_entry_id: 0,
                },
            })
        );

        // New heartbeat with new manifest version
        acc.stat = Some(Stat {
            id: 1,
            region_stats: vec![new_region_stat(RegionId::new(1, 1), 2, RegionRole::Leader)],
            timestamp_millis: 0,
            addr: "127.0.0.1:0000".to_string(),
            region_num: 1,
            node_epoch: 0,
            ..Default::default()
        });
        let control = handler
            .handle(&HeartbeatRequest::default(), &mut ctx, &mut acc)
            .await
            .unwrap();

        assert_eq!(control, HandleControl::Continue);
        let regions = ctx
            .leader_region_registry
            .batch_get(vec![RegionId::new(1, 1)].into_iter());
        assert_eq!(regions.len(), 1);
        assert_eq!(
            regions.get(&RegionId::new(1, 1)),
            Some(&LeaderRegion {
                datanode_id: 1,
                manifest: LeaderRegionManifestInfo::Mito {
                    manifest_version: 2,
                    flushed_entry_id: 0,
                    topic_latest_entry_id: 0,
                },
            })
        );

        // New heartbeat with old manifest version
        acc.stat = Some(Stat {
            id: 1,
            region_stats: vec![new_region_stat(RegionId::new(1, 1), 1, RegionRole::Leader)],
            timestamp_millis: 0,
            addr: "127.0.0.1:0000".to_string(),
            region_num: 1,
            node_epoch: 0,
            ..Default::default()
        });
        let control = handler
            .handle(&HeartbeatRequest::default(), &mut ctx, &mut acc)
            .await
            .unwrap();

        assert_eq!(control, HandleControl::Continue);
        let regions = ctx
            .leader_region_registry
            .batch_get(vec![RegionId::new(1, 1)].into_iter());
        assert_eq!(regions.len(), 1);
        assert_eq!(
            regions.get(&RegionId::new(1, 1)),
            // The manifest version is not updated
            Some(&LeaderRegion {
                datanode_id: 1,
                manifest: LeaderRegionManifestInfo::Mito {
                    manifest_version: 2,
                    flushed_entry_id: 0,
                    topic_latest_entry_id: 0,
                },
            })
        );
    }
}
