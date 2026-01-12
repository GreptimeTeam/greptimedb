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

use std::sync::Arc;

use common_meta::cache_invalidator::{CacheInvalidatorRef, DummyCacheInvalidator};
use common_meta::distributed_time_constants::BASE_HEARTBEAT_INTERVAL;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
use common_meta::region_registry::{LeaderRegionRegistry, LeaderRegionRegistryRef};
use common_meta::sequence::SequenceBuilder;
use common_meta::stats::topic::{TopicStatsRegistry, TopicStatsRegistryRef};

use crate::cluster::{MetaPeerClientBuilder, MetaPeerClientRef};
use crate::handler::{HeartbeatMailbox, Pushers};
use crate::metasrv::Context;
use crate::service::mailbox::MailboxRef;
use crate::service::store::cached_kv::LeaderCachedKvBackend;

pub struct TestEnv {
    in_memory: ResettableKvBackendRef,
    kv_backend: KvBackendRef,
    leader_cached_kv_backend: Arc<LeaderCachedKvBackend>,
    mailbox: MailboxRef,
    meta_peer_client: MetaPeerClientRef,
    table_metadata_manager: TableMetadataManagerRef,
    cache_invalidator: CacheInvalidatorRef,
    leader_region_registry: LeaderRegionRegistryRef,
    topic_stats_registry: TopicStatsRegistryRef,
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEnv {
    pub fn new() -> Self {
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
        Self {
            in_memory,
            kv_backend: kv_backend.clone(),
            leader_cached_kv_backend,
            mailbox,
            meta_peer_client,
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend.clone())),
            cache_invalidator: Arc::new(DummyCacheInvalidator),
            leader_region_registry: Arc::new(LeaderRegionRegistry::new()),
            topic_stats_registry: Arc::new(TopicStatsRegistry::default()),
        }
    }

    /// Returns a new context for testing.
    pub fn ctx(&self) -> Context {
        Context {
            in_memory: self.in_memory.clone(),
            kv_backend: self.kv_backend.clone(),
            leader_cached_kv_backend: self.leader_cached_kv_backend.clone(),
            server_addr: "127.0.0.1:0000".to_string(),
            meta_peer_client: self.meta_peer_client.clone(),
            mailbox: self.mailbox.clone(),
            election: None,
            is_infancy: false,
            table_metadata_manager: self.table_metadata_manager.clone(),
            cache_invalidator: self.cache_invalidator.clone(),
            leader_region_registry: self.leader_region_registry.clone(),
            topic_stats_registry: self.topic_stats_registry.clone(),
            heartbeat_interval: BASE_HEARTBEAT_INTERVAL,
            is_handshake: false,
        }
    }
}
