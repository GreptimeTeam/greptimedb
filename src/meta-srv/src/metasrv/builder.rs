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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::cluster::MetaPeerClient;
use crate::handler::failure_handler::RegionFailureHandler;
use crate::handler::{
    CheckLeaderHandler, CollectStatsHandler, HeartbeatHandlerGroup, KeepLeaseHandler,
    OnLeaderStartHandler, PersistStatsHandler, ResponseHeaderHandler,
};
use crate::lock::DistLockRef;
use crate::metasrv::{ElectionRef, MetaSrv, MetaSrvOptions, SelectorRef, TABLE_ID_SEQ};
use crate::selector::lease_based::LeaseBasedSelector;
use crate::sequence::Sequence;
use crate::service::store::kv::{KvStoreRef, ResettableKvStoreRef};
use crate::service::store::memory::MemStore;

// TODO(fys): try use derive_builder macro
pub struct MetaSrvBuilder {
    options: Option<MetaSrvOptions>,
    kv_store: Option<KvStoreRef>,
    in_memory: Option<ResettableKvStoreRef>,
    selector: Option<SelectorRef>,
    handler_group: Option<HeartbeatHandlerGroup>,
    election: Option<ElectionRef>,
    meta_peer_client: Option<MetaPeerClient>,
    lock: Option<DistLockRef>,
}

impl MetaSrvBuilder {
    pub fn new() -> Self {
        Self {
            kv_store: None,
            in_memory: None,
            selector: None,
            handler_group: None,
            meta_peer_client: None,
            election: None,
            options: None,
            lock: None,
        }
    }

    pub fn options(mut self, options: MetaSrvOptions) -> Self {
        self.options = Some(options);
        self
    }

    pub fn kv_store(mut self, kv_store: KvStoreRef) -> Self {
        self.kv_store = Some(kv_store);
        self
    }

    pub fn in_memory(mut self, in_memory: ResettableKvStoreRef) -> Self {
        self.in_memory = Some(in_memory);
        self
    }

    pub fn selector(mut self, selector: SelectorRef) -> Self {
        self.selector = Some(selector);
        self
    }

    pub fn heartbeat_handler(mut self, handler_group: HeartbeatHandlerGroup) -> Self {
        self.handler_group = Some(handler_group);
        self
    }

    pub fn meta_peer_client(mut self, meta_peer_client: MetaPeerClient) -> Self {
        self.meta_peer_client = Some(meta_peer_client);
        self
    }

    pub fn election(mut self, election: Option<ElectionRef>) -> Self {
        self.election = election;
        self
    }

    pub fn lock(mut self, lock: Option<DistLockRef>) -> Self {
        self.lock = lock;
        self
    }

    pub async fn build(self) -> MetaSrv {
        let started = Arc::new(AtomicBool::new(false));

        let MetaSrvBuilder {
            election,
            meta_peer_client,
            options,
            kv_store,
            in_memory,
            selector,
            handler_group,
            lock,
        } = self;

        let options = options.unwrap_or_default();

        let kv_store = kv_store.unwrap_or_else(|| Arc::new(MemStore::default()));

        let in_memory = in_memory.unwrap_or_else(|| Arc::new(MemStore::default()));

        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector));

        let handler_group = match handler_group {
            Some(handler_group) => handler_group,
            None => {
                let mut region_failure_handler = RegionFailureHandler::new(election.clone());
                region_failure_handler.start().await;

                let group = HeartbeatHandlerGroup::default();
                let keep_lease_handler = KeepLeaseHandler::new(kv_store.clone());
                group.add_handler(ResponseHeaderHandler::default()).await;
                // `KeepLeaseHandler` should preferably be in front of `CheckLeaderHandler`,
                // because even if the current meta-server node is no longer the leader it can
                // still help the datanode to keep lease.
                group.add_handler(keep_lease_handler).await;
                group.add_handler(CheckLeaderHandler::default()).await;
                group.add_handler(OnLeaderStartHandler::default()).await;
                group.add_handler(CollectStatsHandler).await;
                group.add_handler(region_failure_handler).await;
                group.add_handler(PersistStatsHandler::default()).await;
                group
            }
        };

        let table_id_sequence = Arc::new(Sequence::new(TABLE_ID_SEQ, 1024, 10, kv_store.clone()));

        MetaSrv {
            started,
            options,
            in_memory,
            kv_store,
            table_id_sequence,
            selector,
            handler_group,
            election,
            meta_peer_client,
            lock,
        }
    }
}

impl Default for MetaSrvBuilder {
    fn default() -> Self {
        Self::new()
    }
}
