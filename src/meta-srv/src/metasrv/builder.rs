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

use common_procedure::local::{LocalManager, ManagerConfig};

use crate::cluster::MetaPeerClient;
use crate::error::Result;
use crate::handler::mailbox_handler::MailboxHandler;
use crate::handler::{
    CheckLeaderHandler, CollectStatsHandler, HeartbeatHandlerGroup, HeartbeatMailbox,
    KeepLeaseHandler, OnLeaderStartHandler, PersistStatsHandler, Pushers, RegionFailureHandler,
    ResponseHeaderHandler,
};
use crate::lock::memory::MemLock;
use crate::lock::DistLockRef;
use crate::metadata_service::{DefaultMetadataService, MetadataServiceRef};
use crate::metasrv::{
    ElectionRef, MetaSrv, MetaSrvOptions, SelectorContext, SelectorRef, TABLE_ID_SEQ,
};
use crate::procedure::region_failover::RegionFailoverManager;
use crate::procedure::state_store::MetaStateStore;
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
    metadata_service: Option<MetadataServiceRef>,
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
            metadata_service: None,
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

    pub fn metadata_service(mut self, metadata_service: MetadataServiceRef) -> Self {
        self.metadata_service = Some(metadata_service);
        self
    }

    pub async fn build(self) -> Result<MetaSrv> {
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
            metadata_service,
        } = self;

        let options = options.unwrap_or_default();

        let kv_store = kv_store.unwrap_or_else(|| Arc::new(MemStore::default()));

        let in_memory = in_memory.unwrap_or_else(|| Arc::new(MemStore::default()));

        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector));

        let pushers = Pushers::default();
        let mailbox_sequence = Sequence::new("heartbeat_mailbox", 1, 100, kv_store.clone());
        let mailbox = HeartbeatMailbox::create(pushers.clone(), mailbox_sequence);

        let state_store = Arc::new(MetaStateStore::new(kv_store.clone()));
        let procedure_manager = Arc::new(LocalManager::new(ManagerConfig::default(), state_store));

        let lock = lock.unwrap_or_else(|| Arc::new(MemLock::default()));

        let handler_group = match handler_group {
            Some(handler_group) => handler_group,
            None => {
                let region_failover_manager = Arc::new(RegionFailoverManager::new(
                    mailbox.clone(),
                    procedure_manager.clone(),
                    selector.clone(),
                    SelectorContext {
                        server_addr: options.server_addr.clone(),
                        datanode_lease_secs: options.datanode_lease_secs,
                        kv_store: kv_store.clone(),
                        catalog: None,
                        schema: None,
                        table: None,
                    },
                    lock.clone(),
                ));

                let region_failure_handler =
                    RegionFailureHandler::try_new(election.clone(), region_failover_manager)
                        .await?;

                let group = HeartbeatHandlerGroup::new(pushers);
                let keep_lease_handler = KeepLeaseHandler::new(kv_store.clone());
                group.add_handler(ResponseHeaderHandler::default()).await;
                // `KeepLeaseHandler` should preferably be in front of `CheckLeaderHandler`,
                // because even if the current meta-server node is no longer the leader it can
                // still help the datanode to keep lease.
                group.add_handler(keep_lease_handler).await;
                group.add_handler(CheckLeaderHandler::default()).await;
                group.add_handler(OnLeaderStartHandler::default()).await;
                group.add_handler(CollectStatsHandler).await;
                group.add_handler(MailboxHandler).await;
                group.add_handler(region_failure_handler).await;
                group.add_handler(PersistStatsHandler::default()).await;
                group
            }
        };

        let table_id_sequence = Arc::new(Sequence::new(TABLE_ID_SEQ, 1024, 10, kv_store.clone()));

        let metadata_service = metadata_service
            .unwrap_or_else(|| Arc::new(DefaultMetadataService::new(kv_store.clone())));

        Ok(MetaSrv {
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
            procedure_manager,
            metadata_service,
            mailbox,
        })
    }
}

impl Default for MetaSrvBuilder {
    fn default() -> Self {
        Self::new()
    }
}
