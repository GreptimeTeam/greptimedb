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
use std::time::Duration;

use client::client_manager::DatanodeClients;
use common_grpc::channel_manager::ChannelConfig;
use common_meta::key::TableMetadataManager;
use common_procedure::local::{LocalManager, ManagerConfig};

use crate::cluster::{MetaPeerClientBuilder, MetaPeerClientRef};
use crate::ddl::DdlManager;
use crate::error::Result;
use crate::handler::mailbox_handler::MailboxHandler;
use crate::handler::region_lease_handler::RegionLeaseHandler;
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
use crate::service::store::cached_kv::{CheckLeader, LeaderCachedKvStore};
use crate::service::store::kv::{KvBackendAdapter, KvStoreRef, ResettableKvStoreRef};
use crate::service::store::memory::MemStore;

// TODO(fys): try use derive_builder macro
pub struct MetaSrvBuilder {
    options: Option<MetaSrvOptions>,
    kv_store: Option<KvStoreRef>,
    in_memory: Option<ResettableKvStoreRef>,
    selector: Option<SelectorRef>,
    handler_group: Option<HeartbeatHandlerGroup>,
    election: Option<ElectionRef>,
    meta_peer_client: Option<MetaPeerClientRef>,
    lock: Option<DistLockRef>,
    metadata_service: Option<MetadataServiceRef>,
    datanode_clients: Option<Arc<DatanodeClients>>,
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
            datanode_clients: None,
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

    pub fn meta_peer_client(mut self, meta_peer_client: MetaPeerClientRef) -> Self {
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

    pub fn datanode_clients(mut self, clients: Arc<DatanodeClients>) -> Self {
        self.datanode_clients = Some(clients);
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
            datanode_clients,
        } = self;

        let options = options.unwrap_or_default();

        let kv_store = kv_store.unwrap_or_else(|| Arc::new(MemStore::default()));
        let in_memory = in_memory.unwrap_or_else(|| Arc::new(MemStore::default()));
        let leader_cached_kv_store = Arc::new(LeaderCachedKvStore::new(
            Arc::new(CheckLeaderByElection(election.clone())),
            kv_store.clone(),
        ));
        let meta_peer_client = meta_peer_client.unwrap_or_else(|| {
            MetaPeerClientBuilder::default()
                .election(election.clone())
                .in_memory(in_memory.clone())
                .build()
                .map(Arc::new)
                // Safety: all required fields set at initialization
                .unwrap()
        });
        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector));
        let pushers = Pushers::default();
        let mailbox_sequence = Sequence::new("heartbeat_mailbox", 1, 100, kv_store.clone());
        let mailbox = HeartbeatMailbox::create(pushers.clone(), mailbox_sequence);
        let state_store = Arc::new(MetaStateStore::new(kv_store.clone()));

        let manager_config = ManagerConfig {
            max_retry_times: options.procedure.max_retry_times,
            retry_delay: options.procedure.retry_delay,
            ..Default::default()
        };

        let procedure_manager = Arc::new(LocalManager::new(manager_config, state_store));
        let table_id_sequence = Arc::new(Sequence::new(TABLE_ID_SEQ, 1024, 10, kv_store.clone()));
        let metadata_service = metadata_service
            .unwrap_or_else(|| Arc::new(DefaultMetadataService::new(kv_store.clone())));
        let lock = lock.unwrap_or_else(|| Arc::new(MemLock::default()));

        let table_metadata_manager = Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(
            kv_store.clone(),
        )));

        let datanode_clients = datanode_clients.unwrap_or_else(|| {
            let datanode_client_channel_config = ChannelConfig::new()
                .timeout(Duration::from_millis(
                    options.datanode.client_options.timeout_millis,
                ))
                .connect_timeout(Duration::from_millis(
                    options.datanode.client_options.connect_timeout_millis,
                ))
                .tcp_nodelay(options.datanode.client_options.tcp_nodelay);
            Arc::new(DatanodeClients::new(datanode_client_channel_config))
        });

        // TODO(weny): considers to modify the default config of procedure manager
        let ddl_manager = Arc::new(DdlManager::new(
            procedure_manager.clone(),
            kv_store.clone(),
            datanode_clients,
            mailbox.clone(),
            options.server_addr.clone(),
            table_metadata_manager.clone(),
        ));

        let _ = ddl_manager.try_start();

        let handler_group = match handler_group {
            Some(handler_group) => handler_group,
            None => {
                let region_failover_handler = if options.disable_region_failover {
                    None
                } else {
                    let region_failover_manager = Arc::new(RegionFailoverManager::new(
                        mailbox.clone(),
                        procedure_manager.clone(),
                        selector.clone(),
                        SelectorContext {
                            server_addr: options.server_addr.clone(),
                            datanode_lease_secs: options.datanode_lease_secs,
                            kv_store: kv_store.clone(),
                            meta_peer_client: meta_peer_client.clone(),
                            catalog: None,
                            schema: None,
                            table: None,
                        },
                        lock.clone(),
                        table_metadata_manager.clone(),
                    ));

                    Some(
                        RegionFailureHandler::try_new(election.clone(), region_failover_manager)
                            .await?,
                    )
                };

                let group = HeartbeatHandlerGroup::new(pushers);
                group.add_handler(ResponseHeaderHandler::default()).await;
                // `KeepLeaseHandler` should preferably be in front of `CheckLeaderHandler`,
                // because even if the current meta-server node is no longer the leader it can
                // still help the datanode to keep lease.
                group.add_handler(KeepLeaseHandler::default()).await;
                group.add_handler(CheckLeaderHandler::default()).await;
                group.add_handler(OnLeaderStartHandler::default()).await;
                group.add_handler(CollectStatsHandler::default()).await;
                group.add_handler(MailboxHandler::default()).await;
                if let Some(region_failover_handler) = region_failover_handler {
                    group.add_handler(region_failover_handler).await;
                }
                group.add_handler(RegionLeaseHandler::default()).await;
                group.add_handler(PersistStatsHandler::default()).await;
                group
            }
        };

        Ok(MetaSrv {
            started,
            options,
            in_memory,
            kv_store,
            leader_cached_kv_store,
            meta_peer_client,
            table_id_sequence,
            selector,
            handler_group,
            election,
            lock,
            procedure_manager,
            metadata_service,
            mailbox,
            ddl_manager,
            table_metadata_manager,
        })
    }
}

impl Default for MetaSrvBuilder {
    fn default() -> Self {
        Self::new()
    }
}

struct CheckLeaderByElection(Option<ElectionRef>);

impl CheckLeader for CheckLeaderByElection {
    fn check(&self) -> bool {
        self.0
            .as_ref()
            .map_or(false, |election| election.is_leader())
    }
}
