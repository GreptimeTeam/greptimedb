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
use common_base::Plugins;
use common_grpc::channel_manager::ChannelConfig;
use common_meta::ddl_manager::{DdlManager, DdlManagerRef};
use common_meta::distributed_time_constants;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::sequence::{Sequence, SequenceRef};
use common_meta::state_store::KvStateStore;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::ProcedureManagerRef;

use crate::cache_invalidator::MetasrvCacheInvalidator;
use crate::cluster::{MetaPeerClientBuilder, MetaPeerClientRef};
use crate::error::Result;
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::handler::check_leader_handler::CheckLeaderHandler;
use crate::handler::collect_stats_handler::CollectStatsHandler;
use crate::handler::failure_handler::RegionFailureHandler;
use crate::handler::filter_inactive_region_stats::FilterInactiveRegionStatsHandler;
use crate::handler::keep_lease_handler::KeepLeaseHandler;
use crate::handler::mailbox_handler::MailboxHandler;
use crate::handler::on_leader_start_handler::OnLeaderStartHandler;
use crate::handler::persist_stats_handler::PersistStatsHandler;
use crate::handler::publish_heartbeat_handler::PublishHeartbeatHandler;
use crate::handler::region_lease_handler::RegionLeaseHandler;
use crate::handler::response_header_handler::ResponseHeaderHandler;
use crate::handler::{HeartbeatHandlerGroup, HeartbeatMailbox, Pushers};
use crate::lock::memory::MemLock;
use crate::lock::DistLockRef;
use crate::metasrv::{
    ElectionRef, MetaSrv, MetaSrvOptions, MetasrvInfo, SelectorContext, SelectorRef, TABLE_ID_SEQ,
};
use crate::procedure::region_failover::RegionFailoverManager;
use crate::pubsub::PublishRef;
use crate::selector::lease_based::LeaseBasedSelector;
use crate::service::mailbox::MailboxRef;
use crate::service::store::cached_kv::{CheckLeader, LeaderCachedKvStore};
use crate::service::store::kv::{KvBackendAdapter, KvStoreRef, ResettableKvStoreRef};
use crate::service::store::memory::MemStore;
use crate::table_meta_alloc::MetaSrvTableMetadataAllocator;

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
    datanode_clients: Option<Arc<DatanodeClients>>,
    plugins: Option<Plugins>,
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
            datanode_clients: None,
            plugins: None,
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

    pub fn datanode_clients(mut self, clients: Arc<DatanodeClients>) -> Self {
        self.datanode_clients = Some(clients);
        self
    }

    pub fn plugins(mut self, plugins: Plugins) -> Self {
        self.plugins = Some(plugins);
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
            datanode_clients,
            plugins,
        } = self;

        let options = options.unwrap_or_default();

        let kv_store = kv_store.unwrap_or_else(|| Arc::new(MemStore::default()));
        let in_memory = in_memory.unwrap_or_else(|| Arc::new(MemStore::default()));
        let leader_cached_kv_store = build_leader_cached_kv_store(&election, &kv_store);
        let meta_peer_client = meta_peer_client
            .unwrap_or_else(|| build_default_meta_peer_client(&election, &in_memory));
        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector));
        let pushers = Pushers::default();
        let mailbox = build_mailbox(&kv_store, &pushers);
        let procedure_manager = build_procedure_manager(&options, &kv_store);
        let kv_backend = KvBackendAdapter::wrap(kv_store.clone());
        let table_id_sequence = Arc::new(Sequence::new(TABLE_ID_SEQ, 1024, 10, kv_backend.clone()));
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let lock = lock.unwrap_or_else(|| Arc::new(MemLock::default()));
        let selector_ctx = SelectorContext {
            server_addr: options.server_addr.clone(),
            datanode_lease_secs: distributed_time_constants::DATANODE_LEASE_SECS,
            kv_store: kv_store.clone(),
            meta_peer_client: meta_peer_client.clone(),
            table_id: None,
        };
        let ddl_manager = build_ddl_manager(
            &options,
            datanode_clients,
            &procedure_manager,
            &mailbox,
            &table_metadata_manager,
            (&selector, &selector_ctx),
            &table_id_sequence,
        );
        let _ = ddl_manager.try_start();

        let handler_group = match handler_group {
            Some(handler_group) => handler_group,
            None => {
                let region_failover_handler = if options.enable_region_failover {
                    let region_failover_manager = Arc::new(RegionFailoverManager::new(
                        distributed_time_constants::REGION_LEASE_SECS,
                        in_memory.clone(),
                        mailbox.clone(),
                        procedure_manager.clone(),
                        (selector.clone(), selector_ctx.clone()),
                        lock.clone(),
                        table_metadata_manager.clone(),
                    ));
                    Some(
                        RegionFailureHandler::try_new(election.clone(), region_failover_manager)
                            .await?,
                    )
                } else {
                    None
                };

                let publish_heartbeat_handler = plugins
                    .clone()
                    .and_then(|plugins| plugins.get::<PublishRef>())
                    .map(|publish| PublishHeartbeatHandler::new(publish.clone()));

                let region_lease_handler =
                    RegionLeaseHandler::new(distributed_time_constants::REGION_LEASE_SECS);

                let group = HeartbeatHandlerGroup::new(pushers);
                group.add_handler(ResponseHeaderHandler).await;
                // `KeepLeaseHandler` should preferably be in front of `CheckLeaderHandler`,
                // because even if the current meta-server node is no longer the leader it can
                // still help the datanode to keep lease.
                group.add_handler(KeepLeaseHandler).await;
                group.add_handler(CheckLeaderHandler).await;
                group.add_handler(OnLeaderStartHandler).await;
                group.add_handler(CollectStatsHandler).await;
                group.add_handler(MailboxHandler).await;
                group.add_handler(region_lease_handler).await;
                group.add_handler(FilterInactiveRegionStatsHandler).await;
                if let Some(region_failover_handler) = region_failover_handler {
                    group.add_handler(region_failover_handler).await;
                }
                if let Some(publish_heartbeat_handler) = publish_heartbeat_handler {
                    group.add_handler(publish_heartbeat_handler).await;
                }
                group.add_handler(PersistStatsHandler::default()).await;
                group
            }
        };

        let enable_telemetry = options.enable_telemetry;
        let metasrv_home = options.data_home.to_string();

        Ok(MetaSrv {
            started,
            options,
            in_memory,
            kv_store,
            leader_cached_kv_store,
            meta_peer_client: meta_peer_client.clone(),
            table_id_sequence,
            selector,
            handler_group,
            election,
            lock,
            procedure_manager,
            mailbox,
            ddl_executor: ddl_manager,
            table_metadata_manager,
            greptimedb_telemetry_task: get_greptimedb_telemetry_task(
                Some(metasrv_home),
                meta_peer_client,
                enable_telemetry,
            )
            .await,
            plugins: plugins.unwrap_or_else(Plugins::default),
        })
    }
}

fn build_leader_cached_kv_store(
    election: &Option<ElectionRef>,
    kv_store: &KvStoreRef,
) -> Arc<LeaderCachedKvStore> {
    Arc::new(LeaderCachedKvStore::new(
        Arc::new(CheckLeaderByElection(election.clone())),
        kv_store.clone(),
    ))
}

fn build_default_meta_peer_client(
    election: &Option<ElectionRef>,
    in_memory: &ResettableKvStoreRef,
) -> MetaPeerClientRef {
    MetaPeerClientBuilder::default()
        .election(election.clone())
        .in_memory(in_memory.clone())
        .build()
        .map(Arc::new)
        // Safety: all required fields set at initialization
        .unwrap()
}

fn build_mailbox(kv_store: &KvStoreRef, pushers: &Pushers) -> MailboxRef {
    let mailbox_sequence = Sequence::new(
        "heartbeat_mailbox",
        1,
        100,
        KvBackendAdapter::wrap(kv_store.clone()),
    );
    HeartbeatMailbox::create(pushers.clone(), mailbox_sequence)
}

fn build_procedure_manager(options: &MetaSrvOptions, kv_store: &KvStoreRef) -> ProcedureManagerRef {
    let manager_config = ManagerConfig {
        max_retry_times: options.procedure.max_retry_times,
        retry_delay: options.procedure.retry_delay,
        ..Default::default()
    };
    let state_store = Arc::new(KvStateStore::new(KvBackendAdapter::wrap(kv_store.clone())));
    Arc::new(LocalManager::new(manager_config, state_store))
}

fn build_ddl_manager(
    options: &MetaSrvOptions,
    datanode_clients: Option<Arc<DatanodeClients>>,
    procedure_manager: &ProcedureManagerRef,
    mailbox: &MailboxRef,
    table_metadata_manager: &TableMetadataManagerRef,
    (selector, selector_ctx): (&SelectorRef, &SelectorContext),
    table_id_sequence: &SequenceRef,
) -> DdlManagerRef {
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
    let cache_invalidator = Arc::new(MetasrvCacheInvalidator::new(
        mailbox.clone(),
        MetasrvInfo {
            server_addr: options.server_addr.clone(),
        },
    ));

    let table_meta_allocator = Arc::new(MetaSrvTableMetadataAllocator::new(
        selector_ctx.clone(),
        selector.clone(),
        table_id_sequence.clone(),
    ));

    Arc::new(DdlManager::new(
        procedure_manager.clone(),
        datanode_clients,
        cache_invalidator,
        table_metadata_manager.clone(),
        table_meta_allocator,
    ))
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
