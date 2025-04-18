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
use std::sync::{Arc, Mutex, RwLock};

use client::client_manager::NodeClients;
use common_base::Plugins;
use common_catalog::consts::{MIN_USER_FLOW_ID, MIN_USER_TABLE_ID};
use common_grpc::channel_manager::ChannelConfig;
use common_meta::ddl::flow_meta::FlowMetadataAllocator;
use common_meta::ddl::table_meta::{TableMetadataAllocator, TableMetadataAllocatorRef};
use common_meta::ddl::{
    DdlContext, NoopRegionFailureDetectorControl, RegionFailureDetectorControllerRef,
};
use common_meta::ddl_manager::DdlManager;
use common_meta::distributed_time_constants;
use common_meta::key::flow::flow_state::FlowStateManager;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::maintenance::MaintenanceModeManager;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
use common_meta::node_manager::NodeManagerRef;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::region_registry::LeaderRegionRegistry;
use common_meta::sequence::SequenceBuilder;
use common_meta::state_store::KvStateStore;
use common_meta::wal_options_allocator::{build_kafka_client, build_wal_options_allocator};
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::ProcedureManagerRef;
use snafu::ResultExt;

use crate::cache_invalidator::MetasrvCacheInvalidator;
use crate::cluster::{MetaPeerClientBuilder, MetaPeerClientRef};
use crate::error::{self, BuildWalOptionsAllocatorSnafu, Result};
use crate::flow_meta_alloc::FlowPeerAllocator;
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::handler::failure_handler::RegionFailureHandler;
use crate::handler::flow_state_handler::FlowStateHandler;
use crate::handler::region_lease_handler::{CustomizedRegionLeaseRenewerRef, RegionLeaseHandler};
use crate::handler::{HeartbeatHandlerGroupBuilder, HeartbeatMailbox, Pushers};
use crate::lease::MetaPeerLookupService;
use crate::metasrv::{
    ElectionRef, Metasrv, MetasrvInfo, MetasrvOptions, SelectTarget, SelectorContext, SelectorRef,
    FLOW_ID_SEQ, TABLE_ID_SEQ,
};
use crate::procedure::region_migration::manager::RegionMigrationManager;
use crate::procedure::region_migration::DefaultContextFactory;
use crate::procedure::wal_prune::manager::{WalPruneManager, WalPruneTicker};
use crate::procedure::wal_prune::Context as WalPruneContext;
use crate::region::supervisor::{
    HeartbeatAcceptor, RegionFailureDetectorControl, RegionSupervisor, RegionSupervisorTicker,
    DEFAULT_TICK_INTERVAL,
};
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::round_robin::RoundRobinSelector;
use crate::service::mailbox::MailboxRef;
use crate::service::store::cached_kv::LeaderCachedKvBackend;
use crate::state::State;
use crate::table_meta_alloc::MetasrvPeerAllocator;

// TODO(fys): try use derive_builder macro
pub struct MetasrvBuilder {
    options: Option<MetasrvOptions>,
    kv_backend: Option<KvBackendRef>,
    in_memory: Option<ResettableKvBackendRef>,
    selector: Option<SelectorRef>,
    handler_group_builder: Option<HeartbeatHandlerGroupBuilder>,
    election: Option<ElectionRef>,
    meta_peer_client: Option<MetaPeerClientRef>,
    node_manager: Option<NodeManagerRef>,
    plugins: Option<Plugins>,
    table_metadata_allocator: Option<TableMetadataAllocatorRef>,
}

impl MetasrvBuilder {
    pub fn new() -> Self {
        Self {
            kv_backend: None,
            in_memory: None,
            selector: None,
            handler_group_builder: None,
            meta_peer_client: None,
            election: None,
            options: None,
            node_manager: None,
            plugins: None,
            table_metadata_allocator: None,
        }
    }

    pub fn options(mut self, options: MetasrvOptions) -> Self {
        self.options = Some(options);
        self
    }

    pub fn kv_backend(mut self, kv_backend: KvBackendRef) -> Self {
        self.kv_backend = Some(kv_backend);
        self
    }

    pub fn in_memory(mut self, in_memory: ResettableKvBackendRef) -> Self {
        self.in_memory = Some(in_memory);
        self
    }

    pub fn selector(mut self, selector: SelectorRef) -> Self {
        self.selector = Some(selector);
        self
    }

    pub fn heartbeat_handler(
        mut self,
        handler_group_builder: HeartbeatHandlerGroupBuilder,
    ) -> Self {
        self.handler_group_builder = Some(handler_group_builder);
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

    pub fn node_manager(mut self, node_manager: NodeManagerRef) -> Self {
        self.node_manager = Some(node_manager);
        self
    }

    pub fn plugins(mut self, plugins: Plugins) -> Self {
        self.plugins = Some(plugins);
        self
    }

    pub fn table_metadata_allocator(
        mut self,
        table_metadata_allocator: TableMetadataAllocatorRef,
    ) -> Self {
        self.table_metadata_allocator = Some(table_metadata_allocator);
        self
    }

    pub async fn build(self) -> Result<Metasrv> {
        let started = Arc::new(AtomicBool::new(false));

        let MetasrvBuilder {
            election,
            meta_peer_client,
            options,
            kv_backend,
            in_memory,
            selector,
            handler_group_builder,
            node_manager,
            plugins,
            table_metadata_allocator,
        } = self;

        let options = options.unwrap_or_default();

        let kv_backend = kv_backend.unwrap_or_else(|| Arc::new(MemoryKvBackend::new()));
        let in_memory = in_memory.unwrap_or_else(|| Arc::new(MemoryKvBackend::new()));

        let state = Arc::new(RwLock::new(match election {
            None => State::leader(options.server_addr.to_string(), true),
            Some(_) => State::follower(options.server_addr.to_string()),
        }));

        let leader_cached_kv_backend = Arc::new(LeaderCachedKvBackend::new(
            state.clone(),
            kv_backend.clone(),
        ));

        let meta_peer_client = meta_peer_client
            .unwrap_or_else(|| build_default_meta_peer_client(&election, &in_memory));
        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector));
        let pushers = Pushers::default();
        let mailbox = build_mailbox(&kv_backend, &pushers);
        let procedure_manager = build_procedure_manager(&options, &kv_backend);

        let table_metadata_manager = Arc::new(TableMetadataManager::new(
            leader_cached_kv_backend.clone() as _,
        ));
        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(
            leader_cached_kv_backend.clone() as _,
        ));
        let maintenance_mode_manager = Arc::new(MaintenanceModeManager::new(kv_backend.clone()));
        let selector_ctx = SelectorContext {
            server_addr: options.server_addr.clone(),
            datanode_lease_secs: distributed_time_constants::DATANODE_LEASE_SECS,
            flownode_lease_secs: distributed_time_constants::FLOWNODE_LEASE_SECS,
            kv_backend: kv_backend.clone(),
            meta_peer_client: meta_peer_client.clone(),
            table_id: None,
        };

        let wal_options_allocator = build_wal_options_allocator(&options.wal, kv_backend.clone())
            .await
            .context(BuildWalOptionsAllocatorSnafu)?;
        let wal_options_allocator = Arc::new(wal_options_allocator);
        let is_remote_wal = wal_options_allocator.is_remote_wal();
        let table_metadata_allocator = table_metadata_allocator.unwrap_or_else(|| {
            let sequence = Arc::new(
                SequenceBuilder::new(TABLE_ID_SEQ, kv_backend.clone())
                    .initial(MIN_USER_TABLE_ID as u64)
                    .step(10)
                    .build(),
            );
            let peer_allocator = Arc::new(MetasrvPeerAllocator::new(
                selector_ctx.clone(),
                selector.clone(),
            ));
            Arc::new(TableMetadataAllocator::with_peer_allocator(
                sequence,
                wal_options_allocator.clone(),
                peer_allocator,
            ))
        });

        let flow_metadata_allocator = {
            // for now flownode just use round-robin selector
            let flow_selector = RoundRobinSelector::new(SelectTarget::Flownode);
            let flow_selector_ctx = selector_ctx.clone();
            let peer_allocator = Arc::new(FlowPeerAllocator::new(
                flow_selector_ctx,
                Arc::new(flow_selector),
            ));
            let seq = Arc::new(
                SequenceBuilder::new(FLOW_ID_SEQ, kv_backend.clone())
                    .initial(MIN_USER_FLOW_ID as u64)
                    .step(10)
                    .build(),
            );

            Arc::new(FlowMetadataAllocator::with_peer_allocator(
                seq,
                peer_allocator,
            ))
        };
        let flow_state_handler =
            FlowStateHandler::new(FlowStateManager::new(in_memory.clone().as_kv_backend_ref()));

        let memory_region_keeper = Arc::new(MemoryRegionKeeper::default());
        let node_manager = node_manager.unwrap_or_else(|| {
            let datanode_client_channel_config = ChannelConfig::new()
                .timeout(options.datanode.client.timeout)
                .connect_timeout(options.datanode.client.connect_timeout)
                .tcp_nodelay(options.datanode.client.tcp_nodelay);
            Arc::new(NodeClients::new(datanode_client_channel_config))
        });
        let cache_invalidator = Arc::new(MetasrvCacheInvalidator::new(
            mailbox.clone(),
            MetasrvInfo {
                server_addr: options.server_addr.clone(),
            },
        ));
        let peer_lookup_service = Arc::new(MetaPeerLookupService::new(meta_peer_client.clone()));
        if !is_remote_wal && options.enable_region_failover {
            return error::UnexpectedSnafu {
                violated: "Region failover is not supported in the local WAL implementation!",
            }
            .fail();
        }

        let (tx, rx) = RegionSupervisor::channel();
        let (region_failure_detector_controller, region_supervisor_ticker): (
            RegionFailureDetectorControllerRef,
            Option<std::sync::Arc<RegionSupervisorTicker>>,
        ) = if options.enable_region_failover && is_remote_wal {
            (
                Arc::new(RegionFailureDetectorControl::new(tx.clone())) as _,
                Some(Arc::new(RegionSupervisorTicker::new(
                    DEFAULT_TICK_INTERVAL,
                    tx.clone(),
                ))),
            )
        } else {
            (Arc::new(NoopRegionFailureDetectorControl) as _, None as _)
        };

        // region migration manager
        let region_migration_manager = Arc::new(RegionMigrationManager::new(
            procedure_manager.clone(),
            DefaultContextFactory::new(
                table_metadata_manager.clone(),
                memory_region_keeper.clone(),
                region_failure_detector_controller.clone(),
                mailbox.clone(),
                options.server_addr.clone(),
                cache_invalidator.clone(),
            ),
        ));
        region_migration_manager.try_start()?;

        let region_failover_handler = if options.enable_region_failover && is_remote_wal {
            let region_supervisor = RegionSupervisor::new(
                rx,
                options.failure_detector,
                selector_ctx.clone(),
                selector.clone(),
                region_migration_manager.clone(),
                maintenance_mode_manager.clone(),
                peer_lookup_service.clone(),
            );

            Some(RegionFailureHandler::new(
                region_supervisor,
                HeartbeatAcceptor::new(tx),
            ))
        } else {
            None
        };

        let leader_region_registry = Arc::new(LeaderRegionRegistry::default());
        let ddl_manager = Arc::new(
            DdlManager::try_new(
                DdlContext {
                    node_manager,
                    cache_invalidator: cache_invalidator.clone(),
                    memory_region_keeper: memory_region_keeper.clone(),
                    leader_region_registry: leader_region_registry.clone(),
                    table_metadata_manager: table_metadata_manager.clone(),
                    table_metadata_allocator: table_metadata_allocator.clone(),
                    flow_metadata_manager: flow_metadata_manager.clone(),
                    flow_metadata_allocator: flow_metadata_allocator.clone(),
                    region_failure_detector_controller,
                },
                procedure_manager.clone(),
                true,
            )
            .context(error::InitDdlManagerSnafu)?,
        );

        // remote WAL prune ticker and manager
        let wal_prune_ticker = if is_remote_wal && options.wal.enable_active_wal_pruning() {
            let (tx, rx) = WalPruneManager::channel();
            // Safety: Must be remote WAL.
            let remote_wal_options = options.wal.remote_wal_options().unwrap();
            let kafka_client = build_kafka_client(remote_wal_options)
                .await
                .context(error::BuildKafkaClientSnafu)?;
            let wal_prune_context = WalPruneContext {
                client: Arc::new(kafka_client),
                table_metadata_manager: table_metadata_manager.clone(),
                leader_region_registry: leader_region_registry.clone(),
                server_addr: options.server_addr.clone(),
                mailbox: mailbox.clone(),
            };
            let wal_prune_manager = WalPruneManager::new(
                table_metadata_manager.clone(),
                remote_wal_options.auto_prune_parallelism,
                rx,
                procedure_manager.clone(),
                wal_prune_context,
                remote_wal_options.trigger_flush_threshold,
            );
            // Start manager in background. Ticker will be started in the main thread to send ticks.
            wal_prune_manager.try_start().await?;
            let wal_prune_ticker = Arc::new(WalPruneTicker::new(
                remote_wal_options.auto_prune_interval,
                tx.clone(),
            ));
            Some(wal_prune_ticker)
        } else {
            None
        };

        let customized_region_lease_renewer = plugins
            .as_ref()
            .and_then(|plugins| plugins.get::<CustomizedRegionLeaseRenewerRef>());

        let handler_group_builder = match handler_group_builder {
            Some(handler_group_builder) => handler_group_builder,
            None => {
                let region_lease_handler = RegionLeaseHandler::new(
                    distributed_time_constants::REGION_LEASE_SECS,
                    table_metadata_manager.clone(),
                    memory_region_keeper.clone(),
                    customized_region_lease_renewer,
                );

                HeartbeatHandlerGroupBuilder::new(pushers)
                    .with_plugins(plugins.clone())
                    .with_region_failure_handler(region_failover_handler)
                    .with_region_lease_handler(Some(region_lease_handler))
                    .with_flush_stats_factor(Some(options.flush_stats_factor))
                    .with_flow_state_handler(Some(flow_state_handler))
                    .add_default_handlers()
            }
        };

        let enable_telemetry = options.enable_telemetry;
        let metasrv_home = options.data_home.to_string();

        Ok(Metasrv {
            state,
            started,
            start_time_ms: common_time::util::current_time_millis() as u64,
            options,
            in_memory,
            kv_backend,
            leader_cached_kv_backend,
            meta_peer_client: meta_peer_client.clone(),
            selector,
            // TODO(jeremy): We do not allow configuring the flow selector.
            flow_selector: Arc::new(RoundRobinSelector::new(SelectTarget::Flownode)),
            handler_group: RwLock::new(None),
            handler_group_builder: Mutex::new(Some(handler_group_builder)),
            election,
            procedure_manager,
            mailbox,
            procedure_executor: ddl_manager,
            wal_options_allocator,
            table_metadata_manager,
            maintenance_mode_manager,
            greptimedb_telemetry_task: get_greptimedb_telemetry_task(
                Some(metasrv_home),
                meta_peer_client,
                enable_telemetry,
            )
            .await,
            plugins: plugins.unwrap_or_else(Plugins::default),
            memory_region_keeper,
            region_migration_manager,
            region_supervisor_ticker,
            cache_invalidator,
            leader_region_registry,
            wal_prune_ticker,
        })
    }
}

fn build_default_meta_peer_client(
    election: &Option<ElectionRef>,
    in_memory: &ResettableKvBackendRef,
) -> MetaPeerClientRef {
    MetaPeerClientBuilder::default()
        .election(election.clone())
        .in_memory(in_memory.clone())
        .build()
        .map(Arc::new)
        // Safety: all required fields set at initialization
        .unwrap()
}

fn build_mailbox(kv_backend: &KvBackendRef, pushers: &Pushers) -> MailboxRef {
    let mailbox_sequence = SequenceBuilder::new("heartbeat_mailbox", kv_backend.clone())
        .initial(1)
        .step(100)
        .build();

    HeartbeatMailbox::create(pushers.clone(), mailbox_sequence)
}

fn build_procedure_manager(
    options: &MetasrvOptions,
    kv_backend: &KvBackendRef,
) -> ProcedureManagerRef {
    let manager_config = ManagerConfig {
        max_retry_times: options.procedure.max_retry_times,
        retry_delay: options.procedure.retry_delay,
        max_running_procedures: options.procedure.max_running_procedures,
        ..Default::default()
    };
    let kv_state_store = Arc::new(
        KvStateStore::new(kv_backend.clone()).with_max_value_size(
            options
                .procedure
                .max_metadata_value_size
                .map(|v| v.as_bytes() as usize),
        ),
    );

    Arc::new(LocalManager::new(
        manager_config,
        kv_state_store.clone(),
        kv_state_store,
    ))
}

impl Default for MetasrvBuilder {
    fn default() -> Self {
        Self::new()
    }
}
