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

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use client::client_manager::NodeClients;
use client::inserter::InsertOptions;
use common_base::Plugins;
use common_catalog::consts::{MIN_USER_FLOW_ID, MIN_USER_TABLE_ID};
use common_event_recorder::{DEFAULT_COMPACTION_TIME_WINDOW, EventRecorderImpl, EventRecorderRef};
use common_grpc::channel_manager::ChannelConfig;
use common_meta::ddl::flow_meta::FlowMetadataAllocator;
use common_meta::ddl::table_meta::{TableMetadataAllocator, TableMetadataAllocatorRef};
use common_meta::ddl::{
    DdlContext, NoopRegionFailureDetectorControl, RegionFailureDetectorControllerRef,
};
use common_meta::ddl_manager::{DdlManager, DdlManagerConfiguratorRef};
use common_meta::distributed_time_constants::default_distributed_time_constants;
use common_meta::flow_rpc::FlowRpcRef;
use common_meta::key::TableMetadataManager;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::flow::flow_state::FlowStateManager;
use common_meta::key::runtime_switch::{RuntimeSwitchManager, RuntimeSwitchManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
use common_meta::reconciliation::manager::ReconciliationManager;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::region_registry::LeaderRegionRegistry;
use common_meta::region_rpc::RegionRpcRef;
use common_meta::sequence::SequenceBuilder;
use common_meta::state_store::KvStateStore;
use common_meta::stats::topic::TopicStatsRegistry;
use common_meta::wal_provider::{build_kafka_client, build_wal_provider};
use common_procedure::ProcedureManagerRef;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_stat::ResourceStatImpl;
use common_telemetry::{info, warn};
use snafu::{ResultExt, ensure};
use store_api::storage::MAX_REGION_SEQ;

use crate::bootstrap::build_default_meta_peer_client;
use crate::cache_invalidator::MetasrvCacheInvalidator;
use crate::cluster::MetaPeerClientRef;
use crate::error::{self, BuildWalProviderSnafu, OtherSnafu, Result};
use crate::events::EventHandlerImpl;
use crate::gc::GcScheduler;
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::handler::failure_handler::RegionFailureHandler;
use crate::handler::flow_state_handler::FlowStateHandler;
use crate::handler::persist_stats_handler::PersistStatsHandler;
use crate::handler::region_lease_handler::{CustomizedRegionLeaseRenewerRef, RegionLeaseHandler};
use crate::handler::{HeartbeatHandlerGroupBuilder, HeartbeatMailbox, Pushers};
use crate::metasrv::{
    ElectionRef, FLOW_ID_SEQ, METASRV_DATA_DIR, Metasrv, MetasrvInfo, MetasrvOptions,
    RegionStatAwareSelectorRef, SelectTarget, SelectorContext, SelectorRef, TABLE_ID_SEQ,
};
use crate::peer::MetasrvPeerAllocator;
use crate::procedure::region_migration::DefaultContextFactory;
use crate::procedure::region_migration::manager::RegionMigrationManager;
use crate::procedure::repartition::DefaultRepartitionProcedureFactory;
use crate::procedure::wal_prune::Context as WalPruneContext;
use crate::procedure::wal_prune::manager::{WalPruneManager, WalPruneTicker};
use crate::region::flush_trigger::RegionFlushTrigger;
use crate::region::supervisor::{
    DEFAULT_INITIALIZATION_RETRY_PERIOD, DEFAULT_TICK_INTERVAL, HeartbeatAcceptor,
    RegionFailureDetectorControl, RegionSupervisor, RegionSupervisorSelector,
    RegionSupervisorTicker,
};
use crate::selector::lease_based::LeaseBasedSelector;
use crate::selector::round_robin::RoundRobinSelector;
use crate::service::mailbox::MailboxRef;
use crate::service::store::cached_kv::LeaderCachedKvBackend;
use crate::state::State;
use crate::utils::insert_forwarder::InsertForwarder;

/// The time window for twcs compaction of the region stats table.
const REGION_STATS_TABLE_TWCS_COMPACTION_TIME_WINDOW: Duration = Duration::from_days(1);

// TODO(fys): try use derive_builder macro
pub struct MetasrvBuilder {
    options: Option<MetasrvOptions>,
    kv_backend: Option<KvBackendRef>,
    in_memory: Option<ResettableKvBackendRef>,
    selector: Option<SelectorRef>,
    handler_group_builder: Option<HeartbeatHandlerGroupBuilder>,
    election: Option<ElectionRef>,
    meta_peer_client: Option<MetaPeerClientRef>,
    region_rpc: Option<RegionRpcRef>,
    flow_rpc: Option<FlowRpcRef>,
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
            region_rpc: None,
            flow_rpc: None,
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

    pub fn region_rpc(mut self, region_rpc: RegionRpcRef) -> Self {
        self.region_rpc = Some(region_rpc);
        self
    }

    pub fn flow_rpc(mut self, flow_rpc: FlowRpcRef) -> Self {
        self.flow_rpc = Some(flow_rpc);
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
        let MetasrvBuilder {
            election,
            meta_peer_client,
            options,
            kv_backend,
            in_memory,
            selector,
            handler_group_builder,
            region_rpc,
            flow_rpc,
            plugins,
            table_metadata_allocator,
        } = self;

        let options = options.unwrap_or_default();

        let kv_backend = kv_backend.unwrap_or_else(|| Arc::new(MemoryKvBackend::new()));
        let in_memory = in_memory.unwrap_or_else(|| Arc::new(MemoryKvBackend::new()));

        let state = Arc::new(RwLock::new(match election {
            None => State::leader(options.grpc.server_addr.clone(), true),
            Some(_) => State::follower(options.grpc.server_addr.clone()),
        }));

        let leader_cached_kv_backend = Arc::new(LeaderCachedKvBackend::new(
            state.clone(),
            kv_backend.clone(),
        ));

        let meta_peer_client = meta_peer_client
            .unwrap_or_else(|| build_default_meta_peer_client(&election, &in_memory));

        let event_inserter = Box::new(InsertForwarder::new(
            meta_peer_client.clone(),
            Some(InsertOptions {
                ttl: options.event_recorder.ttl,
                append_mode: true,
                twcs_compaction_time_window: Some(DEFAULT_COMPACTION_TIME_WINDOW),
            }),
        ));
        // Builds the event recorder to record important events and persist them as the system table.
        let event_recorder = Arc::new(EventRecorderImpl::new(Box::new(EventHandlerImpl::new(
            event_inserter,
        ))));

        let selector = selector.unwrap_or_else(|| Arc::new(LeaseBasedSelector));
        let pushers = Pushers::default();
        let mailbox = build_mailbox(&kv_backend, &pushers);
        let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend.clone()));
        let procedure_manager = build_procedure_manager(
            &options,
            &kv_backend,
            &runtime_switch_manager,
            event_recorder,
        );

        let table_metadata_manager = Arc::new(TableMetadataManager::new(
            leader_cached_kv_backend.clone() as _,
        ));
        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(
            leader_cached_kv_backend.clone() as _,
        ));

        let selector_ctx = SelectorContext {
            peer_discovery: meta_peer_client.clone(),
        };

        let wal_provider = build_wal_provider(&options.wal, kv_backend.clone())
            .await
            .context(BuildWalProviderSnafu)?;
        let wal_provider = Arc::new(wal_provider);
        let is_remote_wal = wal_provider.is_remote_wal();
        let table_metadata_allocator = table_metadata_allocator.unwrap_or_else(|| {
            let sequence = Arc::new(
                SequenceBuilder::new(TABLE_ID_SEQ, kv_backend.clone())
                    .initial(MIN_USER_TABLE_ID as u64)
                    .step(10)
                    .build(),
            );
            let peer_allocator = Arc::new(
                MetasrvPeerAllocator::new(selector_ctx.clone(), selector.clone())
                    .with_max_items(MAX_REGION_SEQ),
            );
            Arc::new(TableMetadataAllocator::with_peer_allocator(
                sequence,
                wal_provider.clone(),
                peer_allocator,
            ))
        });
        let table_id_allocator = table_metadata_allocator.table_id_allocator();

        let flow_selector =
            Arc::new(RoundRobinSelector::new(SelectTarget::Flownode)) as SelectorRef;

        let flow_metadata_allocator = {
            // for now flownode just use round-robin selector
            let flow_selector_ctx = selector_ctx.clone();
            let peer_allocator = Arc::new(MetasrvPeerAllocator::new(
                flow_selector_ctx,
                flow_selector.clone(),
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
        let datanode_client_channel_config = ChannelConfig::new()
            .timeout(Some(options.datanode.client.timeout))
            .connect_timeout(options.datanode.client.connect_timeout)
            .tcp_nodelay(options.datanode.client.tcp_nodelay);
        let default_node_clients = Arc::new(NodeClients::new(datanode_client_channel_config));
        let region_rpc = region_rpc.unwrap_or_else(|| default_node_clients.clone() as _);
        let flow_rpc = flow_rpc.unwrap_or_else(|| default_node_clients.clone() as _);
        let cache_invalidator = Arc::new(MetasrvCacheInvalidator::new(
            mailbox.clone(),
            MetasrvInfo {
                server_addr: options.grpc.server_addr.clone(),
            },
        ));

        if !is_remote_wal && options.enable_region_failover {
            ensure!(
                options.allow_region_failover_on_local_wal,
                error::UnexpectedSnafu {
                    violated: "Region failover is not supported in the local WAL implementation!
                    If you want to enable region failover for local WAL, please set `allow_region_failover_on_local_wal` to true.",
                }
            );
            if options.allow_region_failover_on_local_wal {
                warn!(
                    "Region failover is force enabled in the local WAL implementation! This may lead to data loss during failover!"
                );
            }
        }

        let (tx, rx) = RegionSupervisor::channel();
        let (region_failure_detector_controller, region_supervisor_ticker): (
            RegionFailureDetectorControllerRef,
            Option<std::sync::Arc<RegionSupervisorTicker>>,
        ) = if options.enable_region_failover {
            (
                Arc::new(RegionFailureDetectorControl::new(tx.clone())) as _,
                Some(Arc::new(RegionSupervisorTicker::new(
                    DEFAULT_TICK_INTERVAL,
                    options.region_failure_detector_initialization_delay,
                    DEFAULT_INITIALIZATION_RETRY_PERIOD,
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
                in_memory.clone(),
                table_metadata_manager.clone(),
                memory_region_keeper.clone(),
                region_failure_detector_controller.clone(),
                mailbox.clone(),
                options.grpc.server_addr.clone(),
                cache_invalidator.clone(),
            ),
        ));
        region_migration_manager.try_start()?;
        let region_supervisor_selector = plugins
            .as_ref()
            .and_then(|plugins| plugins.get::<RegionStatAwareSelectorRef>());

        let supervisor_selector = match region_supervisor_selector {
            Some(selector) => {
                info!("Using region stat aware selector");
                RegionSupervisorSelector::RegionStatAwareSelector(selector)
            }
            None => RegionSupervisorSelector::NaiveSelector(selector.clone()),
        };

        let region_failover_handler = if options.enable_region_failover {
            let region_supervisor = RegionSupervisor::new(
                rx,
                options.failure_detector,
                selector_ctx.clone(),
                supervisor_selector,
                region_migration_manager.clone(),
                runtime_switch_manager.clone(),
                meta_peer_client.clone(),
                leader_cached_kv_backend.clone(),
            )
            .with_state(state.clone());

            Some(RegionFailureHandler::new(
                region_supervisor,
                HeartbeatAcceptor::new(tx),
            ))
        } else {
            None
        };

        let leader_region_registry = Arc::new(LeaderRegionRegistry::default());
        let topic_stats_registry = Arc::new(TopicStatsRegistry::default());

        let ddl_context = DdlContext {
            region_rpc: region_rpc.clone(),
            flow_rpc: flow_rpc.clone(),
            cache_invalidator: cache_invalidator.clone(),
            memory_region_keeper: memory_region_keeper.clone(),
            leader_region_registry: leader_region_registry.clone(),
            table_metadata_manager: table_metadata_manager.clone(),
            table_metadata_allocator: table_metadata_allocator.clone(),
            flow_metadata_manager: flow_metadata_manager.clone(),
            flow_metadata_allocator: flow_metadata_allocator.clone(),
            region_failure_detector_controller,
        };
        let procedure_manager_c = procedure_manager.clone();
        let repartition_procedure_factory = Arc::new(DefaultRepartitionProcedureFactory::new(
            mailbox.clone(),
            options.grpc.server_addr.clone(),
        ));
        let ddl_manager = DdlManager::try_new(
            ddl_context,
            procedure_manager_c,
            repartition_procedure_factory,
            true,
        )
        .context(error::InitDdlManagerSnafu)?;

        let ddl_manager = if let Some(configurator) = plugins
            .as_ref()
            .and_then(|p| p.get::<DdlManagerConfiguratorRef<DdlManagerConfigureContext>>())
        {
            let ctx = DdlManagerConfigureContext {
                kv_backend: kv_backend.clone(),
                meta_peer_client: meta_peer_client.clone(),
            };
            configurator
                .configure(ddl_manager, ctx)
                .await
                .context(OtherSnafu)?
        } else {
            ddl_manager
        };

        let ddl_manager = Arc::new(ddl_manager);

        let region_flush_ticker = if is_remote_wal {
            let remote_wal_options = options.wal.remote_wal_options().unwrap();
            let (region_flush_trigger, region_flush_ticker) = RegionFlushTrigger::new(
                table_metadata_manager.clone(),
                leader_region_registry.clone(),
                topic_stats_registry.clone(),
                mailbox.clone(),
                options.grpc.server_addr.clone(),
                remote_wal_options.flush_trigger_size,
                remote_wal_options.checkpoint_trigger_size,
            );
            region_flush_trigger.try_start()?;

            Some(Arc::new(region_flush_ticker))
        } else {
            None
        };

        // remote WAL prune ticker and manager
        let wal_prune_ticker = if is_remote_wal && options.wal.enable_active_wal_pruning() {
            let (tx, rx) = WalPruneManager::channel();
            // Safety: Must be remote WAL.
            let remote_wal_options = options.wal.remote_wal_options().unwrap();
            let kafka_client = build_kafka_client(&remote_wal_options.connection)
                .await
                .context(error::BuildKafkaClientSnafu)?;
            let wal_prune_context = WalPruneContext {
                client: Arc::new(kafka_client),
                table_metadata_manager: table_metadata_manager.clone(),
                leader_region_registry: leader_region_registry.clone(),
            };
            let wal_prune_manager = WalPruneManager::new(
                remote_wal_options.auto_prune_parallelism,
                rx,
                procedure_manager.clone(),
                wal_prune_context,
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

        let gc_ticker = if options.gc.enable {
            let (gc_scheduler, gc_ticker) = GcScheduler::new_with_config(
                table_metadata_manager.clone(),
                procedure_manager.clone(),
                meta_peer_client.clone(),
                mailbox.clone(),
                options.grpc.server_addr.clone(),
                options.gc.clone(),
            )?;
            gc_scheduler.try_start()?;

            Some(Arc::new(gc_ticker))
        } else {
            None
        };

        let customized_region_lease_renewer = plugins
            .as_ref()
            .and_then(|plugins| plugins.get::<CustomizedRegionLeaseRenewerRef>());

        let persist_region_stats_handler = if !options.stats_persistence.ttl.is_zero() {
            let inserter = Box::new(InsertForwarder::new(
                meta_peer_client.clone(),
                Some(InsertOptions {
                    ttl: options.stats_persistence.ttl,
                    append_mode: true,
                    twcs_compaction_time_window: Some(
                        REGION_STATS_TABLE_TWCS_COMPACTION_TIME_WINDOW,
                    ),
                }),
            ));

            Some(PersistStatsHandler::new(
                inserter,
                options.stats_persistence.interval,
            ))
        } else {
            None
        };

        let handler_group_builder = match handler_group_builder {
            Some(handler_group_builder) => handler_group_builder,
            None => {
                let region_lease_handler = RegionLeaseHandler::new(
                    default_distributed_time_constants().region_lease.as_secs(),
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
                    .with_persist_stats_handler(persist_region_stats_handler)
                    .add_default_handlers()
            }
        };

        let enable_telemetry = options.enable_telemetry;
        let metasrv_home = Path::new(&options.data_home)
            .join(METASRV_DATA_DIR)
            .to_string_lossy()
            .to_string();

        let reconciliation_manager = Arc::new(ReconciliationManager::new(
            region_rpc.clone(),
            table_metadata_manager.clone(),
            cache_invalidator.clone(),
            procedure_manager.clone(),
        ));
        reconciliation_manager
            .try_start()
            .context(error::InitReconciliationManagerSnafu)?;

        let mut resource_stat = ResourceStatImpl::default();
        resource_stat.start_collect_cpu_usage();

        Ok(Metasrv {
            state,
            started: Arc::new(AtomicBool::new(false)),
            start_time_ms: common_time::util::current_time_millis() as u64,
            options,
            in_memory,
            kv_backend,
            leader_cached_kv_backend,
            meta_peer_client: meta_peer_client.clone(),
            selector,
            selector_ctx,
            // TODO(jeremy): We do not allow configuring the flow selector.
            flow_selector,
            handler_group: RwLock::new(None),
            handler_group_builder: Mutex::new(Some(handler_group_builder)),
            election,
            procedure_manager,
            mailbox,
            ddl_manager,
            wal_provider,
            table_metadata_manager,
            runtime_switch_manager,
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
            region_flush_ticker,
            table_id_allocator,
            reconciliation_manager,
            topic_stats_registry,
            resource_stat: Arc::new(resource_stat),
            gc_ticker,
        })
    }
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
    runtime_switch_manager: &RuntimeSwitchManagerRef,
    event_recorder: EventRecorderRef,
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
        Some(runtime_switch_manager.clone()),
        Some(event_recorder),
    ))
}

impl Default for MetasrvBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The context for [`DdlManagerConfiguratorRef`].
pub struct DdlManagerConfigureContext {
    pub kv_backend: KvBackendRef,
    pub meta_peer_client: MetaPeerClientRef,
}
