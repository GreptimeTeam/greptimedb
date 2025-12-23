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

pub mod builder;

use std::fmt::{self, Display};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use clap::ValueEnum;
use common_base::Plugins;
use common_base::readable_size::ReadableSize;
use common_config::{Configurable, DEFAULT_DATA_HOME};
use common_event_recorder::EventRecorderOptions;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl_manager::DdlManagerRef;
use common_meta::distributed_time_constants::{self, default_distributed_time_constants};
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackend, ResettableKvBackendRef};
use common_meta::leadership_notifier::{
    LeadershipChangeNotifier, LeadershipChangeNotifierCustomizerRef,
};
use common_meta::node_expiry_listener::NodeExpiryListener;
use common_meta::peer::{Peer, PeerDiscoveryRef};
use common_meta::reconciliation::manager::ReconciliationManagerRef;
use common_meta::region_keeper::MemoryRegionKeeperRef;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_meta::sequence::SequenceRef;
use common_meta::stats::topic::TopicStatsRegistryRef;
use common_meta::wal_options_allocator::WalOptionsAllocatorRef;
use common_options::datanode::DatanodeClientOptions;
use common_options::memory::MemoryOptions;
use common_procedure::ProcedureManagerRef;
use common_procedure::options::ProcedureConfig;
use common_stat::ResourceStatRef;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_telemetry::{error, info, warn};
use common_time::util::DefaultSystemTimer;
use common_wal::config::MetasrvWalConfig;
use serde::{Deserialize, Serialize};
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;
use servers::tls::TlsOption;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::broadcast::error::RecvError;

use crate::cluster::MetaPeerClientRef;
use crate::discovery;
use crate::election::{Election, LeaderChangeMessage};
use crate::error::{
    self, InitMetadataSnafu, KvBackendSnafu, Result, StartProcedureManagerSnafu,
    StartTelemetryTaskSnafu, StopProcedureManagerSnafu,
};
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::gc::{GcSchedulerOptions, GcTickerRef};
use crate::handler::{HeartbeatHandlerGroupBuilder, HeartbeatHandlerGroupRef};
use crate::procedure::ProcedureManagerListenerAdapter;
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;
use crate::procedure::wal_prune::manager::WalPruneTickerRef;
use crate::pubsub::{PublisherRef, SubscriptionManagerRef};
use crate::region::flush_trigger::RegionFlushTickerRef;
use crate::region::supervisor::RegionSupervisorTickerRef;
use crate::selector::{RegionStatAwareSelector, Selector, SelectorType};
use crate::service::mailbox::MailboxRef;
use crate::service::store::cached_kv::LeaderCachedKvBackend;
use crate::state::{StateRef, become_follower, become_leader};

pub const TABLE_ID_SEQ: &str = "table_id";
pub const FLOW_ID_SEQ: &str = "flow_id";
pub const METASRV_DATA_DIR: &str = "metasrv";

// The datastores that implements metadata kvbackend.
#[derive(Clone, Debug, PartialEq, Serialize, Default, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum BackendImpl {
    // Etcd as metadata storage.
    #[default]
    EtcdStore,
    // In memory metadata storage - mostly used for testing.
    MemoryStore,
    #[cfg(feature = "pg_kvbackend")]
    // Postgres as metadata storage.
    PostgresStore,
    #[cfg(feature = "mysql_kvbackend")]
    // MySql as metadata storage.
    MysqlStore,
}

/// Configuration options for the stats persistence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatsPersistenceOptions {
    /// TTL for the stats table that will be used to store the stats.
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
    /// The interval to persist the stats.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

impl Default for StatsPersistenceOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::ZERO,
            interval: Duration::from_mins(10),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct BackendClientOptions {
    #[serde(with = "humantime_serde")]
    pub keep_alive_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub keep_alive_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
}

impl Default for BackendClientOptions {
    fn default() -> Self {
        Self {
            keep_alive_interval: Duration::from_secs(10),
            keep_alive_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetasrvOptions {
    /// The address the server listens on.
    #[deprecated(note = "Use grpc.bind_addr instead")]
    pub bind_addr: String,
    /// The address the server advertises to the clients.
    #[deprecated(note = "Use grpc.server_addr instead")]
    pub server_addr: String,
    /// The address of the store, e.g., etcd.
    pub store_addrs: Vec<String>,
    /// TLS configuration for kv store backend (PostgreSQL/MySQL)
    /// Only applicable when using PostgreSQL or MySQL as the metadata store
    #[serde(default)]
    pub backend_tls: Option<TlsOption>,
    /// The backend client options.
    /// Currently, only applicable when using etcd as the metadata store.
    #[serde(default)]
    pub backend_client: BackendClientOptions,
    /// The type of selector.
    pub selector: SelectorType,
    /// Whether to enable region failover.
    pub enable_region_failover: bool,
    /// The base heartbeat interval.
    ///
    /// This value is used to calculate the distributed time constants for components.
    /// e.g., the region lease time is `heartbeat_interval * 3 + Duration::from_secs(1)`.
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// The delay before starting region failure detection.
    /// This delay helps prevent Metasrv from triggering unnecessary region failovers before all Datanodes are fully started.
    /// Especially useful when the cluster is not deployed with GreptimeDB Operator and maintenance mode is not enabled.
    #[serde(with = "humantime_serde")]
    pub region_failure_detector_initialization_delay: Duration,
    /// Whether to allow region failover on local WAL.
    ///
    /// If it's true, the region failover will be allowed even if the local WAL is used.
    /// Note that this option is not recommended to be set to true, because it may lead to data loss during failover.
    pub allow_region_failover_on_local_wal: bool,
    pub grpc: GrpcOptions,
    /// The HTTP server options.
    pub http: HttpOptions,
    /// The logging options.
    pub logging: LoggingOptions,
    /// The procedure options.
    pub procedure: ProcedureConfig,
    /// The failure detector options.
    pub failure_detector: PhiAccrualFailureDetectorOptions,
    /// The datanode options.
    pub datanode: DatanodeClientOptions,
    /// Whether to enable telemetry.
    pub enable_telemetry: bool,
    /// The data home directory.
    pub data_home: String,
    /// The WAL options.
    pub wal: MetasrvWalConfig,
    /// The store key prefix. If it is not empty, all keys in the store will be prefixed with it.
    /// This is useful when multiple metasrv clusters share the same store.
    pub store_key_prefix: String,
    /// The max operations per txn
    ///
    /// This value is usually limited by which store is used for the `KvBackend`.
    /// For example, if using etcd, this value should ensure that it is less than
    /// or equal to the `--max-txn-ops` option value of etcd.
    ///
    /// TODO(jeremy): Currently, this option only affects the etcd store, but it may
    /// also affect other stores in the future. In other words, each store needs to
    /// limit the number of operations in a txn because an infinitely large txn could
    /// potentially block other operations.
    pub max_txn_ops: usize,
    /// The factor that determines how often statistics should be flushed,
    /// based on the number of received heartbeats. When the number of heartbeats
    /// reaches this factor, a flush operation is triggered.
    pub flush_stats_factor: usize,
    /// The tracing options.
    pub tracing: TracingOptions,
    /// The memory options.
    pub memory: MemoryOptions,
    /// The datastore for kv metadata.
    pub backend: BackendImpl,
    #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
    /// Table name of rds kv backend.
    pub meta_table_name: String,
    #[cfg(feature = "pg_kvbackend")]
    /// Lock id for meta kv election. Only effect when using pg_kvbackend.
    pub meta_election_lock_id: u64,
    #[cfg(feature = "pg_kvbackend")]
    /// Optional PostgreSQL schema for metadata table (defaults to current search_path if empty).
    pub meta_schema_name: Option<String>,
    #[cfg(feature = "pg_kvbackend")]
    /// Automatically create PostgreSQL schema if it doesn't exist (default: true).
    pub auto_create_schema: bool,
    #[serde(with = "humantime_serde")]
    pub node_max_idle_time: Duration,
    /// The event recorder options.
    pub event_recorder: EventRecorderOptions,
    /// The stats persistence options.
    pub stats_persistence: StatsPersistenceOptions,
    /// The GC scheduler options.
    pub gc: GcSchedulerOptions,
}

impl fmt::Debug for MetasrvOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("MetasrvOptions");
        debug_struct
            .field("store_addrs", &self.sanitize_store_addrs())
            .field("backend_tls", &self.backend_tls)
            .field("selector", &self.selector)
            .field("enable_region_failover", &self.enable_region_failover)
            .field(
                "allow_region_failover_on_local_wal",
                &self.allow_region_failover_on_local_wal,
            )
            .field("grpc", &self.grpc)
            .field("http", &self.http)
            .field("logging", &self.logging)
            .field("procedure", &self.procedure)
            .field("failure_detector", &self.failure_detector)
            .field("datanode", &self.datanode)
            .field("enable_telemetry", &self.enable_telemetry)
            .field("data_home", &self.data_home)
            .field("wal", &self.wal)
            .field("store_key_prefix", &self.store_key_prefix)
            .field("max_txn_ops", &self.max_txn_ops)
            .field("flush_stats_factor", &self.flush_stats_factor)
            .field("tracing", &self.tracing)
            .field("backend", &self.backend)
            .field("event_recorder", &self.event_recorder)
            .field("stats_persistence", &self.stats_persistence)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("backend_client", &self.backend_client);

        #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
        debug_struct.field("meta_table_name", &self.meta_table_name);

        #[cfg(feature = "pg_kvbackend")]
        debug_struct.field("meta_election_lock_id", &self.meta_election_lock_id);
        #[cfg(feature = "pg_kvbackend")]
        debug_struct.field("meta_schema_name", &self.meta_schema_name);

        debug_struct
            .field("node_max_idle_time", &self.node_max_idle_time)
            .finish()
    }
}

const DEFAULT_METASRV_ADDR_PORT: &str = "3002";

impl Default for MetasrvOptions {
    fn default() -> Self {
        Self {
            #[allow(deprecated)]
            bind_addr: String::new(),
            #[allow(deprecated)]
            server_addr: String::new(),
            store_addrs: vec!["127.0.0.1:2379".to_string()],
            backend_tls: None,
            selector: SelectorType::default(),
            enable_region_failover: false,
            heartbeat_interval: distributed_time_constants::BASE_HEARTBEAT_INTERVAL,
            region_failure_detector_initialization_delay: Duration::from_secs(10 * 60),
            allow_region_failover_on_local_wal: false,
            grpc: GrpcOptions {
                bind_addr: format!("127.0.0.1:{}", DEFAULT_METASRV_ADDR_PORT),
                ..Default::default()
            },
            http: HttpOptions::default(),
            logging: LoggingOptions::default(),
            procedure: ProcedureConfig {
                max_retry_times: 12,
                retry_delay: Duration::from_millis(500),
                // The etcd the maximum size of any request is 1.5 MiB
                // 1500KiB = 1536KiB (1.5MiB) - 36KiB (reserved size of key)
                max_metadata_value_size: Some(ReadableSize::kb(1500)),
                max_running_procedures: 128,
            },
            failure_detector: PhiAccrualFailureDetectorOptions::default(),
            datanode: DatanodeClientOptions::default(),
            enable_telemetry: true,
            data_home: DEFAULT_DATA_HOME.to_string(),
            wal: MetasrvWalConfig::default(),
            store_key_prefix: String::new(),
            max_txn_ops: 128,
            flush_stats_factor: 3,
            tracing: TracingOptions::default(),
            memory: MemoryOptions::default(),
            backend: BackendImpl::EtcdStore,
            #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
            meta_table_name: common_meta::kv_backend::DEFAULT_META_TABLE_NAME.to_string(),
            #[cfg(feature = "pg_kvbackend")]
            meta_election_lock_id: common_meta::kv_backend::DEFAULT_META_ELECTION_LOCK_ID,
            #[cfg(feature = "pg_kvbackend")]
            meta_schema_name: None,
            #[cfg(feature = "pg_kvbackend")]
            auto_create_schema: true,
            node_max_idle_time: Duration::from_secs(24 * 60 * 60),
            event_recorder: EventRecorderOptions::default(),
            stats_persistence: StatsPersistenceOptions::default(),
            gc: GcSchedulerOptions::default(),
            backend_client: BackendClientOptions::default(),
        }
    }
}

impl Configurable for MetasrvOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["wal.broker_endpoints", "store_addrs"])
    }
}

impl MetasrvOptions {
    fn sanitize_store_addrs(&self) -> Vec<String> {
        self.store_addrs
            .iter()
            .map(|addr| common_meta::kv_backend::util::sanitize_connection_string(addr))
            .collect()
    }
}

pub struct MetasrvInfo {
    pub server_addr: String,
}
#[derive(Clone)]
pub struct Context {
    pub server_addr: String,
    pub in_memory: ResettableKvBackendRef,
    pub kv_backend: KvBackendRef,
    pub leader_cached_kv_backend: ResettableKvBackendRef,
    pub meta_peer_client: MetaPeerClientRef,
    pub mailbox: MailboxRef,
    pub election: Option<ElectionRef>,
    pub is_infancy: bool,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub leader_region_registry: LeaderRegionRegistryRef,
    pub topic_stats_registry: TopicStatsRegistryRef,
}

impl Context {
    pub fn reset_in_memory(&self) {
        self.in_memory.reset();
        self.leader_region_registry.reset();
    }
}

/// The value of the leader. It is used to store the leader's address.
pub struct LeaderValue(pub String);

impl<T: AsRef<[u8]>> From<T> for LeaderValue {
    fn from(value: T) -> Self {
        let string = String::from_utf8_lossy(value.as_ref());
        Self(string.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetasrvNodeInfo {
    // The metasrv's address
    pub addr: String,
    // The node build version
    pub version: String,
    // The node build git commit hash
    pub git_commit: String,
    // The node start timestamp in milliseconds
    pub start_time_ms: u64,
    // The node total cpu millicores
    #[serde(default)]
    pub total_cpu_millicores: i64,
    // The node total memory bytes
    #[serde(default)]
    pub total_memory_bytes: i64,
    /// The node build cpu usage millicores
    #[serde(default)]
    pub cpu_usage_millicores: i64,
    /// The node build memory usage bytes
    #[serde(default)]
    pub memory_usage_bytes: i64,
    // The node hostname
    #[serde(default)]
    pub hostname: String,
}

// TODO(zyy17): Allow deprecated fields for backward compatibility. Remove this when the deprecated top-level fields are removed from the proto.
#[allow(deprecated)]
impl From<MetasrvNodeInfo> for api::v1::meta::MetasrvNodeInfo {
    fn from(node_info: MetasrvNodeInfo) -> Self {
        Self {
            peer: Some(api::v1::meta::Peer {
                addr: node_info.addr,
                ..Default::default()
            }),
            // TODO(zyy17): The following top-level fields are deprecated. They are kept for backward compatibility and will be removed in a future version.
            // New code should use the fields in `info.NodeInfo` instead.
            version: node_info.version.clone(),
            git_commit: node_info.git_commit.clone(),
            start_time_ms: node_info.start_time_ms,
            cpus: node_info.total_cpu_millicores as u32,
            memory_bytes: node_info.total_memory_bytes as u64,
            // The canonical location for node information.
            info: Some(api::v1::meta::NodeInfo {
                version: node_info.version,
                git_commit: node_info.git_commit,
                start_time_ms: node_info.start_time_ms,
                total_cpu_millicores: node_info.total_cpu_millicores,
                total_memory_bytes: node_info.total_memory_bytes,
                cpu_usage_millicores: node_info.cpu_usage_millicores,
                memory_usage_bytes: node_info.memory_usage_bytes,
                cpus: node_info.total_cpu_millicores as u32,
                memory_bytes: node_info.total_memory_bytes as u64,
                hostname: node_info.hostname,
            }),
        }
    }
}

#[derive(Clone, Copy)]
pub enum SelectTarget {
    Datanode,
    Flownode,
}

impl Display for SelectTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectTarget::Datanode => write!(f, "datanode"),
            SelectTarget::Flownode => write!(f, "flownode"),
        }
    }
}

#[derive(Clone)]
pub struct SelectorContext {
    pub peer_discovery: PeerDiscoveryRef,
}

pub type SelectorRef = Arc<dyn Selector<Context = SelectorContext, Output = Vec<Peer>>>;
pub type RegionStatAwareSelectorRef =
    Arc<dyn RegionStatAwareSelector<Context = SelectorContext, Output = Vec<(RegionId, Peer)>>>;
pub type ElectionRef = Arc<dyn Election<Leader = LeaderValue>>;

pub struct MetaStateHandler {
    subscribe_manager: Option<SubscriptionManagerRef>,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
    leader_cached_kv_backend: Arc<LeaderCachedKvBackend>,
    leadership_change_notifier: LeadershipChangeNotifier,
    mailbox: MailboxRef,
    state: StateRef,
}

impl MetaStateHandler {
    pub async fn on_leader_start(&self) {
        self.state.write().unwrap().next_state(become_leader(false));

        if let Err(e) = self.leader_cached_kv_backend.load().await {
            error!(e; "Failed to load kv into leader cache kv store");
        } else {
            self.state.write().unwrap().next_state(become_leader(true));
        }

        self.leadership_change_notifier
            .notify_on_leader_start()
            .await;

        self.greptimedb_telemetry_task.should_report(true);
    }

    pub async fn on_leader_stop(&self) {
        self.state.write().unwrap().next_state(become_follower());

        // Enforces the mailbox to clear all pushers.
        // The remaining heartbeat connections will be closed by the remote peer or keep-alive detection.
        self.mailbox.reset().await;
        self.leadership_change_notifier
            .notify_on_leader_stop()
            .await;

        // Suspends reporting.
        self.greptimedb_telemetry_task.should_report(false);

        if let Some(sub_manager) = self.subscribe_manager.clone() {
            info!("Leader changed, un_subscribe all");
            if let Err(e) = sub_manager.unsubscribe_all() {
                error!(e; "Failed to un_subscribe all");
            }
        }
    }
}

pub struct Metasrv {
    state: StateRef,
    started: Arc<AtomicBool>,
    start_time_ms: u64,
    options: MetasrvOptions,
    // It is only valid at the leader node and is used to temporarily
    // store some data that will not be persisted.
    in_memory: ResettableKvBackendRef,
    kv_backend: KvBackendRef,
    leader_cached_kv_backend: Arc<LeaderCachedKvBackend>,
    meta_peer_client: MetaPeerClientRef,
    // The selector is used to select a target datanode.
    selector: SelectorRef,
    selector_ctx: SelectorContext,
    // The flow selector is used to select a target flownode.
    flow_selector: SelectorRef,
    handler_group: RwLock<Option<HeartbeatHandlerGroupRef>>,
    handler_group_builder: Mutex<Option<HeartbeatHandlerGroupBuilder>>,
    election: Option<ElectionRef>,
    procedure_manager: ProcedureManagerRef,
    mailbox: MailboxRef,
    ddl_manager: DdlManagerRef,
    wal_options_allocator: WalOptionsAllocatorRef,
    table_metadata_manager: TableMetadataManagerRef,
    runtime_switch_manager: RuntimeSwitchManagerRef,
    memory_region_keeper: MemoryRegionKeeperRef,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
    region_migration_manager: RegionMigrationManagerRef,
    region_supervisor_ticker: Option<RegionSupervisorTickerRef>,
    cache_invalidator: CacheInvalidatorRef,
    leader_region_registry: LeaderRegionRegistryRef,
    topic_stats_registry: TopicStatsRegistryRef,
    wal_prune_ticker: Option<WalPruneTickerRef>,
    region_flush_ticker: Option<RegionFlushTickerRef>,
    table_id_sequence: SequenceRef,
    reconciliation_manager: ReconciliationManagerRef,
    resource_stat: ResourceStatRef,
    gc_ticker: Option<GcTickerRef>,

    plugins: Plugins,
}

impl Metasrv {
    pub async fn try_start(&self) -> Result<()> {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Metasrv already started");
            return Ok(());
        }

        let handler_group_builder =
            self.handler_group_builder
                .lock()
                .unwrap()
                .take()
                .context(error::UnexpectedSnafu {
                    violated: "expected heartbeat handler group builder",
                })?;
        *self.handler_group.write().unwrap() = Some(Arc::new(handler_group_builder.build()?));

        // Creates default schema if not exists
        self.table_metadata_manager
            .init()
            .await
            .context(InitMetadataSnafu)?;

        if let Some(election) = self.election() {
            let procedure_manager = self.procedure_manager.clone();
            let in_memory = self.in_memory.clone();
            let leader_cached_kv_backend = self.leader_cached_kv_backend.clone();
            let subscribe_manager = self.subscription_manager();
            let mut rx = election.subscribe_leader_change();
            let greptimedb_telemetry_task = self.greptimedb_telemetry_task.clone();
            greptimedb_telemetry_task
                .start()
                .context(StartTelemetryTaskSnafu)?;

            // Builds leadership change notifier.
            let mut leadership_change_notifier = LeadershipChangeNotifier::default();
            leadership_change_notifier.add_listener(self.wal_options_allocator.clone());
            leadership_change_notifier
                .add_listener(Arc::new(ProcedureManagerListenerAdapter(procedure_manager)));
            leadership_change_notifier.add_listener(Arc::new(NodeExpiryListener::new(
                self.options.node_max_idle_time,
                self.in_memory.clone(),
            )));
            if let Some(region_supervisor_ticker) = &self.region_supervisor_ticker {
                leadership_change_notifier.add_listener(region_supervisor_ticker.clone() as _);
            }
            if let Some(wal_prune_ticker) = &self.wal_prune_ticker {
                leadership_change_notifier.add_listener(wal_prune_ticker.clone() as _);
            }
            if let Some(region_flush_trigger) = &self.region_flush_ticker {
                leadership_change_notifier.add_listener(region_flush_trigger.clone() as _);
            }
            if let Some(gc_ticker) = &self.gc_ticker {
                leadership_change_notifier.add_listener(gc_ticker.clone() as _);
            }
            if let Some(customizer) = self.plugins.get::<LeadershipChangeNotifierCustomizerRef>() {
                customizer.customize(&mut leadership_change_notifier);
            }

            let state_handler = MetaStateHandler {
                greptimedb_telemetry_task,
                subscribe_manager,
                state: self.state.clone(),
                leader_cached_kv_backend: leader_cached_kv_backend.clone(),
                leadership_change_notifier,
                mailbox: self.mailbox.clone(),
            };
            let _handle = common_runtime::spawn_global(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            in_memory.reset();
                            leader_cached_kv_backend.reset();
                            info!("Leader's cache has bean cleared on leader change: {msg}");
                            match msg {
                                LeaderChangeMessage::Elected(_) => {
                                    state_handler.on_leader_start().await;
                                }
                                LeaderChangeMessage::StepDown(leader) => {
                                    error!("Leader :{:?} step down", leader);

                                    state_handler.on_leader_stop().await;
                                }
                            }
                        }
                        Err(RecvError::Closed) => {
                            error!("Not expected, is leader election loop still running?");
                            break;
                        }
                        Err(RecvError::Lagged(_)) => {
                            break;
                        }
                    }
                }

                state_handler.on_leader_stop().await;
            });

            // Register candidate and keep lease in background.
            {
                let election = election.clone();
                let started = self.started.clone();
                let node_info = self.node_info();
                let _handle = common_runtime::spawn_global(async move {
                    while started.load(Ordering::Acquire) {
                        let res = election.register_candidate(&node_info).await;
                        if let Err(e) = res {
                            warn!(e; "Metasrv register candidate error");
                        }
                    }
                });
            }

            // Campaign
            {
                let election = election.clone();
                let started = self.started.clone();
                let _handle = common_runtime::spawn_global(async move {
                    while started.load(Ordering::Acquire) {
                        let res = election.campaign().await;
                        if let Err(e) = res {
                            warn!(e; "Metasrv election error");
                        }
                        election.reset_campaign().await;
                        info!("Metasrv re-initiate election");
                    }
                    info!("Metasrv stopped");
                });
            }
        } else {
            warn!(
                "Ensure only one instance of Metasrv is running, as there is no election service."
            );

            if let Err(e) = self.wal_options_allocator.start().await {
                error!(e; "Failed to start wal options allocator");
            }
            // Always load kv into cached kv store.
            self.leader_cached_kv_backend
                .load()
                .await
                .context(KvBackendSnafu)?;
            self.procedure_manager
                .start()
                .await
                .context(StartProcedureManagerSnafu)?;
        }

        info!("Metasrv started");

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        if self
            .started
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            warn!("Metasrv already stopped");
            return Ok(());
        }

        self.procedure_manager
            .stop()
            .await
            .context(StopProcedureManagerSnafu)?;

        info!("Metasrv stopped");

        Ok(())
    }

    pub fn start_time_ms(&self) -> u64 {
        self.start_time_ms
    }

    pub fn resource_stat(&self) -> &ResourceStatRef {
        &self.resource_stat
    }

    pub fn node_info(&self) -> MetasrvNodeInfo {
        let build_info = common_version::build_info();
        MetasrvNodeInfo {
            addr: self.options().grpc.server_addr.clone(),
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            start_time_ms: self.start_time_ms(),
            total_cpu_millicores: self.resource_stat.get_total_cpu_millicores(),
            total_memory_bytes: self.resource_stat.get_total_memory_bytes(),
            cpu_usage_millicores: self.resource_stat.get_cpu_usage_millicores(),
            memory_usage_bytes: self.resource_stat.get_memory_usage_bytes(),
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        }
    }

    /// Looks up a datanode peer by peer_id, returning it only when it's alive.
    /// A datanode is considered alive when it's still within the lease period.
    pub(crate) async fn lookup_datanode_peer(&self, peer_id: u64) -> Result<Option<Peer>> {
        discovery::utils::alive_datanode(
            &DefaultSystemTimer,
            self.meta_peer_client.as_ref(),
            peer_id,
            default_distributed_time_constants().datanode_lease,
        )
        .await
    }

    pub fn options(&self) -> &MetasrvOptions {
        &self.options
    }

    pub fn in_memory(&self) -> &ResettableKvBackendRef {
        &self.in_memory
    }

    pub fn kv_backend(&self) -> &KvBackendRef {
        &self.kv_backend
    }

    pub fn meta_peer_client(&self) -> &MetaPeerClientRef {
        &self.meta_peer_client
    }

    pub fn selector(&self) -> &SelectorRef {
        &self.selector
    }

    pub fn selector_ctx(&self) -> &SelectorContext {
        &self.selector_ctx
    }

    pub fn flow_selector(&self) -> &SelectorRef {
        &self.flow_selector
    }

    pub fn handler_group(&self) -> Option<HeartbeatHandlerGroupRef> {
        self.handler_group.read().unwrap().clone()
    }

    pub fn election(&self) -> Option<&ElectionRef> {
        self.election.as_ref()
    }

    pub fn mailbox(&self) -> &MailboxRef {
        &self.mailbox
    }

    pub fn ddl_manager(&self) -> &DdlManagerRef {
        &self.ddl_manager
    }

    pub fn procedure_manager(&self) -> &ProcedureManagerRef {
        &self.procedure_manager
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn runtime_switch_manager(&self) -> &RuntimeSwitchManagerRef {
        &self.runtime_switch_manager
    }

    pub fn memory_region_keeper(&self) -> &MemoryRegionKeeperRef {
        &self.memory_region_keeper
    }

    pub fn region_migration_manager(&self) -> &RegionMigrationManagerRef {
        &self.region_migration_manager
    }

    pub fn publish(&self) -> Option<PublisherRef> {
        self.plugins.get::<PublisherRef>()
    }

    pub fn subscription_manager(&self) -> Option<SubscriptionManagerRef> {
        self.plugins.get::<SubscriptionManagerRef>()
    }

    pub fn table_id_sequence(&self) -> &SequenceRef {
        &self.table_id_sequence
    }

    pub fn reconciliation_manager(&self) -> &ReconciliationManagerRef {
        &self.reconciliation_manager
    }

    pub fn plugins(&self) -> &Plugins {
        &self.plugins
    }

    pub fn started(&self) -> Arc<AtomicBool> {
        self.started.clone()
    }

    #[inline]
    pub fn new_ctx(&self) -> Context {
        let server_addr = self.options().grpc.server_addr.clone();
        let in_memory = self.in_memory.clone();
        let kv_backend = self.kv_backend.clone();
        let leader_cached_kv_backend = self.leader_cached_kv_backend.clone();
        let meta_peer_client = self.meta_peer_client.clone();
        let mailbox = self.mailbox.clone();
        let election = self.election.clone();
        let table_metadata_manager = self.table_metadata_manager.clone();
        let cache_invalidator = self.cache_invalidator.clone();
        let leader_region_registry = self.leader_region_registry.clone();
        let topic_stats_registry = self.topic_stats_registry.clone();

        Context {
            server_addr,
            in_memory,
            kv_backend,
            leader_cached_kv_backend,
            meta_peer_client,
            mailbox,
            election,
            is_infancy: false,
            table_metadata_manager,
            cache_invalidator,
            leader_region_registry,
            topic_stats_registry,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::metasrv::MetasrvNodeInfo;

    #[test]
    fn test_deserialize_metasrv_node_info() {
        let str = r#"{"addr":"127.0.0.1:4002","version":"0.1.0","git_commit":"1234567890","start_time_ms":1715145600}"#;
        let node_info: MetasrvNodeInfo = serde_json::from_str(str).unwrap();
        assert_eq!(node_info.addr, "127.0.0.1:4002");
        assert_eq!(node_info.version, "0.1.0");
        assert_eq!(node_info.git_commit, "1234567890");
        assert_eq!(node_info.start_time_ms, 1715145600);
    }
}
