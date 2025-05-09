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
use common_base::readable_size::ReadableSize;
use common_base::Plugins;
use common_config::Configurable;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl::ProcedureExecutorRef;
use common_meta::distributed_time_constants;
use common_meta::key::maintenance::MaintenanceModeManagerRef;
use common_meta::key::TableMetadataManagerRef;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackend, ResettableKvBackendRef};
use common_meta::leadership_notifier::{
    LeadershipChangeNotifier, LeadershipChangeNotifierCustomizerRef,
};
use common_meta::node_expiry_listener::NodeExpiryListener;
use common_meta::peer::Peer;
use common_meta::region_keeper::MemoryRegionKeeperRef;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_meta::wal_options_allocator::WalOptionsAllocatorRef;
use common_options::datanode::DatanodeClientOptions;
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_telemetry::{error, info, warn};
use common_wal::config::MetasrvWalConfig;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::http::HttpOptions;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::broadcast::error::RecvError;

use crate::cluster::MetaPeerClientRef;
use crate::election::{Election, LeaderChangeMessage};
use crate::error::{
    self, InitMetadataSnafu, KvBackendSnafu, Result, StartProcedureManagerSnafu,
    StartTelemetryTaskSnafu, StopProcedureManagerSnafu,
};
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::handler::{HeartbeatHandlerGroupBuilder, HeartbeatHandlerGroupRef};
use crate::lease::lookup_datanode_peer;
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;
use crate::procedure::wal_prune::manager::WalPruneTickerRef;
use crate::procedure::ProcedureManagerListenerAdapter;
use crate::pubsub::{PublisherRef, SubscriptionManagerRef};
use crate::region::supervisor::RegionSupervisorTickerRef;
use crate::selector::{RegionStatAwareSelector, Selector, SelectorType};
use crate::service::mailbox::MailboxRef;
use crate::service::store::cached_kv::LeaderCachedKvBackend;
use crate::state::{become_follower, become_leader, StateRef};

pub const TABLE_ID_SEQ: &str = "table_id";
pub const FLOW_ID_SEQ: &str = "flow_id";
pub const METASRV_HOME: &str = "./greptimedb_data/metasrv";

#[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
pub const DEFAULT_META_TABLE_NAME: &str = "greptime_metakv";
#[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
pub const DEFAULT_META_ELECTION_LOCK_ID: u64 = 1;

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

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetasrvOptions {
    /// The address the server listens on.
    pub bind_addr: String,
    /// The address the server advertises to the clients.
    pub server_addr: String,
    /// The address of the store, e.g., etcd.
    pub store_addrs: Vec<String>,
    /// The type of selector.
    pub selector: SelectorType,
    /// Whether to use the memory store.
    pub use_memory_store: bool,
    /// Whether to enable region failover.
    pub enable_region_failover: bool,
    /// Whether to allow region failover on local WAL.
    ///
    /// If it's true, the region failover will be allowed even if the local WAL is used.
    /// Note that this option is not recommended to be set to true, because it may lead to data loss during failover.
    pub allow_region_failover_on_local_wal: bool,
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
    /// The metrics export options.
    pub export_metrics: ExportMetricsOption,
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
    /// The datastore for kv metadata.
    pub backend: BackendImpl,
    #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
    /// Table name of rds kv backend.
    pub meta_table_name: String,
    #[cfg(feature = "pg_kvbackend")]
    /// Lock id for meta kv election. Only effect when using pg_kvbackend.
    pub meta_election_lock_id: u64,
    #[serde(with = "humantime_serde")]
    pub node_max_idle_time: Duration,
}

impl fmt::Debug for MetasrvOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("MetasrvOptions");
        debug_struct
            .field("bind_addr", &self.bind_addr)
            .field("server_addr", &self.server_addr)
            .field("store_addrs", &self.sanitize_store_addrs())
            .field("selector", &self.selector)
            .field("use_memory_store", &self.use_memory_store)
            .field("enable_region_failover", &self.enable_region_failover)
            .field(
                "allow_region_failover_on_local_wal",
                &self.allow_region_failover_on_local_wal,
            )
            .field("http", &self.http)
            .field("logging", &self.logging)
            .field("procedure", &self.procedure)
            .field("failure_detector", &self.failure_detector)
            .field("datanode", &self.datanode)
            .field("enable_telemetry", &self.enable_telemetry)
            .field("data_home", &self.data_home)
            .field("wal", &self.wal)
            .field("export_metrics", &self.export_metrics)
            .field("store_key_prefix", &self.store_key_prefix)
            .field("max_txn_ops", &self.max_txn_ops)
            .field("flush_stats_factor", &self.flush_stats_factor)
            .field("tracing", &self.tracing)
            .field("backend", &self.backend);

        #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
        debug_struct.field("meta_table_name", &self.meta_table_name);

        #[cfg(feature = "pg_kvbackend")]
        debug_struct.field("meta_election_lock_id", &self.meta_election_lock_id);

        debug_struct
            .field("node_max_idle_time", &self.node_max_idle_time)
            .finish()
    }
}

const DEFAULT_METASRV_ADDR_PORT: &str = "3002";

impl Default for MetasrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: format!("127.0.0.1:{}", DEFAULT_METASRV_ADDR_PORT),
            // If server_addr is not set, the server will use the local ip address as the server address.
            server_addr: String::new(),
            store_addrs: vec!["127.0.0.1:2379".to_string()],
            selector: SelectorType::default(),
            use_memory_store: false,
            enable_region_failover: false,
            allow_region_failover_on_local_wal: false,
            http: HttpOptions::default(),
            logging: LoggingOptions {
                dir: format!("{METASRV_HOME}/logs"),
                ..Default::default()
            },
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
            data_home: METASRV_HOME.to_string(),
            wal: MetasrvWalConfig::default(),
            export_metrics: ExportMetricsOption::default(),
            store_key_prefix: String::new(),
            max_txn_ops: 128,
            flush_stats_factor: 3,
            tracing: TracingOptions::default(),
            backend: BackendImpl::EtcdStore,
            #[cfg(any(feature = "pg_kvbackend", feature = "mysql_kvbackend"))]
            meta_table_name: DEFAULT_META_TABLE_NAME.to_string(),
            #[cfg(feature = "pg_kvbackend")]
            meta_election_lock_id: DEFAULT_META_ELECTION_LOCK_ID,
            node_max_idle_time: Duration::from_secs(24 * 60 * 60),
        }
    }
}

impl Configurable for MetasrvOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["wal.broker_endpoints", "store_addrs"])
    }
}

impl MetasrvOptions {
    /// Detect server address.
    #[cfg(not(target_os = "android"))]
    pub fn detect_server_addr(&mut self) {
        if self.server_addr.is_empty() {
            match local_ip_address::local_ip() {
                Ok(ip) => {
                    let detected_addr = format!(
                        "{}:{}",
                        ip,
                        self.bind_addr
                            .split(':')
                            .nth(1)
                            .unwrap_or(DEFAULT_METASRV_ADDR_PORT)
                    );
                    info!("Using detected: {} as server address", detected_addr);
                    self.server_addr = detected_addr;
                }
                Err(e) => {
                    error!("Failed to detect local ip address: {}", e);
                }
            }
        }
    }

    #[cfg(target_os = "android")]
    pub fn detect_server_addr(&mut self) {
        if self.server_addr.is_empty() {
            common_telemetry::debug!("detect local IP is not supported on Android");
        }
    }

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
}

impl From<MetasrvNodeInfo> for api::v1::meta::MetasrvNodeInfo {
    fn from(node_info: MetasrvNodeInfo) -> Self {
        Self {
            peer: Some(api::v1::meta::Peer {
                addr: node_info.addr,
                ..Default::default()
            }),
            version: node_info.version,
            git_commit: node_info.git_commit,
            start_time_ms: node_info.start_time_ms,
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
    pub server_addr: String,
    pub datanode_lease_secs: u64,
    pub flownode_lease_secs: u64,
    pub kv_backend: KvBackendRef,
    pub meta_peer_client: MetaPeerClientRef,
    pub table_id: Option<TableId>,
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
    // The flow selector is used to select a target flownode.
    flow_selector: SelectorRef,
    handler_group: RwLock<Option<HeartbeatHandlerGroupRef>>,
    handler_group_builder: Mutex<Option<HeartbeatHandlerGroupBuilder>>,
    election: Option<ElectionRef>,
    procedure_manager: ProcedureManagerRef,
    mailbox: MailboxRef,
    procedure_executor: ProcedureExecutorRef,
    wal_options_allocator: WalOptionsAllocatorRef,
    table_metadata_manager: TableMetadataManagerRef,
    maintenance_mode_manager: MaintenanceModeManagerRef,
    memory_region_keeper: MemoryRegionKeeperRef,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
    region_migration_manager: RegionMigrationManagerRef,
    region_supervisor_ticker: Option<RegionSupervisorTickerRef>,
    cache_invalidator: CacheInvalidatorRef,
    leader_region_registry: LeaderRegionRegistryRef,
    wal_prune_ticker: Option<WalPruneTickerRef>,

    plugins: Plugins,
}

impl Metasrv {
    pub async fn try_start(&self) -> Result<()> {
        if self
            .started
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
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
            if let Some(customizer) = self.plugins.get::<LeadershipChangeNotifierCustomizerRef>() {
                customizer.customize(&mut leadership_change_notifier);
            }

            let state_handler = MetaStateHandler {
                greptimedb_telemetry_task,
                subscribe_manager,
                state: self.state.clone(),
                leader_cached_kv_backend: leader_cached_kv_backend.clone(),
                leadership_change_notifier,
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
                    while started.load(Ordering::Relaxed) {
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
                    while started.load(Ordering::Relaxed) {
                        let res = election.campaign().await;
                        if let Err(e) = res {
                            warn!(e; "Metasrv election error");
                        }
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
        self.started.store(false, Ordering::Relaxed);
        self.procedure_manager
            .stop()
            .await
            .context(StopProcedureManagerSnafu)
    }

    pub fn start_time_ms(&self) -> u64 {
        self.start_time_ms
    }

    pub fn node_info(&self) -> MetasrvNodeInfo {
        let build_info = common_version::build_info();
        MetasrvNodeInfo {
            addr: self.options().server_addr.clone(),
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            start_time_ms: self.start_time_ms(),
        }
    }

    /// Lookup a peer by peer_id, return it only when it's alive.
    pub(crate) async fn lookup_peer(&self, peer_id: u64) -> Result<Option<Peer>> {
        lookup_datanode_peer(
            peer_id,
            &self.meta_peer_client,
            distributed_time_constants::DATANODE_LEASE_SECS,
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

    pub fn procedure_executor(&self) -> &ProcedureExecutorRef {
        &self.procedure_executor
    }

    pub fn procedure_manager(&self) -> &ProcedureManagerRef {
        &self.procedure_manager
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn maintenance_mode_manager(&self) -> &MaintenanceModeManagerRef {
        &self.maintenance_mode_manager
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

    pub fn plugins(&self) -> &Plugins {
        &self.plugins
    }

    #[inline]
    pub fn new_ctx(&self) -> Context {
        let server_addr = self.options().server_addr.clone();
        let in_memory = self.in_memory.clone();
        let kv_backend = self.kv_backend.clone();
        let leader_cached_kv_backend = self.leader_cached_kv_backend.clone();
        let meta_peer_client = self.meta_peer_client.clone();
        let mailbox = self.mailbox.clone();
        let election = self.election.clone();
        let table_metadata_manager = self.table_metadata_manager.clone();
        let cache_invalidator = self.cache_invalidator.clone();
        let leader_region_registry = self.leader_region_registry.clone();

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
        }
    }
}
