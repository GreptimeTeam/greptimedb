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

use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_base::Plugins;
use common_config::Configurable;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_grpc::channel_manager;
use common_meta::ddl::ProcedureExecutorRef;
use common_meta::key::TableMetadataManagerRef;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackend, ResettableKvBackendRef};
use common_meta::peer::Peer;
use common_meta::region_keeper::MemoryRegionKeeperRef;
use common_meta::wal_options_allocator::WalOptionsAllocatorRef;
use common_meta::{distributed_time_constants, ClusterId};
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_telemetry::{error, info, warn};
use common_wal::config::MetasrvWalConfig;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::http::HttpOptions;
use snafu::ResultExt;
use table::metadata::TableId;
use tokio::sync::broadcast::error::RecvError;

use crate::cluster::MetaPeerClientRef;
use crate::election::{Election, LeaderChangeMessage};
use crate::error::{
    InitMetadataSnafu, KvBackendSnafu, Result, StartProcedureManagerSnafu, StartTelemetryTaskSnafu,
    StopProcedureManagerSnafu,
};
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::handler::HeartbeatHandlerGroup;
use crate::lease::lookup_datanode_peer;
use crate::lock::DistLockRef;
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;
use crate::pubsub::{PublisherRef, SubscriptionManagerRef};
use crate::region::supervisor::RegionSupervisorTickerRef;
use crate::selector::{Selector, SelectorType};
use crate::service::mailbox::MailboxRef;
use crate::service::store::cached_kv::LeaderCachedKvBackend;
use crate::state::{become_follower, become_leader, StateRef};

pub const TABLE_ID_SEQ: &str = "table_id";
pub const FLOW_ID_SEQ: &str = "flow_id";
pub const METASRV_HOME: &str = "/tmp/metasrv";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    /// The HTTP server options.
    pub http: HttpOptions,
    /// The logging options.
    pub logging: LoggingOptions,
    /// The procedure options.
    pub procedure: ProcedureConfig,
    /// The failure detector options.
    pub failure_detector: PhiAccrualFailureDetectorOptions,
    /// The datanode options.
    pub datanode: DatanodeOptions,
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
    /// The tracing options.
    pub tracing: TracingOptions,
}

impl Default for MetasrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3002".to_string(),
            server_addr: "127.0.0.1:3002".to_string(),
            store_addrs: vec!["127.0.0.1:2379".to_string()],
            selector: SelectorType::default(),
            use_memory_store: false,
            enable_region_failover: false,
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
            },
            failure_detector: PhiAccrualFailureDetectorOptions::default(),
            datanode: DatanodeOptions::default(),
            enable_telemetry: true,
            data_home: METASRV_HOME.to_string(),
            wal: MetasrvWalConfig::default(),
            export_metrics: ExportMetricsOption::default(),
            store_key_prefix: String::new(),
            max_txn_ops: 128,
            tracing: TracingOptions::default(),
        }
    }
}

impl Configurable for MetasrvOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["wal.broker_endpoints"])
    }
}

pub struct MetasrvInfo {
    pub server_addr: String,
}

// Options for datanode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DatanodeOptions {
    pub client_options: DatanodeClientOptions,
}

// Options for datanode client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatanodeClientOptions {
    pub timeout_millis: u64,
    pub connect_timeout_millis: u64,
    pub tcp_nodelay: bool,
}

impl Default for DatanodeClientOptions {
    fn default() -> Self {
        Self {
            timeout_millis: channel_manager::DEFAULT_GRPC_REQUEST_TIMEOUT_SECS * 1000,
            connect_timeout_millis: channel_manager::DEFAULT_GRPC_CONNECT_TIMEOUT_SECS * 1000,
            tcp_nodelay: true,
        }
    }
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
}

impl Context {
    pub fn reset_in_memory(&self) {
        self.in_memory.reset();
    }

    pub fn reset_leader_cached_kv_backend(&self) {
        self.leader_cached_kv_backend.reset();
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
pub type ElectionRef = Arc<dyn Election<Leader = LeaderValue>>;

pub struct MetaStateHandler {
    procedure_manager: ProcedureManagerRef,
    wal_options_allocator: WalOptionsAllocatorRef,
    subscribe_manager: Option<SubscriptionManagerRef>,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
    leader_cached_kv_backend: Arc<LeaderCachedKvBackend>,
    region_supervisor_ticker: Option<RegionSupervisorTickerRef>,
    state: StateRef,
}

impl MetaStateHandler {
    pub async fn on_become_leader(&self) {
        self.state.write().unwrap().next_state(become_leader(false));

        if let Err(e) = self.leader_cached_kv_backend.load().await {
            error!(e; "Failed to load kv into leader cache kv store");
        } else {
            self.state.write().unwrap().next_state(become_leader(true));
        }

        if let Some(ticker) = self.region_supervisor_ticker.as_ref() {
            ticker.start();
        }

        if let Err(e) = self.procedure_manager.start().await {
            error!(e; "Failed to start procedure manager");
        }

        if let Err(e) = self.wal_options_allocator.start().await {
            error!(e; "Failed to start wal options allocator");
        }

        self.greptimedb_telemetry_task.should_report(true);
    }

    pub async fn on_become_follower(&self) {
        self.state.write().unwrap().next_state(become_follower());

        // Stops the procedures.
        if let Err(e) = self.procedure_manager.stop().await {
            error!(e; "Failed to stop procedure manager");
        }

        if let Some(ticker) = self.region_supervisor_ticker.as_ref() {
            // Stops the supervisor ticker.
            ticker.stop();
        }

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

#[derive(Clone)]
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
    handler_group: HeartbeatHandlerGroup,
    election: Option<ElectionRef>,
    lock: DistLockRef,
    procedure_manager: ProcedureManagerRef,
    mailbox: MailboxRef,
    procedure_executor: ProcedureExecutorRef,
    wal_options_allocator: WalOptionsAllocatorRef,
    table_metadata_manager: TableMetadataManagerRef,
    memory_region_keeper: MemoryRegionKeeperRef,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
    region_migration_manager: RegionMigrationManagerRef,
    region_supervisor_ticker: Option<RegionSupervisorTickerRef>,

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
            let region_supervisor_ticker = self.region_supervisor_ticker.clone();
            let state_handler = MetaStateHandler {
                greptimedb_telemetry_task,
                subscribe_manager,
                procedure_manager,
                wal_options_allocator: self.wal_options_allocator.clone(),
                state: self.state.clone(),
                leader_cached_kv_backend: leader_cached_kv_backend.clone(),
                region_supervisor_ticker,
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
                                    state_handler.on_become_leader().await;
                                }
                                LeaderChangeMessage::StepDown(leader) => {
                                    error!("Leader :{:?} step down", leader);

                                    state_handler.on_become_follower().await;
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

                state_handler.on_become_follower().await;
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
    pub(crate) async fn lookup_peer(
        &self,
        cluster_id: ClusterId,
        peer_id: u64,
    ) -> Result<Option<Peer>> {
        lookup_datanode_peer(
            cluster_id,
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

    pub fn handler_group(&self) -> &HeartbeatHandlerGroup {
        &self.handler_group
    }

    pub fn election(&self) -> Option<&ElectionRef> {
        self.election.as_ref()
    }

    pub fn lock(&self) -> &DistLockRef {
        &self.lock
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
        }
    }
}
