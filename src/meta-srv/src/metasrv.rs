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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::Peer;
use common_base::Plugins;
use common_greptimedb_telemetry::GreptimeDBTelemetryTask;
use common_grpc::channel_manager;
use common_meta::ddl::DdlTaskExecutorRef;
use common_meta::key::TableMetadataManagerRef;
use common_meta::sequence::SequenceRef;
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::LoggingOptions;
use common_telemetry::{error, info, warn};
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;
use snafu::ResultExt;
use table::metadata::TableId;
use tokio::sync::broadcast::error::RecvError;

use crate::cluster::MetaPeerClientRef;
use crate::election::{Election, LeaderChangeMessage};
use crate::error::{
    InitMetadataSnafu, Result, StartProcedureManagerSnafu, StartTelemetryTaskSnafu,
    StopProcedureManagerSnafu,
};
use crate::handler::HeartbeatHandlerGroup;
use crate::lock::DistLockRef;
use crate::pubsub::{PublishRef, SubscribeManagerRef};
use crate::selector::{Selector, SelectorType};
use crate::service::mailbox::MailboxRef;
use crate::service::store::kv::{KvStoreRef, ResettableKvStoreRef};
pub const TABLE_ID_SEQ: &str = "table_id";
pub const METASRV_HOME: &str = "/tmp/metasrv";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetaSrvOptions {
    pub bind_addr: String,
    pub server_addr: String,
    pub store_addr: String,
    pub selector: SelectorType,
    pub use_memory_store: bool,
    pub enable_region_failover: bool,
    pub http: HttpOptions,
    pub logging: LoggingOptions,
    pub procedure: ProcedureConfig,
    pub datanode: DatanodeOptions,
    pub enable_telemetry: bool,
    pub data_home: String,
}

impl Default for MetaSrvOptions {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3002".to_string(),
            server_addr: "127.0.0.1:3002".to_string(),
            store_addr: "127.0.0.1:2379".to_string(),
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
            },
            datanode: DatanodeOptions::default(),
            enable_telemetry: true,
            data_home: METASRV_HOME.to_string(),
        }
    }
}

impl MetaSrvOptions {
    pub fn to_toml_string(&self) -> String {
        toml::to_string(&self).unwrap()
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
    pub in_memory: ResettableKvStoreRef,
    pub kv_store: KvStoreRef,
    pub leader_cached_kv_store: ResettableKvStoreRef,
    pub meta_peer_client: MetaPeerClientRef,
    pub mailbox: MailboxRef,
    pub election: Option<ElectionRef>,
    pub skip_all: Arc<AtomicBool>,
    pub is_infancy: bool,
    pub table_metadata_manager: TableMetadataManagerRef,
}

impl Context {
    pub fn is_skip_all(&self) -> bool {
        self.skip_all.load(Ordering::Relaxed)
    }

    pub fn set_skip_all(&self) {
        self.skip_all.store(true, Ordering::Relaxed);
    }

    pub fn reset_in_memory(&self) {
        self.in_memory.reset();
    }

    pub fn reset_leader_cached_kv_store(&self) {
        self.leader_cached_kv_store.reset();
    }
}

pub struct LeaderValue(pub String);

#[derive(Clone)]
pub struct SelectorContext {
    pub server_addr: String,
    pub datanode_lease_secs: u64,
    pub kv_store: KvStoreRef,
    pub meta_peer_client: MetaPeerClientRef,
    pub table_id: Option<TableId>,
}

pub type SelectorRef = Arc<dyn Selector<Context = SelectorContext, Output = Vec<Peer>>>;
pub type ElectionRef = Arc<dyn Election<Leader = LeaderValue>>;

pub struct MetaStateHandler {
    procedure_manager: ProcedureManagerRef,
    subscribe_manager: Option<SubscribeManagerRef>,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,
}

impl MetaStateHandler {
    pub async fn on_become_leader(&self) {
        if let Err(e) = self.procedure_manager.start().await {
            error!(e; "Failed to start procedure manager");
        }
        self.greptimedb_telemetry_task.should_report(true);
    }

    pub async fn on_become_follower(&self) {
        // Stops the procedures.
        if let Err(e) = self.procedure_manager.stop().await {
            error!(e; "Failed to stop procedure manager");
        }
        // Suspends reporting.
        self.greptimedb_telemetry_task.should_report(false);

        if let Some(sub_manager) = self.subscribe_manager.clone() {
            info!("Leader changed, un_subscribe all");
            if let Err(e) = sub_manager.un_subscribe_all() {
                error!("Failed to un_subscribe all, error: {}", e);
            }
        }
    }
}

#[derive(Clone)]
pub struct MetaSrv {
    started: Arc<AtomicBool>,
    options: MetaSrvOptions,
    // It is only valid at the leader node and is used to temporarily
    // store some data that will not be persisted.
    in_memory: ResettableKvStoreRef,
    kv_store: KvStoreRef,
    leader_cached_kv_store: ResettableKvStoreRef,
    table_id_sequence: SequenceRef,
    meta_peer_client: MetaPeerClientRef,
    selector: SelectorRef,
    handler_group: HeartbeatHandlerGroup,
    election: Option<ElectionRef>,
    lock: DistLockRef,
    procedure_manager: ProcedureManagerRef,
    mailbox: MailboxRef,
    ddl_executor: DdlTaskExecutorRef,
    table_metadata_manager: TableMetadataManagerRef,
    greptimedb_telemetry_task: Arc<GreptimeDBTelemetryTask>,

    plugins: Plugins,
}

impl MetaSrv {
    pub async fn try_start(&self) -> Result<()> {
        if self
            .started
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            warn!("MetaSrv already started");
            return Ok(());
        }

        self.create_default_schema_if_not_exist().await?;

        if let Some(election) = self.election() {
            let procedure_manager = self.procedure_manager.clone();
            let in_memory = self.in_memory.clone();
            let leader_cached_kv_store = self.leader_cached_kv_store.clone();
            let subscribe_manager = self.subscribe_manager();
            let mut rx = election.subscribe_leader_change();
            let greptimedb_telemetry_task = self.greptimedb_telemetry_task.clone();
            greptimedb_telemetry_task
                .start()
                .context(StartTelemetryTaskSnafu)?;
            let state_handler = MetaStateHandler {
                greptimedb_telemetry_task,
                subscribe_manager,
                procedure_manager,
            };
            let _handle = common_runtime::spawn_bg(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            in_memory.reset();
                            leader_cached_kv_store.reset();
                            info!(
                                "Leader's cache has bean cleared on leader change: {:?}",
                                msg
                            );
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

            let election = election.clone();
            let started = self.started.clone();
            let _handle = common_runtime::spawn_bg(async move {
                while started.load(Ordering::Relaxed) {
                    let res = election.campaign().await;
                    if let Err(e) = res {
                        warn!("MetaSrv election error: {}", e);
                    }
                    info!("MetaSrv re-initiate election");
                }
                info!("MetaSrv stopped");
            });
        } else {
            self.procedure_manager
                .start()
                .await
                .context(StartProcedureManagerSnafu)?;
        }

        info!("MetaSrv started");
        Ok(())
    }

    async fn create_default_schema_if_not_exist(&self) -> Result<()> {
        self.table_metadata_manager
            .init()
            .await
            .context(InitMetadataSnafu)
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.started.store(false, Ordering::Relaxed);
        self.procedure_manager
            .stop()
            .await
            .context(StopProcedureManagerSnafu)
    }

    #[inline]
    pub fn options(&self) -> &MetaSrvOptions {
        &self.options
    }

    pub fn in_memory(&self) -> &ResettableKvStoreRef {
        &self.in_memory
    }

    pub fn kv_store(&self) -> &KvStoreRef {
        &self.kv_store
    }

    pub fn leader_cached_kv_store(&self) -> &ResettableKvStoreRef {
        &self.leader_cached_kv_store
    }

    pub fn meta_peer_client(&self) -> &MetaPeerClientRef {
        &self.meta_peer_client
    }

    pub fn table_id_sequence(&self) -> &SequenceRef {
        &self.table_id_sequence
    }

    pub fn selector(&self) -> &SelectorRef {
        &self.selector
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

    pub fn ddl_executor(&self) -> &DdlTaskExecutorRef {
        &self.ddl_executor
    }

    pub fn procedure_manager(&self) -> &ProcedureManagerRef {
        &self.procedure_manager
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn publish(&self) -> Option<PublishRef> {
        self.plugins.get::<PublishRef>()
    }

    pub fn subscribe_manager(&self) -> Option<SubscribeManagerRef> {
        self.plugins.get::<SubscribeManagerRef>()
    }

    pub fn plugins(&self) -> &Plugins {
        &self.plugins
    }

    #[inline]
    pub fn new_ctx(&self) -> Context {
        let server_addr = self.options().server_addr.clone();
        let in_memory = self.in_memory.clone();
        let kv_store = self.kv_store.clone();
        let leader_cached_kv_store = self.leader_cached_kv_store.clone();
        let meta_peer_client = self.meta_peer_client.clone();
        let mailbox = self.mailbox.clone();
        let election = self.election.clone();
        let skip_all = Arc::new(AtomicBool::new(false));
        let table_metadata_manager = self.table_metadata_manager.clone();

        Context {
            server_addr,
            in_memory,
            kv_store,
            leader_cached_kv_store,
            meta_peer_client,
            mailbox,
            election,
            skip_all,
            is_infancy: false,
            table_metadata_manager,
        }
    }
}
