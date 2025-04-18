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

use std::net::SocketAddr;
use std::sync::Arc;
use std::{fs, path};

use async_trait::async_trait;
use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use catalog::information_schema::InformationExtension;
use catalog::kvbackend::KvBackendCatalogManager;
use clap::Parser;
use client::api::v1::meta::RegionRole;
use common_base::readable_size::ReadableSize;
use common_base::Plugins;
use common_catalog::consts::{MIN_USER_FLOW_ID, MIN_USER_TABLE_ID};
use common_config::{metadata_store_dir, Configurable, KvBackendConfig};
use common_error::ext::BoxedError;
use common_meta::cache::LayeredCacheRegistryBuilder;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::cluster::{NodeInfo, NodeStatus};
use common_meta::datanode::RegionStat;
use common_meta::ddl::flow_meta::{FlowMetadataAllocator, FlowMetadataAllocatorRef};
use common_meta::ddl::table_meta::{TableMetadataAllocator, TableMetadataAllocatorRef};
use common_meta::ddl::{DdlContext, NoopRegionFailureDetectorControl, ProcedureExecutorRef};
use common_meta::ddl_manager::DdlManager;
use common_meta::key::flow::flow_state::FlowStat;
use common_meta::key::flow::{FlowMetadataManager, FlowMetadataManagerRef};
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::region_registry::LeaderRegionRegistry;
use common_meta::sequence::SequenceBuilder;
use common_meta::wal_options_allocator::{build_wal_options_allocator, WalOptionsAllocatorRef};
use common_procedure::{ProcedureInfo, ProcedureManagerRef};
use common_telemetry::info;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_time::timezone::set_default_timezone;
use common_version::{short_version, version};
use common_wal::config::DatanodeWalConfig;
use datanode::config::{DatanodeOptions, ProcedureConfig, RegionEngineConfig, StorageConfig};
use datanode::datanode::{Datanode, DatanodeBuilder};
use datanode::region_server::RegionServer;
use file_engine::config::EngineConfig as FileEngineConfig;
use flow::{
    FlowConfig, FlowWorkerManager, FlownodeBuilder, FlownodeInstance, FlownodeOptions,
    FrontendClient, FrontendInvoker,
};
use frontend::frontend::{Frontend, FrontendOptions};
use frontend::instance::builder::FrontendBuilder;
use frontend::instance::{Instance as FeInstance, StandaloneDatanodeManager};
use frontend::server::Services;
use frontend::service_config::{
    InfluxdbOptions, JaegerOptions, MysqlOptions, OpentsdbOptions, PostgresOptions,
    PromStoreOptions,
};
use meta_srv::metasrv::{FLOW_ID_SEQ, TABLE_ID_SEQ};
use mito2::config::MitoConfig;
use query::stats::StatementStatistics;
use serde::{Deserialize, Serialize};
use servers::export_metrics::{ExportMetricsOption, ExportMetricsTask};
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;
use tokio::sync::RwLock;
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::{Result, StartFlownodeSnafu};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::{error, log_versions, App};

pub const APP_NAME: &str = "greptime-standalone";

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(&self, opts: GreptimeOptions<StandaloneOptions>) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(
        &self,
        global_options: &GlobalOptions,
    ) -> Result<GreptimeOptions<StandaloneOptions>> {
        self.subcmd.load_options(global_options)
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(&self, opts: GreptimeOptions<StandaloneOptions>) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(
        &self,
        global_options: &GlobalOptions,
    ) -> Result<GreptimeOptions<StandaloneOptions>> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(global_options),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StandaloneOptions {
    pub enable_telemetry: bool,
    pub default_timezone: Option<String>,
    pub http: HttpOptions,
    pub grpc: GrpcOptions,
    pub mysql: MysqlOptions,
    pub postgres: PostgresOptions,
    pub opentsdb: OpentsdbOptions,
    pub influxdb: InfluxdbOptions,
    pub jaeger: JaegerOptions,
    pub prom_store: PromStoreOptions,
    pub wal: DatanodeWalConfig,
    pub storage: StorageConfig,
    pub metadata_store: KvBackendConfig,
    pub procedure: ProcedureConfig,
    pub flow: FlowConfig,
    pub logging: LoggingOptions,
    pub user_provider: Option<String>,
    /// Options for different store engines.
    pub region_engine: Vec<RegionEngineConfig>,
    pub export_metrics: ExportMetricsOption,
    pub tracing: TracingOptions,
    pub init_regions_in_background: bool,
    pub init_regions_parallelism: usize,
    pub max_in_flight_write_bytes: Option<ReadableSize>,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            enable_telemetry: true,
            default_timezone: None,
            http: HttpOptions::default(),
            grpc: GrpcOptions::default(),
            mysql: MysqlOptions::default(),
            postgres: PostgresOptions::default(),
            opentsdb: OpentsdbOptions::default(),
            influxdb: InfluxdbOptions::default(),
            jaeger: JaegerOptions::default(),
            prom_store: PromStoreOptions::default(),
            wal: DatanodeWalConfig::default(),
            storage: StorageConfig::default(),
            metadata_store: KvBackendConfig::default(),
            procedure: ProcedureConfig::default(),
            flow: FlowConfig::default(),
            logging: LoggingOptions::default(),
            export_metrics: ExportMetricsOption::default(),
            user_provider: None,
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
            tracing: TracingOptions::default(),
            init_regions_in_background: false,
            init_regions_parallelism: 16,
            max_in_flight_write_bytes: None,
        }
    }
}

impl Configurable for StandaloneOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["wal.broker_endpoints"])
    }
}

/// The [`StandaloneOptions`] is only defined in cmd crate,
/// we don't want to make `frontend` depends on it, so impl [`Into`]
/// rather than [`From`].
#[allow(clippy::from_over_into)]
impl Into<FrontendOptions> for StandaloneOptions {
    fn into(self) -> FrontendOptions {
        self.frontend_options()
    }
}

impl StandaloneOptions {
    pub fn frontend_options(&self) -> FrontendOptions {
        let cloned_opts = self.clone();
        FrontendOptions {
            default_timezone: cloned_opts.default_timezone,
            http: cloned_opts.http,
            grpc: cloned_opts.grpc,
            mysql: cloned_opts.mysql,
            postgres: cloned_opts.postgres,
            opentsdb: cloned_opts.opentsdb,
            influxdb: cloned_opts.influxdb,
            jaeger: cloned_opts.jaeger,
            prom_store: cloned_opts.prom_store,
            meta_client: None,
            logging: cloned_opts.logging,
            user_provider: cloned_opts.user_provider,
            // Handle the export metrics task run by standalone to frontend for execution
            export_metrics: cloned_opts.export_metrics,
            max_in_flight_write_bytes: cloned_opts.max_in_flight_write_bytes,
            ..Default::default()
        }
    }

    pub fn datanode_options(&self) -> DatanodeOptions {
        let cloned_opts = self.clone();
        DatanodeOptions {
            node_id: Some(0),
            enable_telemetry: cloned_opts.enable_telemetry,
            wal: cloned_opts.wal,
            storage: cloned_opts.storage,
            region_engine: cloned_opts.region_engine,
            grpc: cloned_opts.grpc,
            init_regions_in_background: cloned_opts.init_regions_in_background,
            init_regions_parallelism: cloned_opts.init_regions_parallelism,
            ..Default::default()
        }
    }
}

pub struct Instance {
    datanode: Datanode,
    frontend: Frontend,
    flownode: FlownodeInstance,
    procedure_manager: ProcedureManagerRef,
    wal_options_allocator: WalOptionsAllocatorRef,
    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    /// Find the socket addr of a server by its `name`.
    pub async fn server_addr(&self, name: &str) -> Option<SocketAddr> {
        self.frontend.server_handlers().addr(name).await
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        self.datanode.start_telemetry();

        self.procedure_manager
            .start()
            .await
            .context(error::StartProcedureManagerSnafu)?;

        self.wal_options_allocator
            .start()
            .await
            .context(error::StartWalOptionsAllocatorSnafu)?;

        plugins::start_frontend_plugins(self.frontend.instance.plugins().clone())
            .await
            .context(error::StartFrontendSnafu)?;

        self.frontend
            .start()
            .await
            .context(error::StartFrontendSnafu)?;

        self.flownode.start().await.context(StartFlownodeSnafu)?;

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.frontend
            .shutdown()
            .await
            .context(error::ShutdownFrontendSnafu)?;

        self.procedure_manager
            .stop()
            .await
            .context(error::StopProcedureManagerSnafu)?;

        self.datanode
            .shutdown()
            .await
            .context(error::ShutdownDatanodeSnafu)?;

        self.flownode
            .shutdown()
            .await
            .context(error::ShutdownFlownodeSnafu)?;

        info!("Datanode instance stopped.");

        Ok(())
    }
}

#[derive(Debug, Default, Parser)]
pub struct StartCommand {
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long, alias = "rpc-addr")]
    rpc_bind_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(short, long)]
    influxdb_enable: bool,
    #[clap(short, long)]
    pub config_file: Option<String>,
    #[clap(long)]
    tls_mode: Option<TlsMode>,
    #[clap(long)]
    tls_cert_path: Option<String>,
    #[clap(long)]
    tls_key_path: Option<String>,
    #[clap(long)]
    user_provider: Option<String>,
    #[clap(long, default_value = "GREPTIMEDB_STANDALONE")]
    pub env_prefix: String,
    /// The working home directory of this standalone instance.
    #[clap(long)]
    data_home: Option<String>,
}

impl StartCommand {
    /// Load the GreptimeDB options from various sources (command line, config file or env).
    pub fn load_options(
        &self,
        global_options: &GlobalOptions,
    ) -> Result<GreptimeOptions<StandaloneOptions>> {
        let mut opts = GreptimeOptions::<StandaloneOptions>::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
        )
        .context(error::LoadLayeredConfigSnafu)?;

        self.merge_with_cli_options(global_options, &mut opts.component)?;

        Ok(opts)
    }

    // The precedence order is: cli > config file > environment variables > default values.
    pub fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        opts: &mut StandaloneOptions,
    ) -> Result<()> {
        if let Some(dir) = &global_options.log_dir {
            opts.logging.dir.clone_from(dir);
        }

        if global_options.log_level.is_some() {
            opts.logging.level.clone_from(&global_options.log_level);
        }

        opts.tracing = TracingOptions {
            #[cfg(feature = "tokio-console")]
            tokio_console_addr: global_options.tokio_console_addr.clone(),
        };

        let tls_opts = TlsOption::new(
            self.tls_mode.clone(),
            self.tls_cert_path.clone(),
            self.tls_key_path.clone(),
        );

        if let Some(addr) = &self.http_addr {
            opts.http.addr.clone_from(addr);
        }

        if let Some(data_home) = &self.data_home {
            opts.storage.data_home.clone_from(data_home);
        }

        if let Some(addr) = &self.rpc_bind_addr {
            // frontend grpc addr conflict with datanode default grpc addr
            let datanode_grpc_addr = DatanodeOptions::default().grpc.bind_addr;
            if addr.eq(&datanode_grpc_addr) {
                return error::IllegalConfigSnafu {
                    msg: format!(
                        "gRPC listen address conflicts with datanode reserved gRPC addr: {datanode_grpc_addr}",
                    ),
                }.fail();
            }
            opts.grpc.bind_addr.clone_from(addr)
        }

        if let Some(addr) = &self.mysql_addr {
            opts.mysql.enable = true;
            opts.mysql.addr.clone_from(addr);
            opts.mysql.tls = tls_opts.clone();
        }

        if let Some(addr) = &self.postgres_addr {
            opts.postgres.enable = true;
            opts.postgres.addr.clone_from(addr);
            opts.postgres.tls = tls_opts;
        }

        if self.influxdb_enable {
            opts.influxdb.enable = self.influxdb_enable;
        }

        if let Some(user_provider) = &self.user_provider {
            opts.user_provider = Some(user_provider.clone());
        }

        Ok(())
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    #[allow(clippy::diverging_sub_expression)]
    /// Build GreptimeDB instance with the loaded options.
    pub async fn build(&self, opts: GreptimeOptions<StandaloneOptions>) -> Result<Instance> {
        common_runtime::init_global_runtimes(&opts.runtime);

        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts.component.logging,
            &opts.component.tracing,
            None,
        );
        log_versions(version(), short_version(), APP_NAME);

        info!("Standalone start command: {:#?}", self);
        info!("Standalone options: {opts:#?}");

        let mut plugins = Plugins::new();
        let plugin_opts = opts.plugins;
        let mut opts = opts.component;
        opts.grpc.detect_server_addr();
        let fe_opts = opts.frontend_options();
        let dn_opts = opts.datanode_options();

        plugins::setup_frontend_plugins(&mut plugins, &plugin_opts, &fe_opts)
            .await
            .context(error::StartFrontendSnafu)?;

        plugins::setup_datanode_plugins(&mut plugins, &plugin_opts, &dn_opts)
            .await
            .context(error::StartDatanodeSnafu)?;

        set_default_timezone(fe_opts.default_timezone.as_deref())
            .context(error::InitTimezoneSnafu)?;

        let data_home = &dn_opts.storage.data_home;
        // Ensure the data_home directory exists.
        fs::create_dir_all(path::Path::new(data_home))
            .context(error::CreateDirSnafu { dir: data_home })?;

        let metadata_dir = metadata_store_dir(data_home);
        let (kv_backend, procedure_manager) = FeInstance::try_build_standalone_components(
            metadata_dir,
            opts.metadata_store,
            opts.procedure,
        )
        .await
        .context(error::StartFrontendSnafu)?;

        // Builds cache registry
        let layered_cache_builder = LayeredCacheRegistryBuilder::default();
        let fundamental_cache_registry = build_fundamental_cache_registry(kv_backend.clone());
        let layered_cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .context(error::BuildCacheRegistrySnafu)?
            .build(),
        );

        let datanode = DatanodeBuilder::new(dn_opts, plugins.clone(), Mode::Standalone)
            .with_kv_backend(kv_backend.clone())
            .with_cache_registry(layered_cache_registry.clone())
            .build()
            .await
            .context(error::StartDatanodeSnafu)?;

        let information_extension = Arc::new(StandaloneInformationExtension::new(
            datanode.region_server(),
            procedure_manager.clone(),
        ));
        let catalog_manager = KvBackendCatalogManager::new(
            information_extension.clone(),
            kv_backend.clone(),
            layered_cache_registry.clone(),
            Some(procedure_manager.clone()),
        );

        let table_metadata_manager =
            Self::create_table_metadata_manager(kv_backend.clone()).await?;

        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let flownode_options = FlownodeOptions {
            flow: opts.flow.clone(),
            ..Default::default()
        };

        // TODO(discord9): for standalone not use grpc, but just somehow get a handler to frontend grpc client without
        // actually make a connection
        let fe_server_addr = fe_opts.grpc.bind_addr.clone();
        let frontend_client = FrontendClient::from_static_grpc_addr(fe_server_addr);
        let flow_builder = FlownodeBuilder::new(
            flownode_options,
            plugins.clone(),
            table_metadata_manager.clone(),
            catalog_manager.clone(),
            flow_metadata_manager.clone(),
            Arc::new(frontend_client),
        );
        let flownode = flow_builder
            .build()
            .await
            .map_err(BoxedError::new)
            .context(error::OtherSnafu)?;

        // set the ref to query for the local flow state
        {
            let flow_worker_manager = flownode.flow_worker_manager();
            information_extension
                .set_flow_worker_manager(flow_worker_manager.clone())
                .await;
        }

        let node_manager = Arc::new(StandaloneDatanodeManager {
            region_server: datanode.region_server(),
            flow_server: flownode.flow_worker_manager(),
        });

        let table_id_sequence = Arc::new(
            SequenceBuilder::new(TABLE_ID_SEQ, kv_backend.clone())
                .initial(MIN_USER_TABLE_ID as u64)
                .step(10)
                .build(),
        );
        let flow_id_sequence = Arc::new(
            SequenceBuilder::new(FLOW_ID_SEQ, kv_backend.clone())
                .initial(MIN_USER_FLOW_ID as u64)
                .step(10)
                .build(),
        );
        let kafka_options = opts.wal.clone().into();
        let wal_options_allocator = build_wal_options_allocator(&kafka_options, kv_backend.clone())
            .await
            .context(error::BuildWalOptionsAllocatorSnafu)?;
        let wal_options_allocator = Arc::new(wal_options_allocator);
        let table_meta_allocator = Arc::new(TableMetadataAllocator::new(
            table_id_sequence,
            wal_options_allocator.clone(),
        ));
        let flow_meta_allocator = Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(
            flow_id_sequence,
        ));

        let ddl_task_executor = Self::create_ddl_task_executor(
            procedure_manager.clone(),
            node_manager.clone(),
            layered_cache_registry.clone(),
            table_metadata_manager,
            table_meta_allocator,
            flow_metadata_manager,
            flow_meta_allocator,
        )
        .await?;

        let fe_instance = FrontendBuilder::new(
            fe_opts.clone(),
            kv_backend.clone(),
            layered_cache_registry.clone(),
            catalog_manager.clone(),
            node_manager.clone(),
            ddl_task_executor.clone(),
            StatementStatistics::new(opts.logging.slow_query.clone()),
        )
        .with_plugin(plugins.clone())
        .try_build()
        .await
        .context(error::StartFrontendSnafu)?;
        let fe_instance = Arc::new(fe_instance);

        let flow_worker_manager = flownode.flow_worker_manager();
        // flow server need to be able to use frontend to write insert requests back
        let invoker = FrontendInvoker::build_from(
            flow_worker_manager.clone(),
            catalog_manager.clone(),
            kv_backend.clone(),
            layered_cache_registry.clone(),
            ddl_task_executor.clone(),
            node_manager,
        )
        .await
        .context(error::StartFlownodeSnafu)?;
        flow_worker_manager.set_frontend_invoker(invoker).await;

        let export_metrics_task = ExportMetricsTask::try_new(&opts.export_metrics, Some(&plugins))
            .context(error::ServersSnafu)?;

        let servers = Services::new(opts, fe_instance.clone(), plugins)
            .build()
            .await
            .context(error::StartFrontendSnafu)?;

        let frontend = Frontend {
            instance: fe_instance,
            servers,
            heartbeat_task: None,
            export_metrics_task,
        };

        Ok(Instance {
            datanode,
            frontend,
            flownode,
            procedure_manager,
            wal_options_allocator,
            _guard: guard,
        })
    }

    pub async fn create_ddl_task_executor(
        procedure_manager: ProcedureManagerRef,
        node_manager: NodeManagerRef,
        cache_invalidator: CacheInvalidatorRef,
        table_metadata_manager: TableMetadataManagerRef,
        table_metadata_allocator: TableMetadataAllocatorRef,
        flow_metadata_manager: FlowMetadataManagerRef,
        flow_metadata_allocator: FlowMetadataAllocatorRef,
    ) -> Result<ProcedureExecutorRef> {
        let procedure_executor: ProcedureExecutorRef = Arc::new(
            DdlManager::try_new(
                DdlContext {
                    node_manager,
                    cache_invalidator,
                    memory_region_keeper: Arc::new(MemoryRegionKeeper::default()),
                    leader_region_registry: Arc::new(LeaderRegionRegistry::default()),
                    table_metadata_manager,
                    table_metadata_allocator,
                    flow_metadata_manager,
                    flow_metadata_allocator,
                    region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
                },
                procedure_manager,
                true,
            )
            .context(error::InitDdlManagerSnafu)?,
        );

        Ok(procedure_executor)
    }

    pub async fn create_table_metadata_manager(
        kv_backend: KvBackendRef,
    ) -> Result<TableMetadataManagerRef> {
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend));

        table_metadata_manager
            .init()
            .await
            .context(error::InitMetadataSnafu)?;

        Ok(table_metadata_manager)
    }
}

pub struct StandaloneInformationExtension {
    region_server: RegionServer,
    procedure_manager: ProcedureManagerRef,
    start_time_ms: u64,
    flow_worker_manager: RwLock<Option<Arc<FlowWorkerManager>>>,
}

impl StandaloneInformationExtension {
    pub fn new(region_server: RegionServer, procedure_manager: ProcedureManagerRef) -> Self {
        Self {
            region_server,
            procedure_manager,
            start_time_ms: common_time::util::current_time_millis() as u64,
            flow_worker_manager: RwLock::new(None),
        }
    }

    /// Set the flow worker manager for the standalone instance.
    pub async fn set_flow_worker_manager(&self, flow_worker_manager: Arc<FlowWorkerManager>) {
        let mut guard = self.flow_worker_manager.write().await;
        *guard = Some(flow_worker_manager);
    }
}

#[async_trait::async_trait]
impl InformationExtension for StandaloneInformationExtension {
    type Error = catalog::error::Error;

    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
        let build_info = common_version::build_info();
        let node_info = NodeInfo {
            // For the standalone:
            // - id always 0
            // - empty string for peer_addr
            peer: Peer {
                id: 0,
                addr: "".to_string(),
            },
            last_activity_ts: -1,
            status: NodeStatus::Standalone,
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            // Use `self.start_time_ms` instead.
            // It's not precise but enough.
            start_time_ms: self.start_time_ms,
        };
        Ok(vec![node_info])
    }

    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
        self.procedure_manager
            .list_procedures()
            .await
            .map_err(BoxedError::new)
            .map(|procedures| {
                procedures
                    .into_iter()
                    .map(|procedure| {
                        let status = procedure.state.as_str_name().to_string();
                        (status, procedure)
                    })
                    .collect::<Vec<_>>()
            })
            .context(catalog::error::ListProceduresSnafu)
    }

    async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
        let stats = self
            .region_server
            .reportable_regions()
            .into_iter()
            .map(|stat| {
                let region_stat = self
                    .region_server
                    .region_statistic(stat.region_id)
                    .unwrap_or_default();
                RegionStat {
                    id: stat.region_id,
                    rcus: 0,
                    wcus: 0,
                    approximate_bytes: region_stat.estimated_disk_size(),
                    engine: stat.engine,
                    role: RegionRole::from(stat.role).into(),
                    num_rows: region_stat.num_rows,
                    memtable_size: region_stat.memtable_size,
                    manifest_size: region_stat.manifest_size,
                    sst_size: region_stat.sst_size,
                    index_size: region_stat.index_size,
                    region_manifest: region_stat.manifest.into(),
                    data_topic_latest_entry_id: region_stat.data_topic_latest_entry_id,
                    metadata_topic_latest_entry_id: region_stat.metadata_topic_latest_entry_id,
                }
            })
            .collect::<Vec<_>>();
        Ok(stats)
    }

    async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error> {
        Ok(Some(
            self.flow_worker_manager
                .read()
                .await
                .as_ref()
                .unwrap()
                .gen_state_report()
                .await,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::default::Default;
    use std::io::Write;
    use std::time::Duration;

    use auth::{Identity, Password, UserProviderRef};
    use common_base::readable_size::ReadableSize;
    use common_config::ENV_VAR_SEP;
    use common_test_util::temp_dir::create_named_temp_file;
    use common_wal::config::DatanodeWalConfig;
    use datanode::config::{FileConfig, GcsConfig};

    use super::*;
    use crate::options::GlobalOptions;

    #[tokio::test]
    async fn test_try_from_start_command_to_anymap() {
        let fe_opts = FrontendOptions {
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let mut plugins = Plugins::new();
        plugins::setup_frontend_plugins(&mut plugins, &[], &fe_opts)
            .await
            .unwrap();

        let provider = plugins.get::<UserProviderRef>().unwrap();
        let result = provider
            .authenticate(
                Identity::UserId("test", None),
                Password::PlainText("test".to_string().into()),
            )
            .await;
        let _ = result.unwrap();
    }

    #[test]
    fn test_toml() {
        let opts = StandaloneOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: StandaloneOptions = toml::from_str(&toml_string).unwrap();
    }

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "distributed"

            enable_memory_catalog = true

            [wal]
            provider = "raft_engine"
            dir = "./greptimedb_data/test/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [storage]
            data_home = "./greptimedb_data/"
            type = "File"

            [[storage.providers]]
            type = "Gcs"
            bucket = "foo"
            endpoint = "bar"

            [[storage.providers]]
            type = "S3"
            access_key_id = "access_key_id"
            secret_access_key = "secret_access_key"

            [storage.compaction]
            max_inflight_tasks = 3
            max_files_in_level0 = 7
            max_purge_tasks = 32

            [storage.manifest]
            checkpoint_margin = 9
            gc_duration = '7s'

            [http]
            addr = "127.0.0.1:4000"
            timeout = "33s"
            body_limit = "128MB"

            [opentsdb]
            enable = true

            [logging]
            level = "debug"
            dir = "./greptimedb_data/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();
        let cmd = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let options = cmd
            .load_options(&GlobalOptions::default())
            .unwrap()
            .component;
        let fe_opts = options.frontend_options();
        let dn_opts = options.datanode_options();
        let logging_opts = options.logging;
        assert_eq!("127.0.0.1:4000".to_string(), fe_opts.http.addr);
        assert_eq!(Duration::from_secs(33), fe_opts.http.timeout);
        assert_eq!(ReadableSize::mb(128), fe_opts.http.body_limit);
        assert_eq!("127.0.0.1:4001".to_string(), fe_opts.grpc.bind_addr);
        assert!(fe_opts.mysql.enable);
        assert_eq!("127.0.0.1:4002", fe_opts.mysql.addr);
        assert_eq!(2, fe_opts.mysql.runtime_size);
        assert_eq!(None, fe_opts.mysql.reject_no_database);
        assert!(fe_opts.influxdb.enable);
        assert!(fe_opts.opentsdb.enable);

        let DatanodeWalConfig::RaftEngine(raft_engine_config) = dn_opts.wal else {
            unreachable!()
        };
        assert_eq!(
            "./greptimedb_data/test/wal",
            raft_engine_config.dir.unwrap()
        );

        assert!(matches!(
            &dn_opts.storage.store,
            datanode::config::ObjectStoreConfig::File(FileConfig { .. })
        ));
        assert_eq!(dn_opts.storage.providers.len(), 2);
        assert!(matches!(
            dn_opts.storage.providers[0],
            datanode::config::ObjectStoreConfig::Gcs(GcsConfig { .. })
        ));
        match &dn_opts.storage.providers[1] {
            datanode::config::ObjectStoreConfig::S3(s3_config) => {
                assert_eq!(
                    "SecretBox<alloc::string::String>([REDACTED])".to_string(),
                    format!("{:?}", s3_config.access_key_id)
                );
            }
            _ => {
                unreachable!()
            }
        }

        assert_eq!("debug", logging_opts.level.as_ref().unwrap());
        assert_eq!("./greptimedb_data/test/logs".to_string(), logging_opts.dir);
    }

    #[test]
    fn test_load_log_options_from_cli() {
        let cmd = StartCommand {
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let opts = cmd
            .load_options(&GlobalOptions {
                log_dir: Some("./greptimedb_data/test/logs".to_string()),
                log_level: Some("debug".to_string()),

                #[cfg(feature = "tokio-console")]
                tokio_console_addr: None,
            })
            .unwrap()
            .component;

        assert_eq!("./greptimedb_data/test/logs", opts.logging.dir);
        assert_eq!("debug", opts.logging.level.unwrap());
    }

    #[test]
    fn test_config_precedence_order() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "standalone"

            [http]
            addr = "127.0.0.1:4000"

            [logging]
            level = "debug"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let env_prefix = "STANDALONE_UT";
        temp_env::with_vars(
            [
                (
                    // logging.dir = /other/log/dir
                    [
                        env_prefix.to_string(),
                        "logging".to_uppercase(),
                        "dir".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("/other/log/dir"),
                ),
                (
                    // logging.level = info
                    [
                        env_prefix.to_string(),
                        "logging".to_uppercase(),
                        "level".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("info"),
                ),
                (
                    // http.addr = 127.0.0.1:24000
                    [
                        env_prefix.to_string(),
                        "http".to_uppercase(),
                        "addr".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("127.0.0.1:24000"),
                ),
            ],
            || {
                let command = StartCommand {
                    config_file: Some(file.path().to_str().unwrap().to_string()),
                    http_addr: Some("127.0.0.1:14000".to_string()),
                    env_prefix: env_prefix.to_string(),
                    ..Default::default()
                };

                let opts = command.load_options(&Default::default()).unwrap().component;

                // Should be read from env, env > default values.
                assert_eq!(opts.logging.dir, "/other/log/dir");

                // Should be read from config file, config file > env > default values.
                assert_eq!(opts.logging.level.as_ref().unwrap(), "debug");

                // Should be read from cli, cli > config file > env > default values.
                let fe_opts = opts.frontend_options();
                assert_eq!(fe_opts.http.addr, "127.0.0.1:14000");
                assert_eq!(ReadableSize::mb(64), fe_opts.http.body_limit);

                // Should be default value.
                assert_eq!(fe_opts.grpc.bind_addr, GrpcOptions::default().bind_addr);
            },
        );
    }

    #[test]
    fn test_load_default_standalone_options() {
        let options =
            StandaloneOptions::load_layered_options(None, "GREPTIMEDB_STANDALONE").unwrap();
        let default_options = StandaloneOptions::default();
        assert_eq!(options.enable_telemetry, default_options.enable_telemetry);
        assert_eq!(options.http, default_options.http);
        assert_eq!(options.grpc, default_options.grpc);
        assert_eq!(options.mysql, default_options.mysql);
        assert_eq!(options.postgres, default_options.postgres);
        assert_eq!(options.opentsdb, default_options.opentsdb);
        assert_eq!(options.influxdb, default_options.influxdb);
        assert_eq!(options.prom_store, default_options.prom_store);
        assert_eq!(options.wal, default_options.wal);
        assert_eq!(options.metadata_store, default_options.metadata_store);
        assert_eq!(options.procedure, default_options.procedure);
        assert_eq!(options.logging, default_options.logging);
        assert_eq!(options.region_engine, default_options.region_engine);
    }
}
