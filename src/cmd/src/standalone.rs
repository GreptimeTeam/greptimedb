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

use std::sync::Arc;
use std::{fs, path};

use async_trait::async_trait;
use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use catalog::kvbackend::KvBackendCatalogManager;
use clap::Parser;
use common_base::Plugins;
use common_catalog::consts::{MIN_USER_FLOW_ID, MIN_USER_TABLE_ID};
use common_config::{metadata_store_dir, Configurable, KvBackendConfig};
use common_error::ext::BoxedError;
use common_meta::cache::LayeredCacheRegistryBuilder;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl::flow_meta::{FlowMetadataAllocator, FlowMetadataAllocatorRef};
use common_meta::ddl::table_meta::{TableMetadataAllocator, TableMetadataAllocatorRef};
use common_meta::ddl::{DdlContext, NoopRegionFailureDetectorControl, ProcedureExecutorRef};
use common_meta::ddl_manager::DdlManager;
use common_meta::key::flow::{FlowMetadataManager, FlowMetadataManagerRef};
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_meta::node_manager::NodeManagerRef;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::sequence::SequenceBuilder;
use common_meta::wal_options_allocator::{WalOptionsAllocator, WalOptionsAllocatorRef};
use common_procedure::ProcedureManagerRef;
use common_telemetry::info;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_time::timezone::set_default_timezone;
use common_version::{short_version, version};
use common_wal::config::DatanodeWalConfig;
use datanode::config::{DatanodeOptions, ProcedureConfig, RegionEngineConfig, StorageConfig};
use datanode::datanode::{Datanode, DatanodeBuilder};
use file_engine::config::EngineConfig as FileEngineConfig;
use flow::{FlowWorkerManager, FlownodeBuilder, FrontendInvoker};
use frontend::frontend::FrontendOptions;
use frontend::instance::builder::FrontendBuilder;
use frontend::instance::{FrontendInstance, Instance as FeInstance, StandaloneDatanodeManager};
use frontend::server::Services;
use frontend::service_config::{
    InfluxdbOptions, MysqlOptions, OpentsdbOptions, PostgresOptions, PromStoreOptions,
};
use meta_srv::metasrv::{FLOW_ID_SEQ, TABLE_ID_SEQ};
use mito2::config::MitoConfig;
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;
use tokio::sync::broadcast;
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::{
    BuildCacheRegistrySnafu, CreateDirSnafu, IllegalConfigSnafu, InitDdlManagerSnafu,
    InitMetadataSnafu, InitTimezoneSnafu, LoadLayeredConfigSnafu, OtherSnafu, Result,
    ShutdownDatanodeSnafu, ShutdownFlownodeSnafu, ShutdownFrontendSnafu, StartDatanodeSnafu,
    StartFlownodeSnafu, StartFrontendSnafu, StartProcedureManagerSnafu,
    StartWalOptionsAllocatorSnafu, StopProcedureManagerSnafu,
};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::{log_versions, App};

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
    pub mode: Mode,
    pub enable_telemetry: bool,
    pub default_timezone: Option<String>,
    pub http: HttpOptions,
    pub grpc: GrpcOptions,
    pub mysql: MysqlOptions,
    pub postgres: PostgresOptions,
    pub opentsdb: OpentsdbOptions,
    pub influxdb: InfluxdbOptions,
    pub prom_store: PromStoreOptions,
    pub wal: DatanodeWalConfig,
    pub storage: StorageConfig,
    pub metadata_store: KvBackendConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
    pub user_provider: Option<String>,
    /// Options for different store engines.
    pub region_engine: Vec<RegionEngineConfig>,
    pub export_metrics: ExportMetricsOption,
    pub tracing: TracingOptions,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_telemetry: true,
            default_timezone: None,
            http: HttpOptions::default(),
            grpc: GrpcOptions::default(),
            mysql: MysqlOptions::default(),
            postgres: PostgresOptions::default(),
            opentsdb: OpentsdbOptions::default(),
            influxdb: InfluxdbOptions::default(),
            prom_store: PromStoreOptions::default(),
            wal: DatanodeWalConfig::default(),
            storage: StorageConfig::default(),
            metadata_store: KvBackendConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
            export_metrics: ExportMetricsOption::default(),
            user_provider: None,
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
            tracing: TracingOptions::default(),
        }
    }
}

impl Configurable for StandaloneOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["wal.broker_endpoints"])
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
            prom_store: cloned_opts.prom_store,
            meta_client: None,
            logging: cloned_opts.logging,
            user_provider: cloned_opts.user_provider,
            // Handle the export metrics task run by standalone to frontend for execution
            export_metrics: cloned_opts.export_metrics,
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
            ..Default::default()
        }
    }
}

pub struct Instance {
    datanode: Datanode,
    frontend: FeInstance,
    // TODO(discord9): wrapped it in flownode instance instead
    flow_worker_manager: Arc<FlowWorkerManager>,
    flow_shutdown: broadcast::Sender<()>,
    procedure_manager: ProcedureManagerRef,
    wal_options_allocator: WalOptionsAllocatorRef,

    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
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
            .context(StartProcedureManagerSnafu)?;

        self.wal_options_allocator
            .start()
            .await
            .context(StartWalOptionsAllocatorSnafu)?;

        plugins::start_frontend_plugins(self.frontend.plugins().clone())
            .await
            .context(StartFrontendSnafu)?;

        self.frontend.start().await.context(StartFrontendSnafu)?;
        self.flow_worker_manager
            .clone()
            .run_background(Some(self.flow_shutdown.subscribe()));
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.frontend
            .shutdown()
            .await
            .context(ShutdownFrontendSnafu)?;

        self.procedure_manager
            .stop()
            .await
            .context(StopProcedureManagerSnafu)?;

        self.datanode
            .shutdown()
            .await
            .context(ShutdownDatanodeSnafu)?;
        self.flow_shutdown
            .send(())
            .map_err(|_e| {
                flow::error::InternalSnafu {
                    reason: "Failed to send shutdown signal to flow worker manager, all receiver end already closed".to_string(),
                }
                .build()
            })
            .context(ShutdownFlownodeSnafu)?;
        info!("Datanode instance stopped.");

        Ok(())
    }
}

#[derive(Debug, Default, Parser)]
pub struct StartCommand {
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    rpc_addr: Option<String>,
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
    fn load_options(
        &self,
        global_options: &GlobalOptions,
    ) -> Result<GreptimeOptions<StandaloneOptions>> {
        let mut opts = GreptimeOptions::<StandaloneOptions>::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
        )
        .context(LoadLayeredConfigSnafu)?;

        self.merge_with_cli_options(global_options, &mut opts.component)?;

        Ok(opts)
    }

    // The precedence order is: cli > config file > environment variables > default values.
    pub fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        opts: &mut StandaloneOptions,
    ) -> Result<()> {
        // Should always be standalone mode.
        opts.mode = Mode::Standalone;

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

        if let Some(addr) = &self.rpc_addr {
            // frontend grpc addr conflict with datanode default grpc addr
            let datanode_grpc_addr = DatanodeOptions::default().grpc.addr;
            if addr.eq(&datanode_grpc_addr) {
                return IllegalConfigSnafu {
                    msg: format!(
                        "gRPC listen address conflicts with datanode reserved gRPC addr: {datanode_grpc_addr}",
                    ),
                }.fail();
            }
            opts.grpc.addr.clone_from(addr)
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
    async fn build(&self, opts: GreptimeOptions<StandaloneOptions>) -> Result<Instance> {
        common_runtime::init_global_runtimes(&opts.runtime);

        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts.component.logging,
            &opts.component.tracing,
            None,
        );
        log_versions(version(), short_version());

        info!("Standalone start command: {:#?}", self);
        info!("Standalone options: {opts:#?}");

        let mut plugins = Plugins::new();
        let opts = opts.component;
        let fe_opts = opts.frontend_options();
        let dn_opts = opts.datanode_options();

        plugins::setup_frontend_plugins(&mut plugins, &fe_opts)
            .await
            .context(StartFrontendSnafu)?;

        plugins::setup_datanode_plugins(&mut plugins, &dn_opts)
            .await
            .context(StartDatanodeSnafu)?;

        set_default_timezone(fe_opts.default_timezone.as_deref()).context(InitTimezoneSnafu)?;

        let data_home = &dn_opts.storage.data_home;
        // Ensure the data_home directory exists.
        fs::create_dir_all(path::Path::new(data_home))
            .context(CreateDirSnafu { dir: data_home })?;

        let metadata_dir = metadata_store_dir(data_home);
        let (kv_backend, procedure_manager) = FeInstance::try_build_standalone_components(
            metadata_dir,
            opts.metadata_store.clone(),
            opts.procedure.clone(),
        )
        .await
        .context(StartFrontendSnafu)?;

        // Builds cache registry
        let layered_cache_builder = LayeredCacheRegistryBuilder::default();
        let fundamental_cache_registry = build_fundamental_cache_registry(kv_backend.clone());
        let layered_cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .context(BuildCacheRegistrySnafu)?
            .build(),
        );

        let catalog_manager = KvBackendCatalogManager::new(
            dn_opts.mode,
            None,
            kv_backend.clone(),
            layered_cache_registry.clone(),
        );

        let table_metadata_manager =
            Self::create_table_metadata_manager(kv_backend.clone()).await?;

        let datanode = DatanodeBuilder::new(dn_opts, plugins.clone())
            .with_kv_backend(kv_backend.clone())
            .build()
            .await
            .context(StartDatanodeSnafu)?;

        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let flow_builder = FlownodeBuilder::new(
            Default::default(),
            plugins.clone(),
            table_metadata_manager.clone(),
            catalog_manager.clone(),
            flow_metadata_manager.clone(),
        );
        let flownode = Arc::new(
            flow_builder
                .build()
                .await
                .map_err(BoxedError::new)
                .context(OtherSnafu)?,
        );

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
        let wal_options_allocator = Arc::new(WalOptionsAllocator::new(
            opts.wal.into(),
            kv_backend.clone(),
        ));
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

        let mut frontend = FrontendBuilder::new(
            fe_opts.clone(),
            kv_backend.clone(),
            layered_cache_registry.clone(),
            catalog_manager.clone(),
            node_manager.clone(),
            ddl_task_executor.clone(),
        )
        .with_plugin(plugins.clone())
        .try_build()
        .await
        .context(StartFrontendSnafu)?;

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
        .context(StartFlownodeSnafu)?;
        flow_worker_manager.set_frontend_invoker(invoker).await;

        let (tx, _rx) = broadcast::channel(1);

        let servers = Services::new(fe_opts, Arc::new(frontend.clone()), plugins)
            .build()
            .await
            .context(StartFrontendSnafu)?;
        frontend
            .build_servers(servers)
            .context(StartFrontendSnafu)?;

        Ok(Instance {
            datanode,
            frontend,
            flow_worker_manager,
            flow_shutdown: tx,
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
                    table_metadata_manager,
                    table_metadata_allocator,
                    flow_metadata_manager,
                    flow_metadata_allocator,
                    region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
                },
                procedure_manager,
                true,
            )
            .context(InitDdlManagerSnafu)?,
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
            .context(InitMetadataSnafu)?;

        Ok(table_metadata_manager)
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
        plugins::setup_frontend_plugins(&mut plugins, &fe_opts)
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
            dir = "/tmp/greptimedb/test/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [storage]
            data_home = "/tmp/greptimedb/"
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
            dir = "/tmp/greptimedb/test/logs"
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
        assert_eq!("127.0.0.1:4001".to_string(), fe_opts.grpc.addr);
        assert!(fe_opts.mysql.enable);
        assert_eq!("127.0.0.1:4002", fe_opts.mysql.addr);
        assert_eq!(2, fe_opts.mysql.runtime_size);
        assert_eq!(None, fe_opts.mysql.reject_no_database);
        assert!(fe_opts.influxdb.enable);
        assert!(fe_opts.opentsdb.enable);

        let DatanodeWalConfig::RaftEngine(raft_engine_config) = dn_opts.wal else {
            unreachable!()
        };
        assert_eq!("/tmp/greptimedb/test/wal", raft_engine_config.dir.unwrap());

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
        assert_eq!("/tmp/greptimedb/test/logs".to_string(), logging_opts.dir);
    }

    #[test]
    fn test_load_log_options_from_cli() {
        let cmd = StartCommand {
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let opts = cmd
            .load_options(&GlobalOptions {
                log_dir: Some("/tmp/greptimedb/test/logs".to_string()),
                log_level: Some("debug".to_string()),

                #[cfg(feature = "tokio-console")]
                tokio_console_addr: None,
            })
            .unwrap()
            .component;

        assert_eq!("/tmp/greptimedb/test/logs", opts.logging.dir);
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
                assert_eq!(fe_opts.grpc.addr, GrpcOptions::default().addr);
            },
        );
    }

    #[test]
    fn test_load_default_standalone_options() {
        let options =
            StandaloneOptions::load_layered_options(None, "GREPTIMEDB_STANDALONE").unwrap();
        let default_options = StandaloneOptions::default();
        assert_eq!(options.mode, default_options.mode);
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
