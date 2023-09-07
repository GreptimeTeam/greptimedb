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

use catalog::remote::DummyKvCacheInvalidator;
use catalog::CatalogManagerRef;
use clap::Parser;
use common_base::Plugins;
use common_config::{kv_store_dir, KvStoreConfig, WalConfig};
use common_meta::kv_backend::KvBackendRef;
use common_procedure::ProcedureManagerRef;
use common_telemetry::info;
use common_telemetry::logging::LoggingOptions;
use datanode::datanode::{Datanode, DatanodeOptions, ProcedureConfig, StorageConfig};
use datanode::region_server::RegionServer;
use frontend::catalog::FrontendCatalogManager;
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance, StandaloneDatanodeManager};
use frontend::service_config::{
    GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, PostgresOptions, PromStoreOptions,
};
use query::QueryEngineRef;
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;

use crate::error::{
    IllegalConfigSnafu, Result, ShutdownDatanodeSnafu, ShutdownFrontendSnafu, StartDatanodeSnafu,
    StartFrontendSnafu,
};
use crate::frontend::load_frontend_plugins;
use crate::options::{MixOptions, Options, TopLevelOptions};

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(self, opts: MixOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, top_level_options: TopLevelOptions) -> Result<Options> {
        self.subcmd.load_options(top_level_options)
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(self, opts: MixOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(&self, top_level_options: TopLevelOptions) -> Result<Options> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(top_level_options),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct StandaloneOptions {
    pub mode: Mode,
    pub enable_telemetry: bool,
    pub http_options: HttpOptions,
    pub grpc_options: GrpcOptions,
    pub mysql_options: MysqlOptions,
    pub postgres_options: PostgresOptions,
    pub opentsdb_options: OpentsdbOptions,
    pub influxdb_options: InfluxdbOptions,
    pub prom_store_options: PromStoreOptions,
    pub wal: WalConfig,
    pub storage: StorageConfig,
    pub kv_store: KvStoreConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_telemetry: true,
            http_options: HttpOptions::default(),
            grpc_options: GrpcOptions::default(),
            mysql_options: MysqlOptions::default(),
            postgres_options: PostgresOptions::default(),
            opentsdb_options: OpentsdbOptions::default(),
            influxdb_options: InfluxdbOptions::default(),
            prom_store_options: PromStoreOptions::default(),
            wal: WalConfig::default(),
            storage: StorageConfig::default(),
            kv_store: KvStoreConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
        }
    }
}

impl StandaloneOptions {
    fn frontend_options(self) -> FrontendOptions {
        FrontendOptions {
            mode: self.mode,
            http_options: self.http_options,
            grpc_options: self.grpc_options,
            mysql_options: self.mysql_options,
            postgres_options: self.postgres_options,
            opentsdb_options: self.opentsdb_options,
            influxdb_options: self.influxdb_options,
            prom_store_options: self.prom_store_options,
            meta_client_options: None,
            logging: self.logging,
            ..Default::default()
        }
    }

    fn datanode_options(self) -> DatanodeOptions {
        DatanodeOptions {
            enable_telemetry: self.enable_telemetry,
            wal: self.wal,
            storage: self.storage,
            ..Default::default()
        }
    }
}

pub struct Instance {
    datanode: Datanode,
    frontend: FeInstance,
}

impl Instance {
    pub async fn start(&mut self) -> Result<()> {
        // Start datanode instance before starting services, to avoid requests come in before internal components are started.
        self.datanode.start().await.context(StartDatanodeSnafu)?;
        info!("Datanode instance started");

        self.frontend.start().await.context(StartFrontendSnafu)?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.frontend
            .shutdown()
            .await
            .context(ShutdownFrontendSnafu)?;

        self.datanode
            .shutdown()
            .await
            .context(ShutdownDatanodeSnafu)?;
        info!("Datanode instance stopped.");

        Ok(())
    }
}

#[derive(Debug, Default, Parser)]
struct StartCommand {
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    rpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(long)]
    opentsdb_addr: Option<String>,
    #[clap(short, long)]
    influxdb_enable: bool,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(long)]
    tls_mode: Option<TlsMode>,
    #[clap(long)]
    tls_cert_path: Option<String>,
    #[clap(long)]
    tls_key_path: Option<String>,
    #[clap(long)]
    user_provider: Option<String>,
    #[clap(long, default_value = "GREPTIMEDB_STANDALONE")]
    env_prefix: String,
}

impl StartCommand {
    fn load_options(&self, top_level_options: TopLevelOptions) -> Result<Options> {
        let mut opts: StandaloneOptions = Options::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
            None,
        )?;

        opts.mode = Mode::Standalone;

        if let Some(dir) = top_level_options.log_dir {
            opts.logging.dir = dir;
        }

        if top_level_options.log_level.is_some() {
            opts.logging.level = top_level_options.log_level;
        }

        let tls_opts = TlsOption::new(
            self.tls_mode.clone(),
            self.tls_cert_path.clone(),
            self.tls_key_path.clone(),
        );

        if let Some(addr) = &self.http_addr {
            opts.http_options.addr = addr.clone()
        }

        if let Some(addr) = &self.rpc_addr {
            // frontend grpc addr conflict with datanode default grpc addr
            let datanode_grpc_addr = DatanodeOptions::default().rpc_addr;
            if addr.eq(&datanode_grpc_addr) {
                return IllegalConfigSnafu {
                    msg: format!(
                        "gRPC listen address conflicts with datanode reserved gRPC addr: {datanode_grpc_addr}",
                    ),
                }
                .fail();
            }
            opts.grpc_options.addr = addr.clone()
        }

        if let Some(addr) = &self.mysql_addr {
            opts.mysql_options.enable = true;
            opts.mysql_options.addr = addr.clone();
            opts.mysql_options.tls = tls_opts.clone();
        }

        if let Some(addr) = &self.postgres_addr {
            opts.postgres_options.enable = true;
            opts.postgres_options.addr = addr.clone();
            opts.postgres_options.tls = tls_opts;
        }

        if let Some(addr) = &self.opentsdb_addr {
            opts.opentsdb_options.enable = true;
            opts.opentsdb_options.addr = addr.clone();
        }

        if self.influxdb_enable {
            opts.influxdb_options.enable = self.influxdb_enable;
        }
        let kv_store_cfg = opts.kv_store.clone();
        let procedure_cfg = opts.procedure.clone();
        let fe_opts = opts.clone().frontend_options();
        let logging_opts = opts.logging.clone();
        let dn_opts = opts.datanode_options();

        Ok(Options::Standalone(Box::new(MixOptions {
            procedure_cfg,
            kv_store_cfg,
            data_home: dn_opts.storage.data_home.to_string(),
            fe_opts,
            dn_opts,
            logging_opts,
        })))
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    #[allow(clippy::diverging_sub_expression)]
    async fn build(self, opts: MixOptions) -> Result<Instance> {
        let plugins = Arc::new(load_frontend_plugins(&self.user_provider)?);
        let fe_opts = opts.fe_opts;
        let dn_opts = opts.dn_opts;

        info!("Standalone start command: {:#?}", self);
        info!(
            "Standalone frontend options: {:#?}, datanode options: {:#?}",
            fe_opts, dn_opts
        );

        let kv_dir = kv_store_dir(&opts.data_home);
        let (kv_store, procedure_manager) = FeInstance::try_build_standalone_components(
            kv_dir,
            opts.kv_store_cfg,
            opts.procedure_cfg,
        )
        .await
        .context(StartFrontendSnafu)?;

        let datanode = Datanode::new(dn_opts.clone(), plugins.clone())
            .await
            .context(StartDatanodeSnafu)?;
        let region_server = datanode.region_server();

        let catalog_manager = Arc::new(FrontendCatalogManager::new(
            kv_store.clone(),
            Arc::new(DummyKvCacheInvalidator),
            Arc::new(StandaloneDatanodeManager(region_server.clone())),
        ));

        // TODO: build frontend instance like in distributed mode
        let mut frontend = build_frontend(
            plugins,
            kv_store,
            procedure_manager,
            catalog_manager,
            datanode.query_engine(),
            region_server,
        )
        .await?;

        frontend
            .build_servers(&fe_opts)
            .await
            .context(StartFrontendSnafu)?;

        Ok(Instance { datanode, frontend })
    }
}

/// Build frontend instance in standalone mode
async fn build_frontend(
    plugins: Arc<Plugins>,
    kv_store: KvBackendRef,
    procedure_manager: ProcedureManagerRef,
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    region_server: RegionServer,
) -> Result<FeInstance> {
    let mut frontend_instance = FeInstance::try_new_standalone(
        kv_store,
        procedure_manager,
        catalog_manager,
        query_engine,
        region_server,
    )
    .await
    .context(StartFrontendSnafu)?;
    frontend_instance.set_plugins(plugins.clone());
    Ok(frontend_instance)
}

#[cfg(test)]
mod tests {
    use std::default::Default;
    use std::io::Write;
    use std::time::Duration;

    use auth::{Identity, Password, UserProviderRef};
    use common_base::readable_size::ReadableSize;
    use common_test_util::temp_dir::create_named_temp_file;
    use servers::Mode;

    use super::*;
    use crate::options::ENV_VAR_SEP;

    #[tokio::test]
    async fn test_try_from_start_command_to_anymap() {
        let command = StartCommand {
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let plugins = load_frontend_plugins(&command.user_provider);
        let plugins = plugins.unwrap();
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
            dir = "/tmp/greptimedb/test/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [storage]
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

            [http_options]
            addr = "127.0.0.1:4000"
            timeout = "30s"
            body_limit = "128MB"

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

        let Options::Standalone(options) = cmd.load_options(TopLevelOptions::default()).unwrap()
        else {
            unreachable!()
        };
        let fe_opts = options.fe_opts;
        let dn_opts = options.dn_opts;
        let logging_opts = options.logging_opts;
        assert_eq!(Mode::Standalone, fe_opts.mode);
        assert_eq!("127.0.0.1:4000".to_string(), fe_opts.http_options.addr);
        assert_eq!(Duration::from_secs(30), fe_opts.http_options.timeout);
        assert_eq!(ReadableSize::mb(128), fe_opts.http_options.body_limit);
        assert_eq!("127.0.0.1:4001".to_string(), fe_opts.grpc_options.addr);
        assert!(fe_opts.mysql_options.enable);
        assert_eq!("127.0.0.1:4002", fe_opts.mysql_options.addr);
        assert_eq!(2, fe_opts.mysql_options.runtime_size);
        assert_eq!(None, fe_opts.mysql_options.reject_no_database);
        assert!(fe_opts.influxdb_options.enable);

        match &dn_opts.storage.store {
            datanode::datanode::ObjectStoreConfig::S3(s3_config) => {
                assert_eq!(
                    "Secret([REDACTED alloc::string::String])".to_string(),
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
    fn test_top_level_options() {
        let cmd = StartCommand {
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
            ..Default::default()
        };

        let Options::Standalone(opts) = cmd
            .load_options(TopLevelOptions {
                log_dir: Some("/tmp/greptimedb/test/logs".to_string()),
                log_level: Some("debug".to_string()),
            })
            .unwrap()
        else {
            unreachable!()
        };

        assert_eq!("/tmp/greptimedb/test/logs", opts.logging_opts.dir);
        assert_eq!("debug", opts.logging_opts.level.unwrap());
    }

    #[test]
    fn test_config_precedence_order() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "standalone"

            [http_options]
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
                    // http_options.addr = 127.0.0.1:24000
                    [
                        env_prefix.to_string(),
                        "http_options".to_uppercase(),
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

                let top_level_opts = TopLevelOptions {
                    log_dir: None,
                    log_level: None,
                };
                let Options::Standalone(opts) = command.load_options(top_level_opts).unwrap()
                else {
                    unreachable!()
                };

                // Should be read from env, env > default values.
                assert_eq!(opts.logging_opts.dir, "/other/log/dir");

                // Should be read from config file, config file > env > default values.
                assert_eq!(opts.logging_opts.level.as_ref().unwrap(), "debug");

                // Should be read from cli, cli > config file > env > default values.
                assert_eq!(opts.fe_opts.http_options.addr, "127.0.0.1:14000");
                assert_eq!(ReadableSize::mb(64), opts.fe_opts.http_options.body_limit);

                // Should be default value.
                assert_eq!(opts.fe_opts.grpc_options.addr, GrpcOptions::default().addr);
            },
        );
    }
}
