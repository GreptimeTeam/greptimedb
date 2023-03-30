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

use clap::Parser;
use common_base::Plugins;
use common_telemetry::info;
use datanode::datanode::{
    CompactionConfig, Datanode, DatanodeOptions, ProcedureConfig, StorageConfig, WalConfig,
};
use datanode::instance::InstanceRef;
use frontend::frontend::FrontendOptions;
use frontend::grpc::GrpcOptions;
use frontend::influxdb::InfluxdbOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance};
use frontend::mysql::MysqlOptions;
use frontend::opentsdb::OpentsdbOptions;
use frontend::postgres::PostgresOptions;
use frontend::prom::PromOptions;
use frontend::prometheus::PrometheusOptions;
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;

use crate::error::{
    Error, IllegalConfigSnafu, Result, ShutdownDatanodeSnafu, ShutdownFrontendSnafu,
    StartDatanodeSnafu, StartFrontendSnafu,
};
use crate::frontend::load_frontend_plugins;
use crate::toml_loader;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(self) -> Result<Instance> {
        self.subcmd.build().await
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(self) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build().await,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct StandaloneOptions {
    pub mode: Mode,
    pub enable_memory_catalog: bool,
    pub http_options: Option<HttpOptions>,
    pub grpc_options: Option<GrpcOptions>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
    pub influxdb_options: Option<InfluxdbOptions>,
    pub prometheus_options: Option<PrometheusOptions>,
    pub prom_options: Option<PromOptions>,
    pub wal: WalConfig,
    pub storage: StorageConfig,
    pub compaction: CompactionConfig,
    pub procedure: Option<ProcedureConfig>,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_memory_catalog: false,
            http_options: Some(HttpOptions::default()),
            grpc_options: Some(GrpcOptions::default()),
            mysql_options: Some(MysqlOptions::default()),
            postgres_options: Some(PostgresOptions::default()),
            opentsdb_options: Some(OpentsdbOptions::default()),
            influxdb_options: Some(InfluxdbOptions::default()),
            prometheus_options: Some(PrometheusOptions::default()),
            prom_options: Some(PromOptions::default()),
            wal: WalConfig::default(),
            storage: StorageConfig::default(),
            compaction: CompactionConfig::default(),
            procedure: None,
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
            prometheus_options: self.prometheus_options,
            prom_options: self.prom_options,
            meta_client_options: None,
        }
    }

    fn datanode_options(self) -> DatanodeOptions {
        DatanodeOptions {
            enable_memory_catalog: self.enable_memory_catalog,
            wal: self.wal,
            storage: self.storage,
            procedure: self.procedure,
            ..Default::default()
        }
    }
}

pub struct Instance {
    datanode: Datanode,
    frontend: FeInstance,
}

impl Instance {
    pub async fn run(&mut self) -> Result<()> {
        // Start datanode instance before starting services, to avoid requests come in before internal components are started.
        self.datanode
            .start_instance()
            .await
            .context(StartDatanodeSnafu)?;
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
            .shutdown_instance()
            .await
            .context(ShutdownDatanodeSnafu)?;
        info!("Datanode instance stopped.");

        Ok(())
    }
}

#[derive(Debug, Parser)]
struct StartCommand {
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    rpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    prom_addr: Option<String>,
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(long)]
    opentsdb_addr: Option<String>,
    #[clap(short, long)]
    influxdb_enable: bool,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(short = 'm', long = "memory-catalog")]
    enable_memory_catalog: bool,
    #[clap(long)]
    tls_mode: Option<TlsMode>,
    #[clap(long)]
    tls_cert_path: Option<String>,
    #[clap(long)]
    tls_key_path: Option<String>,
    #[clap(long)]
    user_provider: Option<String>,
}

impl StartCommand {
    async fn build(self) -> Result<Instance> {
        let enable_memory_catalog = self.enable_memory_catalog;
        let config_file = self.config_file.clone();
        let plugins = Arc::new(load_frontend_plugins(&self.user_provider)?);
        let fe_opts = FrontendOptions::try_from(self)?;
        let dn_opts: DatanodeOptions = {
            let mut opts: StandaloneOptions = if let Some(path) = config_file {
                toml_loader::from_file!(&path)?
            } else {
                StandaloneOptions::default()
            };
            opts.enable_memory_catalog = enable_memory_catalog;
            opts.datanode_options()
        };

        info!(
            "Standalone frontend options: {:#?}, datanode options: {:#?}",
            fe_opts, dn_opts
        );

        let datanode = Datanode::new(dn_opts.clone())
            .await
            .context(StartDatanodeSnafu)?;

        let mut frontend = build_frontend(plugins.clone(), datanode.get_instance()).await?;

        frontend
            .build_servers(&fe_opts, plugins)
            .await
            .context(StartFrontendSnafu)?;

        Ok(Instance { datanode, frontend })
    }
}

/// Build frontend instance in standalone mode
async fn build_frontend(
    plugins: Arc<Plugins>,
    datanode_instance: InstanceRef,
) -> Result<FeInstance> {
    let mut frontend_instance = FeInstance::new_standalone(datanode_instance.clone());
    frontend_instance.set_script_handler(datanode_instance);
    frontend_instance.set_plugins(plugins.clone());
    Ok(frontend_instance)
}

impl TryFrom<StartCommand> for FrontendOptions {
    type Error = Error;

    fn try_from(cmd: StartCommand) -> std::result::Result<Self, Self::Error> {
        let opts: StandaloneOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            StandaloneOptions::default()
        };

        let mut opts = opts.frontend_options();

        opts.mode = Mode::Standalone;

        if let Some(addr) = cmd.http_addr {
            opts.http_options = Some(HttpOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.rpc_addr {
            // frontend grpc addr conflict with datanode default grpc addr
            let datanode_grpc_addr = DatanodeOptions::default().rpc_addr;
            if addr == datanode_grpc_addr {
                return IllegalConfigSnafu {
                    msg: format!(
                        "gRPC listen address conflicts with datanode reserved gRPC addr: {datanode_grpc_addr}",
                    ),
                }
                .fail();
            }
            opts.grpc_options = Some(GrpcOptions {
                addr,
                ..Default::default()
            });
        }

        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_options = Some(MysqlOptions {
                addr,
                ..Default::default()
            })
        }

        if let Some(addr) = cmd.prom_addr {
            opts.prom_options = Some(PromOptions { addr })
        }

        if let Some(addr) = cmd.postgres_addr {
            opts.postgres_options = Some(PostgresOptions {
                addr,
                ..Default::default()
            })
        }

        if let Some(addr) = cmd.opentsdb_addr {
            opts.opentsdb_options = Some(OpentsdbOptions {
                addr,
                ..Default::default()
            });
        }

        if cmd.influxdb_enable {
            opts.influxdb_options = Some(InfluxdbOptions { enable: true });
        }

        let tls_option = TlsOption::new(cmd.tls_mode, cmd.tls_cert_path, cmd.tls_key_path);

        if let Some(mut mysql_options) = opts.mysql_options {
            mysql_options.tls = tls_option.clone();
            opts.mysql_options = Some(mysql_options);
        }

        if let Some(mut postgres_options) = opts.postgres_options {
            postgres_options.tls = tls_option;
            opts.postgres_options = Some(postgres_options);
        }

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use servers::auth::{Identity, Password, UserProviderRef};

    use super::*;

    #[test]
    fn test_read_config_file() {
        let cmd = StartCommand {
            http_addr: None,
            rpc_addr: None,
            mysql_addr: None,
            prom_addr: None,
            postgres_addr: None,
            opentsdb_addr: None,
            config_file: Some(format!(
                "{}/../../config/standalone.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
            influxdb_enable: false,
            enable_memory_catalog: false,
            tls_mode: None,
            tls_cert_path: None,
            tls_key_path: None,
            user_provider: None,
        };

        let fe_opts = FrontendOptions::try_from(cmd).unwrap();
        assert_eq!(Mode::Standalone, fe_opts.mode);
        assert_eq!(
            "127.0.0.1:4000".to_string(),
            fe_opts.http_options.as_ref().unwrap().addr
        );
        assert_eq!(
            Duration::from_secs(30),
            fe_opts.http_options.as_ref().unwrap().timeout
        );
        assert_eq!(
            "127.0.0.1:4001".to_string(),
            fe_opts.grpc_options.unwrap().addr
        );
        assert_eq!(
            "127.0.0.1:4002",
            fe_opts.mysql_options.as_ref().unwrap().addr
        );
        assert_eq!(2, fe_opts.mysql_options.as_ref().unwrap().runtime_size);
        assert_eq!(
            None,
            fe_opts.mysql_options.as_ref().unwrap().reject_no_database
        );
        assert!(fe_opts.influxdb_options.as_ref().unwrap().enable);
    }

    #[tokio::test]
    async fn test_try_from_start_command_to_anymap() {
        let command = StartCommand {
            http_addr: None,
            rpc_addr: None,
            prom_addr: None,
            mysql_addr: None,
            postgres_addr: None,
            opentsdb_addr: None,
            config_file: None,
            influxdb_enable: false,
            enable_memory_catalog: false,
            tls_mode: None,
            tls_cert_path: None,
            tls_key_path: None,
            user_provider: Some("static_user_provider:cmd:test=test".to_string()),
        };

        let plugins = load_frontend_plugins(&command.user_provider);
        assert!(plugins.is_ok());
        let plugins = plugins.unwrap();
        let provider = plugins.get::<UserProviderRef>();
        assert!(provider.is_some());
        let provider = provider.unwrap();
        let result = provider
            .authenticate(Identity::UserId("test", None), Password::PlainText("test"))
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_toml() {
        let opts = StandaloneOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: StandaloneOptions = toml::from_str(&toml_string).unwrap();
    }
}
