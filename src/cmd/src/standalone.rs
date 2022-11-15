use clap::Parser;
use common_telemetry::info;
use datanode::datanode::{Datanode, DatanodeOptions, ObjectStoreConfig};
use datanode::instance::InstanceRef;
use frontend::frontend::{Frontend, FrontendOptions, Mode};
use frontend::grpc::GrpcOptions;
use frontend::influxdb::InfluxdbOptions;
use frontend::instance::Instance as FeInstance;
use frontend::mysql::MysqlOptions;
use frontend::opentsdb::OpentsdbOptions;
use frontend::postgres::PostgresOptions;
use frontend::prometheus::PrometheusOptions;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::try_join;

use crate::error::{
    BuildFrontendSnafu, Error, IllegalConfigSnafu, Result, StartDatanodeSnafu, StartFrontendSnafu,
};
use crate::toml_loader;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn run(self) -> Result<()> {
        self.subcmd.run().await
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Start(cmd) => cmd.run().await,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StandaloneOptions {
    pub http_addr: Option<String>,
    pub grpc_options: Option<GrpcOptions>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
    pub influxdb_options: Option<InfluxdbOptions>,
    pub prometheus_options: Option<PrometheusOptions>,
    pub mode: Mode,
    pub wal_dir: String,
    pub storage: ObjectStoreConfig,
    pub datanode_mysql_addr: String,
    pub datanode_mysql_runtime_size: usize,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            http_addr: Some("0.0.0.0:4000".to_string()),
            grpc_options: Some(GrpcOptions::default()),
            mysql_options: Some(MysqlOptions::default()),
            postgres_options: Some(PostgresOptions::default()),
            opentsdb_options: Some(OpentsdbOptions::default()),
            influxdb_options: Some(InfluxdbOptions::default()),
            prometheus_options: Some(PrometheusOptions::default()),
            mode: Mode::Standalone,
            wal_dir: "/tmp/greptimedb/wal".to_string(),
            storage: ObjectStoreConfig::default(),
            datanode_mysql_addr: "0.0.0.0:3306".to_string(),
            datanode_mysql_runtime_size: 4,
        }
    }
}

impl StandaloneOptions {
    fn frontend_options(self) -> FrontendOptions {
        FrontendOptions {
            http_addr: self.http_addr,
            grpc_options: self.grpc_options,
            mysql_options: self.mysql_options,
            postgres_options: self.postgres_options,
            opentsdb_options: self.opentsdb_options,
            influxdb_options: self.influxdb_options,
            prometheus_options: self.prometheus_options,
            mode: self.mode,
            datanode_rpc_addr: "127.0.0.1:3001".to_string(),
            metasrv_addr: None,
        }
    }

    fn datanode_options(self) -> DatanodeOptions {
        DatanodeOptions {
            wal_dir: self.wal_dir,
            storage: self.storage,
            mysql_addr: self.datanode_mysql_addr,
            mysql_runtime_size: self.datanode_mysql_runtime_size,
            ..Default::default()
        }
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
    postgres_addr: Option<String>,
    #[clap(long)]
    opentsdb_addr: Option<String>,
    #[clap(short, long)]
    influxdb_enable: bool,
    #[clap(short, long)]
    config_file: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        let config_file = self.config_file.clone();
        let fe_opts = FrontendOptions::try_from(self)?;
        let dn_opts: DatanodeOptions = {
            let opts: StandaloneOptions = if let Some(path) = config_file {
                toml_loader::from_file!(&path)?
            } else {
                StandaloneOptions::default()
            };
            opts.datanode_options()
        };

        info!(
            "Standalone frontend options: {:#?}, datanode options: {:#?}",
            fe_opts, dn_opts
        );

        let mut datanode = Datanode::new(dn_opts.clone())
            .await
            .context(StartDatanodeSnafu)?;
        let mut frontend = build_frontend(fe_opts, &dn_opts, datanode.get_instance()).await?;

        try_join!(
            async { datanode.start().await.context(StartDatanodeSnafu) },
            async { frontend.start().await.context(StartFrontendSnafu) }
        )?;

        Ok(())
    }
}

/// Build frontend instance in standalone mode
async fn build_frontend(
    fe_opts: FrontendOptions,
    dn_opts: &DatanodeOptions,
    datanode_instance: InstanceRef,
) -> Result<Frontend<FeInstance>> {
    let grpc_server_addr = &dn_opts.rpc_addr;
    info!(
        "Build frontend with datanode gRPC addr: {}",
        grpc_server_addr
    );
    let mut frontend_instance = FeInstance::try_new(&fe_opts)
        .await
        .context(BuildFrontendSnafu)?;
    frontend_instance.set_catalog_manager(datanode_instance.catalog_manager().clone());
    frontend_instance.set_script_handler(datanode_instance);
    Ok(Frontend::new(fe_opts, frontend_instance))
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
            opts.http_addr = Some(addr);
        }
        if let Some(addr) = cmd.rpc_addr {
            // frontend grpc addr conflict with datanode default grpc addr
            let datanode_grpc_addr = DatanodeOptions::default().rpc_addr;
            if addr == datanode_grpc_addr {
                return IllegalConfigSnafu {
                    msg: format!(
                        "gRPC listen address conflicts with datanode reserved gRPC addr: {}",
                        datanode_grpc_addr
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

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_config_file() {
        let cmd = StartCommand {
            http_addr: None,
            rpc_addr: None,
            mysql_addr: None,
            postgres_addr: None,
            opentsdb_addr: None,
            config_file: Some(format!(
                "{}/../../config/standalone.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
            influxdb_enable: false,
        };

        let fe_opts = FrontendOptions::try_from(cmd).unwrap();
        assert_eq!(Mode::Standalone, fe_opts.mode);
        assert_eq!("127.0.0.1:3001".to_string(), fe_opts.datanode_rpc_addr);
        assert_eq!(Some("0.0.0.0:4000".to_string()), fe_opts.http_addr);
        assert_eq!(
            "0.0.0.0:4001".to_string(),
            fe_opts.grpc_options.unwrap().addr
        );
        assert_eq!("0.0.0.0:4002", fe_opts.mysql_options.as_ref().unwrap().addr);
        assert_eq!(4, fe_opts.mysql_options.as_ref().unwrap().runtime_size);
        assert!(fe_opts.influxdb_options.as_ref().unwrap().enable);
    }
}
