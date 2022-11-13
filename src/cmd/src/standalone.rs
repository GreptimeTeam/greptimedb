use clap::Parser;
use common_telemetry::info;
use datanode::datanode::{Datanode, DatanodeOptions};
use datanode::instance::InstanceRef;
use frontend::frontend::{Frontend, FrontendOptions, Mode};
use frontend::grpc::GrpcOptions;
use frontend::influxdb::InfluxdbOptions;
use frontend::instance::Instance as FeInstance;
use frontend::mysql::MysqlOptions;
use frontend::opentsdb::OpentsdbOptions;
use frontend::postgres::PostgresOptions;
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
        info!("Datanode start command: {:#?}", self);
        let fe_opts = FrontendOptions::try_from(self)?;
        let dn_opts = DatanodeOptions::default();

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
    Ok(Frontend::new(fe_opts, frontend_instance))
}

impl TryFrom<StartCommand> for FrontendOptions {
    type Error = Error;

    fn try_from(cmd: StartCommand) -> std::result::Result<Self, Self::Error> {
        let mut opts: FrontendOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            FrontendOptions::default()
        };

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

        opts.influxdb_options = Some(InfluxdbOptions {
            enable: cmd.influxdb_enable,
        });

        Ok(opts)
    }
}
