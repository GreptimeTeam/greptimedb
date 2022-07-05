use clap::Parser;
use common_options::GreptimeOptions;
use datanode::datanode::Datanode;
use snafu::ResultExt;

use crate::error::{Result, StartDatanodeSnafu};

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
    #[clap(long, default_value = "0.0.0.0:3000")]
    http_addr: String,
    #[clap(long, default_value = "0.0.0.0:3001")]
    rpc_addr: String,
    #[clap(long, default_value = "./greptimedb_wal")]
    wal_dir: String,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        Datanode::new(self.into())
            .await
            .context(StartDatanodeSnafu)?
            .start()
            .await
            .context(StartDatanodeSnafu)
    }
}

impl From<StartCommand> for GreptimeOptions {
    fn from(cmd: StartCommand) -> Self {
        GreptimeOptions {
            http_addr: cmd.http_addr,
            rpc_addr: cmd.rpc_addr,
            wal_dir: cmd.wal_dir,
        }
    }
}
