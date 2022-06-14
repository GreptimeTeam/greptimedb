use clap::Parser;
use datanode::{DataNode, DataNodeOptions};
use snafu::ResultExt;

use crate::error::{Result, StartDataNodeSnafu};

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
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        DataNode::new(&self.into())
            .context(StartDataNodeSnafu)?
            .start()
            .await
            .context(StartDataNodeSnafu)
    }
}

impl From<StartCommand> for DataNodeOptions {
    fn from(cmd: StartCommand) -> Self {
        DataNodeOptions {
            http_addr: cmd.http_addr,
            rpc_addr: cmd.rpc_addr,
        }
    }
}
