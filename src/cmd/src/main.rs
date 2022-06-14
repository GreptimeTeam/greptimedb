use std::fmt;

use clap::Parser;
use common_telemetry::{self, logging::error, logging::info};

use crate::error::Result;

pub mod datanode;
pub mod error;

#[derive(Parser)]
struct Command {
    #[clap(long, default_value = "logs")]
    log_dir: String,
    #[clap(long, default_value = "info")]
    log_level: String,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    async fn run(self) -> Result<()> {
        self.subcmd.run().await
    }
}

#[derive(Parser)]
enum SubCommand {
    DataNode(datanode::Command),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::DataNode(cmd) => cmd.run().await,
        }
    }
}

impl fmt::Display for SubCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubCommand::DataNode(..) => write!(f, "data-node"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Command::parse();
    let subcmd_name = cmd.subcmd.to_string();
    // TODO(dennis):
    // 1. adds ip/port to app
    let app_name = &format!("{subcmd_name:?}").to_lowercase();
    let log_dir = &cmd.log_dir;
    let log_level = &cmd.log_level;

    common_telemetry::set_panic_hook();
    common_telemetry::init_default_metrics_recorder();
    let _guard = common_telemetry::init_global_logging(app_name, log_dir, log_level, false);

    tokio::select! {
        result = cmd.run() => {
            if let Err(err) = result {
                error!(err; "Fatal error occurs!");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Goodbye!");
        }
    }

    Ok(())
}
