use std::fmt;

use clap::Parser;
use cmd::metasrv;
use cmd::{datanode, error::Result, frontend};
use common_telemetry::{self, logging::error, logging::info};

#[derive(Parser)]
#[clap(name = "greptimedb")]
struct Command {
    #[clap(long, default_value = "/tmp/greptimedb/logs")]
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
    #[clap(name = "datanode")]
    Datanode(datanode::Command),
    #[clap(name = "frontend")]
    Frontend(frontend::Command),
    #[clap(name = "metasrv")]
    Metasrv(metasrv::Command),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Datanode(cmd) => cmd.run().await,
            SubCommand::Frontend(cmd) => cmd.run().await,
            SubCommand::Metasrv(cmd) => cmd.run().await,
        }
    }
}

impl fmt::Display for SubCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubCommand::Datanode(..) => write!(f, "greptime-datanode"),
            SubCommand::Frontend(..) => write!(f, "greptime-frontend"),
            SubCommand::Metasrv(..) => write!(f, "greptime-metasrv"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Command::parse();
    // TODO(dennis):
    // 1. adds ip/port to app
    let app_name = &cmd.subcmd.to_string();
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
