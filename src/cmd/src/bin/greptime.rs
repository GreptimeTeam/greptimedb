// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use clap::Parser;
use cmd::error::Result;
use cmd::{datanode, frontend, metasrv, standalone};
use common_telemetry::logging::{error, info};

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
    #[clap(name = "standalone")]
    Standalone(standalone::Command),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Datanode(cmd) => cmd.run().await,
            SubCommand::Frontend(cmd) => cmd.run().await,
            SubCommand::Metasrv(cmd) => cmd.run().await,
            SubCommand::Standalone(cmd) => cmd.run().await,
        }
    }
}

impl fmt::Display for SubCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubCommand::Datanode(..) => write!(f, "greptime-datanode"),
            SubCommand::Frontend(..) => write!(f, "greptime-frontend"),
            SubCommand::Metasrv(..) => write!(f, "greptime-metasrv"),
            SubCommand::Standalone(..) => write!(f, "greptime-standalone"),
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
