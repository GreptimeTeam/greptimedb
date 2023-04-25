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

#![doc = include_str!("../../../../README.md")]

use std::fmt;

use clap::Parser;
use cmd::error::Result;
use cmd::options::ConfigOptions;
use cmd::{cli, datanode, frontend, metasrv, standalone};
use common_telemetry::logging::{error, info};

#[derive(Parser)]
#[clap(name = "greptimedb", version = print_version())]
struct Command {
    #[clap(long)]
    log_dir: Option<String>,
    #[clap(long)]
    log_level: Option<String>,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

pub enum Application {
    Datanode(datanode::Instance),
    Frontend(frontend::Instance),
    Metasrv(metasrv::Instance),
    Standalone(standalone::Instance),
    Cli(cli::Instance),
}

impl Application {
    async fn run(&mut self) -> Result<()> {
        match self {
            Application::Datanode(instance) => instance.run().await,
            Application::Frontend(instance) => instance.run().await,
            Application::Metasrv(instance) => instance.run().await,
            Application::Standalone(instance) => instance.run().await,
            Application::Cli(instance) => instance.run().await,
        }
    }

    async fn stop(&self) -> Result<()> {
        match self {
            Application::Datanode(instance) => instance.stop().await,
            Application::Frontend(instance) => instance.stop().await,
            Application::Metasrv(instance) => instance.stop().await,
            Application::Standalone(instance) => instance.stop().await,
            Application::Cli(instance) => instance.stop().await,
        }
    }
}

impl Command {
    async fn build(self) -> Result<Application> {
        self.subcmd.build().await
    }

    fn load_options(
        &self,
        log_dir: Option<String>,
        log_level: Option<String>,
    ) -> Result<ConfigOptions> {
        self.subcmd.load_options(log_dir, log_level)
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
    #[clap(name = "cli")]
    Cli(cli::Command),
}

impl SubCommand {
    async fn build(self) -> Result<Application> {
        match self {
            SubCommand::Datanode(cmd) => {
                let app = cmd.build().await?;
                Ok(Application::Datanode(app))
            }
            SubCommand::Frontend(cmd) => {
                let app = cmd.build().await?;
                Ok(Application::Frontend(app))
            }
            SubCommand::Metasrv(cmd) => {
                let app = cmd.build().await?;
                Ok(Application::Metasrv(app))
            }
            SubCommand::Standalone(cmd) => {
                let app = cmd.build().await?;
                Ok(Application::Standalone(app))
            }
            SubCommand::Cli(cmd) => {
                let app = cmd.build().await?;
                Ok(Application::Cli(app))
            }
        }
    }

    fn load_options(
        &self,
        log_dir: Option<String>,
        log_level: Option<String>,
    ) -> Result<ConfigOptions> {
        match self {
            SubCommand::Datanode(cmd) => cmd.load_options(log_dir, log_level),
            SubCommand::Frontend(cmd) => cmd.load_options(log_dir, log_level),
            SubCommand::Metasrv(cmd) => cmd.load_options(log_dir, log_level),
            SubCommand::Standalone(cmd) => cmd.load_options(log_dir, log_level),
            SubCommand::Cli(cmd) => cmd.load_options(log_dir, log_level),
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
            SubCommand::Cli(_) => write!(f, "greptime-cli"),
        }
    }
}

fn print_version() -> &'static str {
    concat!(
        "\nbranch: ",
        env!("GIT_BRANCH"),
        "\ncommit: ",
        env!("GIT_COMMIT"),
        "\ndirty: ",
        env!("GIT_DIRTY"),
        "\nversion: ",
        env!("CARGO_PKG_VERSION")
    )
}

#[cfg(feature = "mem-prof")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Command::parse();
    // TODO(dennis):
    // 1. adds ip/port to app
    let app_name = &cmd.subcmd.to_string();
    let log_dir = cmd.log_dir.clone();
    let log_level = cmd.log_level.clone();

    let opts = cmd.load_options(log_dir, log_level)?;
    let logging_opts = opts.logging_options();

    common_telemetry::set_panic_hook();
    common_telemetry::init_default_metrics_recorder();
    let _guard = common_telemetry::init_global_logging(
        app_name,
        &logging_opts.dir,
        &logging_opts.level,
        logging_opts.enable_jaeger_tracing,
    );

    let mut app = cmd.build().await?;

    tokio::select! {
        result = app.run() => {
            if let Err(err) = result {
                error!(err; "Fatal error occurs!");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            if let Err(err) = app.stop().await {
                error!(err; "Fatal error occurs!");
            }
            info!("Goodbye!");
        }
    }

    Ok(())
}
