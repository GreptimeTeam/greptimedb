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
use cmd::options::{Options, TopLevelOptions};
use cmd::{cli, datanode, frontend, metasrv, standalone};
use common_telemetry::logging::{error, info, TracingOptions};
use metrics::gauge;

#[derive(Parser)]
#[clap(name = "greptimedb", version = print_version())]
struct Command {
    #[clap(long)]
    log_dir: Option<String>,
    #[clap(long)]
    log_level: Option<String>,
    #[clap(subcommand)]
    subcmd: SubCommand,

    #[cfg(feature = "tokio-console")]
    #[clap(long)]
    tokio_console_addr: Option<String>,
}

pub enum Application {
    Datanode(datanode::Instance),
    Frontend(frontend::Instance),
    Metasrv(metasrv::Instance),
    Standalone(standalone::Instance),
    Cli(cli::Instance),
}

impl Application {
    async fn start(&mut self) -> Result<()> {
        match self {
            Application::Datanode(instance) => instance.start().await,
            Application::Frontend(instance) => instance.start().await,
            Application::Metasrv(instance) => instance.start().await,
            Application::Standalone(instance) => instance.start().await,
            Application::Cli(instance) => instance.start().await,
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
    async fn build(self, opts: Options) -> Result<Application> {
        self.subcmd.build(opts).await
    }

    fn load_options(&self) -> Result<Options> {
        let top_level_opts = self.top_level_options();
        self.subcmd.load_options(top_level_opts)
    }

    fn top_level_options(&self) -> TopLevelOptions {
        TopLevelOptions {
            log_dir: self.log_dir.clone(),
            log_level: self.log_level.clone(),
        }
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
    async fn build(self, opts: Options) -> Result<Application> {
        match (self, opts) {
            (SubCommand::Datanode(cmd), Options::Datanode(dn_opts)) => {
                let app = cmd.build(*dn_opts).await?;
                Ok(Application::Datanode(app))
            }
            (SubCommand::Frontend(cmd), Options::Frontend(fe_opts)) => {
                let app = cmd.build(*fe_opts).await?;
                Ok(Application::Frontend(app))
            }
            (SubCommand::Metasrv(cmd), Options::Metasrv(meta_opts)) => {
                let app = cmd.build(*meta_opts).await?;
                Ok(Application::Metasrv(app))
            }
            (SubCommand::Standalone(cmd), Options::Standalone(opts)) => {
                let app = cmd.build(opts.fe_opts, opts.dn_opts).await?;
                Ok(Application::Standalone(app))
            }
            (SubCommand::Cli(cmd), Options::Cli(_)) => {
                let app = cmd.build().await?;
                Ok(Application::Cli(app))
            }

            _ => unreachable!(),
        }
    }

    fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        match self {
            SubCommand::Datanode(cmd) => cmd.load_options(top_level_opts),
            SubCommand::Frontend(cmd) => cmd.load_options(top_level_opts),
            SubCommand::Metasrv(cmd) => cmd.load_options(top_level_opts),
            SubCommand::Standalone(cmd) => cmd.load_options(top_level_opts),
            SubCommand::Cli(cmd) => cmd.load_options(top_level_opts),
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

fn short_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

// {app_name}-{branch_name}-{commit_short}
// The branch name (tag) of a release build should already contain the short
// version so the full version doesn't concat the short version explicitly.
fn full_version() -> &'static str {
    concat!(
        "greptimedb-",
        env!("GIT_BRANCH"),
        "-",
        env!("GIT_COMMIT_SHORT")
    )
}

fn log_env_flags() {
    info!("command line arguments");
    for argument in std::env::args() {
        info!("argument: {}", argument);
    }
}

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Command::parse();
    let app_name = &cmd.subcmd.to_string();

    let opts = cmd.load_options()?;
    let logging_opts = opts.logging_options();
    let tracing_opts = TracingOptions {
        #[cfg(feature = "tokio-console")]
        tokio_console_addr: cmd.tokio_console_addr.clone(),
    };

    common_telemetry::set_panic_hook();
    common_telemetry::init_default_metrics_recorder();
    let _guard = common_telemetry::init_global_logging(app_name, logging_opts, tracing_opts);

    // Report app version as gauge.
    gauge!("app_version", 1.0, "short_version" => short_version(), "version" => full_version());

    // Log version and argument flags.
    info!(
        "short_version: {}, full_version: {}",
        short_version(),
        full_version()
    );
    log_env_flags();

    let mut app = cmd.build(opts).await?;

    tokio::select! {
        result = app.start() => {
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
