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

use clap::{FromArgMatches, Parser, Subcommand};
use cmd::error::Result;
use cmd::options::{CliOptions, Options};
use cmd::{
    cli, datanode, frontend, greptimedb_cli, log_versions, metasrv, standalone, start_app, App,
};

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
    async fn build(self, opts: Options) -> Result<Box<dyn App>> {
        let app: Box<dyn App> = match (self, opts) {
            (SubCommand::Datanode(cmd), Options::Datanode(dn_opts)) => {
                let app = cmd.build(*dn_opts).await?;
                Box::new(app) as _
            }
            (SubCommand::Frontend(cmd), Options::Frontend(fe_opts)) => {
                let app = cmd.build(*fe_opts).await?;
                Box::new(app) as _
            }
            (SubCommand::Metasrv(cmd), Options::Metasrv(meta_opts)) => {
                let app = cmd.build(*meta_opts).await?;
                Box::new(app) as _
            }
            (SubCommand::Standalone(cmd), Options::Standalone(opts)) => {
                let app = cmd.build(*opts).await?;
                Box::new(app) as _
            }
            (SubCommand::Cli(cmd), Options::Cli(_)) => {
                let app = cmd.build().await?;
                Box::new(app) as _
            }

            _ => unreachable!(),
        };
        Ok(app)
    }

    fn load_options(&self, cli_options: &CliOptions) -> Result<Options> {
        match self {
            SubCommand::Datanode(cmd) => cmd.load_options(cli_options),
            SubCommand::Frontend(cmd) => cmd.load_options(cli_options),
            SubCommand::Metasrv(cmd) => cmd.load_options(cli_options),
            SubCommand::Standalone(cmd) => cmd.load_options(cli_options),
            SubCommand::Cli(cmd) => cmd.load_options(cli_options),
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

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let metadata = human_panic::Metadata {
        version: env!("CARGO_PKG_VERSION").into(),
        name: "GreptimeDB".into(),
        authors: Default::default(),
        homepage: "https://github.com/GreptimeTeam/greptimedb/discussions".into(),
    };
    human_panic::setup_panic!(metadata);

    common_telemetry::set_panic_hook();

    let cli = greptimedb_cli();

    let cli = SubCommand::augment_subcommands(cli);

    let args = cli.get_matches();

    let subcmd = match SubCommand::from_arg_matches(&args) {
        Ok(subcmd) => subcmd,
        Err(e) => e.exit(),
    };

    let app_name = subcmd.to_string();

    let cli_options = CliOptions::new(&args);

    let opts = subcmd.load_options(&cli_options)?;

    let _guard = common_telemetry::init_global_logging(
        &app_name,
        opts.logging_options(),
        cli_options.tracing_options(),
        opts.node_id(),
    );

    log_versions();

    let app = subcmd.build(opts).await?;

    start_app(app).await
}
