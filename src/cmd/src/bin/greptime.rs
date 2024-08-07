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

use clap::{Parser, Subcommand};
use cmd::error::{Result, SpawnThreadSnafu};
use cmd::options::GlobalOptions;
use cmd::{cli, datanode, flownode, frontend, metasrv, standalone, App};
use common_version::version;
use snafu::ResultExt;

#[derive(Parser)]
#[command(name = "greptime", author, version, long_version = version(), about)]
#[command(propagate_version = true)]
pub(crate) struct Command {
    #[clap(subcommand)]
    pub(crate) subcmd: SubCommand,

    #[clap(flatten)]
    pub(crate) global_options: GlobalOptions,
}

#[derive(Subcommand)]
enum SubCommand {
    /// Start datanode service.
    #[clap(name = "datanode")]
    Datanode(datanode::Command),

    /// Start flownode service.
    #[clap(name = "flownode")]
    Flownode(flownode::Command),

    /// Start frontend service.
    #[clap(name = "frontend")]
    Frontend(frontend::Command),

    /// Start metasrv service.
    #[clap(name = "metasrv")]
    Metasrv(metasrv::Command),

    /// Run greptimedb as a standalone service.
    #[clap(name = "standalone")]
    Standalone(standalone::Command),

    /// Execute the cli tools for greptimedb.
    #[clap(name = "cli")]
    Cli(cli::Command),
}

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    // Set the stack size to 8MB for the thread so it wouldn't crash on large stack usage in debug mode
    // see https://github.com/GreptimeTeam/greptimedb/pull/4317
    // and https://github.com/rust-lang/rust/issues/34283
    let builder = std::thread::Builder::new().name("main_larger_stack".to_string());
    #[cfg(debug_assertions)]
    let builder = builder.stack_size(8 * 1024 * 1024);
    builder
        .spawn(|| {
            let body = async {
                setup_human_panic();
                start(Command::parse()).await
            };
            #[allow(clippy::expect_used, clippy::diverging_sub_expression)]
            {
                let mut builder = tokio::runtime::Builder::new_multi_thread();

                #[cfg(debug_assertions)]
                let builder = builder.thread_stack_size(8 * 1024 * 1024);

                return builder
                    .enable_all()
                    .build()
                    .expect("Failed building the Runtime")
                    .block_on(body);
            }
        })
        .context(SpawnThreadSnafu)?
        .join()
        .expect("Couldn't join on the associated thread")
}

async fn start(cli: Command) -> Result<()> {
    match cli.subcmd {
        SubCommand::Datanode(cmd) => {
            cmd.build(cmd.load_options(&cli.global_options)?)
                .await?
                .run()
                .await
        }
        SubCommand::Flownode(cmd) => {
            cmd.build(cmd.load_options(&cli.global_options)?)
                .await?
                .run()
                .await
        }
        SubCommand::Frontend(cmd) => {
            cmd.build(cmd.load_options(&cli.global_options)?)
                .await?
                .run()
                .await
        }
        SubCommand::Metasrv(cmd) => {
            cmd.build(cmd.load_options(&cli.global_options)?)
                .await?
                .run()
                .await
        }
        SubCommand::Standalone(cmd) => {
            cmd.build(cmd.load_options(&cli.global_options)?)
                .await?
                .run()
                .await
        }
        SubCommand::Cli(cmd) => {
            cmd.build(cmd.load_options(&cli.global_options)?)
                .await?
                .run()
                .await
        }
    }
}

fn setup_human_panic() {
    let metadata = human_panic::Metadata {
        version: env!("CARGO_PKG_VERSION").into(),
        name: "GreptimeDB".into(),
        authors: Default::default(),
        homepage: "https://github.com/GreptimeTeam/greptimedb/discussions".into(),
    };
    human_panic::setup_panic!(metadata);

    common_telemetry::set_panic_hook();
}
