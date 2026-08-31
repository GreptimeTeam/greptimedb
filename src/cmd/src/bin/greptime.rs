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

#![recursion_limit = "256"]
#![doc = include_str!("../../../../README.md")]

use clap::{Parser, Subcommand};
use cmd::datanode::builder::InstanceBuilder;
#[cfg(unix)]
use cmd::error::DaemonizeSnafu;
use cmd::error::{InitTlsProviderSnafu, Result};
use cmd::options::GlobalOptions;
use cmd::{App, cli, datanode, flownode, frontend, metasrv, standalone, user};
use common_base::Plugins;
use common_version::{product_name, verbose_version, version};
use servers::install_default_crypto_provider;

#[derive(Parser)]
#[command(name = product_name(), author, version, long_version = verbose_version(), about)]
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

    /// Start service in standalone mode.
    #[clap(name = "standalone")]
    Standalone(standalone::Command),

    /// Execute the cli tools.
    #[clap(name = "cli")]
    Cli(cli::Command),

    /// Manage user credentials.
    #[clap(name = "user")]
    User(user::Command),
}

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(debug_assertions)]
fn main() -> Result<()> {
    use snafu::ResultExt;

    let cli = Command::parse();
    maybe_daemonize(&cli)?;

    // Set the stack size to 8MB for the thread so it wouldn't overflow on large stack usage in debug mode
    // see https://github.com/GreptimeTeam/greptimedb/pull/4317
    // and https://github.com/rust-lang/rust/issues/34283
    std::thread::Builder::new()
        .name("main_spawn".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(8 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("Failed building the Runtime")
                    .block_on(main_body(cli))
            }
        })
        .context(cmd::error::SpawnThreadSnafu)?
        .join()
        .expect("Couldn't join on the associated thread")
}

#[cfg(not(debug_assertions))]
fn main() -> Result<()> {
    let cli = Command::parse();
    maybe_daemonize(&cli)?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed building the Runtime")
        .block_on(main_body(cli))
}

async fn main_body(cli: Command) -> Result<()> {
    setup_human_panic();
    install_default_crypto_provider().map_err(|msg| InitTlsProviderSnafu { msg }.build())?;
    start(cli).await
}

/// If `--daemon` was passed for `standalone start`, fork into the background
/// before any Tokio runtime is built. On non-Unix platforms the `--daemon`
/// flag does not exist, so this is a no-op.
fn maybe_daemonize(cli: &Command) -> Result<()> {
    let daemon = matches!(&cli.subcmd, SubCommand::Standalone(cmd) if cmd.is_daemon());
    if !daemon {
        return Ok(());
    }

    #[cfg(unix)]
    {
        let cwd = std::env::current_dir().map_err(|e| {
            DaemonizeSnafu {
                msg: format!("failed to read current working directory: {e}"),
            }
            .build()
        })?;

        daemonize::Daemonize::new()
            .working_directory(cwd)
            .start()
            .map_err(|e| {
                DaemonizeSnafu {
                    msg: format!("failed to daemonize: {e}"),
                }
                .build()
            })?;
    }

    Ok(())
}

async fn start(cli: Command) -> Result<()> {
    match cli.subcmd {
        SubCommand::Datanode(cmd) => match cmd.subcmd {
            datanode::SubCommand::Start(ref start) => {
                let opts = start.load_options(&cli.global_options)?;
                let plugins = Plugins::new();
                let builder = InstanceBuilder::try_new_with_init(opts, plugins).await?;
                cmd.build_with(builder).await?.run().await
            }
            datanode::SubCommand::Objbench(ref bench) => bench.run().await,
            datanode::SubCommand::Scanbench(ref bench) => bench.run().await,
            datanode::SubCommand::Parquetbench(ref bench) => bench.run().await,
            #[cfg(feature = "dev-tools")]
            datanode::SubCommand::ParquetMeta(ref cmd) => cmd.run().await,
            #[cfg(feature = "dev-tools")]
            datanode::SubCommand::ParquetRewrite(ref cmd) => cmd.run().await,
            #[cfg(feature = "dev-tools")]
            datanode::SubCommand::SstReplace(ref cmd) => cmd.run().await,
        },
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
        SubCommand::User(cmd) => cmd.run(),
    }
}

fn setup_human_panic() {
    human_panic::setup_panic!(
        human_panic::Metadata::new(product_name(), version())
            .homepage("https://github.com/GreptimeTeam/greptimedb/discussions")
    );

    common_telemetry::set_panic_hook();
}
