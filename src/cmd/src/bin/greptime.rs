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
use cmd::datanode::builder::InstanceBuilder;
use cmd::error::{InitTlsProviderSnafu, Result};
use cmd::options::GlobalOptions;
use cmd::{App, cli, datanode, flownode, frontend, metasrv, standalone};
use common_base::Plugins;
use common_version::{verbose_version, version};
use servers::install_ring_crypto_provider;

#[cfg(unix)]
use nix::sys::wait::waitpid;
#[cfg(unix)]
use nix::unistd::{chdir, close, dup2, fork, setsid, ForkResult};
#[cfg(unix)]
use std::fs::OpenOptions;

#[derive(Parser)]
#[command(name = "greptime", author, version, long_version = verbose_version(), about)]
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

#[cfg(debug_assertions)]
fn main() -> Result<()> {
    use snafu::ResultExt;
    // Set the stack size to 8MB for the thread so it wouldn't overflow on large stack usage in debug mode
    // see https://github.com/GreptimeTeam/greptimedb/pull/4317
    // and https://github.com/rust-lang/rust/issues/34283
    std::thread::Builder::new()
        .name("main_spawn".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(8 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("Failed building the Runtime")
                    .block_on(main_body())
            }
        })
        .context(cmd::error::SpawnThreadSnafu)?
        .join()
        .expect("Couldn't join on the associated thread")
}

#[cfg(not(debug_assertions))]
#[tokio::main]
async fn main() -> Result<()> {
    main_body().await
}

/// Daemonize the process
fn daemonize() -> Result<()> {
    #[cfg(unix)]
    {
        // Fork the first child
        match unsafe { fork().map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())? } {
            ForkResult::Parent { child, .. } => {
                // Wait for the child to initialize
                waitpid(child, None).map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                std::process::exit(0);
            }
            ForkResult::Child => {
                // Create a new session
                setsid().map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                
                // Fork the second child to avoid acquiring a controlling terminal
                match unsafe { fork().map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())? } {
                    ForkResult::Parent { .. } => std::process::exit(0),
                    ForkResult::Child => {
                        // Change working directory to root
                        chdir("/").map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                        
                        // Close standard file descriptors
                        close(0).map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                        close(1).map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                        close(2).map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                        
                        // Open /dev/null and duplicate it to stdin, stdout, stderr
                        let dev_null = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open("/dev/null")
                            .context(cmd::error::FileIoSnafu)?;
                        let dev_null_fd = dev_null.as_raw_fd();
                        dup2(dev_null_fd, 0).map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                        dup2(dev_null_fd, 1).map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                        dup2(dev_null_fd, 2).map_err(|e| cmd::error::OtherSnafu { source: e.into() }.build())?;
                        
                        Ok(())
                    }
                }
            }
        }
    }
    
    #[cfg(windows)]
    {
        // Daemon mode is not supported on Windows
        // Just return Ok for now, could be enhanced with Windows service support in the future
        Ok(())
    }
}

async fn main_body() -> Result<()> {
    setup_human_panic();
    install_ring_crypto_provider().map_err(|msg| InitTlsProviderSnafu { msg }.build())?;
    
    let cli = Command::parse();
    
    // Check if daemon mode is enabled
    if let Command { subcmd: SubCommand::Standalone(standalone_cmd), .. } = &cli {
        if let standalone::SubCommand::Start(start_cmd) = &standalone_cmd.subcmd {
            if start_cmd.daemon {
                daemonize()?;
            }
        }
    }
    
    start(cli).await
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
    }
}

fn setup_human_panic() {
    human_panic::setup_panic!(
        human_panic::Metadata::new("GreptimeDB", version())
            .homepage("https://github.com/GreptimeTeam/greptimedb/discussions")
    );

    common_telemetry::set_panic_hook();
}