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

mod bench;

// Wait for https://github.com/GreptimeTeam/greptimedb/issues/2373
#[allow(unused)]
mod cmd;
mod export;
mod helper;

// Wait for https://github.com/GreptimeTeam/greptimedb/issues/2373
mod database;
mod import;
#[allow(unused)]
mod repl;
mod wal_switch;

use async_trait::async_trait;
use bench::BenchTableMetadataCommand;
use clap::Parser;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
pub use repl::Repl;
use tracing_appender::non_blocking::WorkerGuard;
use wal_switch::{SwitchToLocalWalCommand, SwitchToRemoteWalCommand};

use self::export::ExportCommand;
use crate::cli::import::ImportCommand;
use crate::error::Result;
use crate::options::GlobalOptions;
use crate::App;

pub const APP_NAME: &str = "greptime-cli";

#[async_trait]
pub trait Tool: Send + Sync {
    async fn do_work(&self) -> Result<()>;
}

pub struct Instance {
    tool: Box<dyn Tool>,

    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    fn new(tool: Box<dyn Tool>, guard: Vec<WorkerGuard>) -> Self {
        Self {
            tool,
            _guard: guard,
        }
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        self.tool.do_work().await
    }

    fn wait_signal(&self) -> bool {
        false
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Command {
    pub async fn build(&self, opts: LoggingOptions) -> Result<Instance> {
        let guard = common_telemetry::init_global_logging(
            APP_NAME,
            &opts,
            &TracingOptions::default(),
            None,
        );

        self.cmd.build(guard).await
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<LoggingOptions> {
        let mut logging_opts = LoggingOptions::default();

        if let Some(dir) = &global_options.log_dir {
            logging_opts.dir.clone_from(dir);
        }

        logging_opts.level.clone_from(&global_options.log_level);

        Ok(logging_opts)
    }
}

#[derive(Parser)]
enum SubCommand {
    // Attach(AttachCommand),
    Bench(BenchTableMetadataCommand),
    Export(ExportCommand),
    Import(ImportCommand),
    SwitchToRemoteWal(SwitchToRemoteWalCommand),
    SwitchToLocalWal(SwitchToLocalWalCommand),
}

impl SubCommand {
    async fn build(&self, guard: Vec<WorkerGuard>) -> Result<Instance> {
        match self {
            // SubCommand::Attach(cmd) => cmd.build().await,
            SubCommand::Bench(cmd) => cmd.build(guard).await,
            SubCommand::Export(cmd) => cmd.build(guard).await,
            SubCommand::Import(cmd) => cmd.build(guard).await,
            SubCommand::SwitchToRemoteWal(cmd) => cmd.build(guard).await,
            SubCommand::SwitchToLocalWal(cmd) => cmd.build(guard).await,
        }
    }
}

#[derive(Debug, Parser)]
pub(crate) struct AttachCommand {
    #[clap(long)]
    pub(crate) grpc_addr: String,
    #[clap(long)]
    pub(crate) meta_addr: Option<String>,
    #[clap(long, action)]
    pub(crate) disable_helper: bool,
}

impl AttachCommand {
    #[allow(dead_code)]
    async fn build(self) -> Result<Instance> {
        unimplemented!("Wait for https://github.com/GreptimeTeam/greptimedb/issues/2373")
    }
}
