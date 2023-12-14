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
#[allow(unused)]
mod repl;
// TODO(weny): Removes it
#[allow(deprecated)]
mod upgrade;

use async_trait::async_trait;
use bench::BenchTableMetadataCommand;
use clap::Parser;
use common_telemetry::logging::LoggingOptions;
pub use repl::Repl;
use upgrade::UpgradeCommand;

use self::export::ExportCommand;
use crate::error::Result;
use crate::options::{CliOptions, Options};
use crate::App;

#[async_trait]
pub trait Tool: Send + Sync {
    async fn do_work(&self) -> Result<()>;
}

pub struct Instance {
    tool: Box<dyn Tool>,
}

impl Instance {
    fn new(tool: Box<dyn Tool>) -> Self {
        Self { tool }
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        "greptime-cli"
    }

    async fn start(&mut self) -> Result<()> {
        self.tool.do_work().await
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
    pub async fn build(self) -> Result<Instance> {
        self.cmd.build().await
    }

    pub fn load_options(&self, cli_options: &CliOptions) -> Result<Options> {
        let mut logging_opts = LoggingOptions::default();

        if let Some(dir) = &cli_options.log_dir {
            logging_opts.dir = dir.clone();
        }

        logging_opts.level = cli_options.log_level.clone();

        Ok(Options::Cli(Box::new(logging_opts)))
    }
}

#[derive(Parser)]
enum SubCommand {
    // Attach(AttachCommand),
    Upgrade(UpgradeCommand),
    Bench(BenchTableMetadataCommand),
    Export(ExportCommand),
}

impl SubCommand {
    async fn build(self) -> Result<Instance> {
        match self {
            // SubCommand::Attach(cmd) => cmd.build().await,
            SubCommand::Upgrade(cmd) => cmd.build().await,
            SubCommand::Bench(cmd) => cmd.build().await,
            SubCommand::Export(cmd) => cmd.build().await,
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
