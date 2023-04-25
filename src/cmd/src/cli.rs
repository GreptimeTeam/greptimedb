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

mod cmd;
mod helper;
mod repl;

use clap::Parser;
use common_telemetry::logging::LoggingOptions;
pub use repl::Repl;

use crate::{error::Result, options::ConfigOptions};

pub struct Instance {
    repl: Repl,
}

impl Instance {
    pub async fn run(&mut self) -> Result<()> {
        self.repl.run().await
    }

    pub async fn stop(&self) -> Result<()> {
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

    pub fn load_options(&self, log_dir: Option<String>, log_level: Option<String>) -> Result<ConfigOptions> {
        let mut logging_opts = LoggingOptions::default();
        if let Some(dir) = log_dir {
            logging_opts.dir = dir;
        }
        if let Some(level) = log_level {
            logging_opts.level = level;
        }
        Ok(ConfigOptions::Cli(logging_opts))
    }
}

#[derive(Parser)]
enum SubCommand {
    Attach(AttachCommand),
}

impl SubCommand {
    async fn build(self) -> Result<Instance> {
        match self {
            SubCommand::Attach(cmd) => cmd.build().await,
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
    async fn build(self) -> Result<Instance> {
        let repl = Repl::try_new(&self).await?;
        Ok(Instance { repl })
    }
}
