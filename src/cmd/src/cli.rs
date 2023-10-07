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
mod cmd;
mod export;
mod helper;
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
use crate::options::{Options, TopLevelOptions};

#[async_trait]
pub trait Tool {
    async fn do_work(&self) -> Result<()>;
}

pub enum Instance {
    Repl(Repl),
    Tool(Box<dyn Tool>),
}

impl Instance {
    pub async fn start(&mut self) -> Result<()> {
        match self {
            Instance::Repl(repl) => repl.run().await,
            Instance::Tool(tool) => tool.do_work().await,
        }
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

    pub fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        let mut logging_opts = LoggingOptions::default();
        if let Some(dir) = top_level_opts.log_dir {
            logging_opts.dir = dir;
        }
        if top_level_opts.log_level.is_some() {
            logging_opts.level = top_level_opts.log_level;
        }
        Ok(Options::Cli(Box::new(logging_opts)))
    }
}

#[derive(Parser)]
enum SubCommand {
    Attach(AttachCommand),
    Upgrade(UpgradeCommand),
    Bench(BenchTableMetadataCommand),
    Export(ExportCommand),
}

impl SubCommand {
    async fn build(self) -> Result<Instance> {
        match self {
            SubCommand::Attach(cmd) => cmd.build().await,
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
    async fn build(self) -> Result<Instance> {
        let repl = Repl::try_new(&self).await?;
        Ok(Instance::Repl(repl))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_options() {
        let cmd = Command {
            cmd: SubCommand::Attach(AttachCommand {
                grpc_addr: String::from(""),
                meta_addr: None,
                disable_helper: false,
            }),
        };

        let opts = cmd.load_options(TopLevelOptions::default()).unwrap();
        let logging_opts = opts.logging_options();
        assert_eq!("/tmp/greptimedb/logs", logging_opts.dir);
        assert!(logging_opts.level.is_none());
        assert!(!logging_opts.enable_jaeger_tracing);
    }

    #[test]
    fn test_top_level_options() {
        let cmd = Command {
            cmd: SubCommand::Attach(AttachCommand {
                grpc_addr: String::from(""),
                meta_addr: None,
                disable_helper: false,
            }),
        };

        let opts = cmd
            .load_options(TopLevelOptions {
                log_dir: Some("/tmp/greptimedb/test/logs".to_string()),
                log_level: Some("debug".to_string()),
            })
            .unwrap();
        let logging_opts = opts.logging_options();
        assert_eq!("/tmp/greptimedb/test/logs", logging_opts.dir);
        assert_eq!("debug", logging_opts.level.as_ref().unwrap());
    }
}
