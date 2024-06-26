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

use clap::Parser;
use flow::FlownodeInstance;
use snafu::ResultExt;
use tracing_appender::non_blocking::WorkerGuard;

use crate::error::{Result, ShutdownFlownodeSnafu, StartFlownodeSnafu};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::App;

pub const APP_NAME: &str = "greptime-flownode";

type FlownodeOptions = GreptimeOptions<flow::FlownodeOptions>;

pub struct Instance {
    flownode: FlownodeInstance,

    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    pub fn new(flownode: FlownodeInstance, guard: Vec<WorkerGuard>) -> Self {
        Self {
            flownode,
            _guard: guard,
        }
    }

    pub fn flownode_mut(&mut self) -> &mut FlownodeInstance {
        &mut self.flownode
    }

    pub fn flownode(&self) -> &FlownodeInstance {
        &self.flownode
    }
}

#[async_trait::async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        self.flownode.start().await.context(StartFlownodeSnafu)
    }

    async fn stop(&self) -> Result<()> {
        self.flownode
            .shutdown()
            .await
            .context(ShutdownFlownodeSnafu)
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(&self, opts: FlownodeOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<FlownodeOptions> {
        match &self.subcmd {
            SubCommand::Start(cmd) => cmd.load_options(global_options),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(&self, opts: FlownodeOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }
}

#[derive(Debug, Parser, Default)]
struct StartCommand {}

impl StartCommand {
    fn load_options(&self, global_options: &GlobalOptions) -> Result<FlownodeOptions> {
        todo!()
    }

    async fn build(&self, opts: FlownodeOptions) -> Result<Instance> {
        todo!()
    }
}
