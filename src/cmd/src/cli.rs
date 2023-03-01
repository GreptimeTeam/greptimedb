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
use repl::Repl;

use crate::error::Result;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Command {
    pub async fn run(self) -> Result<()> {
        self.cmd.run().await
    }
}

#[derive(Parser)]
enum SubCommand {
    Attach(AttachCommand),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Attach(cmd) => cmd.run().await,
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
    async fn run(self) -> Result<()> {
        let mut repl = Repl::try_new(&self).await?;
        repl.run().await
    }
}
