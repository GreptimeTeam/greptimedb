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

//! Developer-only tools shipped in the standalone `dev-tools` binary.
//!
//! These are ad-hoc utilities (benchmarks, diagnostics) that should not be part
//! of the production `greptime` binary.

#[allow(clippy::print_stdout)]
mod parquetbench;

use clap::Parser;

use crate::dev_tools::parquetbench::ParquetbenchCommand;
use crate::error::Result;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        self.subcmd.run().await
    }
}

#[derive(Parser)]
pub enum SubCommand {
    /// Benchmark scanning a single parquet SST.
    Parquetbench(ParquetbenchCommand),
}

impl SubCommand {
    pub async fn run(&self) -> Result<()> {
        match self {
            SubCommand::Parquetbench(cmd) => cmd.run().await,
        }
    }
}
