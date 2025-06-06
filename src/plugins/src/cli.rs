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
use cli::{BenchTableMetadataCommand, DataCommand, MetaCommand, Tool};
use common_error::ext::BoxedError;

#[derive(Parser)]
pub enum SubCommand {
    // Attach(AttachCommand),
    Bench(BenchTableMetadataCommand),
    #[clap(subcommand)]
    Data(DataCommand),
    #[clap(subcommand)]
    Meta(MetaCommand),
}

impl SubCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        match self {
            // SubCommand::Attach(cmd) => cmd.build().await,
            SubCommand::Bench(cmd) => cmd.build().await,
            SubCommand::Data(cmd) => cmd.build().await,
            SubCommand::Meta(cmd) => cmd.build().await,
        }
    }
}
