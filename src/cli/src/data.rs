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

mod export;
mod import;

use clap::Subcommand;
use common_error::ext::BoxedError;

use crate::data::export::ExportCommand;
use crate::data::import::ImportCommand;
use crate::Tool;

/// Command for data operations including exporting data from and importing data into GreptimeDB.
#[derive(Subcommand)]
pub enum DataCommand {
    Export(ExportCommand),
    Import(ImportCommand),
}

impl DataCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        match self {
            DataCommand::Export(cmd) => cmd.build().await,
            DataCommand::Import(cmd) => cmd.build().await,
        }
    }
}
