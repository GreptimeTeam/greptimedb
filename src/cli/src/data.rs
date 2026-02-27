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

pub(crate) mod copy;
mod export;
pub mod export_v2;
mod import;
pub mod import_v2;
pub(crate) mod retry;
pub mod snapshot_storage;
mod storage_export;

use clap::Subcommand;
use client::DEFAULT_CATALOG_NAME;
use common_error::ext::BoxedError;

use crate::Tool;
use crate::data::export::ExportCommand;
use crate::data::export_v2::ExportV2Command;
use crate::data::import::ImportCommand;
use crate::data::import_v2::ImportV2Command;

pub(crate) const COPY_PATH_PLACEHOLDER: &str = "<PATH/TO/FILES>";

#[cfg(test)]
mod tests;

/// Command for data operations including exporting data from and importing data into GreptimeDB.
#[derive(Subcommand)]
pub enum DataCommand {
    /// Export data (V1 - legacy).
    Export(ExportCommand),
    /// Import data (V1 - legacy).
    Import(ImportCommand),
    /// Export V2 - JSON-based schema export with manifest support.
    #[clap(subcommand)]
    ExportV2(ExportV2Command),
    /// Import V2 - Import from V2 snapshot.
    ImportV2(ImportV2Command),
}

impl DataCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        match self {
            DataCommand::Export(cmd) => cmd.build().await,
            DataCommand::Import(cmd) => cmd.build().await,
            DataCommand::ExportV2(cmd) => cmd.build().await,
            DataCommand::ImportV2(cmd) => cmd.build().await,
        }
    }
}

pub(crate) fn default_database() -> String {
    format!("{DEFAULT_CATALOG_NAME}-*")
}
