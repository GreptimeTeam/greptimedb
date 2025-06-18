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

mod key;
mod table;

use clap::Subcommand;
use common_error::ext::BoxedError;

use crate::metadata::control::del::key::DelKeyCommand;
use crate::metadata::control::del::table::DelTableCommand;
use crate::Tool;

/// The prefix of the tombstone keys.
pub(crate) const CLI_TOMBSTONE_PREFIX: &str = "__cli_tombstone/";

/// Subcommand for deleting metadata from the metadata store.
#[derive(Subcommand)]
pub enum DelCommand {
    Key(DelKeyCommand),
    Table(DelTableCommand),
}

impl DelCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        match self {
            DelCommand::Key(cmd) => cmd.build().await,
            DelCommand::Table(cmd) => cmd.build().await,
        }
    }
}
