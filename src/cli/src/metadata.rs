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

mod control;
mod repair;
mod snapshot;
mod utils;

use clap::Subcommand;
use common_error::ext::BoxedError;

use crate::Tool;
use crate::metadata::control::{DelCommand, GetCommand};
use crate::metadata::repair::RepairCommand;
use crate::metadata::snapshot::SnapshotCommand;

/// Command for managing metadata operations,
/// including saving and restoring metadata snapshots,
/// controlling metadata operations, and diagnosing and repairing metadata.
#[derive(Subcommand)]
pub enum MetadataCommand {
    #[clap(subcommand)]
    Snapshot(SnapshotCommand),
    #[clap(subcommand)]
    Get(GetCommand),
    #[clap(subcommand)]
    Del(DelCommand),
    #[clap(subcommand)]
    Repair(RepairCommand),
}

impl MetadataCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        match self {
            MetadataCommand::Snapshot(cmd) => cmd.build().await,
            MetadataCommand::Repair(cmd) => cmd.build().await,
            MetadataCommand::Get(cmd) => cmd.build().await,
            MetadataCommand::Del(cmd) => cmd.build().await,
        }
    }
}
