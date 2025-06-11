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

mod common;
mod control;
mod snapshot;

use clap::Subcommand;
use common_error::ext::BoxedError;

use crate::metadata::control::ControlCommand;
use crate::metadata::snapshot::SnapshotCommand;
use crate::Tool;

/// Command for managing metadata operations, including saving metadata snapshots and restoring metadata from snapshots.
#[derive(Subcommand)]
pub enum MetadataCommand {
    #[clap(subcommand)]
    Snapshot(SnapshotCommand),
    #[clap(subcommand)]
    Control(ControlCommand),
}

impl MetadataCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        match self {
            MetadataCommand::Snapshot(cmd) => cmd.build().await,
            MetadataCommand::Control(cmd) => cmd.build().await,
        }
    }
}
