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

//! Delete snapshot command.

use std::io::{self, Write};

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_telemetry::info;

use super::error::{DeletionCancelledSnafu, Result};
use super::manifest::ChunkStatus;
use super::storage::{OpenDalStorage, SnapshotStorage, validate_snapshot_target_uri};
use crate::Tool;
use crate::common::ObjectStoreConfig;

/// Delete a snapshot and all its data.
#[derive(Debug, Parser)]
pub struct ExportDeleteCommand {
    /// Snapshot location to delete (e.g., s3://bucket/snapshots/prod-20250101).
    #[clap(long)]
    snapshot: String,

    /// Skip interactive confirmation.
    #[clap(long)]
    yes: bool,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ExportDeleteCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        validate_snapshot_target_uri(&self.snapshot).map_err(BoxedError::new)?;
        let storage =
            OpenDalStorage::from_uri(&self.snapshot, &self.storage).map_err(BoxedError::new)?;

        Ok(Box::new(ExportDelete {
            snapshot_uri: self.snapshot.clone(),
            storage: Box::new(storage),
            yes: self.yes,
        }))
    }
}

pub struct ExportDelete {
    snapshot_uri: String,
    storage: Box<dyn SnapshotStorage>,
    yes: bool,
}

impl ExportDelete {
    /// Creates a new delete tool (used by tests).
    pub fn new(snapshot_uri: String, storage: Box<dyn SnapshotStorage>, yes: bool) -> Self {
        Self {
            snapshot_uri,
            storage,
            yes,
        }
    }
}

#[async_trait]
impl Tool for ExportDelete {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl ExportDelete {
    async fn run(&self) -> Result<()> {
        // 1. Read manifest to confirm this is a valid snapshot.
        let manifest = self.storage.read_manifest().await?;

        let chunks_summary = if manifest.schema_only {
            "schema-only".to_string()
        } else {
            let all_done = manifest
                .chunks
                .iter()
                .all(|c| matches!(c.status, ChunkStatus::Completed | ChunkStatus::Skipped));
            if all_done {
                format!("{} (all completed)", manifest.chunks.len())
            } else {
                format!(
                    "{} ({} completed, {} incomplete)",
                    manifest.chunks.len(),
                    manifest.completed_count(),
                    manifest.chunks.len() - manifest.completed_count()
                )
            }
        };

        info!("Snapshot: {}", manifest.snapshot_id);
        info!(
            "  Created:  {} UTC",
            manifest.created_at.format("%Y-%m-%d %H:%M:%S")
        );
        info!("  Catalog:  {}", manifest.catalog);
        info!("  Schemas:  {:?}", manifest.schemas);
        info!("  Chunks:   {}", chunks_summary);

        // 2. Confirm deletion.
        if !self.yes {
            println!(
                "\nThis will permanently delete all data under:\n  {}/",
                self.snapshot_uri.trim_end_matches('/')
            );
            print!("Type 'yes' to confirm deletion: ");
            io::stdout().flush().unwrap_or(());

            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap_or(0);
            if input.trim() != "yes" {
                return DeletionCancelledSnafu.fail();
            }
        }

        // 3. Delete.
        info!("Deleting snapshot...");
        self.storage.delete_snapshot().await?;
        info!("Snapshot deleted successfully.");

        Ok(())
    }
}
