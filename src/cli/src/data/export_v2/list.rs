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

//! List snapshots command.

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_telemetry::{info, warn};
use snafu::ResultExt;

use super::error::{Result, StorageOperationSnafu};
use super::manifest::{ChunkStatus, MANIFEST_FILE, Manifest};
use super::storage::{OpenDalStorage, SnapshotStorage, validate_storage_uri};
use crate::Tool;
use crate::common::ObjectStoreConfig;

/// List snapshots at a storage location.
#[derive(Debug, Parser)]
pub struct ExportListCommand {
    /// Storage location to scan for snapshots (e.g., s3://bucket/snapshots).
    /// This should be the parent directory containing snapshot subdirectories.
    #[clap(long)]
    location: String,

    /// Object store configuration for remote storage backends.
    #[clap(flatten)]
    storage: ObjectStoreConfig,
}

impl ExportListCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        validate_storage_uri(&self.location).map_err(BoxedError::new)?;

        Ok(Box::new(ExportList {
            location_uri: self.location.clone(),
            storage_config: self.storage.clone(),
        }))
    }
}

pub struct ExportList {
    location_uri: String,
    storage_config: ObjectStoreConfig,
}

impl ExportList {
    /// Creates a new list tool (used by tests).
    pub fn new(location_uri: String, storage_config: ObjectStoreConfig) -> Self {
        Self {
            location_uri,
            storage_config,
        }
    }
}

pub struct SnapshotInfo {
    #[allow(dead_code)]
    pub name: String,
    pub manifest: Manifest,
}

pub struct ScanResult {
    pub snapshots: Vec<SnapshotInfo>,
    pub unreadable: Vec<String>,
}

#[async_trait]
impl Tool for ExportList {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.run().await.map_err(BoxedError::new)
    }
}

impl ExportList {
    async fn run(&self) -> Result<()> {
        info!("Scanning: {}", self.location_uri);
        let result = self.scan_snapshots().await?;

        if result.snapshots.is_empty() && result.unreadable.is_empty() {
            info!("No snapshots found.");
            return Ok(());
        }

        if result.unreadable.is_empty() {
            info!("Found {} snapshot(s):\n", result.snapshots.len());
        } else {
            info!(
                "Found {} snapshot(s) ({} directory(s) skipped: unreadable manifest):\n",
                result.snapshots.len(),
                result.unreadable.len()
            );
        }

        // Print header
        info!(
            "  {:<38} {:<21} {:<10} {:<8} {:<8} {}",
            "ID", "Created", "Catalog", "Schemas", "Chunks", "Status"
        );
        info!(
            "  {:<38} {:<21} {:<10} {:<8} {:<8} {}",
            "─".repeat(36),
            "─".repeat(19),
            "─".repeat(9),
            "─".repeat(7),
            "─".repeat(7),
            "─".repeat(12)
        );

        for snap in &result.snapshots {
            let m = &snap.manifest;
            let status = snapshot_status(m);
            let chunks_display = if m.schema_only {
                "0".to_string()
            } else {
                format!("{}/{}", m.completed_count(), m.chunks.len())
            };

            info!(
                "  {:<38} {:<21} {:<10} {:<8} {:<8} {}",
                m.snapshot_id,
                m.created_at.format("%Y-%m-%d %H:%M:%S"),
                m.catalog,
                m.schemas.len(),
                chunks_display,
                status,
            );
        }

        if !result.unreadable.is_empty() {
            warn!(
                "\nWarning: {} directory(s) had corrupt/unreadable manifest.json:",
                result.unreadable.len()
            );
            for name in &result.unreadable {
                warn!("  - {}/", name);
            }
        }

        Ok(())
    }

    pub async fn scan_snapshots(&self) -> Result<ScanResult> {
        let parent_storage = OpenDalStorage::from_uri(&self.location_uri, &self.storage_config)?;
        let store = parent_storage.object_store();

        let entries = store.list("/").await.context(StorageOperationSnafu {
            operation: "list location directory",
        })?;

        // Collect unique directory prefixes (first path segment).
        let mut subdirs = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for entry in entries {
            let path = entry.path();
            if let Some(first_segment) = path.split('/').next()
                && !first_segment.is_empty()
                && seen.insert(first_segment.to_string())
            {
                subdirs.push(first_segment.to_string());
            }
        }

        let mut snapshots = Vec::new();
        let mut unreadable = Vec::new();

        for name in subdirs {
            let manifest_path = format!("{}/{}", name, MANIFEST_FILE);
            match store.read(&manifest_path).await {
                Ok(data) => match serde_json::from_slice::<Manifest>(&data.to_vec()) {
                    Ok(manifest) => snapshots.push(SnapshotInfo { name, manifest }),
                    Err(_) => unreadable.push(name),
                },
                Err(e) if e.kind() == object_store::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(e).context(StorageOperationSnafu {
                        operation: format!("read manifest for '{}'", name),
                    });
                }
            }
        }

        snapshots.sort_by(|a, b| b.manifest.created_at.cmp(&a.manifest.created_at));
        Ok(ScanResult {
            snapshots,
            unreadable,
        })
    }
}

fn snapshot_status(manifest: &Manifest) -> &'static str {
    if manifest.schema_only {
        "schema-only"
    } else if manifest
        .chunks
        .iter()
        .all(|c| matches!(c.status, ChunkStatus::Completed | ChunkStatus::Skipped))
    {
        "complete"
    } else {
        "incomplete"
    }
}
