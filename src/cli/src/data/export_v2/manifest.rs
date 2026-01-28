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

//! Manifest data structures for Export/Import V2.

use std::{fmt, str};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Current manifest format version.
pub const MANIFEST_VERSION: u32 = 1;

/// Manifest file name within snapshot directory.
pub const MANIFEST_FILE: &str = "manifest.json";

/// Time range for data export (half-open interval: [start, end)).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeRange {
    /// Start time (inclusive). None means earliest available data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<DateTime<Utc>>,
    /// End time (exclusive). None means current time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<DateTime<Utc>>,
}

impl TimeRange {
    /// Creates a new time range with specified bounds.
    pub fn new(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Self {
        Self { start, end }
    }

    /// Creates an unbounded time range (all data).
    pub fn unbounded() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    /// Returns true if this time range is unbounded.
    pub fn is_unbounded(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }
}

impl Default for TimeRange {
    fn default() -> Self {
        Self::unbounded()
    }
}

/// Status of a chunk during export/import.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ChunkStatus {
    /// Chunk is pending export.
    #[default]
    Pending,
    /// Chunk export is in progress.
    InProgress,
    /// Chunk export completed successfully.
    Completed,
    /// Chunk export failed.
    Failed,
}

/// Metadata for a single chunk of exported data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Chunk identifier (sequential number starting from 1).
    pub id: u32,
    /// Time range covered by this chunk.
    pub time_range: TimeRange,
    /// Export status.
    pub status: ChunkStatus,
    /// List of data files in this chunk (relative paths from snapshot root).
    #[serde(default)]
    pub files: Vec<String>,
    /// SHA256 checksum of all files in this chunk (aggregated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    /// Error message if status is Failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ChunkMeta {
    /// Creates a new pending chunk with the given id and time range.
    pub fn new(id: u32, time_range: TimeRange) -> Self {
        Self {
            id,
            time_range,
            status: ChunkStatus::Pending,
            files: vec![],
            checksum: None,
            error: None,
        }
    }

    /// Marks this chunk as in progress.
    pub fn mark_in_progress(&mut self) {
        self.status = ChunkStatus::InProgress;
        self.error = None;
    }

    /// Marks this chunk as completed with the given files and checksum.
    pub fn mark_completed(&mut self, files: Vec<String>, checksum: Option<String>) {
        self.status = ChunkStatus::Completed;
        self.files = files;
        self.checksum = checksum;
        self.error = None;
    }

    /// Marks this chunk as failed with the given error message.
    pub fn mark_failed(&mut self, error: String) {
        self.status = ChunkStatus::Failed;
        self.error = Some(error);
    }
}

/// Supported data formats for export.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
#[value(rename_all = "lowercase")]
pub enum DataFormat {
    /// Apache Parquet format (default, recommended for production).
    #[default]
    Parquet,
    /// CSV format (human-readable).
    Csv,
    /// JSON format (structured text).
    Json,
}

impl fmt::Display for DataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataFormat::Parquet => write!(f, "parquet"),
            DataFormat::Csv => write!(f, "csv"),
            DataFormat::Json => write!(f, "json"),
        }
    }
}

impl str::FromStr for DataFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(DataFormat::Parquet),
            "csv" => Ok(DataFormat::Csv),
            "json" => Ok(DataFormat::Json),
            _ => Err(format!(
                "invalid format '{}': expected one of parquet, csv, json",
                s
            )),
        }
    }
}

/// Snapshot manifest containing all metadata.
///
/// The manifest is stored as `manifest.json` in the snapshot root directory.
/// It contains:
/// - Snapshot identification (UUID, timestamps)
/// - Scope (catalog, schemas, time range)
/// - Export configuration (format, schema_only)
/// - Chunk metadata for resume support
/// - Integrity checksums
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest format version for compatibility checking.
    pub version: u32,
    /// Unique snapshot identifier.
    pub snapshot_id: Uuid,
    /// Catalog name.
    pub catalog: String,
    /// List of schemas included in this snapshot.
    pub schemas: Vec<String>,
    /// Overall time range covered by this snapshot.
    pub time_range: TimeRange,
    /// Whether this is a schema-only snapshot (no data).
    pub schema_only: bool,
    /// Data format used for export.
    pub format: DataFormat,
    /// Chunk metadata (empty for schema-only snapshots).
    #[serde(default)]
    pub chunks: Vec<ChunkMeta>,
    /// Snapshot-level SHA256 checksum (aggregated from all chunks).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last updated timestamp.
    pub updated_at: DateTime<Utc>,
}

impl Manifest {
    /// Creates a new manifest for schema-only export.
    pub fn new_schema_only(catalog: String, schemas: Vec<String>) -> Self {
        let now = Utc::now();
        Self {
            version: MANIFEST_VERSION,
            snapshot_id: Uuid::new_v4(),
            catalog,
            schemas,
            time_range: TimeRange::unbounded(),
            schema_only: true,
            format: DataFormat::Parquet,
            chunks: vec![],
            checksum: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Creates a new manifest for full export with time range and format.
    pub fn new_full(
        catalog: String,
        schemas: Vec<String>,
        time_range: TimeRange,
        format: DataFormat,
    ) -> Self {
        let now = Utc::now();
        Self {
            version: MANIFEST_VERSION,
            snapshot_id: Uuid::new_v4(),
            catalog,
            schemas,
            time_range,
            schema_only: false,
            format,
            chunks: vec![],
            checksum: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Returns true if all chunks are completed (or if schema-only).
    pub fn is_complete(&self) -> bool {
        self.schema_only
            || self
                .chunks
                .iter()
                .all(|c| c.status == ChunkStatus::Completed)
    }

    /// Returns the number of pending chunks.
    pub fn pending_count(&self) -> usize {
        self.chunks
            .iter()
            .filter(|c| c.status == ChunkStatus::Pending)
            .count()
    }

    /// Returns the number of in-progress chunks.
    pub fn in_progress_count(&self) -> usize {
        self.chunks
            .iter()
            .filter(|c| c.status == ChunkStatus::InProgress)
            .count()
    }

    /// Returns the number of completed chunks.
    pub fn completed_count(&self) -> usize {
        self.chunks
            .iter()
            .filter(|c| c.status == ChunkStatus::Completed)
            .count()
    }

    /// Returns the number of failed chunks.
    pub fn failed_count(&self) -> usize {
        self.chunks
            .iter()
            .filter(|c| c.status == ChunkStatus::Failed)
            .count()
    }

    /// Updates the `updated_at` timestamp to now.
    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }

    /// Adds a chunk to the manifest.
    pub fn add_chunk(&mut self, chunk: ChunkMeta) {
        self.chunks.push(chunk);
        self.touch();
    }

    /// Updates a chunk by id.
    pub fn update_chunk(&mut self, id: u32, updater: impl FnOnce(&mut ChunkMeta)) {
        if let Some(chunk) = self.chunks.iter_mut().find(|c| c.id == id) {
            updater(chunk);
            self.touch();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_range_serialization() {
        let range = TimeRange::unbounded();
        let json = serde_json::to_string(&range).unwrap();
        assert_eq!(json, "{}");

        let range: TimeRange = serde_json::from_str("{}").unwrap();
        assert!(range.is_unbounded());
    }

    #[test]
    fn test_manifest_schema_only() {
        let manifest =
            Manifest::new_schema_only("greptime".to_string(), vec!["public".to_string()]);

        assert_eq!(manifest.version, MANIFEST_VERSION);
        assert!(manifest.schema_only);
        assert!(manifest.chunks.is_empty());
        assert!(manifest.is_complete());
    }

    #[test]
    fn test_manifest_full() {
        let manifest = Manifest::new_full(
            "greptime".to_string(),
            vec!["public".to_string()],
            TimeRange::unbounded(),
            DataFormat::Parquet,
        );

        assert!(!manifest.schema_only);
        assert!(manifest.chunks.is_empty());
        assert!(manifest.is_complete()); // No chunks means complete
    }

    #[test]
    fn test_data_format_parsing() {
        assert_eq!(
            "parquet".parse::<DataFormat>().unwrap(),
            DataFormat::Parquet
        );
        assert_eq!("CSV".parse::<DataFormat>().unwrap(), DataFormat::Csv);
        assert_eq!("JSON".parse::<DataFormat>().unwrap(), DataFormat::Json);
        assert!("invalid".parse::<DataFormat>().is_err());
    }

    #[test]
    fn test_chunk_status_transitions() {
        let mut chunk = ChunkMeta::new(1, TimeRange::unbounded());
        assert_eq!(chunk.status, ChunkStatus::Pending);

        chunk.mark_in_progress();
        assert_eq!(chunk.status, ChunkStatus::InProgress);

        chunk.mark_completed(
            vec!["file1.parquet".to_string()],
            Some("abc123".to_string()),
        );
        assert_eq!(chunk.status, ChunkStatus::Completed);
        assert_eq!(chunk.files.len(), 1);
    }
}
