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

use std::collections::HashSet;
use std::time::Duration;
use std::{fmt, str};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::data::export_v2::chunker::generate_chunks;
use crate::data::export_v2::error::{
    ChunkTimeWindowRequiresBoundsSnafu, Result as ExportResult, TimeParseEndBeforeStartSnafu,
    TimeParseInvalidFormatSnafu,
};

/// Manifest format version produced by the current exporter.
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

    /// Returns true if both bounds are specified.
    pub fn is_bounded(&self) -> bool {
        self.start.is_some() && self.end.is_some()
    }

    /// Parses a time range from optional RFC3339 strings.
    pub fn parse(start: Option<&str>, end: Option<&str>) -> ExportResult<Self> {
        let start = start.map(parse_time).transpose()?;
        let end = end.map(parse_time).transpose()?;

        if let (Some(start), Some(end)) = (start, end)
            && end < start
        {
            return TimeParseEndBeforeStartSnafu.fail();
        }

        Ok(Self::new(start, end))
    }
}

fn parse_time(input: &str) -> ExportResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(input)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| TimeParseInvalidFormatSnafu { input }.build())
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
    /// Chunk had no data to export.
    Skipped,
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

    /// Creates a skipped chunk with the given id and time range.
    pub fn skipped(id: u32, time_range: TimeRange) -> Self {
        let mut chunk = Self::new(id, time_range);
        chunk.mark_skipped();
        chunk
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

    /// Marks this chunk as skipped because no data files were produced.
    pub fn mark_skipped(&mut self) {
        self.status = ChunkStatus::Skipped;
        self.files.clear();
        self.checksum = None;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_layout: Option<String>,
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
    /// Decodes version-specific required fields without changing version-1 defaults.
    pub fn from_json(data: &[u8]) -> serde_json::Result<Self> {
        use serde::de::Error;

        let value: serde_json::Value = serde_json::from_slice(data)?;
        if value["version"] == 2 {
            let chunks = value["chunks"].as_array().ok_or_else(|| {
                serde_json::Error::custom("version-2 manifest requires a chunks array")
            })?;
            for chunk in chunks {
                if !chunk["files"].is_array() {
                    return Err(serde_json::Error::custom(
                        "version-2 chunk requires a files array",
                    ));
                }
            }
        }
        serde_json::from_slice(data)
    }

    /// Validates the encoding contract before any snapshot operation.
    pub fn validate_layout(&self) -> std::result::Result<(), String> {
        match (self.version, self.data_layout.as_deref(), self.format) {
            (1, None, _) => Ok(()),
            (2, Some(common_datasource::packed_snapshot::PACKED_LAYOUT), DataFormat::Parquet) => {
                if self.checksum.is_some() || self.chunks.iter().any(|c| c.checksum.is_some()) {
                    return Err("version-2 checksums are unsupported".into());
                }
                if self.schema_only != self.chunks.is_empty() {
                    return Err("version-2 schema_only requires empty chunks; data snapshots require chunks".into());
                }
                let schemas: HashSet<_> = self.schemas.iter().collect();
                if schemas.len() != self.schemas.len() {
                    return Err("duplicate version-2 schemas".into());
                }
                let mut ids = HashSet::new();
                for chunk in &self.chunks {
                    if chunk.id == 0 || !ids.insert(chunk.id) {
                        return Err("version-2 chunk IDs must be unique and positive".into());
                    }
                    let files: HashSet<_> = chunk.files.iter().collect();
                    if files.len() != chunk.files.len() {
                        return Err("duplicate version-2 chunk files".into());
                    }
                    if chunk.status == ChunkStatus::Skipped && !chunk.files.is_empty() {
                        return Err("skipped version-2 chunks must have no files".into());
                    }
                    for file in &chunk.files {
                        if file.contains('\\')
                            || file
                                .split('/')
                                .any(|part| part.is_empty() || part == "." || part == "..")
                            || !self.schemas.iter().any(|schema| {
                                file.starts_with(&crate::data::path::data_dir_for_schema_chunk(
                                    schema, chunk.id,
                                ))
                            })
                        {
                            return Err(format!("invalid version-2 chunk file: {file}"));
                        }
                    }
                }
                Ok(())
            }
            _ => Err(format!(
                "Manifest version mismatch or unsupported layout: version {}, layout {:?}, format {}",
                self.version, self.data_layout, self.format
            )),
        }
    }

    pub fn is_packed(&self) -> bool {
        self.version == 2
    }

    pub fn new_for_export(
        catalog: String,
        schemas: Vec<String>,
        schema_only: bool,
        time_range: TimeRange,
        format: DataFormat,
        chunk_time_window: Option<Duration>,
    ) -> ExportResult<Self> {
        if chunk_time_window.is_some() && !time_range.is_bounded() {
            return ChunkTimeWindowRequiresBoundsSnafu.fail();
        }

        let mut manifest = if schema_only {
            Self::new_schema_only(catalog, schemas)
        } else {
            Self::new_full(catalog, schemas, time_range, format)
        };

        if !schema_only {
            manifest.chunks = match chunk_time_window {
                Some(window) => generate_chunks(&manifest.time_range, window),
                None => generate_single_chunk(&manifest.time_range),
            };
            manifest.touch();
        }

        Ok(manifest)
    }

    /// Creates a new manifest for schema-only export.
    pub fn new_schema_only(catalog: String, schemas: Vec<String>) -> Self {
        let now = Utc::now();
        Self {
            version: MANIFEST_VERSION,
            data_layout: None,
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
            data_layout: None,
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
            || (!self.chunks.is_empty()
                && self
                    .chunks
                    .iter()
                    .all(|c| matches!(c.status, ChunkStatus::Completed | ChunkStatus::Skipped)))
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

    /// Returns the number of skipped chunks.
    pub fn skipped_count(&self) -> usize {
        self.chunks
            .iter()
            .filter(|c| c.status == ChunkStatus::Skipped)
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

fn generate_single_chunk(time_range: &TimeRange) -> Vec<ChunkMeta> {
    if let (Some(start), Some(end)) = (time_range.start, time_range.end) {
        if start == end {
            return vec![ChunkMeta::skipped(1, time_range.clone())];
        }
        if start > end {
            return Vec::new();
        }
    }
    vec![ChunkMeta::new(1, time_range.clone())]
}

#[cfg(test)]
mod tests {
    #[test]
    fn packed_manifest_read_contract_preserves_legacy_writer() {
        let mut manifest =
            super::Manifest::new_schema_only("greptime".into(), vec!["public".into()]);
        assert!(manifest.validate_layout().is_ok());
        manifest.version = 2;
        assert!(manifest.validate_layout().is_err());
        manifest.data_layout = Some("metric-parquet-packs".into());
        assert!(manifest.validate_layout().is_ok());
        manifest.format = super::DataFormat::Csv;
        assert!(manifest.validate_layout().is_err());
        manifest.format = super::DataFormat::Parquet;
        manifest.version = 3;
        assert!(manifest.validate_layout().is_err());
        manifest.version = 1;
        assert!(manifest.validate_layout().is_err());
    }

    #[test]
    fn packed_required_fields_and_checksums_preserve_v1_defaults() {
        let mut manifest =
            super::Manifest::new_schema_only("greptime".into(), vec!["public".into()]);
        manifest
            .chunks
            .push(super::ChunkMeta::new(1, super::TimeRange::unbounded()));
        let legacy = serde_json::to_value(manifest).unwrap();
        for version in [1, 2] {
            let mut value = legacy.clone();
            value["version"] = version.into();
            if version == 2 {
                value["data_layout"] = "metric-parquet-packs".into();
                value["schema_only"] = false.into();
            }
            let raw = serde_json::to_string(&value).unwrap();
            for duplicate in [
                format!(
                    "{{\"checksum\":\"unsupported\",\"checksum\":null,{}",
                    &raw[1..]
                ),
                format!("{{\"chunks\":[],{}", &raw[1..]),
                raw.replacen("\"files\":[]", "\"files\":[],\"files\":[]", 1),
                raw.replacen(
                    "\"files\":[]",
                    "\"checksum\":\"unsupported\",\"checksum\":null,\"files\":[]",
                    1,
                ),
            ] {
                assert!(super::Manifest::from_json(duplicate.as_bytes()).is_err());
            }
            for field in ["chunks", "files"] {
                let mut missing = value.clone();
                if field == "chunks" {
                    missing.as_object_mut().unwrap().remove(field);
                } else {
                    missing["chunks"][0].as_object_mut().unwrap().remove(field);
                }
                assert_eq!(
                    super::Manifest::from_json(&serde_json::to_vec(&missing).unwrap()).is_ok(),
                    version == 1
                );
            }
            for checksum in [
                None,
                Some(serde_json::Value::Null),
                Some("".into()),
                Some("sha256:abc".into()),
            ] {
                for chunk_level in [false, true] {
                    let mut candidate = value.clone();
                    if let Some(checksum) = &checksum {
                        if chunk_level {
                            candidate["chunks"][0]["checksum"] = checksum.clone();
                        } else {
                            candidate["checksum"] = checksum.clone();
                        }
                    }
                    let decoded =
                        super::Manifest::from_json(&serde_json::to_vec(&candidate).unwrap())
                            .unwrap();
                    assert_eq!(
                        decoded.validate_layout().is_ok(),
                        version == 1 || checksum.as_ref().is_none_or(serde_json::Value::is_null)
                    );
                }
            }
        }
    }

    use std::time::Duration;

    use chrono::{TimeZone, Utc};

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
    fn test_generate_single_chunk_zero_width_range_is_skipped() {
        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let chunks = generate_single_chunk(&TimeRange::new(Some(ts), Some(ts)));

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].status, ChunkStatus::Skipped);
        assert_eq!(chunks[0].time_range.start, Some(ts));
        assert_eq!(chunks[0].time_range.end, Some(ts));
    }

    #[test]
    fn test_generate_single_chunk_invalid_range_is_empty() {
        let start = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let chunks = generate_single_chunk(&TimeRange::new(Some(start), Some(end)));

        assert!(chunks.is_empty());
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
        assert!(!manifest.is_complete());
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

        chunk.mark_skipped();
        assert_eq!(chunk.status, ChunkStatus::Skipped);
        assert!(chunk.files.is_empty());
    }

    #[test]
    fn test_manifest_is_complete_when_chunks_are_completed_or_skipped() {
        let mut manifest = Manifest::new_full(
            "greptime".to_string(),
            vec!["public".to_string()],
            TimeRange::unbounded(),
            DataFormat::Parquet,
        );
        manifest.add_chunk(ChunkMeta::new(1, TimeRange::unbounded()));
        manifest.add_chunk(ChunkMeta::new(2, TimeRange::unbounded()));

        manifest.update_chunk(1, |chunk| {
            chunk.mark_completed(vec!["a.parquet".to_string()], None)
        });
        manifest.update_chunk(2, |chunk| chunk.mark_skipped());

        assert!(manifest.is_complete());
        assert_eq!(manifest.completed_count(), 1);
        assert_eq!(manifest.skipped_count(), 1);
    }

    #[test]
    fn test_manifest_chunk_time_window_none_single_chunk() {
        let start = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
        let range = TimeRange::new(Some(start), Some(end));
        let manifest = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string()],
            false,
            range.clone(),
            DataFormat::Parquet,
            None,
        )
        .unwrap();

        assert_eq!(manifest.chunks.len(), 1);
        assert_eq!(manifest.chunks[0].time_range, range);
    }

    #[test]
    fn test_time_range_parse_requires_order() {
        let result = TimeRange::parse(Some("2025-01-02T00:00:00Z"), Some("2025-01-01T00:00:00Z"));
        assert!(result.is_err());
    }

    #[test]
    fn test_new_for_export_with_chunk_window_requires_bounded_range() {
        let result = Manifest::new_for_export(
            "greptime".to_string(),
            vec!["public".to_string()],
            false,
            TimeRange::new(
                None,
                Some(Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap()),
            ),
            DataFormat::Parquet,
            Some(Duration::from_secs(3600)),
        );
        assert!(result.is_err());
    }
}
