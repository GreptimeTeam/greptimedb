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

//! Structures to describe metadata of files.

use std::fmt;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common_base::readable_size::ReadableSize;
use common_time::Timestamp;
use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};
use store_api::region_request::PathType;
use store_api::storage::RegionId;
use uuid::Uuid;

use crate::sst::file_purger::FilePurgerRef;
use crate::sst::location;

/// Custom serde functions for partition_expr field in FileMeta
fn serialize_partition_expr<S>(
    partition_expr: &Option<PartitionExpr>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::Error;

    match partition_expr {
        None => serializer.serialize_none(),
        Some(expr) => {
            let json_str = expr.as_json_str().map_err(S::Error::custom)?;
            serializer.serialize_some(&json_str)
        }
    }
}

fn deserialize_partition_expr<'de, D>(deserializer: D) -> Result<Option<PartitionExpr>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let opt_json_str: Option<String> = Option::deserialize(deserializer)?;
    match opt_json_str {
        None => Ok(None),
        Some(json_str) => {
            if json_str.is_empty() {
                // Empty string represents explicit "single-region/no-partition" designation
                Ok(None)
            } else {
                // Parse the JSON string to PartitionExpr
                PartitionExpr::from_json_str(&json_str).map_err(D::Error::custom)
            }
        }
    }
}

/// Type to store SST level.
pub type Level = u8;
/// Maximum level of SSTs.
pub const MAX_LEVEL: Level = 2;

#[derive(Debug, Snafu, PartialEq)]
pub struct ParseIdError {
    source: uuid::Error,
}

/// Unique id for [SST File].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct FileId(Uuid);

impl FileId {
    /// Returns a new unique [FileId] randomly.
    pub fn random() -> FileId {
        FileId(Uuid::new_v4())
    }

    /// Parses id from string.
    pub fn parse_str(input: &str) -> std::result::Result<FileId, ParseIdError> {
        Uuid::parse_str(input).map(FileId).context(ParseIdSnafu)
    }

    /// Converts [FileId] as byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<FileId> for Uuid {
    fn from(value: FileId) -> Self {
        value.0
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for FileId {
    type Err = ParseIdError;

    fn from_str(s: &str) -> std::result::Result<FileId, ParseIdError> {
        FileId::parse_str(s)
    }
}

/// Cross-region file id.
///
/// It contains a region id and a file id. The string representation is `{region_id}/{file_id}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegionFileId {
    /// The region that creates the file.
    region_id: RegionId,
    /// The id of the file.
    file_id: FileId,
}

impl RegionFileId {
    /// Creates a new [RegionFileId] from `region_id` and `file_id`.
    pub fn new(region_id: RegionId, file_id: FileId) -> Self {
        Self { region_id, file_id }
    }

    /// Gets the region id.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Gets the file id.
    pub fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl fmt::Display for RegionFileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.region_id, self.file_id)
    }
}

/// Time range (min and max timestamps) of a SST file.
/// Both min and max are inclusive.
pub type FileTimeRange = (Timestamp, Timestamp);

/// Checks if two inclusive timestamp ranges overlap with each other.
pub(crate) fn overlaps(l: &FileTimeRange, r: &FileTimeRange) -> bool {
    let (l, r) = if l.0 <= r.0 { (l, r) } else { (r, l) };
    let (_, l_end) = l;
    let (r_start, _) = r;

    r_start <= l_end
}

/// Metadata of a SST file.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FileMeta {
    /// Region that created the file. The region id may not be the id of the current region.
    pub region_id: RegionId,
    /// Compared to normal file names, FileId ignore the extension
    pub file_id: FileId,
    /// Timestamp range of file. The timestamps have the same time unit as the
    /// data in the SST.
    pub time_range: FileTimeRange,
    /// SST level of the file.
    pub level: Level,
    /// Size of the file.
    pub file_size: u64,
    /// Available indexes of the file.
    pub available_indexes: SmallVec<[IndexType; 4]>,
    /// Size of the index file.
    pub index_file_size: u64,
    /// Number of rows in the file.
    ///
    /// For historical reasons, this field might be missing in old files. Thus
    /// the default value `0` doesn't means the file doesn't contains any rows,
    /// but instead means the number of rows is unknown.
    pub num_rows: u64,
    /// Number of row groups in the file.
    ///
    /// For historical reasons, this field might be missing in old files. Thus
    /// the default value `0` doesn't means the file doesn't contains any rows,
    /// but instead means the number of rows is unknown.
    pub num_row_groups: u64,
    /// Sequence in this file.
    ///
    /// This sequence is the only sequence in this file. And it's retrieved from the max
    /// sequence of the rows on generating this file.
    pub sequence: Option<NonZeroU64>,
    /// Partition expression from the region metadata when the file is created.
    ///
    /// This is stored as a PartitionExpr object in memory for convenience,
    /// but serialized as JSON string for manifest compatibility.
    /// Compatibility behavior:
    /// - None: no partition expr was set when the file was created (legacy files).
    /// - Some(expr): partition expression from region metadata.
    #[serde(
        serialize_with = "serialize_partition_expr",
        deserialize_with = "deserialize_partition_expr"
    )]
    pub partition_expr: Option<PartitionExpr>,
}

impl Debug for FileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("FileMeta");
        debug_struct
            .field("region_id", &self.region_id)
            .field_with("file_id", |f| write!(f, "{} ", self.file_id))
            .field_with("time_range", |f| {
                write!(
                    f,
                    "({}, {}) ",
                    self.time_range.0.to_iso8601_string(),
                    self.time_range.1.to_iso8601_string()
                )
            })
            .field("level", &self.level)
            .field("file_size", &ReadableSize(self.file_size));
        if !self.available_indexes.is_empty() {
            debug_struct
                .field("available_indexes", &self.available_indexes)
                .field("index_file_size", &ReadableSize(self.index_file_size));
        }
        debug_struct
            .field("num_rows", &self.num_rows)
            .field("num_row_groups", &self.num_row_groups)
            .field_with("sequence", |f| match self.sequence {
                None => {
                    write!(f, "None")
                }
                Some(seq) => {
                    write!(f, "{}", seq)
                }
            })
            .field("partition_expr", &self.partition_expr)
            .finish()
    }
}

/// Type of index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    /// Inverted index.
    InvertedIndex,
    /// Full-text index.
    FulltextIndex,
    /// Bloom Filter index
    BloomFilterIndex,
}

impl FileMeta {
    pub fn exists_index(&self) -> bool {
        !self.available_indexes.is_empty()
    }

    /// Returns true if the file has an inverted index
    pub fn inverted_index_available(&self) -> bool {
        self.available_indexes.contains(&IndexType::InvertedIndex)
    }

    /// Returns true if the file has a fulltext index
    pub fn fulltext_index_available(&self) -> bool {
        self.available_indexes.contains(&IndexType::FulltextIndex)
    }

    /// Returns true if the file has a bloom filter index.
    pub fn bloom_filter_index_available(&self) -> bool {
        self.available_indexes
            .contains(&IndexType::BloomFilterIndex)
    }

    pub fn index_file_size(&self) -> u64 {
        self.index_file_size
    }

    /// Returns the cross-region file id.
    pub fn file_id(&self) -> RegionFileId {
        RegionFileId::new(self.region_id, self.file_id)
    }
}

/// Handle to a SST file.
#[derive(Clone)]
pub struct FileHandle {
    inner: Arc<FileHandleInner>,
}

impl fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHandle")
            .field("meta", self.meta_ref())
            .field("compacting", &self.compacting())
            .field("deleted", &self.inner.deleted.load(Ordering::Relaxed))
            .finish()
    }
}

impl FileHandle {
    pub fn new(meta: FileMeta, file_purger: FilePurgerRef) -> FileHandle {
        FileHandle {
            inner: Arc::new(FileHandleInner::new(meta, file_purger)),
        }
    }

    /// Returns the region id of the file.
    pub fn region_id(&self) -> RegionId {
        self.inner.meta.region_id
    }

    /// Returns the cross-region file id.
    pub fn file_id(&self) -> RegionFileId {
        RegionFileId::new(self.inner.meta.region_id, self.inner.meta.file_id)
    }

    /// Returns the complete file path of the file.
    pub fn file_path(&self, file_dir: &str, path_type: PathType) -> String {
        location::sst_file_path(file_dir, self.file_id(), path_type)
    }

    /// Returns the time range of the file.
    pub fn time_range(&self) -> FileTimeRange {
        self.inner.meta.time_range
    }

    /// Mark the file as deleted and will delete it on drop asynchronously
    pub fn mark_deleted(&self) {
        self.inner.deleted.store(true, Ordering::Relaxed);
    }

    pub fn compacting(&self) -> bool {
        self.inner.compacting.load(Ordering::Relaxed)
    }

    pub fn set_compacting(&self, compacting: bool) {
        self.inner.compacting.store(compacting, Ordering::Relaxed);
    }

    /// Returns a reference to the [FileMeta].
    pub fn meta_ref(&self) -> &FileMeta {
        &self.inner.meta
    }

    pub fn file_purger(&self) -> FilePurgerRef {
        self.inner.file_purger.clone()
    }

    pub fn size(&self) -> u64 {
        self.inner.meta.file_size
    }

    pub fn index_size(&self) -> u64 {
        self.inner.meta.index_file_size
    }

    pub fn num_rows(&self) -> usize {
        self.inner.meta.num_rows as usize
    }

    pub fn level(&self) -> Level {
        self.inner.meta.level
    }

    pub fn is_deleted(&self) -> bool {
        self.inner.deleted.load(Ordering::Relaxed)
    }
}

/// Inner data of [FileHandle].
///
/// Contains meta of the file, and other mutable info like whether the file is compacting.
struct FileHandleInner {
    meta: FileMeta,
    compacting: AtomicBool,
    deleted: AtomicBool,
    file_purger: FilePurgerRef,
}

impl Drop for FileHandleInner {
    fn drop(&mut self) {
        self.file_purger
            .remove_file(self.meta.clone(), self.deleted.load(Ordering::Relaxed));
    }
}

impl FileHandleInner {
    fn new(meta: FileMeta, file_purger: FilePurgerRef) -> FileHandleInner {
        file_purger.new_file(&meta);
        FileHandleInner {
            meta,
            compacting: AtomicBool::new(false),
            deleted: AtomicBool::new(false),
            file_purger,
        }
    }
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use partition::expr::{PartitionExpr, col};

    use super::*;

    #[test]
    fn test_file_id() {
        let id = FileId::random();
        let uuid_str = id.to_string();
        assert_eq!(id.0.to_string(), uuid_str);

        let parsed = FileId::parse_str(&uuid_str).unwrap();
        assert_eq!(id, parsed);
        let parsed = uuid_str.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_file_id_serialization() {
        let id = FileId::random();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(format!("\"{id}\""), json);

        let parsed = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    fn create_file_meta(file_id: FileId, level: Level) -> FileMeta {
        FileMeta {
            region_id: 0.into(),
            file_id,
            time_range: FileTimeRange::default(),
            level,
            file_size: 0,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            index_file_size: 0,
            num_rows: 0,
            num_row_groups: 0,
            sequence: None,
            partition_expr: None,
        }
    }

    #[test]
    fn test_deserialize_file_meta() {
        let file_meta = create_file_meta(FileId::random(), 0);
        let serialized_file_meta = serde_json::to_string(&file_meta).unwrap();
        let deserialized_file_meta = serde_json::from_str(&serialized_file_meta);
        assert_eq!(file_meta, deserialized_file_meta.unwrap());
    }

    #[test]
    fn test_deserialize_from_string() {
        let json_file_meta = "{\"region_id\":0,\"file_id\":\"bc5896ec-e4d8-4017-a80d-f2de73188d55\",\
        \"time_range\":[{\"value\":0,\"unit\":\"Millisecond\"},{\"value\":0,\"unit\":\"Millisecond\"}],\
        \"available_indexes\":[\"InvertedIndex\"],\"level\":0}";
        let file_meta = create_file_meta(
            FileId::from_str("bc5896ec-e4d8-4017-a80d-f2de73188d55").unwrap(),
            0,
        );
        let deserialized_file_meta: FileMeta = serde_json::from_str(json_file_meta).unwrap();
        assert_eq!(file_meta, deserialized_file_meta);
    }

    #[test]
    fn test_file_meta_with_partition_expr() {
        let file_id = FileId::random();
        let partition_expr = PartitionExpr::new(
            col("a"),
            partition::expr::RestrictedOp::GtEq,
            Value::UInt32(10).into(),
        );

        let file_meta_with_partition = FileMeta {
            region_id: 0.into(),
            file_id,
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 0,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            index_file_size: 0,
            num_rows: 0,
            num_row_groups: 0,
            sequence: None,
            partition_expr: Some(partition_expr.clone()),
        };

        // Test serialization/deserialization
        let serialized = serde_json::to_string(&file_meta_with_partition).unwrap();
        let deserialized: FileMeta = serde_json::from_str(&serialized).unwrap();
        assert_eq!(file_meta_with_partition, deserialized);

        // Verify the serialized JSON contains the expected partition expression string
        let serialized_value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert!(serialized_value["partition_expr"].as_str().is_some());
        let partition_expr_json = serialized_value["partition_expr"].as_str().unwrap();
        assert!(partition_expr_json.contains("\"Column\":\"a\""));
        assert!(partition_expr_json.contains("\"op\":\"GtEq\""));

        // Test with None (legacy files)
        let file_meta_none = FileMeta {
            partition_expr: None,
            ..file_meta_with_partition.clone()
        };
        let serialized_none = serde_json::to_string(&file_meta_none).unwrap();
        let deserialized_none: FileMeta = serde_json::from_str(&serialized_none).unwrap();
        assert_eq!(file_meta_none, deserialized_none);
    }

    #[test]
    fn test_file_meta_partition_expr_backward_compatibility() {
        // Test that we can deserialize old JSON format with partition_expr as string
        let json_with_partition_expr = r#"{
            "region_id": 0,
            "file_id": "bc5896ec-e4d8-4017-a80d-f2de73188d55",
            "time_range": [
                {"value": 0, "unit": "Millisecond"},
                {"value": 0, "unit": "Millisecond"}
            ],
            "level": 0,
            "file_size": 0,
            "available_indexes": ["InvertedIndex"],
            "index_file_size": 0,
            "num_rows": 0,
            "num_row_groups": 0,
            "sequence": null,
            "partition_expr": "{\"Expr\":{\"lhs\":{\"Column\":\"a\"},\"op\":\"GtEq\",\"rhs\":{\"Value\":{\"UInt32\":10}}}}"
        }"#;

        let file_meta: FileMeta = serde_json::from_str(json_with_partition_expr).unwrap();
        assert!(file_meta.partition_expr.is_some());
        let expr = file_meta.partition_expr.unwrap();
        assert_eq!(format!("{}", expr), "a >= 10");

        // Test empty partition expression string
        let json_with_empty_expr = r#"{
            "region_id": 0,
            "file_id": "bc5896ec-e4d8-4017-a80d-f2de73188d55",
            "time_range": [
                {"value": 0, "unit": "Millisecond"},
                {"value": 0, "unit": "Millisecond"}
            ],
            "level": 0,
            "file_size": 0,
            "available_indexes": [],
            "index_file_size": 0,
            "num_rows": 0,
            "num_row_groups": 0,
            "sequence": null,
            "partition_expr": ""
        }"#;

        let file_meta_empty: FileMeta = serde_json::from_str(json_with_empty_expr).unwrap();
        assert!(file_meta_empty.partition_expr.is_none());

        // Test null partition expression
        let json_with_null_expr = r#"{
            "region_id": 0,
            "file_id": "bc5896ec-e4d8-4017-a80d-f2de73188d55",
            "time_range": [
                {"value": 0, "unit": "Millisecond"},
                {"value": 0, "unit": "Millisecond"}
            ],
            "level": 0,
            "file_size": 0,
            "available_indexes": [],
            "index_file_size": 0,
            "num_rows": 0,
            "num_row_groups": 0,
            "sequence": null,
            "partition_expr": null
        }"#;

        let file_meta_null: FileMeta = serde_json::from_str(json_with_null_expr).unwrap();
        assert!(file_meta_null.partition_expr.is_none());

        // Test partition expression doesn't exist
        let json_with_empty_expr = r#"{
            "region_id": 0,
            "file_id": "bc5896ec-e4d8-4017-a80d-f2de73188d55",
            "time_range": [
                {"value": 0, "unit": "Millisecond"},
                {"value": 0, "unit": "Millisecond"}
            ],
            "level": 0,
            "file_size": 0,
            "available_indexes": [],
            "index_file_size": 0,
            "num_rows": 0,
            "num_row_groups": 0,
            "sequence": null
        }"#;

        let file_meta_empty: FileMeta = serde_json::from_str(json_with_empty_expr).unwrap();
        assert!(file_meta_empty.partition_expr.is_none());
    }
}
