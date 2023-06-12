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

pub(crate) mod parquet;
mod stream_writer;

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common_base::readable_size::ReadableSize;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{debug, error};
use common_time::range::TimestampRange;
use common_time::Timestamp;
use datatypes::schema::SchemaRef;
use futures_util::StreamExt;
use object_store::{util, ObjectStore};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{ResultExt, Snafu};
use store_api::storage::{ChunkReader, RegionId};
use table::predicate::Predicate;
use uuid::Uuid;

use crate::chunk::ChunkReaderImpl;
use crate::error;
use crate::error::{DeleteSstSnafu, Result};
use crate::file_purger::{FilePurgeRequest, FilePurgerRef};
use crate::memtable::BoxedBatchIterator;
use crate::read::{Batch, BoxedBatchReader};
use crate::scheduler::Scheduler;
use crate::schema::ProjectedSchemaRef;
use crate::sst::parquet::{ParquetReader, ParquetWriter};

/// Maximum level of SSTs.
pub const MAX_LEVEL: u8 = 2;

pub type Level = u8;

pub use crate::sst::stream_writer::BufferedWriter;

// We only has fixed number of level, so we use array to hold elements. This implementation
// detail of LevelMetaVec should not be exposed to the user of [LevelMetas].
type LevelMetaVec = [LevelMeta; MAX_LEVEL as usize];

/// Metadata of all SSTs under a region.
///
/// Files are organized into multiple level, though there may be only one level.
#[derive(Clone)]
pub struct LevelMetas {
    levels: LevelMetaVec,
    sst_layer: AccessLayerRef,
    file_purger: FilePurgerRef,
    /// Compaction time window in seconds
    compaction_time_window: Option<i64>,
}

impl std::fmt::Debug for LevelMetas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LevelMetas")
            .field("levels", &self.levels)
            .field("compaction_time_window", &self.compaction_time_window)
            .finish()
    }
}

impl LevelMetas {
    /// Create a new LevelMetas and initialized each level.
    pub fn new(sst_layer: AccessLayerRef, file_purger: FilePurgerRef) -> LevelMetas {
        LevelMetas {
            levels: new_level_meta_vec(),
            sst_layer,
            file_purger,
            compaction_time_window: Default::default(),
        }
    }

    /// Returns total level number.
    #[inline]
    pub fn level_num(&self) -> usize {
        self.levels.len()
    }

    pub fn compaction_time_window(&self) -> Option<i64> {
        self.compaction_time_window
    }

    #[inline]
    pub fn level(&self, level: Level) -> &LevelMeta {
        &self.levels[level as usize]
    }

    /// Merge `self` with files to add/remove to create a new [LevelMetas].
    ///
    /// # Panics
    /// Panics if level of [FileHandle] is greater than [MAX_LEVEL].
    pub fn merge(
        &self,
        files_to_add: impl Iterator<Item = FileMeta>,
        files_to_remove: impl Iterator<Item = FileMeta>,
        compaction_time_window: Option<i64>,
    ) -> LevelMetas {
        let mut merged = self.clone();
        for file in files_to_add {
            let level = file.level;
            let handle = FileHandle::new(file, self.sst_layer.clone(), self.file_purger.clone());
            merged.levels[level as usize].add_file(handle);
        }

        for file in files_to_remove {
            let level = file.level;
            if let Some(removed_file) = merged.levels[level as usize].remove_file(file.file_id) {
                removed_file.mark_deleted();
            }
        }
        // we only update region's compaction time window iff region's window is not set and VersionEdit's
        // compaction time window is present.
        if let Some(window) = compaction_time_window {
            merged.compaction_time_window.get_or_insert(window);
        }
        merged
    }

    pub fn mark_all_files_deleted(&self) -> Vec<FileId> {
        self.levels().iter().fold(vec![], |mut files, level| {
            files.extend(level.files().map(|f| {
                f.mark_deleted();
                f.file_id()
            }));
            files
        })
    }

    pub fn levels(&self) -> &[LevelMeta] {
        &self.levels
    }
}

/// Metadata of files in same SST level.
#[derive(Default, Clone)]
pub struct LevelMeta {
    level: Level,
    /// Handles to the files in this level.
    // TODO(yingwen): Now for simplicity, files are unordered, maybe sort the files by time range
    // or use another structure to hold them.
    files: HashMap<FileId, FileHandle>,
}

impl std::fmt::Debug for LevelMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LevelMeta")
            .field("level", &self.level)
            .field("files", &self.files.keys())
            .finish()
    }
}

impl LevelMeta {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            files: HashMap::new(),
        }
    }

    fn add_file(&mut self, file: FileHandle) {
        self.files.insert(file.file_id(), file);
    }

    fn remove_file(&mut self, file_to_remove: FileId) -> Option<FileHandle> {
        self.files.remove(&file_to_remove)
    }

    /// Returns the level of level meta.
    #[inline]
    pub fn level(&self) -> Level {
        self.level
    }

    /// Returns number of SST files in level.
    #[inline]
    pub fn file_num(&self) -> usize {
        self.files.len()
    }

    /// Returns expired SSTs from current level.
    pub fn get_expired_files(&self, expire_time: &Timestamp) -> Vec<FileHandle> {
        self.files
            .iter()
            .filter_map(|(_, v)| {
                let Some((_, end)) = v.time_range() else { return None; };
                if end < expire_time {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn files(&self) -> impl Iterator<Item = &FileHandle> {
        self.files.values()
    }
}

fn new_level_meta_vec() -> LevelMetaVec {
    (0u8..MAX_LEVEL)
        .map(LevelMeta::new)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap() // safety: LevelMetaVec is a fixed length array with length MAX_LEVEL
}

#[derive(Debug, Clone)]
pub struct FileHandle {
    inner: Arc<FileHandleInner>,
}

impl FileHandle {
    pub fn new(
        meta: FileMeta,
        sst_layer: AccessLayerRef,
        file_purger: FilePurgerRef,
    ) -> FileHandle {
        FileHandle {
            inner: Arc::new(FileHandleInner::new(meta, sst_layer, file_purger)),
        }
    }

    /// Returns level as usize so it can be used as index.
    #[inline]
    pub fn level(&self) -> Level {
        self.inner.meta.level
    }

    #[inline]
    pub fn file_name(&self) -> String {
        self.inner.meta.file_id.as_parquet()
    }

    #[inline]
    pub fn file_path(&self) -> String {
        self.inner
            .sst_layer
            .sst_file_path(&self.inner.meta.file_id.as_parquet())
    }

    #[inline]
    pub fn file_id(&self) -> FileId {
        self.inner.meta.file_id
    }

    #[inline]
    pub fn time_range(&self) -> &Option<(Timestamp, Timestamp)> {
        &self.inner.meta.time_range
    }

    /// Returns true if current file is under compaction.
    #[inline]
    pub fn compacting(&self) -> bool {
        self.inner.compacting.load(Ordering::Relaxed)
    }

    /// Sets the compacting flag.
    #[inline]
    pub fn mark_compacting(&self, compacting: bool) {
        self.inner.compacting.store(compacting, Ordering::Relaxed);
    }

    #[inline]
    pub fn deleted(&self) -> bool {
        self.inner.deleted.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn mark_deleted(&self) {
        self.inner.deleted.store(true, Ordering::Relaxed);
    }

    #[inline]
    pub fn meta(&self) -> FileMeta {
        self.inner.meta.clone()
    }

    #[inline]
    pub fn file_size(&self) -> u64 {
        self.inner.meta.file_size
    }
}

/// Actually data of [FileHandle].
///
/// Contains meta of the file, and other mutable info like metrics.
struct FileHandleInner {
    meta: FileMeta,
    compacting: AtomicBool,
    deleted: AtomicBool,
    sst_layer: AccessLayerRef,
    file_purger: FilePurgerRef,
}

impl fmt::Debug for FileHandleInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHandleInner")
            .field("meta", &self.meta)
            .field("compacting", &self.compacting)
            .field("deleted", &self.deleted)
            .finish()
    }
}

impl Drop for FileHandleInner {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Relaxed) {
            let request = FilePurgeRequest {
                sst_layer: self.sst_layer.clone(),
                file_id: self.meta.file_id,
                region_id: self.meta.region_id,
            };
            match self.file_purger.schedule(request) {
                Ok(res) => {
                    debug!(
                        "Scheduled SST purge task, region: {}, name: {}, res: {}",
                        self.meta.region_id,
                        self.meta.file_id.as_parquet(),
                        res
                    );
                }
                Err(e) => {
                    error!(e; "Failed to schedule SST purge task, region: {}, name: {}",
                           self.meta.region_id, self.meta.file_id.as_parquet());
                }
            }
        }
    }
}

impl FileHandleInner {
    fn new(
        meta: FileMeta,
        sst_layer: AccessLayerRef,
        file_purger: FilePurgerRef,
    ) -> FileHandleInner {
        FileHandleInner {
            meta,
            compacting: AtomicBool::new(false),
            deleted: AtomicBool::new(false),
            sst_layer,
            file_purger,
        }
    }
}

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

    /// Append `.parquet` to file id to make a complete file name
    pub fn as_parquet(&self) -> String {
        format!("{}{}", self.0.hyphenated(), ".parquet")
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

/// Immutable metadata of a sst file.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FileMeta {
    /// Region of file.
    pub region_id: RegionId,
    /// Compared to normal file names, FileId ignore the extension
    #[serde(deserialize_with = "deserialize_from_string")]
    #[serde(alias = "file_name")]
    pub file_id: FileId,
    /// Timestamp range of file.
    pub time_range: Option<(Timestamp, Timestamp)>,
    /// SST level of the file.
    pub level: Level,
    /// Size of the file.
    pub file_size: u64,
}

fn deserialize_from_string<'de, D>(deserializer: D) -> std::result::Result<FileId, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    let stripped = s.strip_suffix(".parquet").unwrap_or(s); // strip parquet suffix if needed.
    FileId::from_str(stripped).map_err(<D::Error as serde::de::Error>::custom)
}

#[derive(Debug)]
pub struct WriteOptions {
    // TODO(yingwen): [flush] row group size.
    pub sst_write_buffer_size: ReadableSize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            sst_write_buffer_size: ReadableSize::mb(8),
        }
    }
}

pub struct ReadOptions {
    /// Suggested size of each batch.
    pub batch_size: usize,
    /// The schema that user expected to read, might not the same as the
    /// schema of the SST file.
    pub projected_schema: ProjectedSchemaRef,

    pub predicate: Predicate,
    pub time_range: TimestampRange,
}

#[derive(Debug, PartialEq)]
pub struct SstInfo {
    pub time_range: Option<(Timestamp, Timestamp)>,
    pub file_size: u64,
    pub num_rows: usize,
}

/// SST access layer.
#[async_trait]
pub trait AccessLayer: Send + Sync + std::fmt::Debug {
    /// Returns the sst file path.
    fn sst_file_path(&self, file_name: &str) -> String;

    /// Writes SST file with given `file_id` and returns the SST info.
    /// If source does not contain any data, `write_sst` will return `Ok(None)`.
    async fn write_sst(
        &self,
        file_id: FileId,
        source: Source,
        opts: &WriteOptions,
    ) -> Result<Option<SstInfo>>;

    /// Read SST file with given `file_handle` and schema.
    async fn read_sst(
        &self,
        file_handle: FileHandle,
        opts: &ReadOptions,
    ) -> Result<BoxedBatchReader>;

    /// Deletes a SST file with given name.
    async fn delete_sst(&self, file_id: FileId) -> Result<()>;
}

pub type AccessLayerRef = Arc<dyn AccessLayer>;

/// Parquet writer data source.
pub enum Source {
    /// Writes rows from memtable to parquet
    Iter(BoxedBatchIterator),
    /// Writes row from ChunkReaderImpl (maybe a set of SSTs) to parquet.
    Reader(ChunkReaderImpl),
    /// Record batch stream yielded by table scan
    Stream(SendableRecordBatchStream),
}

impl Source {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Source::Iter(iter) => iter.next().transpose(),
            Source::Reader(reader) => reader
                .next_chunk()
                .await
                .map(|p| p.map(|chunk| Batch::new(chunk.columns))),
            Source::Stream(stream) => stream
                .next()
                .await
                .transpose()
                .map(|r| r.map(|r| Batch::new(r.columns().to_vec())))
                .context(error::CreateRecordBatchSnafu),
        }
    }

    fn schema(&self) -> SchemaRef {
        match self {
            Source::Iter(iter) => {
                let projected_schema = iter.schema();
                projected_schema.schema_to_read().schema().clone()
            }
            Source::Reader(reader) => reader.projected_schema().schema_to_read().schema().clone(),
            Source::Stream(stream) => stream.schema(),
        }
    }
}

/// Sst access layer.
pub struct FsAccessLayer {
    sst_dir: String,
    object_store: ObjectStore,
}

impl fmt::Debug for FsAccessLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsAccessLayer")
            .field("sst_dir", &self.sst_dir)
            .finish()
    }
}

impl FsAccessLayer {
    pub fn new(sst_dir: &str, object_store: ObjectStore) -> FsAccessLayer {
        FsAccessLayer {
            sst_dir: util::normalize_dir(sst_dir),
            object_store,
        }
    }
}

#[async_trait]
impl AccessLayer for FsAccessLayer {
    fn sst_file_path(&self, file_name: &str) -> String {
        format!("{}{}", self.sst_dir, file_name)
    }

    /// Writes SST file with given `file_id`.
    async fn write_sst(
        &self,
        file_id: FileId,
        source: Source,
        opts: &WriteOptions,
    ) -> Result<Option<SstInfo>> {
        // Now we only supports parquet format. We may allow caller to specific SST format in
        // WriteOptions in the future.
        let file_path = self.sst_file_path(&file_id.as_parquet());
        let writer = ParquetWriter::new(&file_path, source, self.object_store.clone());
        writer.write_sst(opts).await
    }

    /// Read SST file with given `file_handle` and schema.
    async fn read_sst(
        &self,
        file_handle: FileHandle,
        opts: &ReadOptions,
    ) -> Result<BoxedBatchReader> {
        let reader = ParquetReader::new(
            file_handle,
            self.object_store.clone(),
            opts.projected_schema.clone(),
            opts.predicate.clone(),
            opts.time_range,
        );

        let stream = reader.chunk_stream().await?;
        Ok(Box::new(stream))
    }

    /// Deletes a SST file with given file id.
    async fn delete_sst(&self, file_id: FileId) -> Result<()> {
        let path = self.sst_file_path(&file_id.as_parquet());
        self.object_store
            .delete(&path)
            .await
            .context(DeleteSstSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::file_purger::noop::NoopFilePurgeHandler;
    use crate::scheduler::{LocalScheduler, SchedulerConfig};

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

    #[test]
    fn test_deserialize_file_meta() {
        let file_meta = create_file_meta(FileId::random(), 0);
        let serialized_file_meta = serde_json::to_string(&file_meta).unwrap();
        let deserialized_file_meta = serde_json::from_str(&serialized_file_meta);
        assert_eq!(file_meta, deserialized_file_meta.unwrap());
    }

    #[test]
    fn test_deserialize_from_string() {
        let json_file_meta = "{\"region_id\":0,\"file_id\":\"bc5896ec-e4d8-4017-a80d-f2de73188d55\",\"time_range\":null,\"level\":0}";
        let file_meta = create_file_meta(
            FileId::from_str("bc5896ec-e4d8-4017-a80d-f2de73188d55").unwrap(),
            0,
        );
        let deserialized_file_meta: FileMeta = serde_json::from_str(json_file_meta).unwrap();
        assert_eq!(file_meta, deserialized_file_meta);
    }
    #[test]
    fn test_deserialize_from_string_parquet() {
        let json_file_meta = "{\"region_id\":0,\"file_id\":\"bc5896ec-e4d8-4017-a80d-f2de73188d55.parquet\",\"time_range\":null,\"level\":0}";
        let file_meta = create_file_meta(
            FileId::from_str("bc5896ec-e4d8-4017-a80d-f2de73188d55").unwrap(),
            0,
        );
        let deserialized_file_meta: FileMeta = serde_json::from_str(json_file_meta).unwrap();
        assert_eq!(file_meta, deserialized_file_meta);
    }

    #[test]
    fn test_deserialize_from_string_parquet_file_name() {
        let json_file_meta = "{\"region_id\":0,\"file_name\":\"bc5896ec-e4d8-4017-a80d-f2de73188d55.parquet\",\"time_range\":null,\"level\":0}";
        let file_meta = create_file_meta(
            FileId::from_str("bc5896ec-e4d8-4017-a80d-f2de73188d55").unwrap(),
            0,
        );
        let deserialized_file_meta: FileMeta = serde_json::from_str(json_file_meta).unwrap();
        assert_eq!(file_meta, deserialized_file_meta);
    }

    #[test]
    fn test_file_id_as_parquet() {
        let id = FileId::from_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
        assert_eq!(
            "67e55044-10b1-426f-9247-bb680e5fe0c8.parquet",
            id.as_parquet()
        );
    }

    fn create_file_meta(file_id: FileId, level: Level) -> FileMeta {
        FileMeta {
            region_id: 0,
            file_id,
            time_range: None,
            level,
            file_size: 0,
        }
    }

    #[test]
    fn test_level_metas_add_and_remove() {
        let layer = Arc::new(crate::test_util::access_layer_util::MockAccessLayer {});
        let purger = Arc::new(LocalScheduler::new(
            SchedulerConfig::default(),
            NoopFilePurgeHandler,
        ));
        let file_ids = [
            FileId::random(),
            FileId::random(),
            FileId::random(),
            FileId::random(),
        ];

        let metas = LevelMetas::new(layer, purger);
        let merged = metas.merge(
            vec![
                create_file_meta(file_ids[0], 0),
                create_file_meta(file_ids[1], 0),
            ]
            .into_iter(),
            vec![].into_iter(),
            None,
        );

        assert_eq!(
            HashSet::from([file_ids[0], file_ids[1]]),
            merged.level(0).files().map(|f| f.file_id()).collect()
        );

        let merged1 = merged.merge(
            vec![
                create_file_meta(file_ids[2], 1),
                create_file_meta(file_ids[3], 1),
            ]
            .into_iter(),
            vec![].into_iter(),
            None,
        );
        assert_eq!(
            HashSet::from([file_ids[0], file_ids[1]]),
            merged1.level(0).files().map(|f| f.file_id()).collect()
        );

        assert_eq!(
            HashSet::from([file_ids[2], file_ids[3]]),
            merged1.level(1).files().map(|f| f.file_id()).collect()
        );

        let removed1 = merged1.merge(
            vec![].into_iter(),
            vec![
                create_file_meta(file_ids[0], 0),
                create_file_meta(file_ids[2], 0),
            ]
            .into_iter(),
            None,
        );
        assert_eq!(
            HashSet::from([file_ids[1]]),
            removed1.level(0).files().map(|f| f.file_id()).collect()
        );

        assert_eq!(
            HashSet::from([file_ids[2], file_ids[3]]),
            removed1.level(1).files().map(|f| f.file_id()).collect()
        );

        let removed2 = removed1.merge(
            vec![].into_iter(),
            vec![
                create_file_meta(file_ids[2], 1),
                create_file_meta(file_ids[3], 1),
            ]
            .into_iter(),
            None,
        );
        assert_eq!(
            HashSet::from([file_ids[1]]),
            removed2.level(0).files().map(|f| f.file_id()).collect()
        );

        assert_eq!(
            HashSet::new(),
            removed2.level(1).files().map(|f| f.file_id()).collect()
        );
    }
}
