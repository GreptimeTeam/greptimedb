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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::{error, info};
use common_time::range::TimestampRange;
use common_time::Timestamp;
use object_store::{util, ObjectStore};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{ChunkReader, RegionId};
use table::predicate::Predicate;

use crate::chunk::ChunkReaderImpl;
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

// We only has fixed number of level, so we use array to hold elements. This implementation
// detail of LevelMetaVec should not be exposed to the user of [LevelMetas].
type LevelMetaVec = [LevelMeta; MAX_LEVEL as usize];

/// Metadata of all SSTs under a region.
///
/// Files are organized into multiple level, though there may be only one level.
#[derive(Debug, Clone)]
pub struct LevelMetas {
    levels: LevelMetaVec,
    sst_layer: AccessLayerRef,
    file_purger: FilePurgerRef,
}

impl LevelMetas {
    /// Create a new LevelMetas and initialized each level.
    pub fn new(sst_layer: AccessLayerRef, file_purger: FilePurgerRef) -> LevelMetas {
        LevelMetas {
            levels: new_level_meta_vec(),
            sst_layer,
            file_purger,
        }
    }

    /// Returns total level number.
    #[inline]
    pub fn level_num(&self) -> usize {
        self.levels.len()
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
    ) -> LevelMetas {
        let mut merged = self.clone();
        for file in files_to_add {
            let level = file.level;
            let handle = FileHandle::new(file, self.sst_layer.clone(), self.file_purger.clone());
            merged.levels[level as usize].add_file(handle);
        }

        for file in files_to_remove {
            let level = file.level;
            if let Some(removed_file) = merged.levels[level as usize].remove_file(&file.file_name) {
                removed_file.mark_deleted();
            }
        }
        merged
    }

    pub fn levels(&self) -> &[LevelMeta] {
        &self.levels
    }
}

/// Metadata of files in same SST level.
#[derive(Debug, Default, Clone)]
pub struct LevelMeta {
    level: Level,
    /// Handles to the files in this level.
    // TODO(yingwen): Now for simplicity, files are unordered, maybe sort the files by time range
    // or use another structure to hold them.
    files: HashMap<String, FileHandle>,
}

impl LevelMeta {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            files: HashMap::new(),
        }
    }

    fn add_file(&mut self, file: FileHandle) {
        self.files.insert(file.file_name().to_string(), file);
    }

    fn remove_file(&mut self, file_to_remove: &str) -> Option<FileHandle> {
        self.files.remove(file_to_remove)
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

/// In-memory handle to a file.
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
    pub fn file_name(&self) -> &str {
        &self.inner.meta.file_name
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
#[derive(Debug)]
struct FileHandleInner {
    meta: FileMeta,
    compacting: AtomicBool,
    deleted: AtomicBool,
    sst_layer: AccessLayerRef,
    file_purger: FilePurgerRef,
}

impl Drop for FileHandleInner {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Relaxed) {
            let request = FilePurgeRequest {
                sst_layer: self.sst_layer.clone(),
                file_name: self.meta.file_name.clone(),
                region_id: self.meta.region_id,
            };
            match self.file_purger.schedule(request) {
                Ok(res) => {
                    info!(
                        "Scheduled SST purge task, region: {}, name: {}, res: {}",
                        self.meta.region_id, self.meta.file_name, res
                    );
                }
                Err(e) => {
                    error!(e; "Failed to schedule SST purge task, region: {}, name: {}", self.meta.region_id, self.meta.file_name);
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

/// Immutable metadata of a sst file.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileMeta {
    /// Region of file.
    pub region_id: RegionId,
    /// File name
    pub file_name: String,
    /// Timestamp range of file.
    pub time_range: Option<(Timestamp, Timestamp)>,
    /// SST level of the file.
    pub level: Level,
    /// Size of the file.
    pub file_size: u64,
}

#[derive(Debug, Default)]
pub struct WriteOptions {
    // TODO(yingwen): [flush] row group size.
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
}

/// SST access layer.
#[async_trait]
pub trait AccessLayer: Send + Sync + std::fmt::Debug {
    /// Writes SST file with given `file_name`.
    async fn write_sst(
        &self,
        file_name: &str,
        source: Source,
        opts: &WriteOptions,
    ) -> Result<SstInfo>;

    /// Read SST file with given `file_name` and schema.
    async fn read_sst(&self, file_name: &str, opts: &ReadOptions) -> Result<BoxedBatchReader>;

    /// Deletes a SST file with given name.
    async fn delete_sst(&self, file_name: &str) -> Result<()>;
}

pub type AccessLayerRef = Arc<dyn AccessLayer>;

/// Parquet writer data source.
pub enum Source {
    /// Writes rows from memtable to parquet
    Iter(BoxedBatchIterator),
    /// Writes row from ChunkReaderImpl (maybe a set of SSTs) to parquet.
    Reader(ChunkReaderImpl),
}

impl Source {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Source::Iter(iter) => iter.next().transpose(),
            Source::Reader(reader) => reader
                .next_chunk()
                .await
                .map(|p| p.map(|chunk| Batch::new(chunk.columns))),
        }
    }

    fn projected_schema(&self) -> ProjectedSchemaRef {
        match self {
            Source::Iter(iter) => iter.schema(),
            Source::Reader(reader) => reader.projected_schema().clone(),
        }
    }
}

/// Sst access layer based on local file system.
#[derive(Debug)]
pub struct FsAccessLayer {
    sst_dir: String,
    object_store: ObjectStore,
}

impl FsAccessLayer {
    pub fn new(sst_dir: &str, object_store: ObjectStore) -> FsAccessLayer {
        FsAccessLayer {
            sst_dir: util::normalize_dir(sst_dir),
            object_store,
        }
    }

    #[inline]
    fn sst_file_path(&self, file_name: &str) -> String {
        format!("{}{}", self.sst_dir, file_name)
    }
}

#[async_trait]
impl AccessLayer for FsAccessLayer {
    async fn write_sst(
        &self,
        file_name: &str,
        source: Source,
        opts: &WriteOptions,
    ) -> Result<SstInfo> {
        // Now we only supports parquet format. We may allow caller to specific SST format in
        // WriteOptions in the future.
        let file_path = self.sst_file_path(file_name);
        let writer = ParquetWriter::new(&file_path, source, self.object_store.clone());
        writer.write_sst(opts).await
    }

    async fn read_sst(&self, file_name: &str, opts: &ReadOptions) -> Result<BoxedBatchReader> {
        let file_path = self.sst_file_path(file_name);
        let reader = ParquetReader::new(
            &file_path,
            self.object_store.clone(),
            opts.projected_schema.clone(),
            opts.predicate.clone(),
            opts.time_range,
        );

        let stream = reader.chunk_stream().await?;
        Ok(Box::new(stream))
    }

    async fn delete_sst(&self, file_name: &str) -> Result<()> {
        let path = self.sst_file_path(file_name);
        let object = self.object_store.object(&path);
        object.delete().await.context(DeleteSstSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::file_purger::noop::NoopFilePurgeHandler;
    use crate::scheduler::{LocalScheduler, SchedulerConfig};

    fn create_file_meta(name: &str, level: Level) -> FileMeta {
        FileMeta {
            region_id: 0,
            file_name: name.to_string(),
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
        let metas = LevelMetas::new(layer, purger);
        let merged = metas.merge(
            vec![create_file_meta("a", 0), create_file_meta("b", 0)].into_iter(),
            vec![].into_iter(),
        );

        assert_eq!(
            HashSet::from(["a".to_string(), "b".to_string()]),
            merged
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        let merged1 = merged.merge(
            vec![create_file_meta("c", 1), create_file_meta("d", 1)].into_iter(),
            vec![].into_iter(),
        );
        assert_eq!(
            HashSet::from(["a".to_string(), "b".to_string()]),
            merged1
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        assert_eq!(
            HashSet::from(["c".to_string(), "d".to_string()]),
            merged1
                .level(1)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        let removed1 = merged1.merge(
            vec![].into_iter(),
            vec![create_file_meta("a", 0), create_file_meta("c", 0)].into_iter(),
        );
        assert_eq!(
            HashSet::from(["b".to_string()]),
            removed1
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        assert_eq!(
            HashSet::from(["c".to_string(), "d".to_string()]),
            removed1
                .level(1)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        let removed2 = removed1.merge(
            vec![].into_iter(),
            vec![create_file_meta("c", 1), create_file_meta("d", 1)].into_iter(),
        );
        assert_eq!(
            HashSet::from(["b".to_string()]),
            removed2
                .level(0)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );

        assert_eq!(
            HashSet::new(),
            removed2
                .level(1)
                .files()
                .map(|f| f.file_name().to_string())
                .collect()
        );
    }
}
