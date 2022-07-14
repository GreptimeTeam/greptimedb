mod parquet;

use std::sync::Arc;

use async_trait::async_trait;
use object_store::{util, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::memtable::BatchIteratorPtr;
use crate::sst::parquet::ParquetWriter;

/// Maximum level of ssts.
pub const MAX_LEVEL: usize = 1;

// We only has fixed number of level, so we array to hold elements. This implement
// detail of LevelMetaVec should not be exposed to the user of [LevelMetas].
type LevelMetaVec = [LevelMeta; MAX_LEVEL];

/// Metadata of all ssts under a region.
///
/// Files are organized into multiple level, though there may be only one level.
#[derive(Debug, Clone)]
pub struct LevelMetas {
    levels: LevelMetaVec,
}

impl LevelMetas {
    /// Create a new LevelMetas and initialized each level.
    pub fn new() -> LevelMetas {
        LevelMetas {
            levels: [LevelMeta::default(); MAX_LEVEL],
        }
    }

    /// Merge `self` with files to add/remove to create a new [LevelMetas].
    ///
    /// # Panics
    /// Panics if level of [FileHandle] is greater than [MAX_LEVEL].
    pub fn merge(&self, files_to_add: impl Iterator<Item = FileHandle>) -> LevelMetas {
        let mut merged = self.clone();
        for file in files_to_add {
            let level = file.level_index();

            merged.levels[level].add_file(file);
        }

        // TODO(yingwen): Support file removal.

        merged
    }
}

impl Default for LevelMetas {
    fn default() -> LevelMetas {
        LevelMetas::new()
    }
}

/// Metadata of files in same sst level.
#[derive(Debug, Default, Clone)]
pub struct LevelMeta {
    /// Handles to the files in this level.
    // TODO(yingwen): Now for simplicity, files are unordered, maybe sort the files by time range
    // or use another structure to hold them.
    files: Vec<FileHandle>,
}

impl LevelMeta {
    fn add_file(&mut self, file: FileHandle) {
        self.files.push(file);
    }
}

/// In-memory handle to a file.
#[derive(Debug, Clone)]
pub struct FileHandle {
    inner: Arc<FileHandleInner>,
}

impl FileHandle {
    pub fn new(meta: FileMeta) -> FileHandle {
        FileHandle {
            inner: Arc::new(FileHandleInner::new(meta)),
        }
    }

    /// Returns level as usize so it can be used as index.
    #[inline]
    pub fn level_index(&self) -> usize {
        self.inner.meta.level.into()
    }
}

/// Actually data of [FileHandle].
///
/// Contains meta of the file, and other mutable info like metrics.
#[derive(Debug)]
struct FileHandleInner {
    meta: FileMeta,
}

impl FileHandleInner {
    fn new(meta: FileMeta) -> FileHandleInner {
        FileHandleInner { meta }
    }
}

/// Immutable metadata of a sst file.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileMeta {
    pub file_path: String,
    /// SST level of the file.
    pub level: u8,
}

#[derive(Debug, Default)]
pub struct WriteOptions {
    // TODO(yingwen): [flush] row group size.
}

/// Sst access layer.
#[async_trait]
pub trait AccessLayer: Send + Sync {
    // Writes SST file with given name and returns the full path.
    async fn write_sst(
        &self,
        file_name: &str,
        iter: BatchIteratorPtr,
        opts: WriteOptions,
    ) -> Result<String>;
}

pub type AccessLayerRef = Arc<dyn AccessLayer>;

/// Sst access layer based on local file system.
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
        iter: BatchIteratorPtr,
        opts: WriteOptions,
    ) -> Result<String> {
        // Now we only supports parquet format. We may allow caller to specific sst format in
        // WriteOptions in the future.
        let file_path = self.sst_file_path(file_name);
        let writer = ParquetWriter::new(&file_path, iter, self.object_store.clone());

        writer.write_sst(opts).await?;
        Ok(file_path)
    }
}
