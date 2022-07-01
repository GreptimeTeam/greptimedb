use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::memtable::BatchIteratorPtr;

/// Metadata of all ssts under a region.
#[derive(Debug)]
struct LevelMetas {}

/// Metadata of a sst file.
#[derive(Debug)]
pub struct FileMeta {
    pub file_name: String,
}

#[derive(Debug)]
pub struct WriteOptions {
    pub row_group_size: usize,
}

/// Sst access layer.
#[async_trait]
pub trait AccessLayer: Send + Sync {
    async fn write_sst(
        &self,
        file_name: &str,
        iter: &BatchIteratorPtr,
        opts: WriteOptions,
    ) -> Result<()>;
}

pub type AccessLayerRef = Arc<dyn AccessLayer>;

/// Sst access layer based on local file system.
pub struct FsAccessLayer {}

#[async_trait]
impl AccessLayer for FsAccessLayer {
    async fn write_sst(
        &self,
        _file_name: &str,
        _iter: &BatchIteratorPtr,
        _opts: WriteOptions,
    ) -> Result<()> {
        unimplemented!()
    }
}
