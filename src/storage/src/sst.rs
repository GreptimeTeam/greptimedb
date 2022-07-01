mod parquet;

use std::sync::Arc;

use async_trait::async_trait;
use object_store::ObjectStore;

use crate::error::Result;
use crate::memtable::BatchIteratorPtr;
use crate::sst::parquet::ParquetWriter;

/// Metadata of all ssts under a region.
#[derive(Debug)]
struct LevelMetas {}

/// Metadata of a sst file.
#[derive(Debug)]
pub struct FileMeta {
    pub file_name: String,
}

#[derive(Debug, Default)]
pub struct WriteOptions {
    // TODO(yingwen): [flush] row group size.
}

/// Sst access layer.
#[async_trait]
pub trait AccessLayer: Send + Sync {
    async fn write_sst(
        &self,
        file_name: &str,
        iter: BatchIteratorPtr,
        opts: WriteOptions,
    ) -> Result<()>;
}

pub type AccessLayerRef = Arc<dyn AccessLayer>;

/// Sst access layer based on local file system.
pub struct FsAccessLayer {
    object_store: ObjectStore,
}

impl FsAccessLayer {
    pub fn new(object_store: ObjectStore) -> FsAccessLayer {
        FsAccessLayer { object_store }
    }
}

#[async_trait]
impl AccessLayer for FsAccessLayer {
    async fn write_sst(
        &self,
        file_name: &str,
        iter: BatchIteratorPtr,
        opts: WriteOptions,
    ) -> Result<()> {
        // Now we only supports parquet format. We may allow caller to specific sst format in
        // WriteOptions in the future.
        let writer = ParquetWriter::new(file_name, iter, self.object_store.clone());

        writer.write_sst(opts).await
    }
}
