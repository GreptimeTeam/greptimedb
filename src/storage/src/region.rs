use std::sync::Arc;

use async_trait::async_trait;
use snafu::ensure;
use store_api::storage::{ReadContext, Region, RegionMeta, WriteContext, WriteResponse};
use tokio::sync::Mutex;

use crate::error::{self, Error, Result};
use crate::memtable::{DefaultMemtableBuilder, MemtableBuilder, MemtableSchema, MemtableSet};
use crate::metadata::{RegionMetaImpl, RegionMetadata};
use crate::region_writer::RegionWriter;
use crate::snapshot::SnapshotImpl;
use crate::version::{VersionControl, VersionControlRef};
use crate::write_batch::WriteBatch;

/// [Region] implementation.
#[derive(Clone)]
pub struct RegionImpl {
    inner: Arc<RegionInner>,
}

#[async_trait]
impl Region for RegionImpl {
    type Error = Error;
    type Meta = RegionMetaImpl;
    type WriteRequest = WriteBatch;
    type Snapshot = SnapshotImpl;

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        self.inner.in_memory_metadata()
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        self.inner.write(ctx, request).await
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<SnapshotImpl> {
        unimplemented!()
    }
}

impl RegionImpl {
    pub fn new(name: String, metadata: RegionMetadata) -> RegionImpl {
        let memtable_builder = Arc::new(DefaultMemtableBuilder {});
        let memtable_schema = MemtableSchema::new(metadata.columns_row_key.clone());
        let mem = memtable_builder.build(memtable_schema);
        let memtables = MemtableSet::new(mem);

        let version = VersionControl::new(metadata, memtables);
        let inner = Arc::new(RegionInner {
            name,
            version: Arc::new(version),
            writer: Mutex::new(RegionWriter::new(memtable_builder)),
        });

        RegionImpl { inner }
    }
}

struct RegionInner {
    name: String,
    version: VersionControlRef,
    writer: Mutex<RegionWriter>,
}

impl RegionInner {
    fn in_memory_metadata(&self) -> RegionMetaImpl {
        let metadata = self.version.metadata();

        RegionMetaImpl::new(metadata)
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        let metadata = self.in_memory_metadata();
        let schema = metadata.schema();
        // Only compare column schemas.
        ensure!(
            schema.column_schemas() == request.schema().column_schemas(),
            error::InvalidInputSchemaSnafu { region: &self.name }
        );

        // Now altering schema is not allowed, so it is safe to validate schema outside of the lock.
        let mut writer = self.writer.lock().await;
        writer.write(ctx, &self.version, request).await
    }
}

#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;
    use store_api::storage::consts;

    use super::*;
    use crate::test_util::descriptor_util::RegionDescBuilder;
    use crate::test_util::schema_util;

    #[test]
    fn test_new_region() {
        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_value_column(("v1", LogicalTypeId::Float32, true))
            .build();
        let metadata = desc.try_into().unwrap();

        let region = RegionImpl::new(region_name.to_string(), metadata);

        let expect_schema = schema_util::new_schema_ref(&[
            ("k1", LogicalTypeId::Int32, false),
            ("timestamp", LogicalTypeId::UInt64, false),
            (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
            ("v1", LogicalTypeId::Float32, true),
        ]);

        assert_eq!(region_name, region.name());
        assert_eq!(expect_schema, *region.in_memory_metadata().schema());
    }
}
