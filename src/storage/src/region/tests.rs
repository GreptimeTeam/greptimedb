//! Region tests.

mod basic;
mod flush;
mod projection;

use common_telemetry::logging;
use common_time::timestamp::{TimeUnit, Timestamp};
use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{Int64Vector, TimestampVector};
use log_store::fs::{log::LocalFileLogStore, noop::NoopLogStore};
use object_store::{backend::fs, ObjectStore};
use store_api::storage::{
    consts, Chunk, ChunkReader, PutOperation, ScanRequest, SequenceNumber, Snapshot, WriteRequest,
};
use tempdir::TempDir;

use super::*;
use crate::manifest::action::{RegionChange, RegionMetaActionList};
use crate::manifest::test_utils::*;
use crate::test_util::{
    self, config_util, descriptor_util::RegionDescBuilder, schema_util, write_batch_util,
};
use crate::write_batch::PutData;

/// Create metadata of a region with schema: (timestamp, v1).
pub fn new_metadata(region_name: &str, enable_version_column: bool) -> RegionMetadata {
    let desc = RegionDescBuilder::new(region_name)
        .enable_version_column(enable_version_column)
        .push_value_column(("v1", LogicalTypeId::Int64, true))
        .build();
    desc.try_into().unwrap()
}

/// Test region with schema (timestamp, v1).
pub struct TesterBase<S: LogStore> {
    pub region: RegionImpl<S>,
    write_ctx: WriteContext,
    pub read_ctx: ReadContext,
}

impl<S: LogStore> TesterBase<S> {
    pub fn with_region(region: RegionImpl<S>) -> TesterBase<S> {
        TesterBase {
            region,
            write_ctx: WriteContext::default(),
            read_ctx: ReadContext::default(),
        }
    }

    /// Put without version specified.
    ///
    /// Format of data: (timestamp, v1), timestamp is key, v1 is value.
    pub async fn put(&self, data: &[(Timestamp, Option<i64>)]) -> WriteResponse {
        // Build a batch without version.
        let mut batch = new_write_batch_for_test(false);
        let put_data = new_put_data(data);
        batch.put(put_data).unwrap();

        self.region.write(&self.write_ctx, batch).await.unwrap()
    }

    /// Scan all data.
    pub async fn full_scan(&self) -> Vec<(Timestamp, Option<i64>)> {
        logging::info!("Full scan with ctx {:?}", self.read_ctx);
        let snapshot = self.region.snapshot(&self.read_ctx).unwrap();

        let resp = snapshot
            .scan(&self.read_ctx, ScanRequest::default())
            .await
            .unwrap();
        let mut reader = resp.reader;

        let metadata = self.region.in_memory_metadata();
        assert_eq!(metadata.schema(), reader.schema());

        let mut dst = Vec::new();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            append_chunk_to(&chunk, &mut dst);
        }

        dst
    }

    pub fn committed_sequence(&self) -> SequenceNumber {
        self.region.committed_sequence()
    }
}

pub type FileTesterBase = TesterBase<LocalFileLogStore>;

fn new_write_batch_for_test(enable_version_column: bool) -> WriteBatch {
    if enable_version_column {
        write_batch_util::new_write_batch(
            &[
                (
                    test_util::TIMESTAMP_NAME,
                    LogicalTypeId::Timestamp(TimeUnit::Millisecond),
                    false,
                ),
                (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
                ("v1", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    } else {
        write_batch_util::new_write_batch(
            &[
                (
                    test_util::TIMESTAMP_NAME,
                    LogicalTypeId::Timestamp(TimeUnit::Millisecond),
                    false,
                ),
                ("v1", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    }
}

fn new_put_data(data: &[(Timestamp, Option<i64>)]) -> PutData {
    let mut put_data = PutData::with_num_columns(2);

    let timestamps = TimestampVector::from_vecs(data.iter().map(|v| v.0).collect());
    let values = Int64Vector::from_iter(data.iter().map(|kv| kv.1));

    put_data
        .add_key_column(test_util::TIMESTAMP_NAME, Arc::new(timestamps))
        .unwrap();
    put_data.add_value_column("v1", Arc::new(values)).unwrap();

    put_data
}

fn append_chunk_to(chunk: &Chunk, dst: &mut Vec<(Timestamp, Option<i64>)>) {
    assert_eq!(2, chunk.columns.len());

    let timestamps = chunk.columns[0]
        .as_any()
        .downcast_ref::<TimestampVector>()
        .unwrap();
    let values = chunk.columns[1]
        .as_any()
        .downcast_ref::<Int64Vector>()
        .unwrap();
    for (ts, value) in timestamps.iter_data().zip(values.iter_data()) {
        dst.push((ts.unwrap(), value));
    }
}

#[tokio::test]
async fn test_new_region() {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .enable_version_column(true)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v1", LogicalTypeId::Float32, true))
        .build();
    let metadata = desc.try_into().unwrap();

    let store_dir = TempDir::new("test_new_region")
        .unwrap()
        .path()
        .to_string_lossy()
        .to_string();

    let store_config = config_util::new_store_config(region_name, &store_dir).await;

    let region = RegionImpl::new(Version::new(Arc::new(metadata)), store_config);

    let expect_schema = schema_util::new_schema_ref(
        &[
            ("k1", LogicalTypeId::Int32, false),
            (
                test_util::TIMESTAMP_NAME,
                LogicalTypeId::Timestamp(TimeUnit::Millisecond),
                false,
            ),
            (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
            ("v1", LogicalTypeId::Float32, true),
        ],
        Some(1),
    );

    assert_eq!(region_name, region.name());
    assert_eq!(expect_schema, *region.in_memory_metadata().schema());
}

#[tokio::test]
async fn test_recover_region_manifets() {
    let tmp_dir = TempDir::new("test_new_region").unwrap();

    let object_store = ObjectStore::new(
        fs::Backend::build()
            .root(&tmp_dir.path().to_string_lossy())
            .finish()
            .await
            .unwrap(),
    );

    let manifest = RegionManifest::new("/manifest/", object_store);
    let region_meta = Arc::new(build_region_meta());

    // Recover from empty
    assert!(RegionImpl::<NoopLogStore>::recover_from_manifest(&manifest)
        .await
        .unwrap()
        .is_none());

    {
        // save some actions into region_meta
        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.as_ref().into(),
                },
            )))
            .await
            .unwrap();

        manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Edit(build_region_edit(1, &["f1"], &[])),
                RegionMetaAction::Edit(build_region_edit(2, &["f2", "f3"], &[])),
            ]))
            .await
            .unwrap();
    }

    // try to recover
    let version = RegionImpl::<NoopLogStore>::recover_from_manifest(&manifest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*version.metadata(), region_meta);
    assert_eq!(version.flushed_sequence(), 2);
    assert_eq!(version.manifest_version(), 1);
    let ssts = version.ssts();
    let files = ssts.levels()[0].files();
    assert_eq!(3, files.len());
    for (i, file) in files.iter().enumerate() {
        assert_eq!(format!("f{}", i + 1), file.file_name());
    }
    assert!(version.mutable_memtables().is_empty());

    // check manifest state
    assert_eq!(2, manifest.last_version());
}
