//! Region tests.

mod alter;
mod basic;
mod flush;
mod projection;

use common_telemetry::logging;
use common_time::timestamp::Timestamp;
use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{Int64Vector, TimestampVector};
use log_store::fs::log::LocalFileLogStore;
use log_store::fs::noop::NoopLogStore;
use object_store::backend::fs;
use object_store::ObjectStore;
use store_api::storage::{
    consts, Chunk, ChunkReader, PutOperation, ScanRequest, SequenceNumber, Snapshot, WriteRequest,
};
use tempdir::TempDir;

use super::*;
use crate::manifest::action::{RegionChange, RegionMetaActionList};
use crate::manifest::test_utils::*;
use crate::memtable::DefaultMemtableBuilder;
use crate::test_util::descriptor_util::RegionDescBuilder;
use crate::test_util::{self, config_util, schema_util, write_batch_util};
use crate::write_batch::PutData;

/// Create metadata of a region with schema: (timestamp, v0).
pub fn new_metadata(region_name: &str, enable_version_column: bool) -> RegionMetadata {
    let desc = RegionDescBuilder::new(region_name)
        .enable_version_column(enable_version_column)
        .push_value_column(("v0", LogicalTypeId::Int64, true))
        .build();
    desc.try_into().unwrap()
}

/// Test region with schema (timestamp, v0).
pub struct TesterBase<S: LogStore> {
    pub region: RegionImpl<S>,
    pub write_ctx: WriteContext,
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
    /// Format of data: (timestamp, v0), timestamp is key, v0 is value.
    pub async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        let data: Vec<(Timestamp, Option<i64>)> =
            data.iter().map(|(l, r)| ((*l).into(), *r)).collect();
        // Build a batch without version.
        let mut batch = new_write_batch_for_test(false);
        let put_data = new_put_data(&data);
        batch.put(put_data).unwrap();

        self.region.write(&self.write_ctx, batch).await.unwrap()
    }

    /// Put without version specified directly to inner writer.
    pub async fn put_inner(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        let data: Vec<(Timestamp, Option<i64>)> =
            data.iter().map(|(l, r)| ((*l).into(), *r)).collect();
        let mut batch = new_write_batch_for_test(false);
        let put_data = new_put_data(&data);
        batch.put(put_data).unwrap();

        self.region
            .write_inner(&self.write_ctx, batch)
            .await
            .unwrap()
    }

    pub async fn replay_inner(&self, recovered_metadata: RecoveredMetadataMap) {
        self.region.replay_inner(recovered_metadata).await.unwrap()
    }

    /// Scan all data.
    pub async fn full_scan(&self) -> Vec<(i64, Option<i64>)> {
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
                (test_util::TIMESTAMP_NAME, LogicalTypeId::Timestamp, false),
                (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
                ("v0", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    } else {
        write_batch_util::new_write_batch(
            &[
                (test_util::TIMESTAMP_NAME, LogicalTypeId::Timestamp, false),
                ("v0", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    }
}

fn new_put_data(data: &[(Timestamp, Option<i64>)]) -> PutData {
    let mut put_data = PutData::with_num_columns(2);

    let timestamps = TimestampVector::from_vec(data.iter().map(|v| v.0).collect());
    let values = Int64Vector::from_iter(data.iter().map(|kv| kv.1));

    put_data
        .add_key_column(test_util::TIMESTAMP_NAME, Arc::new(timestamps))
        .unwrap();
    put_data.add_value_column("v0", Arc::new(values)).unwrap();

    put_data
}

fn append_chunk_to(chunk: &Chunk, dst: &mut Vec<(i64, Option<i64>)>) {
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
        dst.push((ts.unwrap().value(), value));
    }
}

#[tokio::test]
async fn test_new_region() {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .enable_version_column(true)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v0", LogicalTypeId::Float32, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();

    let store_dir = TempDir::new("test_new_region")
        .unwrap()
        .path()
        .to_string_lossy()
        .to_string();

    let store_config = config_util::new_store_config(region_name, &store_dir).await;
    let placeholder_memtable = store_config
        .memtable_builder
        .build(metadata.schema().clone());

    let region = RegionImpl::new(
        Version::new(Arc::new(metadata), placeholder_memtable),
        store_config,
    );

    let expect_schema = schema_util::new_schema_ref(
        &[
            ("k1", LogicalTypeId::Int32, false),
            (test_util::TIMESTAMP_NAME, LogicalTypeId::Timestamp, false),
            (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
            ("v0", LogicalTypeId::Float32, true),
        ],
        Some(1),
    );

    assert_eq!(region_name, region.name());
    assert_eq!(expect_schema, *region.in_memory_metadata().schema());
}

#[tokio::test]
async fn test_recover_region_manifets() {
    let tmp_dir = TempDir::new("test_new_region").unwrap();
    let memtable_builder = Arc::new(DefaultMemtableBuilder::default()) as _;

    let object_store = ObjectStore::new(
        fs::Builder::default()
            .root(&tmp_dir.path().to_string_lossy())
            .build()
            .unwrap(),
    );

    let manifest = RegionManifest::new("/manifest/", object_store);
    let region_meta = Arc::new(build_region_meta());

    // Recover from empty
    assert!(
        RegionImpl::<NoopLogStore>::recover_from_manifest(&manifest, &memtable_builder)
            .await
            .unwrap()
            .0
            .is_none()
    );

    {
        // save some actions into region_meta
        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.as_ref().into(),
                    committed_sequence: 40,
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

        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.as_ref().into(),
                    committed_sequence: 42,
                },
            )))
            .await
            .unwrap();
    }

    // try to recover
    let (version, recovered_metadata) =
        RegionImpl::<NoopLogStore>::recover_from_manifest(&manifest, &memtable_builder)
            .await
            .unwrap();

    assert_eq!(42, *recovered_metadata.first_key_value().unwrap().0);
    let version = version.unwrap();
    assert_eq!(*version.metadata(), region_meta);
    assert_eq!(version.flushed_sequence(), 2);
    assert_eq!(version.manifest_version(), 1);
    let ssts = version.ssts();
    let files = ssts.levels()[0].files();
    assert_eq!(3, files.len());
    for (i, file) in files.iter().enumerate() {
        assert_eq!(format!("f{}", i + 1), file.file_name());
    }

    // check manifest state
    assert_eq!(3, manifest.last_version());
}
