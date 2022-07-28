//! Region read/write tests.

use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::Int64Vector;
use log_store::fs::noop::NoopLogStore;
use store_api::storage::{
    consts, Chunk, ChunkReader, PutOperation, ReadContext, Region, RegionMeta, ScanRequest,
    SequenceNumber, Snapshot, WriteContext, WriteRequest, WriteResponse,
};
use tempdir::TempDir;

use crate::region::{RegionImpl, RegionMetadata};
use crate::test_util::{self, config_util, descriptor_util::RegionDescBuilder, write_batch_util};
use crate::write_batch::{PutData, WriteBatch};

pub fn new_metadata(region_name: &str, enable_version_column: bool) -> RegionMetadata {
    let desc = RegionDescBuilder::new(region_name)
        .enable_version_column(enable_version_column)
        .push_value_column(("v1", LogicalTypeId::Int64, true))
        .build();
    desc.try_into().unwrap()
}

/// Create a new region for read/write test
async fn new_region_for_rw(
    store_dir: &str,
    enable_version_column: bool,
) -> RegionImpl<NoopLogStore> {
    let region_name = "region-rw-0";

    let metadata = new_metadata(region_name, enable_version_column);

    let store_config = config_util::new_store_config(store_dir, region_name).await;

    RegionImpl::new(0, region_name.to_string(), metadata, store_config)
}

fn new_write_batch_for_test(enable_version_column: bool) -> WriteBatch {
    if enable_version_column {
        write_batch_util::new_write_batch(
            &[
                (test_util::TIMESTAMP_NAME, LogicalTypeId::Int64, false),
                (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
                ("v1", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    } else {
        write_batch_util::new_write_batch(
            &[
                (test_util::TIMESTAMP_NAME, LogicalTypeId::Int64, false),
                ("v1", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    }
}

fn new_put_data(data: &[(i64, Option<i64>)]) -> PutData {
    let mut put_data = PutData::with_num_columns(2);

    let timestamps = Int64Vector::from_values(data.iter().map(|kv| kv.0));
    let values = Int64Vector::from_iter(data.iter().map(|kv| kv.1));

    put_data
        .add_key_column(test_util::TIMESTAMP_NAME, Arc::new(timestamps))
        .unwrap();
    put_data.add_value_column("v1", Arc::new(values)).unwrap();

    put_data
}

fn append_chunk_to(chunk: &Chunk, dst: &mut Vec<(i64, Option<i64>)>) {
    assert_eq!(2, chunk.columns.len());

    let timestamps = chunk.columns[0]
        .as_any()
        .downcast_ref::<Int64Vector>()
        .unwrap();
    let values = chunk.columns[1]
        .as_any()
        .downcast_ref::<Int64Vector>()
        .unwrap();
    for (ts, value) in timestamps.iter_data().zip(values.iter_data()) {
        dst.push((ts.unwrap(), value));
    }
}

/// Test region without considering version column.
pub struct Tester {
    pub region: RegionImpl<NoopLogStore>,
    write_ctx: WriteContext,
    read_ctx: ReadContext,
}

impl Tester {
    async fn new(store_dir: &str) -> Tester {
        let region = new_region_for_rw(store_dir, false).await;

        Tester::with_region(region)
    }

    pub fn with_region(region: RegionImpl<NoopLogStore>) -> Tester {
        Tester {
            region,
            write_ctx: WriteContext::default(),
            read_ctx: ReadContext::default(),
        }
    }

    /// Put without version specified.
    ///
    /// Format of data: (timestamp, v1), timestamp is key, v1 is value.
    pub async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        // Build a batch without version.
        let mut batch = new_write_batch_for_test(false);
        let put_data = new_put_data(data);
        batch.put(put_data).unwrap();

        self.region.write(&self.write_ctx, batch).await.unwrap()
    }

    /// Scan all data.
    pub async fn full_scan(&self) -> Vec<(i64, Option<i64>)> {
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

    fn committed_sequence(&self) -> SequenceNumber {
        self.region.committed_sequence()
    }
}

#[tokio::test]
async fn test_simple_put_scan() {
    let dir = TempDir::new("write_parquet").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let tester = Tester::new(store_dir).await;

    let data = vec![
        (1000, Some(100)),
        (1001, Some(101)),
        (1002, None),
        (1003, Some(103)),
        (1004, Some(104)),
    ];

    tester.put(&data).await;

    let output = tester.full_scan().await;
    assert_eq!(data, output);
}
#[tokio::test]
async fn test_sequence_increase() {
    let dir = TempDir::new("write_parquet").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let tester = Tester::new(store_dir).await;

    let mut committed_sequence = tester.committed_sequence();
    for i in 0..100 {
        tester.put(&[(i, Some(1234))]).await;
        committed_sequence += 1;

        assert_eq!(committed_sequence, tester.committed_sequence());
    }
}
