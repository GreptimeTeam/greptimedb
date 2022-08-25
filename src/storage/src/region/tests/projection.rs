use std::sync::Arc;

use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::Int64Vector;
use log_store::fs::log::LocalFileLogStore;
use store_api::logstore::LogStore;
use store_api::storage::{
    Chunk, ChunkReader, PutOperation, ReadContext, Region, ScanRequest, Snapshot, WriteContext,
    WriteRequest,
};
use tempdir::TempDir;

use crate::region::RegionImpl;
use crate::region::RegionMetadata;
use crate::test_util::{self, config_util, descriptor_util, write_batch_util};
use crate::write_batch::{PutData, WriteBatch};

/// Create metadata with schema (k0, timestamp, v0, v1)
fn new_metadata(region_name: &str) -> RegionMetadata {
    let desc = descriptor_util::desc_with_value_columns(region_name, 2);
    desc.try_into().unwrap()
}

fn new_write_batch_for_test() -> WriteBatch {
    write_batch_util::new_write_batch(
        &[
            ("k0", LogicalTypeId::Int64, false),
            (test_util::TIMESTAMP_NAME, LogicalTypeId::Int64, false),
            ("v0", LogicalTypeId::Int64, true),
            ("v1", LogicalTypeId::Int64, true),
        ],
        Some(1),
    )
}

/// Build put data
///
/// ```text
/// k0: [key_start, key_start + 1, ... key_start + len - 1]
/// timestamp: [ts_start, ts_start + 1, ... ts_start + len - 1]
/// v0: [initial_value, ...., initial_value]
/// v1: [initial_value, ..., initial_value + len - 1]
/// ```
fn new_put_data(len: usize, key_start: i64, ts_start: i64, initial_value: i64) -> PutData {
    let mut put_data = PutData::with_num_columns(4);

    let k0 = Int64Vector::from_values((0..len).map(|v| key_start + v as i64));
    let ts = Int64Vector::from_values((0..len).map(|v| ts_start + v as i64));
    let v0 = Int64Vector::from_values(std::iter::repeat(initial_value).take(len));
    let v1 = Int64Vector::from_values((0..len).map(|v| initial_value + v as i64));

    put_data.add_key_column("k0", Arc::new(k0)).unwrap();
    put_data
        .add_key_column(test_util::TIMESTAMP_NAME, Arc::new(ts))
        .unwrap();
    put_data.add_value_column("v0", Arc::new(v0)).unwrap();
    put_data.add_value_column("v1", Arc::new(v1)).unwrap();

    put_data
}

fn append_chunk_to(chunk: &Chunk, dst: &mut Vec<Vec<i64>>) {
    if chunk.columns.is_empty() {
        return;
    }
    let num_rows = chunk.columns[0].len();
    dst.resize(num_rows, Vec::new());
    for (i, row) in dst.iter_mut().enumerate() {
        for col in &chunk.columns {
            let val = col
                .as_any()
                .downcast_ref::<Int64Vector>()
                .unwrap()
                .get_data(i)
                .unwrap();
            row.push(val);
        }
    }
}

struct ProjectionTester<S: LogStore> {
    region: RegionImpl<S>,
    write_ctx: WriteContext,
    read_ctx: ReadContext,
}

impl<S: LogStore> ProjectionTester<S> {
    fn with_region(region: RegionImpl<S>) -> ProjectionTester<S> {
        ProjectionTester {
            region,
            write_ctx: WriteContext::default(),
            read_ctx: ReadContext::default(),
        }
    }

    async fn put(&self, len: usize, key_start: i64, ts_start: i64, initial_value: i64) {
        let mut batch = new_write_batch_for_test();
        let put_data = new_put_data(len, key_start, ts_start, initial_value);
        batch.put(put_data).unwrap();

        self.region.write(&self.write_ctx, batch).await.unwrap();
    }

    async fn scan(&self, projection: Option<Vec<usize>>) -> Vec<Vec<i64>> {
        let snapshot = self.region.snapshot(&self.read_ctx).unwrap();

        let request = ScanRequest {
            projection,
            ..Default::default()
        };
        let resp = snapshot.scan(&self.read_ctx, request).await.unwrap();
        let mut reader = resp.reader;

        let mut dst = Vec::new();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            append_chunk_to(&chunk, &mut dst);
        }

        dst
    }
}

const REGION_NAME: &str = "region-projection-0";

async fn new_tester(store_dir: &str) -> ProjectionTester<LocalFileLogStore> {
    let metadata = new_metadata(REGION_NAME);

    let store_config = config_util::new_store_config(REGION_NAME, store_dir).await;
    let region = RegionImpl::create(metadata, store_config).await.unwrap();

    ProjectionTester::with_region(region)
}

#[tokio::test]
async fn test_projection_ordered() {
    let dir = TempDir::new("projection-ordered").unwrap();
    let store_dir = dir.path().to_str().unwrap();

    let tester = new_tester(store_dir).await;
    tester.put(4, 1, 10, 100).await;

    // timestamp, v1
    let output = tester.scan(Some(vec![1, 3])).await;
    let expect = vec![vec![10, 100], vec![11, 101], vec![12, 102], vec![13, 103]];
    assert_eq!(expect, output);
}

#[tokio::test]
async fn test_projection_unordered() {
    let dir = TempDir::new("projection-unordered").unwrap();
    let store_dir = dir.path().to_str().unwrap();

    let tester = new_tester(store_dir).await;
    tester.put(4, 1, 10, 100).await;

    // v1, k0
    let output = tester.scan(Some(vec![3, 0])).await;
    let expect = vec![vec![100, 1], vec![101, 2], vec![102, 3], vec![103, 4]];
    assert_eq!(expect, output);
}
