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

use std::collections::HashMap;
use std::sync::Arc;

use common_test_util::temp_dir::create_temp_dir;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, VectorRef};
use log_store::raft_engine::log_store::RaftEngineLogStore;
use store_api::logstore::LogStore;
use store_api::storage::{
    Chunk, ChunkReader, ReadContext, Region, ScanRequest, Snapshot, WriteContext, WriteRequest,
};

use crate::region::{RegionImpl, RegionMetadata};
use crate::test_util::{self, config_util, descriptor_util, write_batch_util};
use crate::write_batch::WriteBatch;

/// Create metadata with schema (k0, timestamp, v0, v1)
fn new_metadata(region_name: &str) -> RegionMetadata {
    let desc = descriptor_util::desc_with_field_columns(region_name, 2);
    desc.try_into().unwrap()
}

fn new_write_batch_for_test() -> WriteBatch {
    write_batch_util::new_write_batch(
        &[
            ("k0", LogicalTypeId::Int64, false),
            (
                test_util::TIMESTAMP_NAME,
                LogicalTypeId::TimestampMillisecond,
                false,
            ),
            ("v0", LogicalTypeId::Int64, true),
            ("v1", LogicalTypeId::Int64, true),
        ],
        Some(1),
        2,
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
fn new_put_data(
    len: usize,
    key_start: i64,
    ts_start: i64,
    initial_value: i64,
) -> HashMap<String, VectorRef> {
    let mut put_data = HashMap::with_capacity(4);

    let k0 = Arc::new(Int64Vector::from_values(
        (0..len).map(|v| key_start + v as i64),
    )) as VectorRef;
    let ts = Arc::new(TimestampMillisecondVector::from_values(
        (0..len).map(|v| ts_start + v as i64),
    )) as VectorRef;
    let v0 = Arc::new(Int64Vector::from_values(
        std::iter::repeat(initial_value).take(len),
    )) as VectorRef;
    let v1 = Arc::new(Int64Vector::from_values(
        (0..len).map(|v| initial_value + v as i64),
    )) as VectorRef;

    put_data.insert("k0".to_string(), k0);
    put_data.insert(test_util::TIMESTAMP_NAME.to_string(), ts);
    put_data.insert("v0".to_string(), v0);
    put_data.insert("v1".to_string(), v1);

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
            match col.data_type() {
                ConcreteDataType::Int64(_) => {
                    let val = col
                        .as_any()
                        .downcast_ref::<Int64Vector>()
                        .unwrap()
                        .get_data(i)
                        .unwrap();
                    row.push(val);
                }
                ConcreteDataType::Timestamp(_) => {
                    let val = col
                        .as_any()
                        .downcast_ref::<TimestampMillisecondVector>()
                        .unwrap()
                        .get_data(i)
                        .unwrap();
                    row.push(val.into());
                }
                _ => unreachable!(),
            }
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
        let resp = snapshot.scan(&self.read_ctx, request, None).await.unwrap();
        let mut reader = resp.reader;

        let mut dst = Vec::new();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            let chunk = reader.project_chunk(chunk);
            append_chunk_to(&chunk, &mut dst);
        }

        dst
    }
}

const REGION_NAME: &str = "region-projection-0";

async fn new_tester(store_dir: &str) -> ProjectionTester<RaftEngineLogStore> {
    let metadata = new_metadata(REGION_NAME);

    let store_config = config_util::new_store_config(REGION_NAME, store_dir).await;
    let region = RegionImpl::create(metadata, store_config).await.unwrap();

    ProjectionTester::with_region(region)
}

#[tokio::test]
async fn test_projection_ordered() {
    let dir = create_temp_dir("projection-ordered");
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
    let dir = create_temp_dir("projection-unordered");
    let store_dir = dir.path().to_str().unwrap();

    let tester = new_tester(store_dir).await;
    tester.put(4, 1, 10, 100).await;

    // v1, k0
    let output = tester.scan(Some(vec![3, 0])).await;
    let expect = vec![vec![100, 1], vec![101, 2], vec![102, 3], vec![103, 4]];
    assert_eq!(expect, output);
}
