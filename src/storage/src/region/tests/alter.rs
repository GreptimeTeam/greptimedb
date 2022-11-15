// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;

use common_time::Timestamp;
use datatypes::prelude::*;
use datatypes::vectors::{Int64Vector, TimestampVector};
use log_store::fs::log::LocalFileLogStore;
use store_api::storage::{
    AddColumn, AlterOperation, AlterRequest, Chunk, ChunkReader, ColumnDescriptor,
    ColumnDescriptorBuilder, ColumnId, PutOperation, Region, RegionMeta, ScanRequest, SchemaRef,
    Snapshot, WriteRequest, WriteResponse,
};
use tempdir::TempDir;

use crate::region::tests::{self, FileTesterBase};
use crate::region::{OpenOptions, RawRegionMetadata, RegionImpl, RegionMetadata};
use crate::test_util;
use crate::test_util::config_util;
use crate::test_util::descriptor_util::RegionDescBuilder;
use crate::write_batch::PutData;

const REGION_NAME: &str = "region-alter-0";

async fn create_region_for_alter(store_dir: &str) -> RegionImpl<LocalFileLogStore> {
    // Always disable version column in this test.
    let metadata = tests::new_metadata(REGION_NAME, false);

    let store_config = config_util::new_store_config(REGION_NAME, store_dir).await;

    RegionImpl::create(metadata, store_config).await.unwrap()
}

/// Tester for region alter.
struct AlterTester {
    store_dir: String,
    base: Option<FileTesterBase>,
}

#[derive(Debug, Clone, PartialEq)]
struct DataRow {
    key: Option<i64>,
    ts: Timestamp,
    v0: Option<i64>,
    v1: Option<i64>,
}

impl DataRow {
    fn new(key: Option<i64>, ts: i64, v0: Option<i64>, v1: Option<i64>) -> Self {
        DataRow {
            key,
            ts: ts.into(),
            v0,
            v1,
        }
    }
}

fn new_put_data(data: &[DataRow]) -> PutData {
    let mut put_data = PutData::with_num_columns(4);

    let keys = Int64Vector::from_iter(data.iter().map(|v| v.key));
    let timestamps = TimestampVector::from_vec(data.iter().map(|v| v.ts).collect());
    let values1 = Int64Vector::from_iter(data.iter().map(|kv| kv.v0));
    let values2 = Int64Vector::from_iter(data.iter().map(|kv| kv.v1));

    put_data.add_key_column("k0", Arc::new(keys)).unwrap();
    put_data
        .add_key_column(test_util::TIMESTAMP_NAME, Arc::new(timestamps))
        .unwrap();

    put_data.add_value_column("v0", Arc::new(values1)).unwrap();
    put_data.add_value_column("v1", Arc::new(values2)).unwrap();

    put_data
}

impl AlterTester {
    async fn new(store_dir: &str) -> AlterTester {
        let region = create_region_for_alter(store_dir).await;

        AlterTester {
            base: Some(FileTesterBase::with_region(region)),
            store_dir: store_dir.to_string(),
        }
    }

    async fn reopen(&mut self) {
        // Close the old region.
        self.base = None;
        // Reopen the region.
        let store_config = config_util::new_store_config(REGION_NAME, &self.store_dir).await;
        let opts = OpenOptions::default();
        let region = RegionImpl::open(REGION_NAME.to_string(), store_config, &opts)
            .await
            .unwrap()
            .unwrap();
        self.base = Some(FileTesterBase::with_region(region));
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    fn schema(&self) -> SchemaRef {
        let metadata = self.base().region.in_memory_metadata();
        metadata.schema().clone()
    }

    // Put with schema k0, ts, v0, v1
    async fn put(&self, data: &[DataRow]) -> WriteResponse {
        let mut batch = self.base().region.write_request();
        let put_data = new_put_data(data);
        batch.put(put_data).unwrap();

        self.base()
            .region
            .write(&self.base().write_ctx, batch)
            .await
            .unwrap()
    }

    /// Put data with initial schema.
    async fn put_with_init_schema(&self, data: &[(i64, Option<i64>)]) {
        // put of FileTesterBase always use initial schema version.
        self.base().put(data).await;
    }

    /// Put data to inner writer with initial schema.
    async fn put_inner_with_init_schema(&self, data: &[(i64, Option<i64>)]) {
        // put of FileTesterBase always use initial schema version.
        self.base().put_inner(data).await;
    }

    async fn alter(&self, mut req: AlterRequest) {
        let version = self.version();
        req.version = version;

        self.base().region.alter(req).await.unwrap();
    }

    fn version(&self) -> u32 {
        let metadata = self.base().region.in_memory_metadata();
        metadata.version()
    }

    async fn full_scan_with_init_schema(&self) -> Vec<(i64, Option<i64>)> {
        self.base().full_scan().await
    }

    async fn full_scan(&self) -> Vec<DataRow> {
        let read_ctx = &self.base().read_ctx;
        let snapshot = self.base().region.snapshot(read_ctx).unwrap();

        let resp = snapshot
            .scan(read_ctx, ScanRequest::default())
            .await
            .unwrap();
        let mut reader = resp.reader;

        let metadata = self.base().region.in_memory_metadata();
        assert_eq!(metadata.schema(), reader.schema());

        let mut dst = Vec::new();
        while let Some(chunk) = reader.next_chunk().await.unwrap() {
            append_chunk_to(&chunk, &mut dst);
        }

        dst
    }
}

fn append_chunk_to(chunk: &Chunk, dst: &mut Vec<DataRow>) {
    assert_eq!(4, chunk.columns.len());

    let k0_vector = chunk.columns[0]
        .as_any()
        .downcast_ref::<Int64Vector>()
        .unwrap();
    let ts_vector = chunk.columns[1]
        .as_any()
        .downcast_ref::<TimestampVector>()
        .unwrap();
    let v0_vector = chunk.columns[2]
        .as_any()
        .downcast_ref::<Int64Vector>()
        .unwrap();
    let v1_vector = chunk.columns[3]
        .as_any()
        .downcast_ref::<Int64Vector>()
        .unwrap();
    for i in 0..k0_vector.len() {
        dst.push(DataRow::new(
            k0_vector.get_data(i),
            ts_vector.get_data(i).unwrap().value(),
            v0_vector.get_data(i),
            v1_vector.get_data(i),
        ));
    }
}

fn new_column_desc(id: ColumnId, name: &str) -> ColumnDescriptor {
    ColumnDescriptorBuilder::new(id, name, ConcreteDataType::int64_datatype())
        .is_nullable(true)
        .build()
        .unwrap()
}

fn add_column_req(desc_and_is_key: &[(ColumnDescriptor, bool)]) -> AlterRequest {
    let columns = desc_and_is_key
        .iter()
        .map(|(desc, is_key)| AddColumn {
            desc: desc.clone(),
            is_key: *is_key,
        })
        .collect();
    let operation = AlterOperation::AddColumns { columns };

    AlterRequest {
        operation,
        version: 0,
    }
}

fn drop_column_req(names: &[&str]) -> AlterRequest {
    let names = names.iter().map(|s| s.to_string()).collect();
    let operation = AlterOperation::DropColumns { names };

    AlterRequest {
        operation,
        version: 0,
    }
}

fn check_schema_names(schema: &SchemaRef, names: &[&str]) {
    assert_eq!(names.len(), schema.num_columns());

    for (idx, name) in names.iter().enumerate() {
        assert_eq!(*name, schema.column_name_by_index(idx));
        assert!(schema.column_schema_by_name(name).is_some());
    }
}

#[tokio::test]
async fn test_alter_region_with_reopen() {
    common_telemetry::init_default_ut_logging();

    let dir = TempDir::new("alter-region").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = AlterTester::new(store_dir).await;

    let data = vec![(1000, Some(100)), (1001, Some(101)), (1002, Some(102))];
    tester.put_with_init_schema(&data).await;
    assert_eq!(3, tester.full_scan_with_init_schema().await.len());

    let req = add_column_req(&[
        (new_column_desc(4, "k0"), true),  // key column k0
        (new_column_desc(5, "v1"), false), // value column v1
    ]);
    tester.alter(req).await;

    let schema = tester.schema();
    check_schema_names(&schema, &["k0", "timestamp", "v0", "v1"]);

    // Put data after schema altered.
    let data = vec![
        DataRow::new(Some(10000), 1003, Some(103), Some(201)),
        DataRow::new(Some(10001), 1004, Some(104), Some(202)),
        DataRow::new(Some(10002), 1005, Some(105), Some(203)),
    ];
    tester.put(&data).await;

    // Scan with new schema before reopen.
    let mut expect = vec![
        DataRow::new(None, 1000, Some(100), None),
        DataRow::new(None, 1001, Some(101), None),
        DataRow::new(None, 1002, Some(102), None),
    ];
    expect.extend_from_slice(&data);
    let scanned = tester.full_scan().await;
    assert_eq!(expect, scanned);

    // Reopen and put more data.
    tester.reopen().await;
    let data = vec![
        DataRow::new(Some(10003), 1006, Some(106), Some(204)),
        DataRow::new(Some(10004), 1007, Some(107), Some(205)),
        DataRow::new(Some(10005), 1008, Some(108), Some(206)),
    ];
    tester.put(&data).await;
    // Extend expected result.
    expect.extend_from_slice(&data);

    // add columns,then remove them without writing data.
    let req = add_column_req(&[
        (new_column_desc(6, "v2"), false), // key column k0
        (new_column_desc(7, "v3"), false), // value column v1
    ]);
    tester.alter(req).await;

    let req = drop_column_req(&["v2", "v3"]);
    tester.alter(req).await;

    // reopen and write again
    tester.reopen().await;
    let schema = tester.schema();
    check_schema_names(&schema, &["k0", "timestamp", "v0", "v1"]);

    let data = vec![DataRow::new(Some(10006), 1009, Some(109), Some(207))];
    tester.put(&data).await;
    expect.extend_from_slice(&data);

    // Scan with new schema after reopen and write.
    let scanned = tester.full_scan().await;
    assert_eq!(expect, scanned);
}

#[tokio::test]
async fn test_alter_region() {
    let dir = TempDir::new("alter-region").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let tester = AlterTester::new(store_dir).await;

    let data = vec![(1000, Some(100)), (1001, Some(101)), (1002, Some(102))];

    tester.put_with_init_schema(&data).await;

    let schema = tester.schema();
    check_schema_names(&schema, &["timestamp", "v0"]);

    let req = add_column_req(&[
        (new_column_desc(4, "k0"), true),  // key column k0
        (new_column_desc(5, "v1"), false), // value column v1
    ]);
    tester.alter(req).await;

    let schema = tester.schema();
    check_schema_names(&schema, &["k0", "timestamp", "v0", "v1"]);

    let req = add_column_req(&[
        (new_column_desc(6, "v2"), false),
        (new_column_desc(7, "v3"), false),
    ]);
    tester.alter(req).await;

    let schema = tester.schema();
    check_schema_names(&schema, &["k0", "timestamp", "v0", "v1", "v2", "v3"]);

    // Remove v0, v1
    let req = drop_column_req(&["v0", "v1"]);
    tester.alter(req).await;

    let schema = tester.schema();
    check_schema_names(&schema, &["k0", "timestamp", "v2", "v3"]);
}

#[tokio::test]
async fn test_put_old_schema_after_alter() {
    let dir = TempDir::new("put-old").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let tester = AlterTester::new(store_dir).await;

    let data = vec![(1000, Some(100)), (1001, Some(101)), (1002, Some(102))];

    tester.put_with_init_schema(&data).await;

    let req = add_column_req(&[
        (new_column_desc(4, "k0"), true),  // key column k0
        (new_column_desc(5, "v1"), false), // value column v1
    ]);
    tester.alter(req).await;

    // Put with old schema.
    let data = vec![(1005, Some(105)), (1006, Some(106))];
    tester.put_with_init_schema(&data).await;

    // Put data with old schema directly to the inner writer, to check that the region
    // writer could compat the schema of write batch.
    let data = vec![(1003, Some(103)), (1004, Some(104))];
    tester.put_inner_with_init_schema(&data).await;

    let expect = vec![
        DataRow::new(None, 1000, Some(100), None),
        DataRow::new(None, 1001, Some(101), None),
        DataRow::new(None, 1002, Some(102), None),
        DataRow::new(None, 1003, Some(103), None),
        DataRow::new(None, 1004, Some(104), None),
        DataRow::new(None, 1005, Some(105), None),
        DataRow::new(None, 1006, Some(106), None),
    ];
    let scanned = tester.full_scan().await;
    assert_eq!(expect, scanned);
}

#[tokio::test]
async fn test_replay_metadata_after_open() {
    let dir = TempDir::new("replay-metadata-after-open").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = AlterTester::new(store_dir).await;

    let data = vec![(1000, Some(100)), (1001, Some(101)), (1002, Some(102))];

    tester.put_with_init_schema(&data).await;

    tester.reopen().await;

    let committed_sequence = tester.base().committed_sequence();
    let manifest_version = tester.base().region.current_manifest_version();
    let version = tester.version();

    let mut recovered_metadata = BTreeMap::new();

    let desc = RegionDescBuilder::new(REGION_NAME)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v0", LogicalTypeId::Float32, true))
        .build();
    let metadata: &RegionMetadata = &desc.try_into().unwrap();
    let mut raw_metadata: RawRegionMetadata = metadata.into();
    raw_metadata.version = version + 1;
    recovered_metadata.insert(committed_sequence, (manifest_version + 1, raw_metadata));
    tester.base().replay_inner(recovered_metadata).await;
    let schema = tester.schema();
    check_schema_names(&schema, &["k1", "timestamp", "v0"]);
}
