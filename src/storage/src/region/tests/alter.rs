use std::sync::Arc;

use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::Int64Vector;
use datatypes::vectors::TimestampVector;
use log_store::fs::log::LocalFileLogStore;
use store_api::storage::PutOperation;
use store_api::storage::WriteRequest;
use store_api::storage::{
    AddColumn, AlterOperation, AlterRequest, ColumnDescriptor, ColumnDescriptorBuilder, ColumnId,
    Region, RegionMeta, SchemaRef, WriteResponse,
};
use tempdir::TempDir;

use crate::region::tests::{self, FileTesterBase};
use crate::region::OpenOptions;
use crate::region::RegionImpl;
use crate::test_util::config_util;
use crate::test_util::{self, write_batch_util};
use crate::write_batch::PutData;
use crate::write_batch::WriteBatch;

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

fn new_write_batch_for_test() -> WriteBatch {
    write_batch_util::new_write_batch(
        &[
            ("k0", LogicalTypeId::Int64, true),
            (test_util::TIMESTAMP_NAME, LogicalTypeId::Timestamp, false),
            ("v0", LogicalTypeId::Int64, true),
            ("v1", LogicalTypeId::Int64, true),
        ],
        Some(1),
    )
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

    async fn put(&self, data: &[DataRow]) -> WriteResponse {
        let mut batch = new_write_batch_for_test();
        let put_data = new_put_data(data);
        batch.put(put_data).unwrap();

        self.base()
            .region
            .write(&self.base().write_ctx, batch)
            .await
            .unwrap()
    }

    /// Put data with initial schema.
    async fn put_before_alter(&self, data: &[(i64, Option<i64>)]) {
        self.base().put(data).await;
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

    async fn full_scan(&self) -> Vec<(i64, Option<i64>)> {
        self.base().full_scan().await
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

    tester.put_before_alter(&data).await;
    assert_eq!(3, tester.full_scan().await.len());

    let schema = tester.schema();
    check_schema_names(&schema, &["timestamp", "v0"]);

    let req = add_column_req(&[
        (new_column_desc(4, "k0"), true),  // key column k0
        (new_column_desc(5, "v1"), false), // value column v1
    ]);
    tester.alter(req).await;

    let schema = tester.schema();
    check_schema_names(&schema, &["k0", "timestamp", "v0", "v1"]);

    let data = vec![
        DataRow::new(Some(10000), 1003, Some(103), Some(201)),
        DataRow::new(Some(10001), 1004, Some(104), Some(202)),
        DataRow::new(Some(10002), 1005, Some(105), Some(203)),
    ];
    tester.put(&data).await;

    tester.reopen().await;
    let data = vec![
        DataRow::new(Some(10003), 1006, Some(106), Some(204)),
        DataRow::new(Some(10004), 1007, Some(107), Some(205)),
        DataRow::new(Some(10005), 1008, Some(108), Some(206)),
    ];
    tester.put(&data).await;
}

#[tokio::test]
async fn test_alter_region() {
    let dir = TempDir::new("alter-region").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let tester = AlterTester::new(store_dir).await;

    let data = vec![(1000, Some(100)), (1001, Some(101)), (1002, Some(102))];

    tester.put_before_alter(&data).await;

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
