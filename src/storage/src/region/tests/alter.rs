use datatypes::prelude::ConcreteDataType;
use log_store::fs::log::LocalFileLogStore;
use store_api::storage::{
    AddColumn, AlterOperation, AlterRequest, ColumnDescriptor, ColumnDescriptorBuilder, ColumnId,
    Region, RegionMeta, SchemaRef,
};
use tempdir::TempDir;

use crate::region::tests::{self, FileTesterBase};
use crate::region::RegionImpl;
use crate::test_util::config_util;

const REGION_NAME: &str = "region-alter-0";

async fn create_region_for_alter(store_dir: &str) -> RegionImpl<LocalFileLogStore> {
    // Always disable version column in this test.
    let metadata = tests::new_metadata(REGION_NAME, false);

    let store_config = config_util::new_store_config(REGION_NAME, store_dir).await;

    RegionImpl::create(metadata, store_config).await.unwrap()
}

/// Tester for region alter.
struct AlterTester {
    base: Option<FileTesterBase>,
}

impl AlterTester {
    async fn new(store_dir: &str) -> AlterTester {
        let region = create_region_for_alter(store_dir).await;

        AlterTester {
            base: Some(FileTesterBase::with_region(region)),
        }
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    fn schema(&self) -> SchemaRef {
        let metadata = self.base().region.in_memory_metadata();
        metadata.schema().clone()
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
