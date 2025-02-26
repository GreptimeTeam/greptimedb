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

use std::time::Duration;

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::region::options::MemtableOptions;
use crate::test_util::{build_rows, put_rows, rows_schema, CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_engine_create_new_region() {
    let mut env = TestEnv::with_prefix("new-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_engine_create_existing_region() {
    let mut env = TestEnv::with_prefix("create-existing");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new();
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();

    // Create the same region again.
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_engine_create_close_create_region() {
    // This test will trigger create_or_open function.
    let mut env = TestEnv::with_prefix("create-close-create");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new();
    // Create a region with id 1.
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();
    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    // Create the same region id again.
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();

    assert!(engine.is_region_exists(region_id));

    let region = engine.get_region(region_id).unwrap();

    assert!(region.is_writable());
}

#[tokio::test]
async fn test_engine_create_with_different_id() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new();
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();

    // Creates with different id.
    engine
        .handle_request(RegionId::new(2, 1), RegionRequest::Create(builder.build()))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn test_engine_create_with_different_schema() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new();
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();

    // Creates with different schema.
    let builder = builder.tag_num(2);
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn test_engine_create_with_different_primary_key() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new().tag_num(2);
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();

    // Creates with different schema.
    let builder = builder.primary_key(vec![1]);
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn test_engine_create_with_options() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("ttl", "10d")
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    assert!(engine.is_region_exists(region_id));
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        region.version().options.ttl,
        Some(Duration::from_secs(3600 * 24 * 10).into())
    );
}

#[tokio::test]
async fn test_engine_create_with_custom_store() {
    let mut env = TestEnv::new();
    let engine = env
        .create_engine_with_multiple_object_stores(MitoConfig::default(), None, None, &["Gcs"])
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("storage", "Gcs")
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    assert!(engine.is_region_exists(region_id));
    let region = engine.get_region(region_id).unwrap();
    let region_dir = region.access_layer.region_dir();

    let object_store_manager = env.get_object_store_manager().unwrap();
    assert!(object_store_manager
        .find("Gcs")
        .unwrap()
        .exists(region_dir)
        .await
        .unwrap());
    assert!(!object_store_manager
        .default_object_store()
        .exists(region_dir)
        .await
        .unwrap());
}

#[tokio::test]
async fn test_engine_create_with_memtable_opts() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("memtable.type", "partition_tree")
        .insert_option("memtable.partition_tree.index_max_keys_per_shard", "2")
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let Some(MemtableOptions::PartitionTree(memtable_opts)) = &region.version().options.memtable
    else {
        unreachable!();
    };
    assert_eq!(2, memtable_opts.index_max_keys_per_shard);

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}
