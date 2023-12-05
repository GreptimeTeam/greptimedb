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

use std::sync::Arc;
use std::time::Duration;

use api::v1::Rows;
use object_store::util::join_path;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionDropRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::engine::listener::DropListener;
use crate::engine::MitoEngine;
use crate::test_util::{
    build_rows_for_key, flush_region, put_rows, rows_schema, CreateRequestBuilder, TestEnv,
};
use crate::worker::DROPPING_MARKER_FILE;

#[tokio::test]
async fn test_engine_drop_region() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("drop");
    let listener = Arc::new(DropListener::new(Duration::from_millis(100)));
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()))
        .await;

    let region_id = RegionId::new(1, 1);
    // It's okay to drop a region doesn't exist.
    engine
        .handle_request(region_id, RegionRequest::Drop(RegionDropRequest {}))
        .await
        .unwrap_err();

    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let region_dir = region.access_layer.region_dir().to_string();
    // no dropping marker file
    assert!(!env
        .get_object_store()
        .unwrap()
        .is_exist(&join_path(&region_dir, DROPPING_MARKER_FILE))
        .await
        .unwrap());

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // drop the created region.
    engine
        .handle_request(region_id, RegionRequest::Drop(RegionDropRequest {}))
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    // Wait for drop task.
    listener.wait().await;

    let object_store = env.get_object_store().unwrap();
    assert!(!object_store.is_exist(&region_dir).await.unwrap());
}

#[tokio::test]
async fn test_engine_drop_region_for_custom_store() {
    common_telemetry::init_default_ut_logging();
    async fn setup(engine: &MitoEngine, region_id: RegionId, storage_name: &str) {
        let request = CreateRequestBuilder::new()
            .insert_option("storage", storage_name)
            .region_dir(storage_name)
            .build();
        let column_schema = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        let rows = Rows {
            schema: column_schema.clone(),
            rows: build_rows_for_key("a", 0, 2, 0),
        };
        put_rows(engine, region_id, rows).await;
        flush_region(engine, region_id, None).await;
    }
    let mut env = TestEnv::with_prefix("drop");
    let listener = Arc::new(DropListener::new(Duration::from_millis(100)));
    let engine = env
        .create_engine_with_multiple_object_stores(
            MitoConfig::default(),
            None,
            Some(listener.clone()),
            &["Gcs"],
        )
        .await;
    let object_store_manager = env.get_object_store_manager().unwrap();

    let global_region_id = RegionId::new(1, 1);
    setup(&engine, global_region_id, "default").await;
    let custom_region_id = RegionId::new(2, 1);
    setup(&engine, custom_region_id, "Gcs").await;

    let global_region = engine.get_region(global_region_id).unwrap();
    let global_region_dir = global_region.access_layer.region_dir().to_string();

    let custom_region = engine.get_region(custom_region_id).unwrap();
    let custom_region_dir = custom_region.access_layer.region_dir().to_string();

    // Both these regions should exist before dropping the custom region.
    assert!(object_store_manager
        .find("Gcs")
        .unwrap()
        .is_exist(&custom_region_dir)
        .await
        .unwrap());
    assert!(object_store_manager
        .find("default")
        .unwrap()
        .is_exist(&global_region_dir)
        .await
        .unwrap());

    // Drop the custom region.
    engine
        .handle_request(custom_region_id, RegionRequest::Drop(RegionDropRequest {}))
        .await
        .unwrap();
    assert!(!engine.is_region_exists(custom_region_id));

    // Wait for drop task.
    listener.wait().await;

    assert!(!object_store_manager
        .find("Gcs")
        .unwrap()
        .is_exist(&custom_region_dir)
        .await
        .unwrap());
    assert!(object_store_manager
        .find("default")
        .unwrap()
        .is_exist(&global_region_dir)
        .await
        .unwrap());
}
