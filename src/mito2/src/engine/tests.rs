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

//! Tests for mito engine.

use std::collections::HashMap;

use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{Row, Rows};
use store_api::metadata::ColumnMetadata;
use store_api::region_request::{RegionCloseRequest, RegionOpenRequest, RegionPutRequest};
use store_api::storage::RegionId;

use super::*;
use crate::error::Error;
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_engine_new_stop() {
    let env = TestEnv::with_prefix("engine-stop");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Stop the engine to reject further requests.
    engine.stop().await.unwrap();
    assert!(!engine.is_region_exists(region_id));

    let request = CreateRequestBuilder::new().build();
    let err = engine
        .handle_request(RegionId::new(1, 2), RegionRequest::Create(request))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::WorkerStopped { .. }),
        "unexpected err: {err}"
    );
}

#[tokio::test]
async fn test_engine_create_new_region() {
    let env = TestEnv::with_prefix("new-region");
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
async fn test_engine_create_region_if_not_exists() {
    let env = TestEnv::with_prefix("create-not-exists");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new().create_if_not_exists(true);
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
async fn test_engine_create_existing_region() {
    let env = TestEnv::with_prefix("create-existing");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new();
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();

    // Create the same region again.
    let err = engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::RegionExists { .. }),
        "unexpected err: {err}"
    );
}

#[tokio::test]
async fn test_engine_open_empty() {
    let env = TestEnv::with_prefix("open-empty");
    let engine = env.create_engine(MitoConfig::default()).await;

    let err = engine
        .handle_request(
            RegionId::new(1, 1),
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: "empty".to_string(),
                options: HashMap::default(),
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::RegionNotFound { .. }),
        "unexpected err: {err}"
    );
}

#[tokio::test]
async fn test_engine_open_existing() {
    let env = TestEnv::with_prefix("open-exiting");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_engine_close_region() {
    let env = TestEnv::with_prefix("close");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // It's okay to close a region doesn't exist.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the created region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    // It's okay to close this region again.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_engine_reopen_region() {
    let env = TestEnv::with_prefix("reopen-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the region again.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
            }),
        )
        .await
        .unwrap();
    assert!(engine.is_region_exists(region_id));
}

fn column_metadata_to_column_schema(metadata: &ColumnMetadata) -> api::v1::ColumnSchema {
    api::v1::ColumnSchema {
        column_name: metadata.column_schema.name.clone(),
        datatype: ColumnDataTypeWrapper::try_from(metadata.column_schema.data_type.clone())
            .unwrap()
            .datatype() as i32,
        semantic_type: metadata.semantic_type as i32,
    }
}

fn build_rows(num_rows: usize) -> Vec<Row> {
    let values = (0..num_rows)
        .map(|i| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(i.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(i as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TsMillisecondValue(i as i64)),
                },
            ],
        })
        .collect();
    values
}

#[tokio::test]
async fn test_write_to_region() {
    let env = TestEnv::with_prefix("write-to-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(|c| column_metadata_to_column_schema(c))
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let num_rows = 42;
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(num_rows),
    };
    let output = engine
        .handle_request(region_id, RegionRequest::Put(RegionPutRequest { rows }))
        .await
        .unwrap();
    let Output::AffectedRows(rows_inserted) = output else {
        unreachable!()
    };
    assert_eq!(num_rows, rows_inserted);
}
