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

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use store_api::region_engine::{RegionEngine, SetReadonlyResponse};
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::test_util::{build_rows, put_rows, rows_schema, CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_set_readonly_gracefully() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let result = engine.set_readonly_gracefully(region_id).await.unwrap();
    assert_eq!(
        SetReadonlyResponse::Success {
            last_entry_id: Some(0)
        },
        result
    );

    // set readonly again.
    let result = engine.set_readonly_gracefully(region_id).await.unwrap();
    assert_eq!(
        SetReadonlyResponse::Success {
            last_entry_id: Some(0)
        },
        result
    );

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };

    let error = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest { rows: rows.clone() }),
        )
        .await
        .unwrap_err();

    assert_eq!(error.status_code(), StatusCode::RegionReadonly);

    engine.set_writable(region_id, true).unwrap();

    put_rows(&engine, region_id, rows).await;

    let result = engine.set_readonly_gracefully(region_id).await.unwrap();

    assert_eq!(
        SetReadonlyResponse::Success {
            last_entry_id: Some(1)
        },
        result
    );
}

#[tokio::test]
async fn test_set_readonly_gracefully_not_exist() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let non_exist_region_id = RegionId::new(1, 1);

    // For fast-path.
    let result = engine
        .set_readonly_gracefully(non_exist_region_id)
        .await
        .unwrap();
    assert_eq!(SetReadonlyResponse::NotFound, result);
}
