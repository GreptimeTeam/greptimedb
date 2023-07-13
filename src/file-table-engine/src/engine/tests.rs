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

use std::assert_matches::assert_matches;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, IMMUTABLE_FILE_ENGINE};
use table::engine::{EngineContext, TableEngine, TableEngineProcedure};
use table::requests::{AlterKind, AlterTableRequest, DropTableRequest, OpenTableRequest};
use table::{error as table_error, Table};

use crate::config::EngineConfig;
use crate::engine::immutable::ImmutableFileTableEngine;
use crate::manifest::immutable::manifest_path;
use crate::table::immutable::ImmutableFileTable;
use crate::test_util::{self, TestEngineComponents, TEST_TABLE_NAME};

#[tokio::test]
async fn test_get_table() {
    let TestEngineComponents {
        table_engine,
        table_ref: table,
        dir: _dir,
        ..
    } = test_util::setup_test_engine_and_table("test_get_table").await;
    let table_info = table.table_info();

    let got = table_engine
        .get_table(&EngineContext::default(), table_info.ident.table_id)
        .unwrap()
        .unwrap();

    assert_eq!(table.schema(), got.schema());
}

#[tokio::test]
async fn test_open_table() {
    common_telemetry::init_default_ut_logging();
    let ctx = EngineContext::default();
    // the test table id is 1
    let table_id = 1;
    let open_req = OpenTableRequest {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: test_util::TEST_TABLE_NAME.to_string(),
        // the test table id is 1
        table_id,
        region_numbers: vec![0],
    };

    let TestEngineComponents {
        table_engine,
        table_ref: table,
        dir: _dir,
        ..
    } = test_util::setup_test_engine_and_table("test_open_table").await;

    assert_eq!(IMMUTABLE_FILE_ENGINE, table_engine.name());

    table_engine.close_table(table_id).await.unwrap();

    let reopened = table_engine
        .open_table(&ctx, open_req.clone())
        .await
        .unwrap()
        .unwrap();

    let reopened = reopened
        .as_any()
        .downcast_ref::<ImmutableFileTable>()
        .unwrap();

    let left = table.table_info();
    let right = reopened.table_info();

    // assert recovered table_info is correct
    assert_eq!(left, right);
}

#[tokio::test]
async fn test_close_all_table() {
    common_telemetry::init_default_ut_logging();

    let TestEngineComponents {
        table_engine,
        dir: _dir,
        table_ref: table,
        ..
    } = test_util::setup_test_engine_and_table("test_close_all_table").await;

    table_engine.close().await.unwrap();

    let table_id = table.table_info().ident.table_id;
    let exist = table_engine.table_exists(&EngineContext::default(), table_id);

    assert!(!exist);
}

#[tokio::test]
async fn test_alter_table() {
    common_telemetry::init_default_ut_logging();
    let TestEngineComponents {
        table_engine,
        dir: _dir,
        table_ref,
        ..
    } = test_util::setup_test_engine_and_table("test_alter_table").await;

    let alter_req = AlterTableRequest {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: TEST_TABLE_NAME.to_string(),
        table_id: table_ref.table_info().ident.table_id,
        alter_kind: AlterKind::RenameTable {
            new_table_name: "foo".to_string(),
        },
        table_version: None,
    };

    let unsupported = table_engine
        .alter_table(&EngineContext::default(), alter_req)
        .await
        .err()
        .unwrap();

    assert_matches!(unsupported, table_error::Error::Unsupported { .. })
}

#[tokio::test]
async fn test_drop_table() {
    common_telemetry::init_default_ut_logging();

    let TestEngineComponents {
        table_engine,
        object_store,
        dir: _dir,
        table_dir,
        table_ref: table,
        ..
    } = test_util::setup_test_engine_and_table("test_drop_table").await;

    let table_info = table.table_info();

    let drop_req = DropTableRequest {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: TEST_TABLE_NAME.to_string(),
        table_id: table_info.ident.table_id,
    };
    let dropped = table_engine
        .drop_table(&EngineContext::default(), drop_req)
        .await
        .unwrap();

    assert!(dropped);

    let exist = table_engine.table_exists(&EngineContext::default(), table_info.ident.table_id);
    assert!(!exist);

    // check table_dir manifest
    let exist = object_store
        .is_exist(&manifest_path(&table_dir))
        .await
        .unwrap();

    assert!(!exist);
}

#[tokio::test]
async fn test_create_drop_table_procedure() {
    let (_dir, object_store) = test_util::new_test_object_store("procedure");

    let table_engine = ImmutableFileTableEngine::new(EngineConfig::default(), object_store.clone());

    let schema = Arc::new(test_util::test_schema());

    let engine_ctx = EngineContext::default();
    // Test create table by procedure.
    let create_request = test_util::new_create_request(schema);
    let table_id = create_request.id;
    let mut procedure = table_engine
        .create_table_procedure(&engine_ctx, create_request.clone())
        .unwrap();
    common_procedure_test::execute_procedure_until_done(&mut procedure).await;

    assert!(table_engine
        .get_table(&engine_ctx, table_id)
        .unwrap()
        .is_some());

    // Test drop table by procedure.
    let drop_request = DropTableRequest {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: TEST_TABLE_NAME.to_string(),
        table_id,
    };
    let mut procedure = table_engine
        .drop_table_procedure(&engine_ctx, drop_request)
        .unwrap();
    common_procedure_test::execute_procedure_until_done(&mut procedure).await;

    assert!(table_engine
        .get_table(&engine_ctx, table_id)
        .unwrap()
        .is_none());
}
