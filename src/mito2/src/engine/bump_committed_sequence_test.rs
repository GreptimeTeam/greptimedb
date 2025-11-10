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

use api::v1::Rows;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{PathType, RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::manifest::action::RegionEdit;
use crate::sst::file::FileMeta;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[tokio::test]
async fn test_bump_committed_sequence() {
    test_bump_committed_sequence_with_format(false).await;
    test_bump_committed_sequence_with_format(true).await;
}

async fn test_bump_committed_sequence_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let table_dir = request.table_dir.clone();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let _ = engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(region.version_control.current().committed_sequence, 0);
    assert_eq!(region.version_control.current().version.flushed_sequence, 0);

    let column_schemas = rows_schema(&request);
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 42),
    };
    put_rows(&engine, region_id, rows).await;
    assert_eq!(region.version_control.current().committed_sequence, 42);
    assert_eq!(region.version_control.current().version.flushed_sequence, 0);

    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![FileMeta::default()],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(region.version_control.current().version.flushed_sequence, 0);
    assert_eq!(region.version_control.committed_sequence(), 43);

    // Reopen region.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: table_dir.clone(),
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    region.set_role(RegionRole::Leader);
    assert_eq!(region.version_control.current().version.flushed_sequence, 0);
    assert_eq!(region.version_control.committed_sequence(), 43);

    // Write another 2 rows after editing.
    let column_schemas = rows_schema(&request);
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 2),
    };
    put_rows(&engine, region_id, rows).await;
    assert_eq!(region.version_control.committed_sequence(), 45);
    assert_eq!(region.version_control.current().version.flushed_sequence, 0);

    // Reopen region.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: HashMap::default(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    region.set_role(RegionRole::Leader);
    assert_eq!(region.version_control.current().version.flushed_sequence, 0);
    assert_eq!(region.version_control.committed_sequence(), 45);
}
