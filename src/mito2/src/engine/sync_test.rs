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

use std::assert_matches;

use api::v1::{Rows, SemanticType};
use common_error::ext::ErrorExt;
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{RegionEngine, RegionManifestInfo, RegionRole};
use store_api::region_request::{
    AddColumn, AddColumnLocation, AlterKind, PathType, RegionAlterRequest, RegionOpenRequest,
    RegionPutRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use super::MitoEngine;
use crate::config::MitoConfig;
use crate::error::Error;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

fn add_tag1() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_1",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 3,
                },
                location: Some(AddColumnLocation::First),
            }],
        },
    }
}

async fn scan_check(
    engine: &MitoEngine,
    region_id: RegionId,
    expected: &str,
    num_memtable: usize,
    num_files: usize,
) {
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(num_memtable, scanner.num_memtables());
    assert_eq!(num_files, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_sync_after_flush_region() {
    test_sync_after_flush_region_with_format(false).await;
    test_sync_after_flush_region_with_format(true).await;
}

async fn test_sync_after_flush_region_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
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

    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Open the region on the follower engine
    let follower_engine = env
        .create_follower_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: Default::default(),
                // Ensure the region is not replayed from the WAL.
                skip_wal_replay: true,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    flush_region(&engine, region_id, None).await;

    // Scan the region on the leader engine
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    scan_check(&engine, region_id, expected, 0, 1).await;

    common_telemetry::info!("Scan the region on the follower engine");
    // Scan the region on the follower engine
    let expected = "\
+-------+---------+----+
| tag_0 | field_0 | ts |
+-------+---------+----+
+-------+---------+----+";
    scan_check(&follower_engine, region_id, expected, 0, 0).await;

    // Returns error since the max manifest is 1
    let manifest_info = RegionManifestInfo::mito(2, 0, 0);
    let err = follower_engine
        .sync_region(region_id, manifest_info.into())
        .await
        .unwrap_err();
    let err = err.as_any().downcast_ref::<Error>().unwrap();
    assert_matches!(err, Error::InstallManifestTo { .. });

    let manifest_info = RegionManifestInfo::mito(1, 0, 0);
    follower_engine
        .sync_region(region_id, manifest_info.into())
        .await
        .unwrap();
    common_telemetry::info!("Scan the region on the follower engine after sync");
    let follower = follower_engine.get_region(region_id).unwrap();
    assert_eq!(follower.version_control.current().last_sequence, 3);
    // Sync does not update committed sequence or entry id.
    assert_eq!(follower.version_control.committed_sequence(), 0);
    assert_eq!(follower.version_control.current().last_entry_id, 0);
    // Scan the region on the follower engine
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    scan_check(&follower_engine, region_id, expected, 0, 1).await;

    // An edit may have a committed sequence above the flushed sequence.
    let file = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()[0]
        .files
        .values()
        .next()
        .unwrap()
        .meta_ref()
        .clone();
    engine
        .edit_region(
            region_id,
            crate::manifest::action::RegionEdit {
                files_to_add: vec![file],
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
    let manifest_version = engine
        .get_region(region_id)
        .unwrap()
        .manifest_ctx
        .manifest_version()
        .await;
    follower_engine
        .sync_region(
            region_id,
            RegionManifestInfo::mito(manifest_version, 0, 0).into(),
        )
        .await
        .unwrap();
    assert_eq!(follower.version_control.current().last_sequence, 4);
    assert_eq!(follower.version_control.committed_sequence(), 0);

    // Writes after promotion must use a higher sequence.
    follower_engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    let request = CreateRequestBuilder::new().build();
    // Skip WAL to isolate row sequence allocation.
    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows {
                    schema: rows_schema(&request),
                    rows: build_rows(0, 1),
                },
                skip_wal: true,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        5,
        follower_engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .committed_sequence()
    );
}

#[tokio::test]
async fn test_sync_after_alter_region() {
    test_sync_after_alter_region_with_format(false).await;
    test_sync_after_alter_region_with_format(true).await;
}

async fn test_sync_after_alter_region_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

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

    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Open the region on the follower engine
    let follower_engine = env
        .create_follower_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: Default::default(),
                // Ensure the region is not replayed from the WAL.
                skip_wal_replay: true,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let request = add_tag1();
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();

    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 0     | 0.0     | 1970-01-01T00:00:00 |
|       | 1     | 1.0     | 1970-01-01T00:00:01 |
|       | 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";

    scan_check(&engine, region_id, expected, 0, 1).await;
    let expected = "\
+-------+---------+----+
| tag_0 | field_0 | ts |
+-------+---------+----+
+-------+---------+----+";
    scan_check(&follower_engine, region_id, expected, 0, 0).await;

    // Sync the region from the leader engine to the follower engine
    let manifest_info = RegionManifestInfo::mito(2, 0, 0);
    follower_engine
        .sync_region(region_id, manifest_info.into())
        .await
        .unwrap();
    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 0     | 0.0     | 1970-01-01T00:00:00 |
|       | 1     | 1.0     | 1970-01-01T00:00:01 |
|       | 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    scan_check(&follower_engine, region_id, expected, 0, 1).await;
}
