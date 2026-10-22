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
use std::sync::Arc;
use std::time::Duration;

use api::helper::encode_json_value;
use api::v1::helper::row;
use api::v1::region::{StrictWindow, compact_request};
use api::v1::value::ValueData;
use api::v1::{Rows, WriteHint};
use common_error::ext::{ErrorExt, WhateverResult};
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use datatypes::json::value::JsonValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::{RegionEngine, RegionManifestInfo, RegionRole};
use store_api::region_request::{
    RegionCloseRequest, RegionCompactRequest, RegionOpenRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::error::Error;
use crate::manifest::action::{RegionChange, RegionMetaAction, RegionMetaActionList};
use crate::manifest::storage::{delta_file, manifest_dir};
use crate::region::options::MemtableOptions;
use crate::sst::FormatType;
use crate::sst::location::region_dir_from_table_dir;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, reopen_region, rows_schema,
};

#[tokio::test]
async fn test_engine_column_compression_validation() {
    use datatypes::schema::COMPRESSION_KEY;
    use store_api::region_request::{AlterKind, RegionAlterRequest, SetRegionOption};

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    for (column_index, compression, options, accepted) in [
        (1, r#"{"level":23}"#, vec![], false),
        (
            0,
            r#"{"level":3}"#,
            vec![
                ("sst_format", "primary_key"),
                ("memtable.type", "time_series"),
            ],
            false,
        ),
        (
            0,
            r#"{"level":3}"#,
            vec![("primary_key_encoding", "sparse")],
            false,
        ),
        (0, r#"{"level":3}"#, vec![], true),
    ] {
        let mut request = CreateRequestBuilder::new().build();
        request.options.extend(
            options
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        let column = &mut request.column_metadatas[column_index].column_schema;
        *column = column
            .clone()
            .with_metadata(std::collections::HashMap::from([(
                COMPRESSION_KEY.to_string(),
                compression.to_string(),
            )]));
        let result = engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await;
        if accepted {
            result.unwrap();
        } else {
            assert_eq!(
                StatusCode::InvalidArguments,
                result.unwrap_err().status_code()
            );
            assert!(engine.get_region(region_id).is_none());
        }
    }

    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::Format("primary_key".to_string())],
                },
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
}

#[tokio::test]
async fn test_engine_create_new_region() {
    test_engine_create_new_region_with_format(false).await;
    test_engine_create_new_region_with_format(true).await;
}

#[rstest::rstest]
#[case::implicit_time_series(false, None, None, false)]
#[case::explicit_time_series(false, Some("time_series"), None, false)]
#[case::explicit_bulk(false, Some("bulk"), None, true)]
#[case::flat_overrides_time_series(false, Some("time_series"), Some("flat"), true)]
#[case::default_flat(true, None, None, true)]
#[case::default_flat_overrides_time_series(true, Some("time_series"), None, true)]
#[case::primary_key_overrides_default_flat(true, None, Some("primary_key"), false)]
#[tokio::test]
async fn test_engine_create_json2_memtable_validation(
    #[case] default_flat_format: bool,
    #[case] memtable: Option<&str>,
    #[case] sst_format: Option<&str>,
    #[case] accepted: bool,
) -> WhateverResult<()> {
    let mut env = TestEnv::with_prefix("create-json2-memtable-validation").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new()
        .field_datatype(ConcreteDataType::json2(JsonNativeType::Object(
            JsonObjectType::new(),
        )))
        .insert_option("append_mode", "true")
        .build();
    if let Some(memtable) = memtable {
        request
            .options
            .insert("memtable.type".into(), memtable.into());
    }
    if let Some(sst_format) = sst_format {
        request
            .options
            .insert("sst_format".into(), sst_format.into());
    }

    let result = engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await;
    if accepted {
        result?;
    } else {
        let err = result.unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(
            err.output_msg()
                .contains("JSON2 columns require sst_format=flat")
        );
        assert!(engine.get_region(region_id).is_none());
        request
            .options
            .insert("memtable.type".into(), "bulk".into());
        engine
            .handle_request(region_id, RegionRequest::Create(request.clone()))
            .await?;
    }
    reopen_region(&engine, region_id, request.table_dir, true, request.options).await;
    Ok(())
}

#[rstest::rstest]
#[case::implicit_primary_key(false, false)]
#[case::default_flat(true, false)]
#[case::explicit_primary_key(false, true)]
#[tokio::test]
async fn test_engine_recover_json2_region(
    #[case] default_flat_format: bool,
    #[case] explicit_primary_key: bool,
    #[values(false, true)] use_create: bool,
    #[values(FormatType::PrimaryKey, FormatType::Flat)] persisted_format: FormatType,
) -> WhateverResult<()> {
    let mut env = TestEnv::with_prefix("recover-legacy-json2").await;
    let config = MitoConfig {
        default_flat_format,
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new()
        .field_datatype(ConcreteDataType::json2(JsonNativeType::Object(
            JsonObjectType::new(),
        )))
        .insert_option("append_mode", "true")
        .insert_option("memtable.type", "bulk")
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await?;
    let schema = rows_schema(&request);
    let json_row = |n: i64| {
        row(vec![
            ValueData::StringValue("tag".into()),
            ValueData::JsonValue(encode_json_value(JsonValue::from([("a", n)]))),
            ValueData::TimestampMillisecondValue(n * 1000),
        ])
    };
    for n in 1..=2 {
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows: vec![json_row(n)],
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;
    }
    // Leave one row in the WAL to exercise replay into the recovered Bulk memtable.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: schema.clone(),
            rows: vec![json_row(3)],
        },
    )
    .await;
    let expected = scan_region_data(&engine, region_id).await;

    request.options.remove("memtable.type");
    if explicit_primary_key {
        request
            .options
            .insert("sst_format".into(), "primary_key".into());
        request
            .options
            .insert("memtable.type".into(), "time_series".into());
    }
    let open_request = RegionOpenRequest {
        engine: request.engine.clone(),
        table_dir: request.table_dir.clone(),
        path_type: request.path_type,
        options: request.options.clone(),
        skip_wal_replay: false,
        checkpoint: None,
        requirements: Default::default(),
    };
    let follower = env.create_follower_engine(config).await;
    follower
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                skip_wal_replay: true,
                ..open_request.clone()
            }),
        )
        .await?;

    let region = engine.get_region(region_id).unwrap();
    // Reproduce the persisted state from older binaries, bypassing the new ALTER validation.
    region
        .manifest_ctx
        .manifest_manager
        .write()
        .await
        .update(
            RegionMetaActionList::new(vec![RegionMetaAction::Change(RegionChange {
                metadata: region.metadata(),
                sst_format: persisted_format,
                append_mode: Some(true),
            })]),
            false,
        )
        .await?;
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(2, manifest.files.len());
    assert_eq!(persisted_format, manifest.sst_format);
    follower
        .sync_region(
            region_id,
            RegionManifestInfo::mito(manifest.manifest_version, 0, 0).into(),
        )
        .await?;
    let follower_region = follower.get_region(region_id).unwrap();
    assert_eq!(
        Some(FormatType::Flat),
        follower_region.version().options.sst_format
    );
    assert_eq!(
        manifest.files,
        follower_region.manifest_ctx.manifest().await.files
    );
    follower.stop().await?;

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await?;
    let recovery = if use_create {
        RegionRequest::Create(request.clone())
    } else {
        RegionRequest::Open(open_request)
    };
    engine.handle_request(region_id, recovery).await?;
    let region = engine.get_region(region_id).unwrap();
    let recovered_manifest = region.manifest_ctx.manifest().await;
    assert_eq!(
        manifest.manifest_version,
        recovered_manifest.manifest_version
    );
    assert_eq!(manifest.metadata, recovered_manifest.metadata);
    assert_eq!(manifest.files, recovered_manifest.files);
    assert_eq!(persisted_format, recovered_manifest.sst_format);
    assert_eq!(Some(FormatType::Flat), region.version().options.sst_format);
    assert_eq!(expected, scan_region_data(&engine, region_id).await);

    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: vec![json_row(4)],
        },
    )
    .await;
    let expected = scan_region_data(&engine, region_id).await;
    flush_region(&engine, region_id, None).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest {
                options: compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 3600,
                }),
                ..Default::default()
            }),
        )
        .await?;
    assert_eq!(1, region.manifest_ctx.manifest().await.files.len());
    assert_eq!(expected, scan_region_data(&engine, region_id).await);
    reopen_region(
        &engine,
        region_id,
        request.table_dir,
        false,
        request.options,
    )
    .await;
    assert_eq!(expected, scan_region_data(&engine, region_id).await);
    Ok(())
}

async fn scan_region_data(engine: &MitoEngine, region_id: RegionId) -> String {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    sort_batches_and_print(&batches, &["ts"])
}

#[tokio::test]
async fn test_engine_create_preserves_manifest_on_open_error() -> WhateverResult<()> {
    let mut env = TestEnv::with_prefix("create-open-error").await;
    let engine = env
        .create_engine(MitoConfig {
            compress_manifest: false,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await?;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    let expected = scan_region_data(&engine, region_id).await;
    let manifest = engine
        .get_region(region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await?;

    let store = env.get_object_store().unwrap();
    let dir = manifest_dir(&region_dir_from_table_dir(
        &request.table_dir,
        region_id,
        request.path_type,
    ));
    let path = format!("{dir}{}", delta_file(0));
    let original = store.read(&path).await.unwrap().to_bytes();
    store.write(&path, "invalid manifest").await.unwrap();
    let mut manifest_bytes = Vec::new();
    for entry in store.list(&dir).await.unwrap() {
        if entry.metadata().is_file() {
            manifest_bytes.push((
                entry.path().to_string(),
                store.read(entry.path()).await.unwrap().to_bytes(),
            ));
        }
    }
    let err = engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap_err();
    assert_matches!(
        err.as_any().downcast_ref::<Error>().unwrap(),
        Error::SerdeJson { .. }
    );
    assert!(!engine.is_region_exists(region_id));
    for (path, bytes) in manifest_bytes {
        assert_eq!(bytes, store.read(&path).await.unwrap().to_bytes());
    }

    store.write(&path, original).await.unwrap();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await?;
    let recovered = engine
        .get_region(region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(manifest.manifest_version, recovered.manifest_version);
    assert_eq!(manifest.files, recovered.files);
    assert_eq!(expected, scan_region_data(&engine, region_id).await);
    Ok(())
}

#[tokio::test]
async fn test_engine_create_json2_sparse_requires_flat() -> WhateverResult<()> {
    let mut env = TestEnv::with_prefix("create-json2-sparse").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new()
        .field_datatype(ConcreteDataType::json2(JsonNativeType::Object(
            JsonObjectType::new(),
        )))
        .insert_option("memtable.type", "time_series")
        .insert_option("primary_key_encoding", "sparse")
        .insert_option("sst_format", "primary_key")
        .build();
    let json_column = request.column_metadatas[1].column_schema.clone();
    let metadata = Arc::new(sst_region_metadata_with_encoding(
        PrimaryKeyEncoding::Sparse,
    ));
    request.column_metadatas = metadata.column_metadatas.clone();
    request.primary_key = metadata.primary_key.clone();
    request.column_metadatas[4].column_schema = json_column;
    let err = engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap_err();
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
    assert!(
        err.output_msg()
            .contains("JSON2 columns require sst_format=flat")
    );
    assert!(!engine.is_region_exists(region_id));

    request.options.insert("sst_format".into(), "flat".into());
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await?;
    let schema = rows_schema(&request);
    let mut pk_schema = schema[0].clone();
    pk_schema.column_name = PRIMARY_KEY_COLUMN_NAME.into();
    pk_schema.datatype = api::v1::ColumnDataType::Binary.into();
    let rows = Rows {
        schema: vec![pk_schema, schema[5].clone(), schema[4].clone()],
        rows: vec![row(vec![
            ValueData::BinaryValue(new_sparse_primary_key(&["a", "x"], &metadata, 10, 0)),
            ValueData::TimestampMillisecondValue(1000),
            ValueData::JsonValue(encode_json_value(JsonValue::from([("a", 1i64)]))),
        ])],
    };
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows,
                hint: Some(WriteHint {
                    primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
                }),
                skip_wal: false,
                partition_expr_version: None,
            }),
        )
        .await?;
    let expected = scan_region_data(&engine, region_id).await;
    flush_region(&engine, region_id, None).await;
    reopen_region(
        &engine,
        region_id,
        request.table_dir,
        false,
        request.options,
    )
    .await;
    assert_eq!(expected, scan_region_data(&engine, region_id).await);
    Ok(())
}

async fn test_engine_create_new_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("new-region").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    test_engine_create_existing_region_with_format(false).await;
    test_engine_create_existing_region_with_format(true).await;
}

async fn test_engine_create_existing_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("create-existing").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    test_engine_create_close_create_region_with_format(false).await;
    test_engine_create_close_create_region_with_format(true).await;
}

async fn test_engine_create_close_create_region_with_format(flat_format: bool) {
    // This test will trigger create_or_open function.
    let mut env = TestEnv::with_prefix("create-close-create").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let builder = CreateRequestBuilder::new();
    // Create a region with id 1.
    engine
        .handle_request(region_id, RegionRequest::Create(builder.build()))
        .await
        .unwrap();
    // Close the region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
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
    test_engine_create_with_different_id_with_format(false).await;
    test_engine_create_with_different_id_with_format(true).await;
}

async fn test_engine_create_with_different_id_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
        .unwrap();
}

#[tokio::test]
async fn test_engine_create_with_different_schema() {
    test_engine_create_with_different_schema_with_format(false).await;
    test_engine_create_with_different_schema_with_format(true).await;
}

async fn test_engine_create_with_different_schema_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    test_engine_create_with_different_primary_key_with_format(false).await;
    test_engine_create_with_different_primary_key_with_format(true).await;
}

async fn test_engine_create_with_different_primary_key_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    test_engine_create_with_options_with_format(false).await;
    test_engine_create_with_options_with_format(true).await;
}

async fn test_engine_create_with_options_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    test_engine_create_with_custom_store_with_format(false).await;
    test_engine_create_with_custom_store_with_format(true).await;
}

async fn test_engine_create_with_custom_store_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine_with_multiple_object_stores(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            None,
            &["Gcs"],
        )
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
    let region_dir = region.access_layer.build_region_dir(region_id);

    let object_store_manager = env.get_object_store_manager().unwrap();
    assert!(
        object_store_manager
            .find("Gcs")
            .unwrap()
            .exists(&region_dir)
            .await
            .unwrap()
    );
    assert!(
        !object_store_manager
            .default_object_store()
            .exists(&region_dir)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_engine_create_with_memtable_opts() {
    test_engine_create_with_memtable_opts_with_format(false).await;
    test_engine_create_with_memtable_opts_with_format(true).await;
}

async fn test_engine_create_with_memtable_opts_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("memtable.type", "bulk")
        .insert_option("memtable.bulk.merge_threshold", "7")
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    match version
        .options
        .memtable
        .as_ref()
        .expect("memtable options should be set")
    {
        MemtableOptions::Bulk(config) => assert_eq!(config.merge_threshold, 7),
        other => panic!("expected bulk memtable, got {other:?}"),
    }

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

#[tokio::test]
async fn create_with_partition_expr_persists_manifest() {
    create_with_partition_expr_persists_manifest_with_format(false).await;
    create_with_partition_expr_persists_manifest_with_format(true).await;
}

async fn create_with_partition_expr_persists_manifest_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let expr_json = r#"{"Expr":{"lhs":{"Column":"a"},"op":"GtEq","rhs":{"Value":{"UInt32":10}}}}"#;
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(expr_json.to_string()))
        .build();
    let table_dir = request.table_dir.clone();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.metadata.partition_expr.as_deref(), Some(expr_json));

    // Reopen the region with options containing partition.expr_json
    reopen_region(&engine, region_id, table_dir, false, Default::default()).await;
    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.metadata.partition_expr.as_deref(), Some(expr_json));
}

#[tokio::test]
async fn test_engine_create_with_format() {
    common_telemetry::init_default_ut_logging();

    test_engine_create_with_format_one_case("primary_key", false).await;
    test_engine_create_with_format_one_case("primary_key", true).await;
    test_engine_create_with_format_one_case("flat", false).await;
    test_engine_create_with_format_one_case("flat", true).await;
}

async fn test_engine_create_with_format_one_case(create_format: &str, default_flat_format: bool) {
    common_telemetry::info!(
        "Test engine create with format, create_format: {}, default_flat_format: {}",
        create_format,
        default_flat_format
    );

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("sst_format", create_format)
        .build();

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
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());

    flush_region(&engine, region_id, None).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}
