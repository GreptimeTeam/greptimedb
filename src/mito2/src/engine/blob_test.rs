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

//! Tests for external storage of large field values.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{Rows, SemanticType};
use common_base::readable_size::ReadableSize;
use common_recordbatch::RecordBatches;
use common_test_util::temp_dir::create_temp_dir;
use datafusion_expr::{col, lit};
use datatypes::arrow::array::AsArray;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, StorageOptions};
use datatypes::value::Value;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{MitoCopyRegionFromRequest, RegionEngine};
use store_api::region_request::{
    AddColumn, AlterKind, RegionAlterRequest, RegionCreateRequest, RegionRequest,
};
use store_api::storage::{FileId, RegionId, ScanRequest};

use crate::cache::file_cache::{FileType, IndexKey};
use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::PickerOutput;
use crate::compaction::{PickHistory, pick_automatic_for_test};
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::compact;
use crate::manifest::action::RegionEdit;
use crate::sst::file::{BlobFileRef, FileMeta};
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::{PARQUET_EXTERNAL_METADATA_KEY, PARQUET_METADATA_KEY};
use crate::test_util::{
    CreateRequestBuilder, TestEnv, delete_rows, delete_rows_schema, flush_region, put_rows,
    reopen_region, rows_schema,
};

/// Long values on even rows go to the sidecar; odd rows stay inline, and one of them looks
/// like a reference.
fn payload(i: usize, version: usize) -> String {
    match i % 4 {
        1 => format!("short-{i}-{version}"),
        3 => format!("\0greptime-blob:{i}:{version}:0"),
        _ => format!("{i:06}-{version}-").repeat(16 * 1024),
    }
}

async fn put(
    engine: &MitoEngine,
    region_id: RegionId,
    schema: &[api::v1::ColumnSchema],
    ts: &[usize],
    version: usize,
) {
    let rows = ts
        .iter()
        .map(|i| {
            row(vec![
                ValueData::StringValue("a".to_string()),
                ValueData::StringValue(payload(*i, version)),
                ValueData::TimestampMillisecondValue(*i as i64 * 1000),
            ])
        })
        .collect();
    put_rows(
        engine,
        region_id,
        Rows {
            schema: schema.to_vec(),
            rows,
        },
    )
    .await;
}

/// Returns `(ts, payload)` of every visible row, ordered by ts.
async fn scan(
    engine: &MitoEngine,
    region_id: RegionId,
    request: ScanRequest,
) -> Vec<(i64, String)> {
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<datatypes::arrow::datatypes::TimestampMillisecondType>();
        let values = batch.column_by_name("field_0").unwrap().as_string::<i32>();
        for i in 0..batch.num_rows() {
            rows.push((ts.value(i), values.value(i).to_string()));
        }
    }
    rows.sort();
    rows
}

/// Returns the footer key-value keys of every SST.
async fn sst_footer_keys(env: &TestEnv) -> Vec<Vec<String>> {
    let store = env.get_object_store().unwrap();
    let mut keys = Vec::new();
    for entry in store.list_with("").recursive(true).await.unwrap() {
        if !entry.path().ends_with(".parquet") {
            continue;
        }
        let size = store.stat(entry.path()).await.unwrap().content_length();
        let footer = MetadataLoader::new(store.clone(), entry.path(), size)
            .load(&mut Default::default())
            .await
            .unwrap();
        keys.push(
            footer
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .iter()
                .map(|kv| kv.key.clone())
                .collect(),
        );
    }
    keys
}

/// Returns the ids of the sidecars in the object store.
async fn stored_blobs(env: &TestEnv) -> HashSet<FileId> {
    let store = env.get_object_store().unwrap();
    store
        .list_with("")
        .recursive(true)
        .await
        .unwrap()
        .iter()
        .filter_map(|e| e.name().strip_suffix(".blob"))
        .map(|id| FileId::parse_str(id).unwrap())
        .collect()
}

/// Returns the ids of the sidecars that the region's SSTs reference.
fn referenced_blobs(engine: &MitoEngine, region_id: RegionId) -> HashSet<FileId> {
    engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .flat_map(|file| {
            file.meta_ref()
                .blob_files()
                .map(|blob| blob.file_id)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Waits until the object store holds exactly the sidecars the region references.
async fn assert_stored_blobs_are_referenced(
    env: &TestEnv,
    engine: &MitoEngine,
    region_id: RegionId,
) {
    let referenced = referenced_blobs(engine, region_id);
    for _ in 0..50 {
        if stored_blobs(env).await == referenced {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(referenced, stored_blobs(env).await);
}

async fn blob_files(env: &TestEnv) -> usize {
    let store = env.get_object_store().unwrap();
    store
        .list_with("")
        .recursive(true)
        .await
        .unwrap()
        .iter()
        .filter(|e| e.path().ends_with(".blob"))
        .count()
}

fn create_request(external: bool) -> RegionCreateRequest {
    let mut request = CreateRequestBuilder::new()
        .tag_num(1)
        .field_num(1)
        .field_datatype(ConcreteDataType::string_datatype())
        .build();
    if external {
        let field = request
            .column_metadatas
            .iter_mut()
            .find(|c| c.semantic_type == SemanticType::Field)
            .unwrap();
        field.column_schema = field
            .column_schema
            .clone()
            .with_storage_options(StorageOptions::default())
            .unwrap();
    }
    request
}

#[tokio::test]
async fn test_large_values_stay_in_sst_without_external_storage() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(false);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put(&engine, region_id, &schema, &[0, 1], 0).await;
    flush_region(&engine, region_id, None).await;
    assert_eq!(0, blob_files(&env).await);
}

#[tokio::test]
async fn test_large_values_survive_flush_filter_and_compaction() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    let delete_schema = delete_rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put(&engine, region_id, &schema, &[0, 1, 2, 3], 0).await;
    flush_region(&engine, region_id, None).await;
    put(&engine, region_id, &schema, &[4, 5, 6, 7], 0).await;
    flush_region(&engine, region_id, None).await;
    assert_eq!(2, blob_files(&env).await);
    // Readers without external storage support must fail on these SSTs, not read references.
    for keys in sst_footer_keys(&env).await {
        assert!(keys.iter().any(|k| k == PARQUET_EXTERNAL_METADATA_KEY));
        assert!(keys.iter().all(|k| k != PARQUET_METADATA_KEY));
    }
    // Size estimates (compaction batch size and memory) must count values in the sidecar.
    // Each file holds two long values.
    {
        // Scoped: the version holds file handles, which would block the purge checked below.
        let version = engine.get_region(region_id).unwrap().version();
        for level in version.ssts.levels() {
            for file in level.files.values() {
                assert!(
                    file.meta_ref().max_row_group_decoded_size >= 2 * payload(0, 0).len() as u64
                );
                assert!(file.meta_ref().blob_file_size > 0);
            }
        }
    }

    let expected: Vec<_> = (0..8).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );

    // The predicate must see values, not references, in every filter path.
    let filtered = scan(
        &engine,
        region_id,
        ScanRequest {
            filters: vec![col("field_0").eq(lit(payload(4, 0)))],
            ..Default::default()
        },
    )
    .await;
    assert_eq!(vec![(4000, payload(4, 0))], filtered);

    // Overwrite and delete across files, then merge everything.
    put(&engine, region_id, &schema, &[1, 6], 1).await;
    let rows = Rows {
        schema: delete_schema,
        rows: vec![row(vec![
            ValueData::StringValue("a".to_string()),
            ValueData::TimestampMillisecondValue(2000),
        ])],
    };
    delete_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    compact(&engine, region_id).await;

    let expected: Vec<_> = [0, 1, 3, 4, 5, 6, 7]
        .into_iter()
        .map(|i| (i as i64 * 1000, payload(i, usize::from(i == 1 || i == 6))))
        .collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );

    // Sidecars no output references are deleted with the compacted inputs.
    assert_stored_blobs_are_referenced(&env, &engine, region_id).await;
}

#[tokio::test]
async fn test_copy_region_requires_declared_sidecars() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    for id in [source_id, RegionId::new(1, 2), RegionId::new(1, 3)] {
        engine
            .handle_request(id, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    put(&engine, source_id, &schema, &[0, 1, 2, 3], 0).await;
    flush_region(&engine, source_id, None).await;
    let copy = |target_id| {
        engine.copy_region_from(
            target_id,
            MitoCopyRegionFromRequest {
                source_region_id: source_id,
                parallelism: 1,
            },
        )
    };

    copy(RegionId::new(1, 2)).await.unwrap();
    let expected: Vec<_> = (0..4).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, RegionId::new(1, 2), ScanRequest::default()).await
    );

    let store = env.get_object_store().unwrap();
    for entry in store.list_with("").recursive(true).await.unwrap() {
        if entry.path().contains("1_0000000001") && entry.path().ends_with(".blob") {
            store.delete(entry.path()).await.unwrap();
        }
    }
    copy(RegionId::new(1, 3)).await.unwrap_err();
}

/// Returns the file ids of the region's SSTs.
fn sst_ids(engine: &MitoEngine, region_id: RegionId) -> Vec<FileId> {
    engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values().map(|f| f.file_id().file_id()))
        .collect()
}

async fn delete_remote_sidecars(env: &TestEnv) {
    let store = env.get_object_store().unwrap();
    for entry in store.list_with("").recursive(true).await.unwrap() {
        if entry.path().ends_with(".blob") {
            store.delete(entry.path()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn test_sidecars_are_served_from_write_cache() {
    let mut env = TestEnv::new().await;
    let cache_dir = create_temp_dir("blob_write_cache");
    let config = MitoConfig::default().enable_write_cache(
        cache_dir.path().to_str().unwrap().to_string(),
        ReadableSize::mb(512),
        None,
    );
    let engine = env.create_engine(config).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let file_cache = engine.cache_manager().write_cache().unwrap().file_cache();
    let cached =
        |file_id| file_cache.contains_key(&IndexKey::new(region_id, file_id, FileType::Blob));

    put(&engine, region_id, &schema, &[0, 1, 2, 3], 0).await;
    flush_region(&engine, region_id, None).await;
    put(&engine, region_id, &schema, &[4, 5, 6, 7], 0).await;
    flush_region(&engine, region_id, None).await;
    let inputs = sst_ids(&engine, region_id);
    assert!(inputs.iter().all(|id| cached(*id)));
    assert_eq!(2, blob_files(&env).await);

    // Scans and compaction read the cached sidecars, not the remote ones.
    delete_remote_sidecars(&env).await;
    let expected: Vec<_> = (0..8).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
    // The output keeps pointing at the cached sidecars of its inputs.
    compact(&engine, region_id).await;
    assert!(
        sst_ids(&engine, region_id)
            .iter()
            .all(|id| !inputs.contains(id))
    );
    assert_eq!(
        inputs.iter().copied().collect::<HashSet<_>>(),
        referenced_blobs(&engine, region_id)
    );
    assert!(inputs.iter().all(|id| cached(*id)));
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
}

#[tokio::test]
async fn test_sidecars_bypass_write_cache_when_disabled() {
    let mut env = TestEnv::new().await;
    let cache_dir = create_temp_dir("blob_write_cache");
    let mut config = MitoConfig::default().enable_write_cache(
        cache_dir.path().to_str().unwrap().to_string(),
        ReadableSize::mb(512),
        None,
    );
    config.write_cache_blobs = false;
    let engine = env.create_engine(config).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let file_cache = engine.cache_manager().write_cache().unwrap().file_cache();
    let cached =
        |file_id| file_cache.contains_key(&IndexKey::new(region_id, file_id, FileType::Blob));

    put(&engine, region_id, &schema, &[0, 1, 2, 3], 0).await;
    flush_region(&engine, region_id, None).await;
    put(&engine, region_id, &schema, &[4, 5, 6, 7], 0).await;
    flush_region(&engine, region_id, None).await;
    assert_eq!(2, blob_files(&env).await);

    let expected: Vec<_> = (0..8).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
    compact(&engine, region_id).await;
    assert_eq!(2, blob_files(&env).await);
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
    // Reads must not refill the cache with sidecars either.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(sst_ids(&engine, region_id).iter().all(|id| !cached(*id)));
}

#[tokio::test]
async fn test_compaction_keeps_references_to_input_sidecars() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put(&engine, region_id, &schema, &[0, 1, 2, 3], 0).await;
    flush_region(&engine, region_id, None).await;
    put(&engine, region_id, &schema, &[4, 5, 6, 7], 0).await;
    flush_region(&engine, region_id, None).await;
    let inputs: HashSet<_> = sst_ids(&engine, region_id).into_iter().collect();

    compact(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let outputs: Vec<_> = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .collect();
    assert_eq!(1, outputs.len());
    let output = outputs[0].meta_ref();
    // No value is copied: the output has no sidecar and references both inputs' in full.
    assert_eq!(0, output.blob_file_size);
    assert_eq!(
        inputs,
        output.blob_refs.iter().map(|blob| blob.file_id).collect()
    );
    assert!(
        output
            .blob_refs
            .iter()
            .all(|blob| blob.referenced_bytes == blob.size)
    );
    // Row group sizing counts kept values at their decoded size, like the inputs did.
    assert!(output.max_row_group_decoded_size >= payload(0, 0).len() as u64);
    assert_stored_blobs_are_referenced(&env, &engine, region_id).await;

    let expected: Vec<_> = (0..8).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
    // The output has no sidecar of its own, yet its references must not reach predicates.
    let filtered = scan(
        &engine,
        region_id,
        ScanRequest {
            filters: vec![col("field_0").eq(lit(payload(4, 0)))],
            ..Default::default()
        },
    )
    .await;
    assert_eq!(vec![(4000, payload(4, 0))], filtered);
    // The references survive in the manifest.
    let table_dir = region.access_layer.table_dir().to_string();
    drop(version);
    drop(region);
    reopen_region(&engine, region_id, table_dir, true, Default::default()).await;
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
}

#[tokio::test]
async fn test_compaction_keeps_references_next_to_inputs_without_sidecars() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put(&engine, region_id, &schema, &[0, 2], 0).await;
    flush_region(&engine, region_id, None).await;
    let with_sidecar = sst_ids(&engine, region_id)[0];
    // Only short values: this SST has no sidecar.
    put(&engine, region_id, &schema, &[1, 5], 0).await;
    flush_region(&engine, region_id, None).await;

    compact(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let output = version.ssts.levels()[1].files().next().unwrap().meta_ref();
    assert_eq!(0, output.blob_file_size);
    assert_eq!(
        vec![with_sidecar],
        output
            .blob_refs
            .iter()
            .map(|blob| blob.file_id)
            .collect::<Vec<_>>()
    );
    let expected: Vec<_> = [0, 1, 2, 5]
        .into_iter()
        .map(|i| (i as i64 * 1000, payload(i, 0)))
        .collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
}

async fn pick_automatic(engine: &MitoEngine, region_id: RegionId) -> Option<PickerOutput> {
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let compaction_region = CompactionRegion {
        region_id,
        region_options: version.options.clone(),
        engine_config: Arc::new(MitoConfig::default()),
        region_metadata: version.metadata.clone(),
        cache_manager: engine.cache_manager(),
        access_layer: region.access_layer.clone(),
        manifest_ctx: region.manifest_ctx.clone(),
        current_version: version.into(),
        file_purger: None,
        ttl: None,
        max_parallelism: 1,
        plugins: common_base::Plugins::new(),
    };
    pick_automatic_for_test(&compaction_region, &mut PickHistory::default(), None)
        .await
        .unwrap()
}

/// Returns the physical values of `column` in the SST `file_id`, without resolving references.
async fn physical_values(env: &TestEnv, file_id: FileId, column: &str) -> Vec<String> {
    let store = env.get_object_store().unwrap();
    let suffix = format!("{file_id}.parquet");
    for entry in store.list_with("").recursive(true).await.unwrap() {
        if !entry.path().ends_with(&suffix) {
            continue;
        }
        let data = store.read(entry.path()).await.unwrap().to_bytes();
        let reader = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .build()
            .unwrap();
        let mut values = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let column = batch.column_by_name(column).unwrap().as_string::<i32>();
            values.extend(column.iter().map(|v| v.unwrap().to_string()));
        }
        return values;
    }
    panic!("SST {file_id} not found");
}

#[tokio::test]
async fn test_compaction_copies_reference_like_values_of_undeclared_inputs() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put(&engine, region_id, &schema, &[0], 0).await;
    flush_region(&engine, region_id, None).await;
    let old = sst_ids(&engine, region_id)[0];
    let real_ref = physical_values(&env, old, "field_0").await.remove(0);

    // `old` predates `field_1`, so its footer does not declare it external, and compaction
    // fills it with a default that is byte-identical to a real reference into `old`.
    let field_1 = ColumnSchema::new("field_1", ConcreteDataType::string_datatype(), true)
        .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(
            real_ref.as_str(),
        ))))
        .unwrap()
        .with_storage_options(StorageOptions::default())
        .unwrap();
    let alter = RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: field_1,
                    semantic_type: SemanticType::Field,
                    column_id: 3,
                },
                location: None,
            }],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(alter))
        .await
        .unwrap();
    let mut schema = schema;
    schema.push(api::v1::ColumnSchema {
        column_name: "field_1".to_string(),
        datatype: api::v1::ColumnDataType::String as i32,
        semantic_type: api::v1::SemanticType::Field as i32,
        ..Default::default()
    });
    let rows = vec![row(vec![
        ValueData::StringValue("a".to_string()),
        ValueData::StringValue(payload(2, 0)),
        ValueData::TimestampMillisecondValue(2000),
        ValueData::StringValue(payload(4, 0)),
    ])];
    put_rows(&engine, region_id, Rows { schema, rows }).await;
    flush_region(&engine, region_id, None).await;

    compact(&engine, region_id).await;
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let mut values = Vec::new();
    for batch in batches.iter() {
        let column = batch
            .df_record_batch()
            .column_by_name("field_1")
            .unwrap()
            .as_string::<i32>();
        values.extend(column.iter().map(|v| v.unwrap().to_string()));
    }
    assert_eq!(vec![real_ref, payload(4, 0)], values);
}

/// Writes `first` to one SST, overwrites `overwritten` in a second one, and compacts twice:
/// after the first compaction the first sidecar is mostly garbage, so the second one copies
/// its live values out. Returns the ids of the first and the second sidecar.
async fn rewrite_garbage_sidecar(
    engine: &MitoEngine,
    region_id: RegionId,
    first: &[usize],
    overwritten: &[usize],
) -> (FileId, FileId) {
    let schema = rows_schema(&create_request(true));
    put(engine, region_id, &schema, first, 0).await;
    flush_region(engine, region_id, None).await;
    let sidecar = sst_ids(engine, region_id)[0];
    put(engine, region_id, &schema, overwritten, 1).await;
    flush_region(engine, region_id, None).await;
    let second = sst_ids(engine, region_id)
        .into_iter()
        .find(|id| *id != sidecar)
        .unwrap();
    compact(engine, region_id).await;
    assert!(referenced_blobs(engine, region_id).contains(&sidecar));

    put(engine, region_id, &schema, &[8], 0).await;
    flush_region(engine, region_id, None).await;
    compact(engine, region_id).await;
    (sidecar, second)
}

#[tokio::test]
async fn test_compaction_rewrites_sidecars_that_are_mostly_garbage() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(region_id, RegionRequest::Create(create_request(true)))
        .await
        .unwrap();
    let (first, second) =
        rewrite_garbage_sidecar(&engine, region_id, &[0, 2, 4, 6], &[0, 2, 4]).await;

    let expected = vec![
        (0, payload(0, 1)),
        (2000, payload(2, 1)),
        (4000, payload(4, 1)),
        (6000, payload(6, 0)),
        (8000, payload(8, 0)),
    ];
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
    // Only the garbage sidecar is copied out; the output keeps referencing the other one.
    let referenced = referenced_blobs(&engine, region_id);
    assert!(!referenced.contains(&first));
    assert!(referenced.contains(&second));
    assert_stored_blobs_are_referenced(&env, &engine, region_id).await;
}

#[tokio::test]
async fn test_rewritten_sidecar_evacuates_values_that_look_like_references() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(region_id, RegionRequest::Create(create_request(true)))
        .await
        .unwrap();
    // Row 3 holds a value that starts like a reference, so it always sits in a sidecar.
    let (first, _) = rewrite_garbage_sidecar(&engine, region_id, &[0, 2, 3, 4], &[0, 2, 4]).await;

    let expected = vec![
        (0, payload(0, 1)),
        (2000, payload(2, 1)),
        (3000, payload(3, 0)),
        (4000, payload(4, 1)),
        (8000, payload(8, 0)),
    ];
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
    assert!(!referenced_blobs(&engine, region_id).contains(&first));
    assert_stored_blobs_are_referenced(&env, &engine, region_id).await;
}

/// A hex value that compresses only about two to one, unlike [payload].
fn noisy_payload(i: usize, version: usize) -> String {
    let mut state = (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version as u64;
    (0..256 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from_digit((state & 0xF) as u32, 16).unwrap()
        })
        .collect()
}

#[tokio::test]
async fn test_files_pointing_at_garbage_sidecars_are_rewritten_without_new_data() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let put = |ts: &[usize], version: usize| {
        let rows = ts
            .iter()
            .map(|i| {
                row(vec![
                    ValueData::StringValue("a".to_string()),
                    ValueData::StringValue(noisy_payload(*i, version)),
                    ValueData::TimestampMillisecondValue(*i as i64 * 1000),
                ])
            })
            .collect();
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows,
            },
        )
    };
    put(&[0, 1, 2, 3], 0).await;
    flush_region(&engine, region_id, None).await;
    let first = sst_ids(&engine, region_id)[0];
    put(&[0, 1, 2], 1).await;
    flush_region(&engine, region_id, None).await;
    compact(&engine, region_id).await;
    assert!(referenced_blobs(&engine, region_id).contains(&first));

    // The window gets no new data, yet the file that keeps the garbage alive is rewritten,
    // by automatic and manual compaction alike.
    let picked = pick_automatic(&engine, region_id).await.unwrap();
    let inputs: Vec<_> = picked
        .outputs
        .iter()
        .flat_map(|output| &output.inputs)
        .collect();
    assert_eq!(1, inputs.len());
    assert!(
        inputs[0]
            .meta_ref()
            .blob_files()
            .any(|blob| blob.file_id == first)
    );
    // The picked handles would keep the garbage sidecar alive.
    drop(picked);
    compact(&engine, region_id).await;
    assert!(!referenced_blobs(&engine, region_id).contains(&first));
    let expected: Vec<_> = [(0, 1), (1, 1), (2, 1), (3, 0)]
        .into_iter()
        .map(|(i, version)| (i as i64 * 1000, noisy_payload(i, version)))
        .collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
    assert_stored_blobs_are_referenced(&env, &engine, region_id).await;
}

#[tokio::test]
async fn test_compaction_edit_rejects_references_to_dropped_sidecars() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put(&engine, region_id, &schema, &[0, 2], 0).await;
    flush_region(&engine, region_id, None).await;
    let input = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()[0]
        .files()
        .next()
        .unwrap()
        .meta_ref()
        .clone();
    // An output pointing at a sidecar that no file of the region references any more, as
    // when its inputs were removed while the compaction ran.
    let output = FileMeta {
        file_id: FileId::random(),
        blob_file_size: 0,
        blob_refs: vec![BlobFileRef {
            file_id: FileId::random(),
            size: 10,
            referenced_bytes: 10,
        }],
        ..input.clone()
    };
    let edit = RegionEdit {
        files_to_add: vec![output],
        files_to_remove: vec![input],
        ..Default::default()
    };
    assert!(engine.edit_region(region_id, edit).await.is_err());
}
