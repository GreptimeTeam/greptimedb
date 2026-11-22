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
use datatypes::schema::StorageOptions;
use store_api::region_engine::{MitoCopyRegionFromRequest, RegionEngine};
use store_api::region_request::{RegionCreateRequest, RegionRequest};
use store_api::storage::{FileId, RegionId, ScanRequest};

use crate::cache::file_cache::{FileType, IndexKey};
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
                    file.meta_ref().max_row_group_uncompressed_size
                        >= 2 * payload(0, 0).len() as u64
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
    assert!(output.max_row_group_uncompressed_size >= payload(0, 0).len() as u64);
    assert_stored_blobs_are_referenced(&env, &engine, region_id).await;

    let expected: Vec<_> = (0..8).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );
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

/// Writes `first` to one SST, overwrites `overwritten` in a second one, and compacts twice:
/// after the first compaction the first sidecar is mostly garbage, so the second one copies
/// its live values out. Returns the id of the first sidecar.
async fn rewrite_garbage_sidecar(
    engine: &MitoEngine,
    region_id: RegionId,
    first: &[usize],
    overwritten: &[usize],
) -> FileId {
    let schema = rows_schema(&create_request(true));
    put(engine, region_id, &schema, first, 0).await;
    flush_region(engine, region_id, None).await;
    let sidecar = sst_ids(engine, region_id)[0];
    put(engine, region_id, &schema, overwritten, 1).await;
    flush_region(engine, region_id, None).await;
    compact(engine, region_id).await;
    assert!(referenced_blobs(engine, region_id).contains(&sidecar));

    put(engine, region_id, &schema, &[8], 0).await;
    flush_region(engine, region_id, None).await;
    compact(engine, region_id).await;
    sidecar
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
    let first = rewrite_garbage_sidecar(&engine, region_id, &[0, 2, 4, 6], &[0, 2, 4]).await;

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
    assert!(!referenced_blobs(&engine, region_id).contains(&first));
    assert_stored_blobs_are_referenced(&env, &engine, region_id).await;
}

#[tokio::test]
async fn test_rewritten_sidecar_keeps_values_that_look_like_references() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(region_id, RegionRequest::Create(create_request(true)))
        .await
        .unwrap();
    // Row 3 holds a value that starts like a reference, so it always sits in a sidecar.
    let first = rewrite_garbage_sidecar(&engine, region_id, &[0, 2, 3, 4], &[0, 2, 4]).await;

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
    // Handing that value over inline would make it a reference, so it keeps its own.
    assert!(referenced_blobs(&engine, region_id).contains(&first));
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

    // The window gets no new data, yet the file that keeps the garbage alive is rewritten.
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
