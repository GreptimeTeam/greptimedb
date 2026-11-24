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
use std::{assert_matches, fs};

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use object_store::layers::mock::{Error as MockError, ErrorKind, MockLayerBuilder};
use store_api::region_engine::{MitoCopyRegionFromRequest, RegionEngine, RegionRole};
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::error::Error;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[cfg(feature = "enterprise")]
#[rstest::rstest]
#[case::shared_only(false, false)]
#[case::mixed(true, false)]
#[case::metadata_rejected_before_copy(true, true)]
#[tokio::test]
async fn test_copy_shared_reference_contract(#[case] mixed: bool, #[case] metadata: bool) {
    use crate::manifest::action::RegionEdit;
    use crate::sst::file::{FileMeta, LEVEL_SHARED, RegionFileId};
    use crate::test_util::flush_region;
    use std::collections::HashSet;
    use store_api::region_request::PathType;

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let owner_id = RegionId::new(1, 0);
    let source_id = RegionId::new(1, 1);
    let target_id = RegionId::new(1, 2);
    let path_type = if metadata {
        PathType::Metadata
    } else {
        PathType::Bare
    };
    let mut create = CreateRequestBuilder::new().build();
    create.path_type = path_type;
    for id in [owner_id, source_id, target_id] {
        engine
            .handle_request(id, RegionRequest::Create(create.clone()))
            .await
            .unwrap();
    }
    for (id, start) in [(owner_id, 0), (source_id, 10)] {
        if id == source_id && !mixed {
            continue;
        }
        put_rows(
            &engine,
            id,
            Rows {
                schema: rows_schema(&create),
                rows: build_rows(start, start + 3),
            },
        )
        .await;
        flush_region(&engine, id, None).await;
    }
    let owner = engine.get_region(owner_id).unwrap();
    let shared = FileMeta {
        level: LEVEL_SHARED,
        ..owner.version().ssts.levels()[0]
            .files()
            .next()
            .unwrap()
            .meta_ref()
            .clone()
    };
    for id in [owner_id, source_id] {
        engine
            .edit_region(
                id,
                RegionEdit {
                    files_to_add: vec![shared.clone()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let source = engine
        .get_region(source_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    let target = engine.get_region(target_id).unwrap();
    let before = target.manifest_ctx.manifest().await;
    let target_path = |file_id| {
        crate::sst::location::sst_file_path(
            target.access_layer.table_dir(),
            RegionFileId::new(target_id, file_id),
            path_type,
        )
    };
    let result = engine
        .copy_region_from(
            target_id,
            MitoCopyRegionFromRequest {
                source_region_id: source_id,
                parallelism: 1,
            },
        )
        .await;
    if metadata {
        assert!(matches!(result, Err(Error::InvalidRequest { .. })));
        let after = target.manifest_ctx.manifest().await;
        assert_eq!(before.manifest_version, after.manifest_version);
        assert_eq!(before.files, after.files);
        assert_eq!(0, target.version_control.current().last_sequence);
        for id in source.files.keys() {
            assert!(
                !target
                    .access_layer
                    .object_store()
                    .exists(&target_path(*id))
                    .await
                    .unwrap()
            );
        }
        return;
    }
    let expected_ids = source.files.keys().copied().collect::<HashSet<_>>();
    assert_eq!(
        expected_ids,
        result
            .unwrap()
            .copied_file_ids
            .into_iter()
            .collect::<HashSet<_>>()
    );
    let installed = target.manifest_ctx.manifest().await;
    assert_eq!(
        expected_ids,
        installed.files.keys().copied().collect::<HashSet<_>>()
    );
    for file in installed.files.values() {
        assert_eq!(
            if file.level == LEVEL_SHARED {
                owner_id
            } else {
                target_id
            },
            file.region_id
        );
        assert_eq!(
            file.level != LEVEL_SHARED,
            target
                .access_layer
                .object_store()
                .exists(&target_path(file.file_id))
                .await
                .unwrap()
        );
        assert_eq!(Some(1), file.sequence.map(|sequence| sequence.get()));
    }
    let sequence = target.version_control.current().last_sequence;
    let retry = engine
        .copy_region_from(
            target_id,
            MitoCopyRegionFromRequest {
                source_region_id: source_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        expected_ids,
        retry.copied_file_ids.into_iter().collect::<HashSet<_>>()
    );
    assert_eq!(installed.files, target.manifest_ctx.manifest().await.files);
    assert_eq!(sequence, target.version_control.current().last_sequence);
    let rows = RecordBatches::try_collect(
        engine
            .scan_to_stream(target_id, Default::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        if mixed { 6 } else { 3 },
        rows.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

#[tokio::test]
async fn test_engine_copy_region_from() {
    common_telemetry::init_default_ut_logging();

    test_engine_copy_region_from_with_format(true, true).await;
    test_engine_copy_region_from_with_format(true, false).await;
    test_engine_copy_region_from_with_format(false, true).await;
    test_engine_copy_region_from_with_format(false, false).await;
}

async fn test_engine_copy_region_from_with_format(flat_format: bool, with_index: bool) {
    let mut env = TestEnv::with_prefix("copy-region-from").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    // Creates a source region and adds some data
    let source_region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    if with_index {
        request
            .column_metadatas
            .iter_mut()
            .find(|c| c.column_schema.name == "tag_0")
            .unwrap()
            .column_schema
            .set_inverted_index(true);
    }

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(source_region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 42),
    };
    put_rows(&engine, source_region_id, rows).await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    // Creates a target region and enters staging mode
    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    common_telemetry::debug!("copy region from");
    let resp = engine
        .copy_region_from(
            target_region_id,
            MitoCopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap();

    let manifest = engine
        .get_region(target_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert!(!manifest.files.is_empty());
    for meta in manifest.files.values() {
        assert_eq!(meta.region_id, target_region_id);
        assert_eq!(meta.exists_index(), with_index);
    }

    let source_region_dir = format!("{}/data/test/1_0000000001", env.data_home().display());
    let source_region_files = collect_filename_in_dir(&source_region_dir);
    let target_region_dir = format!("{}/data/test/1_0000000002", env.data_home().display());
    let target_region_files = collect_filename_in_dir(&target_region_dir);
    assert_eq!(source_region_files, target_region_files);

    if with_index {
        let source_region_index_files =
            collect_filename_in_dir(&format!("{}/index", source_region_dir));
        let target_region_index_files =
            collect_filename_in_dir(&format!("{}/index", target_region_dir));
        assert_eq!(source_region_index_files, target_region_index_files);
    }
    common_telemetry::debug!("copy region from again");
    let resp2 = engine
        .copy_region_from(
            target_region_id,
            MitoCopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.copied_file_ids, resp2.copied_file_ids);
}

#[tokio::test]
async fn test_engine_copy_region_failure() {
    common_telemetry::init_default_ut_logging();
    test_engine_copy_region_failure_with_format(false).await;
    test_engine_copy_region_failure_with_format(true).await;
}

async fn test_engine_copy_region_failure_with_format(flat_format: bool) {
    let mock_layer = MockLayerBuilder::default()
        .copy_interceptor(Arc::new(|from, _, _args| {
            if from.contains(".puffin") {
                Some(Err(MockError::new(ErrorKind::Unexpected, "mock err")))
            } else {
                None
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    // Creates a source region and adds some data
    let source_region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request
        .column_metadatas
        .iter_mut()
        .find(|c| c.column_schema.name == "tag_0")
        .unwrap()
        .column_schema
        .set_inverted_index(true);

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(source_region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 42),
    };
    put_rows(&engine, source_region_id, rows).await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    let source_region_dir = format!("{}/data/test/1_0000000001", env.data_home().display());
    assert_file_num_in_dir(&source_region_dir, 1);
    assert_file_num_in_dir(&format!("{}/index", source_region_dir), 1);
    let source_region_files = collect_filename_in_dir(&source_region_dir);
    let source_region_index_files =
        collect_filename_in_dir(&format!("{}/index", source_region_dir));

    // Creates a target region and enters staging mode
    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let err = engine
        .copy_region_from(
            target_region_id,
            MitoCopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::StorageUnavailable);

    // Check target region directory is empty
    let target_region_dir = format!("{}/data/test/1_0000000002", env.data_home().display());
    assert_file_num_in_dir(&target_region_dir, 0);
    assert!(!fs::exists(format!("{}/index", target_region_dir)).unwrap());

    // Check source region directory is not affected
    let source_region_dir = format!("{}/data/test/1_0000000001", env.data_home().display());
    assert_file_num_in_dir(&source_region_dir, 1);
    assert_file_num_in_dir(&format!("{}/index", source_region_dir), 1);
    assert_eq!(
        source_region_files,
        collect_filename_in_dir(&source_region_dir)
    );
    assert_eq!(
        source_region_index_files,
        collect_filename_in_dir(&format!("{}/index", source_region_dir))
    );
}

fn assert_file_num_in_dir(dir: &str, expected_num: usize) {
    let files = fs::read_dir(dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|f| f.metadata().unwrap().is_file())
        .collect::<Vec<_>>();
    assert_eq!(
        files.len(),
        expected_num,
        "The number of files in the directory should be {}, got: {:?}",
        expected_num,
        files
    );
}

fn collect_filename_in_dir(dir: &str) -> Vec<String> {
    let mut files = fs::read_dir(dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|f| f.metadata().unwrap().is_file())
        .map(|f| {
            f.path()
                .to_string_lossy()
                .rsplit("/")
                .last()
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    files.sort_unstable();

    files
}

#[tokio::test]
async fn test_engine_copy_region_invalid_args() {
    common_telemetry::init_default_ut_logging();
    test_engine_copy_region_invalid_args_with_format(false).await;
    test_engine_copy_region_invalid_args_with_format(true).await;
}

async fn test_engine_copy_region_invalid_args_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let err = engine
        .copy_region_from(
            region_id,
            MitoCopyRegionFromRequest {
                source_region_id: RegionId::new(2, 1),
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    let err = engine
        .copy_region_from(
            region_id,
            MitoCopyRegionFromRequest {
                source_region_id: RegionId::new(1, 1),
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);
}

#[tokio::test]
async fn test_engine_copy_region_unexpected_state() {
    common_telemetry::init_default_ut_logging();
    test_engine_copy_region_unexpected_state_with_format(false).await;
    test_engine_copy_region_unexpected_state_with_format(true).await;
}

async fn test_engine_copy_region_unexpected_state_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();

    let err = engine
        .copy_region_from(
            region_id,
            MitoCopyRegionFromRequest {
                source_region_id: RegionId::new(1, 2),
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.as_any().downcast_ref::<Error>().unwrap(),
        Error::RegionState { .. }
    )
}

/// Regression for #8865: `copy_region_from` must not carry the source region's
/// `FileMeta::preserve_row_sequence` trust marker into the target region, whose
/// sequence domain is independent. The copied file's physical per-row sequences
/// belong to the source region only; trusting them in the target would let an
/// exact sequence-range request replay source rows as if they were target
/// sequences. The marker must be cleared so the target fails closed with
/// `SequenceRangeUnsupported` instead.
#[tokio::test]
async fn test_copy_region_from_clears_preserve_row_sequence_marker() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("copy-region-from-preserve-sequence").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    // Source and target regions both preserve per-row sequences.
    let source_region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = rows_schema(&request);

    engine
        .handle_request(source_region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 42),
    };
    put_rows(&engine, source_region_id, rows).await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    // The flushed source file keeps the trust marker: the source region's own
    // sequence domain is intact.
    let source_manifest = engine
        .get_region(source_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(1, source_manifest.files.len());
    assert!(
        source_manifest
            .files
            .values()
            .all(|meta| meta.preserve_row_sequence),
        "source files must keep the preserve_row_sequence marker"
    );

    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .copy_region_from(
            target_region_id,
            MitoCopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap();

    // The copied file in the target must NOT be trusted: its physical per-row
    // sequences belong to the source region's independent sequence domain.
    // Both the `preserve_row_sequence` marker and the source-domain max
    // `sequence` hint must be cleared, so exact reads fail closed instead of
    // silently skipping the file as "proven disjoint".
    let target_manifest = engine
        .get_region(target_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(1, target_manifest.files.len());
    assert!(
        target_manifest
            .files
            .values()
            .all(|meta| !meta.preserve_row_sequence),
        "copied files must have the preserve_row_sequence marker cleared"
    );
    assert!(
        target_manifest
            .files
            .values()
            .all(|meta| meta.sequence.is_none()),
        "copied files must have their source-domain sequence hint cleared"
    );

    // An exact sequence-range request intersecting the copied rows' sequences
    // (rows 3..=7 in the source domain) must fail closed with
    // `SequenceRangeUnsupported` instead of replaying source-domain rows.
    let err = engine
        .scanner(
            target_region_id,
            ScanRequest {
                memtable_min_sequence: Some(2),
                memtable_max_sequence: Some(7),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected sequence-range unsupported error");
    assert!(
        matches!(err, Error::SequenceRangeUnsupported { .. }),
        "unexpected err: {err}"
    );
    assert_eq!(err.status_code(), StatusCode::Unsupported);

    // Sanity: the source region keeps the marker and still serves the same
    // exact range with its own row-level sequences (3..=7, 5 rows).
    let stream = engine
        .scan_to_stream(
            source_region_id,
            ScanRequest {
                memtable_min_sequence: Some(2),
                memtable_max_sequence: Some(7),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        5, row_count,
        "source exact range should return rows with sequence 3..=7"
    );
}

#[rstest::rstest]
#[case::samples_only(false, false, false)]
#[case::physical_copy(false, true, false)]
#[case::remap_references(true, true, false)]
#[case::copy_retry(false, true, true)]
#[tokio::test]
async fn test_narthex_copy_and_remap(
    #[case] remap: bool,
    #[case] with_index: bool,
    #[case] fail_copy: bool,
) {
    use crate::manifest::action::RegionEdit;
    use crate::sst::file::FileFormat;
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;
    use crate::test_util::{flush_region, reopen_region};
    use datatypes::value::Value;
    use partition::expr::col;
    use std::collections::HashMap;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::region_engine::{RemapManifestsRequest, SettableRegionRoleState};
    use store_api::region_request::{
        ApplyStagingManifestRequest, EnterStagingRequest, RegionDropRequest,
        StagingPartitionDirective,
    };

    let mut env = TestEnv::new().await.with_file_ref_manager(Arc::new(
        crate::sst::file_ref::FileReferenceManager::with_gc_enabled(None, true),
    ));
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            gc: crate::gc::GcConfig {
                enable: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    let source = RegionId::new(2, 1);
    // Different decimal width exercises the rewritten manifest's byte counts.
    let target = RegionId::new(2, u32::MAX);
    let mut request = CreateRequestBuilder::new()
        .tag_num(1)
        .field_num(1)
        .insert_option("primary_key_encoding", "sparse")
        .insert_option("sst_format", "narthex")
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let reserved = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    request
        .column_metadatas
        .extend(reserved.column_metadatas.into_iter().take(2));
    request
        .primary_key
        .splice(0..0, reserved.primary_key.into_iter().take(2));
    for region in [source, target] {
        engine
            .handle_request(region, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    let metadata = engine.get_region(source).unwrap().metadata();
    let put = |label: &str, tsid, value| {
        let key = crate::test_util::sst_util::new_sparse_primary_key(&[label], &metadata, 42, tsid);
        crate::test_util::sparse_metric_put(key, 1000, Some(value))
    };
    for (label, tsid) in [("api", 7), ("worker", 8)] {
        engine
            .handle_request(source, put(label, tsid, 1.0))
            .await
            .unwrap();
        flush_region(&engine, source, None).await;
    }
    let owner = engine.get_region(source).unwrap();
    let sources: Vec<_> = owner
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values().cloned())
        .collect();
    if with_index {
        let index =
            crate::sst::narthex::write_series(&owner.access_layer, owner.metadata(), &sources)
                .await
                .unwrap();
        engine
            .edit_region(
                source,
                RegionEdit {
                    series_indexes_to_add: vec![index],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let source_dir = owner.access_layer.build_region_dir(source);
    let before = owner.manifest_ctx.manifest().await;
    if fail_copy {
        let missing = before
            .files
            .values()
            .next()
            .unwrap()
            .narthex_objects(&source_dir)
            .unwrap()[0]
            .path
            .clone();
        let store = owner.access_layer.object_store();
        let saved = store.read(&missing).await.unwrap().to_bytes();
        store.delete(&missing).await.unwrap();
        assert!(
            engine
                .copy_region_from(
                    target,
                    MitoCopyRegionFromRequest {
                        source_region_id: source,
                        parallelism: 2,
                    }
                )
                .await
                .is_err()
        );
        let destination = engine.get_region(target).unwrap();
        let failed = destination.manifest_ctx.manifest().await;
        assert!(failed.files.is_empty());
        assert!(failed.series_indexes.is_empty());
        let from = owner.access_layer.build_region_dir(source);
        let to = owner.access_layer.build_region_dir(target);
        for object in before
            .all_files()
            .flat_map(|file| file.narthex_objects(&source_dir).unwrap())
        {
            let target_path = format!("{to}{}", object.path.strip_prefix(&from).unwrap());
            assert!(
                !store.exists(&target_path).await.unwrap(),
                "partial copy leaked {target_path}"
            );
        }
        for file in before.all_files() {
            let dir = crate::sst::location::narthex_file_dir(&to, file.file_id);
            assert!(
                !store.exists(&dir).await.unwrap(),
                "partial copy leaked {dir}"
            );
        }
        store.write(&missing, saved).await.unwrap();
    }
    if remap {
        engine
            .set_region_role_state_gracefully(source, SettableRegionRoleState::StagingLeader)
            .await
            .unwrap();
        let partition_expr = col("tag_0")
            .eq(Value::String("api".into()))
            .as_json_str()
            .unwrap();
        let result = engine
            .remap_manifests(RemapManifestsRequest {
                region_id: source,
                input_regions: vec![source],
                region_mapping: HashMap::from([(source, vec![target])]),
                new_partition_exprs: HashMap::from([(target, partition_expr.clone())]),
            })
            .await
            .unwrap();
        engine
            .handle_request(
                target,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                        partition_expr.clone(),
                    ),
                }),
            )
            .await
            .unwrap();
        engine
            .handle_request(
                target,
                RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                    partition_expr,
                    central_region_id: source,
                    manifest_path: result.manifest_paths[&target].clone(),
                }),
            )
            .await
            .unwrap();
    } else {
        let results = futures::future::join_all((0..2).map(|_| {
            engine.copy_region_from(
                target,
                MitoCopyRegionFromRequest {
                    source_region_id: source,
                    parallelism: 2,
                },
            )
        }))
        .await;
        for result in results {
            assert_eq!(result.unwrap().copied_file_ids.len(), 2);
        }
    }
    let destination = engine.get_region(target).unwrap();
    let after = destination.manifest_ctx.manifest().await;
    assert_eq!(after.files.len(), 2);
    assert_eq!(after.committed_sequence, if remap { Some(1) } else { None });
    for file in after.files.values() {
        assert_eq!(
            file.sequence.map(|s| s.get()),
            if remap { Some(1) } else { None }
        );
        assert!(!file.preserve_row_sequence);
        assert!(matches!(file.format, FileFormat::NarthexSamples(_)));
        assert_eq!(file.is_effective_target_sequence_trusted(target), remap);
    }

    assert_eq!(after.series_indexes.len(), usize::from(with_index));
    let physical_owner = if remap { source } else { target };
    for meta in after.all_files() {
        assert_eq!(meta.region_id, physical_owner);
        let prefix = crate::sst::location::narthex_file_dir(
            &destination.access_layer.build_region_dir(physical_owner),
            meta.file_id,
        );
        assert!(
            meta.narthex_objects(&destination.access_layer.build_region_dir(physical_owner))
                .unwrap()
                .iter()
                .all(|object| object.path.starts_with(&prefix))
        );
        for object in meta
            .narthex_objects(&destination.access_layer.build_region_dir(physical_owner))
            .unwrap()
        {
            let actual = destination
                .access_layer
                .object_store()
                .stat(&object.path)
                .await
                .unwrap();
            assert_eq!(actual.content_length(), object.file_size);
        }
        if let FileFormat::NarthexSeriesIndex(index) = &meta.format {
            assert_eq!(index.sources.len(), 2);
            assert!(
                index
                    .sources
                    .iter()
                    .all(|id| id.region_id() == physical_owner)
            );
            if remap {
                assert_eq!(meta, &before.series_indexes[&meta.file_id]);
            }
        }
    }
    // Physical copies survive complete deletion of their source. Remaps retain
    // source-owned objects via references and use the partial-drop contract.
    drop(sources);
    drop(owner);
    engine
        .handle_request(
            source,
            RegionRequest::Drop(RegionDropRequest {
                partial_drop: remap,
                fast_path: false,
                force: false,
            }),
        )
        .await
        .unwrap();
    if !remap {
        let paths: Vec<_> = before
            .all_files()
            .flat_map(|file| file.narthex_objects(&source_dir).unwrap())
            .map(|object| object.path.clone())
            .collect();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let mut exists = false;
                for path in &paths {
                    exists |= destination
                        .access_layer
                        .object_store()
                        .exists(path)
                        .await
                        .unwrap();
                }
                if !exists {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    reopen_region(
        &engine,
        target,
        request.table_dir.clone(),
        true,
        request.options.clone(),
    )
    .await;
    let exact = engine
        .scan_to_stream(
            target,
            ScanRequest {
                memtable_min_sequence: Some(0),
                memtable_max_sequence: Some(1),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await;
    let stream = if remap {
        exact.unwrap()
    } else {
        assert!(exact.is_err());
        engine
            .scan_to_stream(target, ScanRequest::default())
            .await
            .unwrap()
    };
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        if remap { 1 } else { 2 }
    );
    let text = batches.pretty_print().unwrap();
    assert!(text.contains("api"));
    assert_eq!(text.contains("worker"), !remap);
    assert_eq!(
        engine
            .get_region(target)
            .unwrap()
            .version()
            .ssts
            .series_indexes()
            .len(),
        usize::from(with_index)
    );
    crate::engine::compaction_test::compact(&engine, target).await;
    let compacted = engine.get_region(target).unwrap();
    assert_eq!(
        compacted.version().ssts.series_indexes().len(),
        usize::from(with_index)
    );
    assert!(
        compacted
            .version()
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files.values())
            .all(|file| file.meta_ref().region_id == target
                && matches!(file.meta_ref().format, FileFormat::NarthexSamples(_)))
    );
    let result = RecordBatches::try_collect(
        engine
            .scan_to_stream(
                target,
                ScanRequest {
                    memtable_min_sequence: remap.then_some(0),
                    memtable_max_sequence: remap.then_some(1),
                    exact_sequence_range: remap,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(result.pretty_print().unwrap(), text);
    if with_index && !remap && !fail_copy {
        // The index is still present, but its original samples were compacted away.
        let next = RegionId::new(2, 3);
        engine
            .handle_request(next, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
        engine
            .copy_region_from(
                next,
                MitoCopyRegionFromRequest {
                    source_region_id: target,
                    parallelism: 2,
                },
            )
            .await
            .unwrap();
        let stream = engine
            .scan_to_stream(next, ScanRequest::default())
            .await
            .unwrap();
        assert_eq!(
            RecordBatches::try_collect(stream)
                .await
                .unwrap()
                .pretty_print()
                .unwrap(),
            text
        );
    }
    if remap {
        // Repartition admission remains target-local: a subsequent write wins
        // over imported rows even though the source had larger sequences.
        use futures::TryStreamExt;
        use store_api::region_engine::{BoundTag, QueryScanContext, RawLabelSource};
        engine
            .handle_request(target, put("api", 7, 99.0))
            .await
            .unwrap();
        assert_eq!(
            engine
                .get_region(target)
                .unwrap()
                .version_control
                .committed_sequence(),
            2
        );
        for compact in [false, true] {
            if compact {
                flush_region(&engine, target, None).await;
                crate::engine::compaction_test::compact(&engine, target).await;
            }
            let scan = engine
                .handle_query(
                    target,
                    ScanRequest {
                        distribution: Some(store_api::storage::TimeSeriesDistribution::PerSeries),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let binding = RawLabelSource::PrimaryKey {
                metric_name: Arc::from("copy"),
                physical_metadata: scan.metadata(),
                logical_tags: vec![BoundTag {
                    column_id: 0,
                    name: Arc::from("tag_0"),
                }]
                .into(),
            };
            let source = scan
                .prepare_series(
                    &QueryScanContext::default(),
                    &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
                    &binding,
                )
                .await
                .unwrap()
                .unwrap();
            let chunks = source
                .read_series(0..source.num_series())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let values: Vec<_> = chunks
                .iter()
                .flat_map(|chunk| {
                    (0..chunk.time.len()).map(|i| {
                        let datatypes::series::samples::SampleRef::Float(value) =
                            chunk.fields.get(0).unwrap().sample(i)
                        else {
                            panic!("expected float")
                        };
                        value
                    })
                })
                .collect();
            assert_eq!(values, vec![99.0]);
        }
    }
    engine.stop().await.unwrap();
}

// Storage-family rejection must happen before publication, including empty regions.
#[rstest::rstest]
#[case::parquet_to_narthex("flat", "narthex")]
#[case::narthex_to_parquet("narthex", "flat")]
#[tokio::test]
async fn test_region_storage_format_admission(
    #[case] source_format: &str,
    #[case] target_format: &str,
) {
    use crate::manifest::action::RegionEdit;
    use crate::sst::file::{FileFormat, FileMeta, NarthexSamplesMeta, NarthexSeriesIndexMeta};
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::region_request::{AlterKind, RegionAlterRequest, SetRegionOption};

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(Default::default()).await;
    let source = RegionId::new(42, 0);
    let target = RegionId::new(42, 1);
    for (id, format) in [(source, source_format), (target, target_format)] {
        let mut request = CreateRequestBuilder::new()
            .tag_num(1)
            .field_num(1)
            .insert_option("primary_key_encoding", "sparse")
            .insert_option("sst_format", format)
            .build();
        let reserved = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
        request
            .column_metadatas
            .extend(reserved.column_metadatas.into_iter().take(2));
        request
            .primary_key
            .splice(0..0, reserved.primary_key.into_iter().take(2));
        engine
            .handle_request(id, RegionRequest::Create(request))
            .await
            .unwrap();
    }
    let region = engine.get_region(target).unwrap();
    let before = region.manifest_ctx.manifest().await;
    let sequence = region.version_control.current().last_sequence;

    let error = engine
        .handle_request(
            target,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::Format(source_format.to_string())],
                },
            }),
        )
        .await
        .unwrap_err();
    assert!(error.output_msg().contains("to or from Narthex"), "{error}");

    // Reapplying the selected storage format is an idempotent operation.
    engine
        .handle_request(
            target,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::Format(target_format.to_string())],
                },
            }),
        )
        .await
        .unwrap();

    let error = engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert!(
        error
            .output_msg()
            .contains("Cannot copy between Parquet and Narthex"),
        "{error}"
    );

    let foreign_format = if source_format == "narthex" {
        FileFormat::NarthexSamples(Box::new(NarthexSamplesMeta {
            manifest_size: 1,
            parts: Default::default(),
        }))
    } else {
        FileFormat::Parquet
    };
    let error = engine
        .edit_region(
            target,
            RegionEdit {
                files_to_add: vec![FileMeta {
                    region_id: target,
                    format: foreign_format,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        error
            .output_msg()
            .contains("does not match the region storage format"),
        "{error}"
    );

    // Index metadata must not bypass the region's storage-family admission.
    let invalid_index_format = if target_format == "narthex" {
        FileFormat::Parquet
    } else {
        FileFormat::NarthexSeriesIndex(Box::new(NarthexSeriesIndexMeta {
            manifest_size: 1,
            parts: Default::default(),
            sources: Vec::new(),
        }))
    };
    let error = engine
        .edit_region(
            target,
            RegionEdit {
                series_indexes_to_add: vec![FileMeta {
                    region_id: target,
                    format: invalid_index_format,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        error
            .output_msg()
            .contains("Series indexes require Narthex"),
        "{error}"
    );
    let after = region.manifest_ctx.manifest().await;
    assert_eq!(before.manifest_version, after.manifest_version);
    assert_eq!(before.sst_format, after.sst_format);
    assert!(after.files.is_empty());
    assert!(after.series_indexes.is_empty());
    assert_eq!(sequence, region.version_control.current().last_sequence);
}
