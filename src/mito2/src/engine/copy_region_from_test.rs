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
            experimental_enable_series_index: with_index,
            experimental_series_index_min_files: 1,
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
    if with_index {
        engine
            .handle_request(source, crate::test_util::build_series_index_request())
            .await
            .unwrap();
    }
    let source_dir = owner.access_layer.build_region_dir(source);
    let before = owner.manifest_ctx.manifest().await;
    assert_eq!(before.series_indexes.len(), usize::from(with_index));
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

    assert!(after.series_indexes.is_empty());
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
    }
    // Physical copies survive complete deletion of their source. Remaps retain
    // source-owned objects via references and use the partial-drop contract.
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
    assert!(
        engine
            .get_region(target)
            .unwrap()
            .version()
            .ssts
            .series_indexes()
            .is_empty()
    );
    crate::engine::compaction_test::compact(&engine, target).await;
    let compacted = engine.get_region(target).unwrap();
    assert!(compacted.version().ssts.series_indexes().is_empty());
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
        // A second copy uses the compacted samples.
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

/// A physical COPY installs SSTs that keep the source region's encoded row
/// sequences. A `snapshot_on_scan` that derives its bound at open (no explicit
/// `memtable_max_sequence`) binds the empty destination's committed sequence
/// (0), which must fence memtable ranges only: it must not hide rows that the
/// pinned version already exposes through immutable SSTs.
#[tokio::test]
async fn test_narthex_physical_copy_visible_to_snapshot_on_scan() {
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;
    use crate::test_util::{flush_region, reopen_region};
    use store_api::codec::PrimaryKeyEncoding;

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let source = RegionId::new(4, 1);
    let target = RegionId::new(4, 2);
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
    // Two distinct series, each flushed into its own source SST.
    for (label, tsid, value) in [("api", 7, 11.0), ("worker", 8, 22.0)] {
        engine
            .handle_request(source, put(label, tsid, value))
            .await
            .unwrap();
        flush_region(&engine, source, None).await;
    }

    let scanned = |scanner: crate::read::scan_region::Scanner| async move {
        let batches = RecordBatches::try_collect(scanner.scan().await.unwrap())
            .await
            .unwrap();
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        (rows, batches.pretty_print().unwrap())
    };

    // Opened against the still empty destination.
    let before_copy = engine
        .scanner(
            target,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(before_copy.snapshot_sequence(), Some(0));

    let copied = engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(copied.copied_file_ids.len(), 2);
    assert_eq!(
        engine
            .get_region(target)
            .unwrap()
            .version_control
            .committed_sequence(),
        0
    );

    // The scanner pinned before the COPY does not observe copied files.
    let (rows, text) = scanned(before_copy).await;
    assert_eq!(0, rows, "pre-COPY snapshot scanner saw rows:\n{text}");

    // A scanner opened after the COPY binds the same committed sequence but
    // must return both copied series.
    let after_copy = engine
        .scanner(
            target,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(after_copy.snapshot_sequence(), Some(0));
    let (rows, text) = scanned(after_copy).await;
    assert_eq!(2, rows, "physical COPY is invisible to the scan:\n{text}");
    for expected in ["api", "11.0", "worker", "22.0"] {
        assert!(text.contains(expected), "{expected} missing:\n{text}");
    }

    // The bound survives a reopen of the destination.
    reopen_region(
        &engine,
        target,
        request.table_dir.clone(),
        true,
        request.options.clone(),
    )
    .await;
    let after_reopen = engine
        .scanner(
            target,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(after_reopen.snapshot_sequence(), Some(0));
    let (rows, text) = scanned(after_reopen).await;
    assert_eq!(2, rows, "copied rows lost after reopen:\n{text}");
    for expected in ["api", "11.0", "worker", "22.0"] {
        assert!(text.contains(expected), "{expected} missing:\n{text}");
    }

    // The implicit bound still fences memtable ranges: a distinct series
    // written into the destination after open stays invisible to that scanner
    // and is returned by a fresh one.
    engine
        .handle_request(target, put("db", 9, 33.0))
        .await
        .unwrap();
    let pinned = engine
        .scanner(
            target,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(pinned.snapshot_sequence(), Some(1));
    engine
        .handle_request(target, put("cache", 10, 44.0))
        .await
        .unwrap();
    let (rows, text) = scanned(pinned).await;
    assert_eq!(3, rows, "pinned scanner result:\n{text}");
    assert!(text.contains("33.0"), "pre-open write missing:\n{text}");
    assert!(
        !text.contains("cache") && !text.contains("44.0"),
        "write after scan open leaked into the pinned scanner:\n{text}"
    );
    let fresh = engine
        .scanner(
            target,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(fresh.snapshot_sequence(), Some(2));
    let (rows, text) = scanned(fresh).await;
    assert_eq!(4, rows, "fresh scanner result:\n{text}");
    assert!(
        text.contains("cache") && text.contains("44.0"),
        "fresh scanner missed the newer write:\n{text}"
    );

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
            window: crate::sst::file::WindowSequence {
                start: 0,
                end: 10,
                max_sequence: 7,
            },
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

/// Holds the manifest writes a test arms, so it can observe the window between a
/// publication and the install that follows it.
#[derive(Debug, Default)]
struct GatedManifestHook {
    gates: std::sync::Mutex<std::collections::VecDeque<Arc<ManifestGate>>>,
}

/// One armed manifest write.
#[derive(Debug)]
struct ManifestGate {
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl GatedManifestHook {
    /// Holds the next manifest write. The receiver fires when the write starts and the
    /// sender releases it; a send that no write has awaited yet stays buffered.
    fn arm(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        self.gates.lock().unwrap().push_back(Arc::new(ManifestGate {
            entered: std::sync::Mutex::new(Some(entered)),
            release: std::sync::Mutex::new(Some(release_rx)),
        }));
        (entered_rx, release)
    }
}

#[async_trait::async_trait]
impl crate::engine::region_hook::RegionHook for GatedManifestHook {
    async fn on_manifest_updated(
        &self,
        _region_id: RegionId,
        _action_list: &crate::manifest::action::RegionMetaActionList,
        _manifest_version: store_api::ManifestVersion,
    ) {
        let gate = self.gates.lock().unwrap().pop_front();
        let Some(gate) = gate else {
            return;
        };
        if let Some(entered) = gate.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        let release = gate.release.lock().unwrap().take();
        if let Some(release) = release {
            let _ = release.await;
        }
    }
}

/// Creates the Narthex copy request both copy-lifecycle tests share.
fn narthex_copy_request() -> store_api::region_request::RegionCreateRequest {
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;
    use store_api::codec::PrimaryKeyEncoding;

    let mut request = CreateRequestBuilder::new()
        .tag_num(1)
        .field_num(1)
        .insert_option("primary_key_encoding", "sparse")
        .insert_option("sst_format", "narthex")
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    // Reserved columns keep a rewritten artifact manifest observably different.
    let reserved = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    request
        .column_metadatas
        .extend(reserved.column_metadatas.into_iter().take(2));
    request
        .primary_key
        .splice(0..0, reserved.primary_key.into_iter().take(2));
    request
}

/// Builds a sparse-encoded put request for `region_id`.
fn sparse_metric_put_for(
    engine: &crate::engine::MitoEngine,
    region_id: RegionId,
    label: &str,
    tsid: u64,
    value: f64,
) -> RegionRequest {
    let metadata = engine.get_region(region_id).unwrap().metadata();
    let key = crate::test_util::sst_util::new_sparse_primary_key(&[label], &metadata, 42, tsid);
    crate::test_util::sparse_metric_put(key, 1000, Some(value))
}

/// A remap keeps physical ownership: a reference in another region's manifest still
/// names artifacts of the owner's region dir. COPY decides by membership in the
/// target's manifest, not by ownership, so installing such a reference back into the
/// owner must plan no object operation: a planned copy targets the owner's own paths,
/// and a failure elsewhere in the copy would delete an artifact still serving its
/// source.
#[tokio::test]
async fn test_narthex_copy_borrows_target_owned_objects() {
    use crate::manifest::action::RegionEdit;
    use crate::test_util::flush_region;
    use datatypes::value::Value;
    use partition::expr::col;
    use std::collections::HashMap;
    use store_api::region_engine::{RemapManifestsRequest, SettableRegionRoleState};
    use store_api::region_request::{
        ApplyStagingManifestRequest, EnterStagingRequest, StagingPartitionDirective,
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
    // The copy source references artifacts that the copy target physically owns.
    let source = RegionId::new(2, 1);
    let target = RegionId::new(2, u32::MAX);
    let request = narthex_copy_request();
    for region in [source, target] {
        engine
            .handle_request(region, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    // The target's rows survive the partition filter its references keep in the
    // source: the remapped source applies the new partition expression to them.
    for (label, tsid) in [("api", 7), ("api", 8)] {
        let put = sparse_metric_put_for(&engine, target, label, tsid, 1.0);
        engine.handle_request(target, put).await.unwrap();
        flush_region(&engine, target, None).await;
    }
    let owned = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(2, owned.files.len());
    assert!(owned.series_indexes.is_empty());

    // Remap T -> S: the target's artifacts move into the source's manifest as
    // references that still name the target's region dir.
    engine
        .set_region_role_state_gracefully(target, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    let partition_expr = col("tag_0")
        .eq(Value::String("api".into()))
        .as_json_str()
        .unwrap();
    let remapped = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: target,
            input_regions: vec![target],
            region_mapping: HashMap::from([(target, vec![source])]),
            new_partition_exprs: HashMap::from([(source, partition_expr.clone())]),
        })
        .await
        .unwrap();
    engine
        .handle_request(
            source,
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
            source,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr,
                central_region_id: target,
                manifest_path: remapped.manifest_paths[&source].clone(),
            }),
        )
        .await
        .unwrap();
    // The target returns to writable and stops referencing its own artifacts.
    engine
        .set_region_role_state_gracefully(target, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    engine
        .edit_region(
            target,
            RegionEdit {
                files_to_remove: owned
                    .files
                    .values()
                    .map(|file| file.as_ref().clone())
                    .collect(),
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let emptied = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert!(
        emptied.files.is_empty(),
        "the target no longer references its artifacts"
    );
    assert!(emptied.series_indexes.is_empty());

    // The source now owns one artifact and borrows the target's.
    let put = sparse_metric_put_for(&engine, source, "api", 9, 2.0);
    engine.handle_request(source, put).await.unwrap();
    flush_region(&engine, source, None).await;
    let source_region = engine.get_region(source).unwrap();
    let source_dir = source_region.access_layer.build_region_dir(source);
    let target_dir = source_region.access_layer.build_region_dir(target);
    let store = source_region.access_layer.object_store().clone();
    let before_copy = source_region.manifest_ctx.manifest().await;
    let borrowed: Vec<_> = before_copy
        .all_files()
        .filter(|file| file.region_id == target)
        .map(|file| file.as_ref().clone())
        .collect();
    let own: Vec<_> = before_copy
        .files
        .values()
        .filter(|file| file.region_id == source)
        .map(|file| file.as_ref().clone())
        .collect();
    assert_eq!(2, borrowed.len(), "two samples are borrowed");
    assert_eq!(1, own.len());
    let mut borrowed_objects = Vec::new();
    for meta in &borrowed {
        for object in meta.narthex_objects(&target_dir).unwrap() {
            let bytes = store.read(&object.path).await.unwrap().to_bytes();
            assert_eq!(object.file_size, bytes.len() as u64);
            borrowed_objects.push((object.path, bytes));
        }
    }

    // Fail the copy on an artifact the source owns, after the borrowed entries were
    // planned with the copy's target paths.
    let failed = own[0]
        .narthex_objects(&source_dir)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .path;
    let saved = store.read(&failed).await.unwrap().to_bytes();
    store.delete(&failed).await.unwrap();
    let copy = engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 2,
            },
        )
        .await;
    assert!(
        copy.is_err(),
        "the copy must fail on the missing source artifact: {copy:?}"
    );
    // The failure cleanup must leave every artifact the source still references.
    for (path, bytes) in &borrowed_objects {
        let actual = store
            .read(path)
            .await
            .unwrap_or_else(|error| {
                panic!("the copy cleanup deleted {path}, which still serves its source: {error}")
            })
            .to_bytes();
        assert_eq!(&actual, bytes, "the copy cleanup rewrote {path}");
    }
    let after_failure = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert!(
        after_failure.files.is_empty(),
        "a failed copy must not install entries"
    );
    assert!(after_failure.series_indexes.is_empty());
    // Newly created siblings are reclaimed.
    for object in own[0].narthex_objects(&source_dir).unwrap() {
        let sibling = format!(
            "{target_dir}{}",
            object.path.strip_prefix(&source_dir).unwrap()
        );
        assert!(
            !store.exists(&sibling).await.unwrap(),
            "the failed copy leaked {sibling}"
        );
    }

    // The source keeps serving the borrowed rows once its own artifact is back.
    store.write(&failed, saved).await.unwrap();
    let rows = RecordBatches::try_collect(
        engine
            .scan_to_stream(source, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        3,
        rows.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        "the source must still serve its borrowed rows: {}",
        rows.pretty_print().unwrap()
    );

    // Retrying installs the borrowed entries in the target without rewriting them.
    let retried = engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, retried.copied_file_ids.len());
    let installed = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(3, installed.files.len());
    assert!(
        installed.series_indexes.is_empty(),
        "a COPY installs samples only"
    );
    for meta in &borrowed {
        let found = installed
            .all_files()
            .find(|file| file.file_id == meta.file_id)
            .map(|file| file.as_ref().clone())
            .unwrap();
        // The reference keeps the owner's inventory and sizes, but not the admission
        // sequence of the region it came from: that domain does not bound the rows of a
        // file whose physical sequences are the target's own.
        assert_eq!(target, found.region_id);
        assert_eq!(meta.file_size, found.file_size);
        assert_eq!(
            std::mem::discriminant(&meta.format),
            std::mem::discriminant(&found.format)
        );
        assert_eq!(
            meta.narthex_objects(&target_dir),
            found.narthex_objects(&target_dir),
            "the borrowed inventory and sizes must stay unchanged"
        );
        assert!(
            found.sequence.is_none(),
            "the foreign sequence bound must be cleared"
        );
        assert!(!found.preserve_row_sequence);
    }
    for (path, bytes) in &borrowed_objects {
        let actual = store
            .read(path)
            .await
            .unwrap_or_else(|error| panic!("the retry deleted {path}: {error}"))
            .to_bytes();
        assert_eq!(&actual, bytes, "the retry rewrote {path}");
    }

    // The borrowed entry no longer carries a foreign admission bound, so an exact
    // sequence-range read fails closed instead of pruning the file and answering with a
    // wrong empty result.
    let err = engine
        .scanner(
            target,
            ScanRequest {
                memtable_min_sequence: Some(2),
                memtable_max_sequence: Some(3),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("the target must not trust a foreign admission sequence");
    assert!(
        matches!(err, Error::SequenceRangeUnsupported { .. }),
        "unexpected err: {err}"
    );

    engine.stop().await.unwrap();
}

/// Publication and installation of a physical COPY run on the worker's edit lane.
/// A COPY may only be acknowledged once the worker installed its edit, so an ordinary
/// edit admitted meanwhile cannot leave a published-but-uninstalled version that a
/// retry would acknowledge without repairing.
#[tokio::test]
async fn test_physical_copy_installs_before_acknowledging() {
    use crate::engine::region_hook::RegionHookRef;
    use crate::manifest::action::RegionEdit;
    use crate::test_util::flush_region;
    use common_base::Plugins;
    use std::time::Duration;

    let hook = Arc::new(GatedManifestHook::default());
    let plugins = Plugins::new();
    plugins.insert::<RegionHookRef>(hook.clone());
    let mut env = TestEnv::new().await.with_file_ref_manager(Arc::new(
        crate::sst::file_ref::FileReferenceManager::with_gc_enabled(None, true),
    ));
    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                schedule_compaction_after_edit: false,
                gc: crate::gc::GcConfig {
                    enable: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            plugins,
        )
        .await;
    let source = RegionId::new(3, 1);
    let target = RegionId::new(3, 2);
    let request = narthex_copy_request();
    for region in [source, target] {
        engine
            .handle_request(region, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    for (label, tsid) in [("api", 7), ("worker", 8)] {
        let put = sparse_metric_put_for(&engine, source, label, tsid, 1.0);
        engine.handle_request(source, put).await.unwrap();
        flush_region(&engine, source, None).await;
    }
    // The target owns one file, which the unrelated edit removes.
    let put = sparse_metric_put_for(&engine, target, "own", 3, 5.0);
    engine.handle_request(target, put).await.unwrap();
    flush_region(&engine, target, None).await;
    let target_manifest = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    let own: Vec<_> = target_manifest
        .files
        .values()
        .map(|file| file.as_ref().clone())
        .collect();
    assert_eq!(1, own.len());
    let own_id = own[0].file_id;

    // Hold the COPY after its manifest write.
    let (copy_published, release_copy) = hook.arm();
    let copy = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .copy_region_from(
                    target,
                    MitoCopyRegionFromRequest {
                        source_region_id: source,
                        parallelism: 1,
                    },
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(30), copy_published)
        .await
        .expect("the copy must reach its manifest write")
        .unwrap();

    // An unrelated ordinary edit reaches the worker while the published version is
    // not installed yet. It must wait in the edit queue behind the copy.
    let (edit_admitted, release_edit) = hook.arm();
    let ordinary = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .edit_region(
                    target,
                    RegionEdit {
                        files_to_remove: own,
                        timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    let admitted = tokio::time::timeout(Duration::from_millis(300), edit_admitted)
        .await
        .is_ok();
    let _ = release_copy.send(());
    let copied = tokio::time::timeout(Duration::from_secs(30), copy)
        .await
        .expect("the copy task must finish")
        .unwrap();
    let _ = release_edit.send(());
    let edited = tokio::time::timeout(Duration::from_secs(30), ordinary)
        .await
        .expect("the unrelated edit must finish")
        .unwrap();

    // A retry of an installed copy is a no-op; neither may it repair a missing install.
    let retried = engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await;
    // Observe the live version without reopening the region.
    let target_region = engine.get_region(target).unwrap();
    let live = target_region.version();
    let sample_ids: Vec<_> = live
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .map(|file| file.meta_ref().file_id)
        .collect();
    let live_indexes = live.ssts.series_indexes().len();
    assert!(
        copied.is_ok(),
        "the copy must publish and install its edit; copied: {copied:?}, retried: {retried:?}, \
         live samples: {sample_ids:?}, live indexes: {live_indexes}"
    );
    assert!(
        retried.is_ok(),
        "retrying an installed copy must succeed: {retried:?}"
    );
    assert!(
        edited.is_ok(),
        "the unrelated edit must complete: {edited:?}"
    );
    assert_eq!(
        2,
        sample_ids.len(),
        "the live sample inventory holds the copied entries"
    );
    assert_eq!(
        0, live_indexes,
        "a COPY installs samples only: the live series-index inventory stays empty"
    );
    assert!(
        !sample_ids.contains(&own_id),
        "the unrelated edit removed the target's own file"
    );
    let rows = RecordBatches::try_collect(
        engine
            .scan_to_stream(target, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let text = rows.pretty_print().unwrap();
    assert_eq!(
        2,
        rows.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        "{text}"
    );
    assert!(text.contains("api"), "{text}");
    assert!(text.contains("worker"), "{text}");
    // The retry found every entry installed and installed nothing twice.
    let installed = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(2, installed.files.len());
    assert!(installed.series_indexes.is_empty());
    assert!(
        !admitted,
        "the unrelated edit must wait behind the copy in the edit queue"
    );

    engine.stop().await.unwrap();
}

/// A copy that finds its ids in the target manifest must not delete a destination another
/// region still retains: the manifest lost the reference, but the objects serve a remapped
/// reader. Rollback reclaims only the objects this attempt created.
#[tokio::test]
async fn test_narthex_copy_cleanup_keeps_retained_destination() {
    use crate::manifest::action::RegionEdit;
    use crate::test_util::flush_region;
    use datatypes::value::Value;
    use partition::expr::col;
    use std::collections::HashMap;
    use store_api::region_engine::{RemapManifestsRequest, SettableRegionRoleState};
    use store_api::region_request::{
        ApplyStagingManifestRequest, EnterStagingRequest, StagingPartitionDirective,
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
    let source = RegionId::new(4, 1);
    let target = RegionId::new(4, 2);
    let retained = RegionId::new(4, 3);
    let request = narthex_copy_request();
    for region in [source, target, retained] {
        engine
            .handle_request(region, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    // The source owns the file the target copies first.
    let put = sparse_metric_put_for(&engine, source, "api", 7, 1.0);
    engine.handle_request(source, put).await.unwrap();
    flush_region(&engine, source, None).await;
    let source_region = engine.get_region(source).unwrap();
    let source_dir = source_region.access_layer.build_region_dir(source);
    let target_dir = source_region.access_layer.build_region_dir(target);
    let store = source_region.access_layer.object_store().clone();
    let first_id = source_region
        .manifest_ctx
        .manifest()
        .await
        .files
        .keys()
        .next()
        .cloned()
        .unwrap();
    engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    let copied = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    let retained_meta = copied.all_files().next().unwrap().as_ref().clone();
    let mut retained_objects = Vec::new();
    for object in retained_meta.narthex_objects(&target_dir).unwrap() {
        let bytes = store.read(&object.path).await.unwrap().to_bytes();
        retained_objects.push((object.path, bytes));
    }

    // A remap retains the target's objects in another region while the target loses the
    // reference, so the objects outlive it.
    engine
        .set_region_role_state_gracefully(target, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    let partition_expr = col("tag_0")
        .eq(Value::String("api".into()))
        .as_json_str()
        .unwrap();
    let remapped = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: target,
            input_regions: vec![target],
            region_mapping: HashMap::from([(target, vec![retained])]),
            new_partition_exprs: HashMap::from([(retained, partition_expr.clone())]),
        })
        .await
        .unwrap();
    engine
        .handle_request(
            retained,
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
            retained,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr,
                central_region_id: target,
                manifest_path: remapped.manifest_paths[&retained].clone(),
            }),
        )
        .await
        .unwrap();
    engine
        .set_region_role_state_gracefully(target, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    engine
        .edit_region(
            target,
            RegionEdit {
                files_to_remove: vec![retained_meta.clone()],
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let emptied = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert!(emptied.files.is_empty());

    // The source owns one more artifact, whose copy fails.
    let put = sparse_metric_put_for(&engine, source, "api", 8, 2.0);
    engine.handle_request(source, put).await.unwrap();
    flush_region(&engine, source, None).await;
    let before_copy = engine
        .get_region(source)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    let own: Vec<_> = before_copy
        .files
        .values()
        .filter(|file| file.region_id == source && file.file_id != first_id)
        .map(|file| file.as_ref().clone())
        .collect();
    assert_eq!(1, own.len(), "the source owns the new artifact");
    let failing = own[0]
        .narthex_objects(&source_dir)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .path;
    let saved = store.read(&failing).await.unwrap().to_bytes();
    store.delete(&failing).await.unwrap();

    let copy = engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await;
    assert!(
        copy.is_err(),
        "the copy must fail on the missing source artifact: {copy:?}"
    );
    // The retained destination was reused, not rewritten, and must survive the rollback.
    for (path, bytes) in &retained_objects {
        let actual = store
            .read(path)
            .await
            .unwrap_or_else(|error| {
                panic!("the rollback deleted {path}, which the remapped region retains: {error}")
            })
            .to_bytes();
        assert_eq!(&actual, bytes, "the rollback rewrote {path}");
    }
    let after_failure = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert!(
        after_failure.files.is_empty(),
        "a failed copy must not install entries"
    );
    // The retained reader still serves the rows.
    let rows = RecordBatches::try_collect(
        engine
            .scan_to_stream(retained, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        1,
        rows.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        "the remapped region must still serve its retained rows"
    );
    // Newly created siblings are reclaimed.
    for object in own[0].narthex_objects(&source_dir).unwrap() {
        let sibling = format!(
            "{target_dir}{}",
            object.path.strip_prefix(&source_dir).unwrap()
        );
        assert!(
            !store.exists(&sibling).await.unwrap(),
            "the failed copy leaked {sibling}"
        );
    }

    // Restoring the source artifact lets a retry install both entries; the retained
    // destination is reused as it is.
    store.write(&failing, saved).await.unwrap();
    engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    let installed = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(2, installed.files.len());
    for (path, bytes) in &retained_objects {
        let actual = store.read(path).await.unwrap().to_bytes();
        assert_eq!(&actual, bytes, "the retry rewrote {path}");
    }

    engine.stop().await.unwrap();
}

/// Builds a stat gate that pauses the first `stat` of a path containing `needle`, so a test
/// can act while a copy is still preparing its objects.
fn stat_gate(
    needle: String,
) -> (
    Arc<object_store::layers::mock::MockLayer>,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let release = Arc::new(std::sync::Mutex::new(Some(release_rx)));
    let layer = MockLayerBuilder::default()
        .stat_interceptor(Arc::new(move |path: &str| {
            let held = path.contains(&needle);
            let entered = held.then(|| entered.lock().unwrap().take()).flatten();
            let release = held.then(|| release.lock().unwrap().take()).flatten();
            Box::pin(async move {
                if let Some(entered) = entered {
                    let _ = entered.send(());
                }
                if let Some(release) = release {
                    let _ = release.await;
                }
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        }))
        .build()
        .unwrap();
    (Arc::new(layer), entered_rx, release_tx)
}

/// A copy prepares its objects before it submits its edit. When the target region is
/// reopened meanwhile, the prepared edit belongs to a replaced instance and must not publish
/// into the replacement.
#[tokio::test]
async fn test_physical_copy_rejects_reopened_target() {
    use crate::test_util::flush_region;

    let (layer, copy_paused, release_copy) = stat_gate("5_0000000001".to_string());
    let mut env = TestEnv::new().await.with_mock_layer((*layer).clone());
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let source = RegionId::new(5, 1);
    let target = RegionId::new(5, 2);
    let request = narthex_copy_request();
    for region in [source, target] {
        engine
            .handle_request(region, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    let put = sparse_metric_put_for(&engine, source, "api", 7, 1.0);
    engine.handle_request(source, put).await.unwrap();
    flush_region(&engine, source, None).await;

    let copy = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .copy_region_from(
                    target,
                    MitoCopyRegionFromRequest {
                        source_region_id: source,
                        parallelism: 1,
                    },
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), copy_paused)
        .await
        .expect("the copy must reach its object copy")
        .unwrap();
    // The copy holds a preparation against the instance that is about to be replaced.
    crate::test_util::reopen_region(
        &engine,
        target,
        request.table_dir.clone(),
        true,
        request.options.clone(),
    )
    .await;
    let _ = release_copy.send(());
    let copy = tokio::time::timeout(std::time::Duration::from_secs(30), copy)
        .await
        .expect("the copy task must finish")
        .unwrap();
    assert!(
        copy.is_err(),
        "a copy prepared against a replaced instance must not publish: {copy:?}"
    );
    let target_region = engine.get_region(target).unwrap();
    let manifest = target_region.manifest_ctx.manifest().await;
    assert!(
        manifest.files.is_empty(),
        "the stale preparation must not publish"
    );
    assert!(
        target_region
            .version()
            .ssts
            .levels()
            .iter()
            .all(|level| level.files.is_empty())
    );

    // The same copy against the current instance is accepted.
    engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    let manifest = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(1, manifest.files.len());

    engine.stop().await.unwrap();
}

/// A copy that finds every id in the target manifest must verify the live version: another
/// edit may have published the ids and not installed them yet.
#[tokio::test]
async fn test_physical_copy_noop_verifies_the_live_version() {
    use crate::engine::region_hook::RegionHookRef;
    use crate::manifest::action::RegionEdit;
    use crate::test_util::flush_region;
    use common_base::Plugins;
    use std::time::Duration;

    let (layer, copy_paused, release_copy) = stat_gate("6_0000000001".to_string());
    let hook = Arc::new(GatedManifestHook::default());
    let plugins = Plugins::new();
    plugins.insert::<RegionHookRef>(hook.clone());
    let mut env = TestEnv::new().await.with_mock_layer((*layer).clone());
    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                schedule_compaction_after_edit: false,
                ..Default::default()
            },
            plugins,
        )
        .await;
    let source = RegionId::new(6, 1);
    let target = RegionId::new(6, 2);
    let request = narthex_copy_request();
    for region in [source, target] {
        engine
            .handle_request(region, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    let put = sparse_metric_put_for(&engine, source, "api", 7, 1.0);
    engine.handle_request(source, put).await.unwrap();
    flush_region(&engine, source, None).await;
    let source_manifest = engine
        .get_region(source)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    let source_meta = source_manifest
        .files
        .values()
        .next()
        .unwrap()
        .as_ref()
        .clone();
    let source_dir = engine
        .get_region(source)
        .unwrap()
        .access_layer
        .build_region_dir(source);
    let store = engine
        .get_region(source)
        .unwrap()
        .access_layer
        .object_store()
        .clone();
    // The import publishes a reference to the source's objects under the target's id.
    let mut imported = source_meta.clone();
    imported.region_id = target;
    imported.sequence = None;
    imported.preserve_row_sequence = false;

    // A first copy holds the copy lock while it prepares, so the second one is accepted and
    // waits for it with the region still `Writable`.
    let first = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .copy_region_from(
                    target,
                    MitoCopyRegionFromRequest {
                        source_region_id: source,
                        parallelism: 1,
                    },
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(30), copy_paused)
        .await
        .expect("the first copy must reach its object copy")
        .unwrap();
    let noop = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .copy_region_from(
                    target,
                    MitoCopyRegionFromRequest {
                        source_region_id: source,
                        parallelism: 1,
                    },
                )
                .await
        }
    });
    // Let the queued request reach the worker before an edit takes the region.
    tokio::time::sleep(Duration::from_millis(200)).await;
    // An import of the same file id publishes the entry and pauses before installing it.
    let (import_published, release_import) = hook.arm();
    let edit = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .edit_region(
                    target,
                    RegionEdit {
                        files_to_add: vec![imported],
                        timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(30), import_published)
        .await
        .expect("the import must publish its entry")
        .unwrap();
    assert_eq!(
        1,
        engine
            .get_region(target)
            .unwrap()
            .manifest_ctx
            .manifest()
            .await
            .files
            .len(),
        "the import must publish the entry before the copy resumes"
    );
    // Fail the first copy so it releases the lock while the entry is still uninstalled.
    let failing = source_meta
        .narthex_objects(&source_dir)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .path;
    let saved = store.read(&failing).await.unwrap().to_bytes();
    store.delete(&failing).await.unwrap();
    let _ = release_copy.send(());
    let first = tokio::time::timeout(Duration::from_secs(30), first)
        .await
        .expect("the first copy must finish")
        .unwrap();
    assert!(
        first.is_err(),
        "the injected fault must fail the first copy: {first:?}"
    );
    let noop = tokio::time::timeout(Duration::from_secs(30), noop)
        .await
        .expect("the queued copy must finish")
        .unwrap();
    assert!(
        noop.is_err(),
        "a manifested entry the live version does not serve must not be acknowledged: {noop:?}"
    );
    assert!(
        matches!(noop, Err(Error::RegionBusy { .. })),
        "the copy must report the uninstalled entry as transient: {noop:?}"
    );
    assert!(
        engine
            .get_region(target)
            .unwrap()
            .version()
            .ssts
            .levels()
            .iter()
            .all(|level| level.files.is_empty()),
        "the entry must still be uninstalled"
    );

    // Once the import installs the entry, the same copy verifies it and succeeds.
    let _ = release_import.send(());
    tokio::time::timeout(Duration::from_secs(30), edit)
        .await
        .expect("the import must finish")
        .unwrap()
        .unwrap();
    store.write(&failing, saved).await.unwrap();
    engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        1,
        engine
            .get_region(target)
            .unwrap()
            .version()
            .ssts
            .levels()
            .iter()
            .map(|level| level.files.len())
            .sum::<usize>(),
        "the acknowledged copy must be served by the live version"
    );

    engine.stop().await.unwrap();
}

/// A physical copy publishes objects the target must own outright. A graceful staging
/// transition racing admission must never turn the copy into a staged edit that completion
/// would not install while still acknowledging success.
#[tokio::test]
async fn test_physical_copy_admits_only_from_writable() {
    use crate::test_util::flush_region;
    use store_api::region_engine::SettableRegionRoleState;

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let request = narthex_copy_request();
    for attempt in 0..8u32 {
        let source = RegionId::new(7, attempt * 3 + 1);
        let target = RegionId::new(7, attempt * 3 + 2);
        for region in [source, target] {
            engine
                .handle_request(region, RegionRequest::Create(request.clone()))
                .await
                .unwrap();
        }
        let put = sparse_metric_put_for(&engine, source, "api", 7, 1.0);
        engine.handle_request(source, put).await.unwrap();
        flush_region(&engine, source, None).await;
        let source_ids: Vec<_> = engine
            .get_region(source)
            .unwrap()
            .manifest_ctx
            .manifest()
            .await
            .files
            .keys()
            .cloned()
            .collect();

        // Staging starts first: its transition runs off the worker, so a copy admitted while
        // it is in flight must not be staged by it.
        let staging = tokio::spawn({
            let engine = engine.clone();
            async move {
                engine
                    .set_region_role_state_gracefully(
                        target,
                        SettableRegionRoleState::StagingLeader,
                    )
                    .await
            }
        });
        tokio::task::yield_now().await;
        let copy = tokio::spawn({
            let engine = engine.clone();
            async move {
                engine
                    .copy_region_from(
                        target,
                        MitoCopyRegionFromRequest {
                            source_region_id: source,
                            parallelism: 1,
                        },
                    )
                    .await
            }
        });
        let (copied, staged) = tokio::join!(copy, staging);
        let copied = copied.unwrap();
        let staged = staged.unwrap();
        if copied.is_ok() {
            // An acknowledged copy is installed in the live version, whatever the staging
            // transition did.
            let region = engine.get_region(target).unwrap();
            let installed: Vec<_> = region
                .version()
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files.keys().cloned())
                .collect();
            assert!(
                source_ids.iter().all(|id| installed.contains(id)),
                "an acknowledged copy must be installed, staged: {:?}, installed: {:?}",
                staged.is_ok(),
                installed
            );
        }
        if staged.is_ok() {
            engine
                .set_region_role_state_gracefully(target, SettableRegionRoleState::Leader)
                .await
                .unwrap();
        }
        // The entries are either installed and served, or absent: staging must never hold
        // the only copy of an acknowledged edit.
        let region = engine.get_region(target).unwrap();
        let manifest = region.manifest_ctx.manifest().await;
        let installed: Vec<_> = region
            .version()
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files.keys().cloned())
            .collect();
        if manifest.files.is_empty() {
            assert!(installed.is_empty());
        } else {
            assert_eq!(manifest.files.len(), installed.len());
        }
    }

    engine.stop().await.unwrap();
}

/// A mixed COPY publishes both a shared reference and physically copied objects. Its edit is
/// bound to the instance that prepared it like a purely physical one, so a target reopened
/// during preparation must not take the stale edit.
#[cfg(feature = "enterprise")]
#[tokio::test]
async fn test_mixed_copy_rejects_reopened_target() {
    use crate::manifest::action::RegionEdit;
    use crate::sst::file::{FileMeta, LEVEL_SHARED};
    use crate::test_util::flush_region;

    let (layer, copy_paused, release_copy) = stat_gate("8_0000000001".to_string());
    let mut env = TestEnv::new().await.with_mock_layer((*layer).clone());
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let owner = RegionId::new(8, 0);
    let source = RegionId::new(8, 1);
    let target = RegionId::new(8, 2);
    let request = CreateRequestBuilder::new().build();
    for id in [owner, source, target] {
        engine
            .handle_request(id, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    for (id, start) in [(owner, 0), (source, 10)] {
        put_rows(
            &engine,
            id,
            Rows {
                schema: rows_schema(&request),
                rows: build_rows(start, start + 3),
            },
        )
        .await;
        flush_region(&engine, id, None).await;
    }
    let shared = FileMeta {
        level: LEVEL_SHARED,
        ..engine.get_region(owner).unwrap().version().ssts.levels()[0]
            .files()
            .next()
            .unwrap()
            .meta_ref()
            .clone()
    };
    for id in [owner, source] {
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

    let copy = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .copy_region_from(
                    target,
                    MitoCopyRegionFromRequest {
                        source_region_id: source,
                        parallelism: 1,
                    },
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), copy_paused)
        .await
        .expect("the mixed copy must reach its object copy")
        .unwrap();
    let before = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    crate::test_util::reopen_region(
        &engine,
        target,
        request.table_dir.clone(),
        true,
        request.options.clone(),
    )
    .await;
    let _ = release_copy.send(());
    let copy = tokio::time::timeout(std::time::Duration::from_secs(30), copy)
        .await
        .expect("the mixed copy task must finish")
        .unwrap();
    assert!(
        copy.is_err(),
        "a mixed copy prepared against a replaced instance must not publish: {copy:?}"
    );
    let target_region = engine.get_region(target).unwrap();
    let after = target_region.manifest_ctx.manifest().await;
    assert_eq!(
        before.files, after.files,
        "the stale preparation must not publish"
    );
    assert!(
        target_region
            .version()
            .ssts
            .levels()
            .iter()
            .all(|level| level.files.is_empty())
    );

    // The same copy against the current instance is accepted.
    engine
        .copy_region_from(
            target,
            MitoCopyRegionFromRequest {
                source_region_id: source,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    let installed = engine
        .get_region(target)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(2, installed.files.len());

    engine.stop().await.unwrap();
}
