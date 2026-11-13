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
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

/// A copy must publish the shared references it installs inside the publish window it
/// holds, exactly like physically copied files. Handing them to the region edit lane
/// instead queues them behind that window (the region is `Editing` for the whole copy) and
/// waits for a completion the worker only produces after the window is released, which
/// deadlocked both copy cases below.
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
    // The source-domain hint is replaced — not kept — by the target's own barrier, so
    // the copied rows are virtualized at `B` and the file is never skipped as
    // "proven disjoint" under a source-domain bound.
    let target_boundary = engine
        .get_region(target_region_id)
        .unwrap()
        .version()
        .last_invalid_seq();
    assert!(
        target_manifest
            .files
            .values()
            .all(|meta| meta.sequence.map(|sequence| sequence.get()) == Some(target_boundary)),
        "copied files must carry the target's backfilled barrier"
    );

    // An exact sequence-range request above the target barrier therefore excludes the
    // copied rows instead of replaying them as target-domain sequences.
    let stream = engine
        .scan_to_stream(
            target_region_id,
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
    assert_eq!(
        0,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        "copied rows are virtualized at the target barrier and stay out of (2, 7]"
    );

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

/// `copy_region_from` moves rows into the target region's sequence domain, so the
/// target must allocate an edit boundary (`last_sequence + 1`) exactly
/// like a generic edit does: a stale cursor is rejected and the boundary is
/// persisted in the target manifest.
#[tokio::test]
async fn test_copy_region_from_advances_invalid_seq_boundary() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("copy-region-from-edit-boundary").await;
    let engine = env.create_engine(MitoConfig::default()).await;

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
    put_rows(
        &engine,
        source_region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 5),
        },
    )
    .await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    // The source region's own data ownership never changed.
    assert_eq!(
        0,
        engine
            .get_region(source_region_id)
            .unwrap()
            .version()
            .last_invalid_seq()
    );

    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let target_last_sequence = engine
        .get_region(target_region_id)
        .unwrap()
        .version_control
        .current()
        .last_sequence;
    let boundary = target_last_sequence + 1;

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

    let region = engine.get_region(target_region_id).unwrap();
    let version = region.version();
    assert_eq!(
        boundary,
        version.last_invalid_seq(),
        "a region copy must allocate the target's next sequence as the boundary"
    );
    // The copy holds the publish window while it allocates, persists and publishes, so a
    // racing write is queued instead of being admitted with a sequence at or below the
    // boundary. The in-memory `last_sequence` is only raised by the publish itself, so a
    // failed copy cannot leave an advanced allocation behind; every write admitted after
    // the copy therefore obtains `boundary + 1` or later.
    assert_eq!(
        boundary,
        region.version_control.current().last_sequence,
        "the copy must publish the boundary without reserving a slot above it"
    );
    assert_eq!(
        boundary,
        region.version_control.current().committed_sequence,
        "the publish must raise the commit watermark to the boundary"
    );
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(
        Some(boundary),
        manifest.last_invalid_seq,
        "the copy boundary must be persisted in the target manifest"
    );
    assert_eq!(
        Some(boundary),
        manifest.committed_sequence,
        "the published commit watermark must cover the boundary"
    );

    // A cursor below the copy boundary belonged to the pre-copy layout.
    let err = engine
        .scanner(
            target_region_id,
            ScanRequest {
                memtable_min_sequence: Some(boundary - 1),
                memtable_max_sequence: Some(boundary + 1),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("a cursor below the copy boundary must be rejected");
    assert_eq!(StatusCode::RequestOutdated, err.status_code());
    let err_msg = err.to_string();
    assert!(
        err_msg.starts_with("sequence range cursor invalid:"),
        "unexpected err msg: {err_msg}"
    );
    assert!(
        matches!(
            err,
            Error::IncrementalQueryStale {
                given_seq,
                min_readable_seq,
                ..
            } if given_seq == boundary - 1 && min_readable_seq == boundary
        ),
        "unexpected err msg: {err_msg}"
    );

    // The copy backfills the barrier on the copied files in the same publish, so at
    // the boundary they are provably consumed: the exact delta is available and empty
    // instead of failing closed forever on a barrier-less file.
    let stream = engine
        .scan_to_stream(
            target_region_id,
            ScanRequest {
                memtable_min_sequence: Some(boundary),
                memtable_max_sequence: Some(boundary + 1),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        0,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        "the copied rows are fully consumed at the boundary"
    );

    // Recovery discovery therefore sees a provably complete candidate set rather than
    // reporting the region as barrier-less on every round.
    let scanner = engine
        .scanner(
            target_region_id,
            ScanRequest {
                memtable_min_sequence: Some(boundary - 1),
                exact_sequence_range: true,
                repair_discovery_scan: true,
                ..Default::default()
            },
        )
        .await
        .expect("discovery must find the copied file as a candidate");
    assert_eq!(
        1,
        scanner.num_files(),
        "the copied file is a candidate at its own barrier"
    );
}

/// Waits until `region` reaches `state`.
async fn wait_until_region_state(region: &MitoRegionRef, state: RegionLeaderState) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while region.state() != RegionRoleState::Leader(state) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("region did not reach the expected state in time");
}

/// The copy holds the worker-coordinated publish window from the moment it is admitted in
/// the worker loop until the copied files are visible, so the boundary it allocates is above
/// every sequence already attributed to a row and ordinary writes arriving meanwhile are
/// queued instead of being admitted at or below that boundary.
#[tokio::test(flavor = "multi_thread")]
async fn test_copy_region_from_publish_window_queues_writes() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("copy-region-from-publish-window").await;
    let engine = env.create_engine(MitoConfig::default()).await;

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
    put_rows(
        &engine,
        source_region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 5),
        },
    )
    .await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let target = engine.get_region(target_region_id).unwrap();
    let last_sequence_before_copy = target.version_control.current().last_sequence;

    // Holding the target's manifest write lock keeps the copy inside its publish window: the
    // worker loop opens the window when it admits the copy, and the background task then
    // blocks on the lock while persisting the copied files.
    let manifest_guard = target.manifest_ctx.manifest_manager.write().await;
    let copy_engine = engine.clone();
    let copy_task = tokio::spawn(async move {
        copy_engine
            .copy_region_from(
                target_region_id,
                MitoCopyRegionFromRequest {
                    source_region_id,
                    parallelism: 1,
                },
            )
            .await
    });
    wait_until_region_state(&target, RegionLeaderState::Editing).await;

    // A write that arrives during the copy is queued, not attributed a sequence.
    let write_engine = engine.clone();
    let write_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(5, 7),
    };
    let write_task =
        tokio::spawn(async move { put_rows(&write_engine, target_region_id, write_rows).await });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !write_task.is_finished(),
        "a write must be queued while the copy publish window is open"
    );
    assert_eq!(
        last_sequence_before_copy,
        target.version_control.current().last_sequence,
        "a queued write must not allocate a sequence during the copy"
    );

    // Releasing the lock lets the copied files become visible; the window then closes and the
    // queued write is admitted.
    drop(manifest_guard);
    copy_task.await.unwrap().unwrap();
    write_task.await.unwrap();

    let boundary = target.version().last_invalid_seq();
    assert_eq!(
        last_sequence_before_copy + 1,
        boundary,
        "the copy must publish at last_sequence + 1"
    );
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        target.state(),
        "the copy publish window must be released after the publish"
    );
    assert!(
        target.version_control.current().last_sequence > boundary,
        "the queued write must be attributed a sequence above the published boundary, \
         last_sequence: {}, boundary: {}",
        target.version_control.current().last_sequence,
        boundary
    );
}
