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
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::test_util::{
    CreateRequestBuilder, DeltaFaults, TestEnv, build_rows, put_rows, rows_schema,
};

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
    // the copied rows are virtualized at that barrier and the file is never skipped as
    // "proven disjoint" under a source-domain bound.
    let target_boundary = target_manifest
        .files
        .values()
        .next()
        .expect("published file")
        .sequence
        .expect("the edit assigns a barrier")
        .get();
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

async fn wait_until_region_state(region: &MitoRegionRef, state: RegionLeaderState) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while region.state() != RegionRoleState::Leader(state) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("region did not reach the expected state in time");
}

/// A copy moves the objects of the source region with the target region writable: it must not
/// stall the region's writes. The publish window — and with it the admission barrier
/// `B = last_sequence + 1` — is opened only where the copied files are applied, so a write
/// admitted while the copy runs gets a sequence of its own, below `B`.
///
/// A mock copy stalls the physical copy so the write lands inside it deterministically: the
/// copy is in progress (the test asserts it has not finished) and the region is still writable.
#[tokio::test(flavor = "multi_thread")]
async fn test_copy_region_from_does_not_block_writes() {
    common_telemetry::init_default_ut_logging();

    let mock_layer = MockLayerBuilder::default()
        .copy_interceptor(Arc::new(|_, _, _| {
            // Stall only the physical copy: it is what a publish window must not cover.
            std::thread::sleep(std::time::Duration::from_millis(200));
            None
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::with_prefix("copy-region-from-writable")
        .await
        .with_mock_layer(mock_layer);
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
    // The copy is now copying the file: it must still be running, and the region writable.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !copy_task.is_finished(),
        "the mock layer must keep the copy in its file copying phase"
    );
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        target.state(),
        "a copy in progress must leave the region writable"
    );

    // A write submitted while the copy copies its files is admitted, not queued behind it.
    let write_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(5, 7),
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        put_rows(&engine, target_region_id, write_rows),
    )
    .await
    .expect("a write must be admitted while the copy runs");
    let admitted_sequence = target.version_control.current().last_sequence;
    assert!(
        admitted_sequence > last_sequence_before_copy,
        "the write admitted during the copy must be attributed a sequence, \
         last_sequence_before_copy: {last_sequence_before_copy}, last_sequence: {admitted_sequence}"
    );

    copy_task.await.unwrap().unwrap();

    let published = target.manifest_ctx.manifest().await;
    let boundary = published
        .files
        .values()
        .next()
        .and_then(|meta| meta.sequence.map(|sequence| sequence.get()))
        .expect("the copy must publish its files at a barrier");
    assert_eq!(
        admitted_sequence + 1,
        boundary,
        "the barrier must be allocated at publish time, above the write the copy admitted"
    );
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        target.state(),
        "the copy publish window must be released after the publish"
    );
}

/// The publish window of a copy guards the files it publishes, not the copy: the loop opens it
/// only when the copied files are applied, so the barrier is above every sequence already
/// attributed to a row, and a write submitted after it was allocated is admitted only once the
/// copied files are visible.
#[tokio::test(flavor = "multi_thread")]
async fn test_copy_region_from_write_after_barrier_waits_for_files() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("copy-region-from-window").await;
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

    // Holding the manifest write lock stalls the publish of the copied files, which the worker
    // loop spawns after it opened the window. The copy itself needs no manifest lock: it can
    // only reach the window after it copied the files, with the region writable.
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
    // The published version needs no manifest lock, which this test holds: the copied files
    // are not visible yet, the barrier is allocated but nothing is attributed to a row.
    assert!(
        target
            .version()
            .ssts
            .levels()
            .iter()
            .all(|level| level.files.is_empty()),
        "the window must be open before the copied files are visible"
    );
    assert_eq!(
        last_sequence_before_copy,
        target.version_control.current().last_sequence,
        "allocating the barrier must not attribute a sequence"
    );

    // A write submitted now is admitted only after the copied files became visible.
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
        "a write submitted after the barrier was allocated must wait for the copied files"
    );
    assert_eq!(
        last_sequence_before_copy,
        target.version_control.current().last_sequence,
        "a queued write must not allocate a sequence while the window is open"
    );

    // Releasing the lock publishes the copied files, which closes the window and admits the
    // write it queued.
    drop(manifest_guard);
    copy_task.await.unwrap().unwrap();
    write_task.await.unwrap();

    let boundary = last_sequence_before_copy + 1;
    assert_eq!(
        Some(boundary),
        target
            .manifest_ctx
            .manifest()
            .await
            .files
            .values()
            .next()
            .and_then(|meta| meta.sequence.map(|sequence| sequence.get())),
        "the copied files must be published at the barrier allocated in the window"
    );
    assert!(
        target.version_control.current().last_sequence > boundary,
        "the queued write must be admitted above the published barrier, last_sequence: {}, boundary: {boundary}",
        target.version_control.current().last_sequence
    );
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        target.state(),
        "the copy publish window must be released after the publish"
    );
}

/// Creates a source region holding one flushed file of rows `0..5` and an empty target region.
/// Returns the schema and both region ids of the pair.
async fn create_copy_pair(
    engine: &crate::engine::MitoEngine,
) -> (Vec<api::v1::ColumnSchema>, RegionId, RegionId) {
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
        engine,
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

    (column_schemas, source_region_id, target_region_id)
}

/// The admission barrier of a copy must be above the committed watermark the manifest records, not
/// just above the in-memory `last_sequence` of the version.
///
/// A tail replay commits the watermark its replayed rows carry before the worker applies the edit,
/// so the manifest can record a committed sequence the version has not taken yet. A barrier
/// allocated from the version alone would fall under rows the region has already committed, and a
/// reader whose committed watermark reached it would prune the copied files as already consumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_copy_region_from_barrier_above_manifest_commit() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("copy-region-from-manifest-barrier").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let (_schema, source_region_id, target_region_id) = create_copy_pair(&engine).await;

    let target = engine.get_region(target_region_id).unwrap();
    let last_sequence_before_copy = target.version_control.current().last_sequence;
    // What a replay of the pending tails commits: rows the region already owns sit above the
    // in-memory `last_sequence`, because the worker has not applied their edit yet.
    let replayed_watermark = last_sequence_before_copy + 10;
    let replayed_edit = RegionEdit {
        files_to_add: vec![],
        files_to_remove: vec![],
        timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: Some(replayed_watermark),
        ..Default::default()
    };
    target
        .manifest_ctx
        .update_manifest(
            RegionLeaderState::Writable,
            RegionMetaActionList::with_action(RegionMetaAction::Edit(replayed_edit)),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        last_sequence_before_copy,
        target.version_control.current().last_sequence,
        "the replayed edit is committed but not applied to the version yet"
    );

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

    let barrier = target
        .manifest_ctx
        .manifest()
        .await
        .files
        .values()
        .next()
        .and_then(|meta| meta.sequence.map(|sequence| sequence.get()))
        .expect("the copy must publish its files at a barrier");
    assert_eq!(
        replayed_watermark + 1,
        barrier,
        "the barrier must be above the committed watermark the manifest records, \
         last_sequence_before_copy: {last_sequence_before_copy}"
    );
    assert_eq!(
        replayed_watermark + 1,
        target.version_control.current().committed_sequence,
        "the copied files are virtualized above the replayed rows"
    );
}

/// A copy publish whose manifest write may have landed without being applied fences the region
/// instead of leaving it writable.
///
/// The manifest then holds the copied files at the barrier while the version does not, so a write
/// admitted afterwards would be attributed a sequence at or below a committed one. The reload that
/// follows recovers the published layout, and writes resume strictly above the barrier.
#[tokio::test(flavor = "multi_thread")]
async fn test_copy_region_from_uncertain_publish_fences_region() {
    common_telemetry::init_default_ut_logging();

    let faults = DeltaFaults::default();
    let mut env = TestEnv::with_prefix("copy-region-from-uncertain-publish")
        .await
        .with_mock_layer(faults.layer());
    let engine = env.create_engine(MitoConfig::default()).await;
    let (schema, source_region_id, target_region_id) = create_copy_pair(&engine).await;

    let target = engine.get_region(target_region_id).unwrap();
    let last_sequence_before_copy = target.version_control.current().last_sequence;
    // The delta write lands but reports an error, like a lost response.
    faults.lose_next_response();
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
    assert!(
        err.may_have_persisted_manifest_update(),
        "the test must inject an uncertain manifest write, got: {err}"
    );
    assert_eq!(
        RegionRoleState::Follower,
        target.state(),
        "a region whose copy publish may have landed must not stay writable"
    );
    assert_eq!(
        last_sequence_before_copy,
        target.version_control.current().last_sequence,
        "the failed publish must not apply anything to the version"
    );

    // The worker reloads the fenced region from the manifest, which holds the copied files.
    let reloaded = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let region = engine.get_region(target_region_id).unwrap();
            if !region
                .version()
                .ssts
                .levels()
                .iter()
                .all(|l| l.files.is_empty())
            {
                return region;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the fenced region must be reloaded with the copied files, not left stale");
    let barrier = reloaded
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .filter_map(|file| file.meta_ref().sequence.map(|sequence| sequence.get()))
        .next()
        .expect("the reloaded region must see the copied files at their barrier");
    assert!(
        barrier > last_sequence_before_copy,
        "the published barrier must be above the boundary the copy allocated at, barrier: {barrier}, \
         last_sequence_before_copy: {last_sequence_before_copy}"
    );
    // A reloaded region serves nothing until the route promotes it again, which is what a lease
    // does on a real node.
    engine
        .set_region_role(target_region_id, RegionRole::Leader)
        .unwrap();
    put_rows(
        &engine,
        target_region_id,
        Rows {
            schema: schema.clone(),
            rows: build_rows(5, 7),
        },
    )
    .await;
    assert!(
        reloaded.version_control.current().last_sequence > barrier,
        "a write after the reload must be attributed a sequence above the published barrier, \
         last_sequence: {}, barrier: {barrier}",
        reloaded.version_control.current().last_sequence
    );
}

/// The copy window keeps the writes it stalled behind an ordered drain, which the notification of
/// the release drops together with admitting them.
///
/// The drain is registered when the loop opens the window and removed only by that notification, so
/// the test also proves it does not outlive the window: a drain left behind would stall every later
/// write of the region. The write the window stalled must be attributed a sequence below the one a
/// write submitted after the window closed obtains.
#[tokio::test(flavor = "multi_thread")]
async fn test_copy_region_from_releases_the_write_drain() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("copy-region-from-drain").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let (schema, source_region_id, target_region_id) = create_copy_pair(&engine).await;

    let target = engine.get_region(target_region_id).unwrap();
    let last_sequence_before_copy = target.version_control.current().last_sequence;

    // Holding the manifest write lock stalls the publish after the loop opened the window.
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

    let stalled_engine = engine.clone();
    let stalled_schema = schema.clone();
    let stalled_write = tokio::spawn(async move {
        put_rows(
            &stalled_engine,
            target_region_id,
            Rows {
                schema: stalled_schema,
                rows: build_rows(5, 6),
            },
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !stalled_write.is_finished(),
        "a write submitted inside the window must stall behind it"
    );

    drop(manifest_guard);
    copy_task.await.unwrap().unwrap();
    stalled_write.await.unwrap();
    let stalled_sequence = target.version_control.current().last_sequence;

    // A write submitted after the window closed is admitted without waiting for a drain that
    // outlived it.
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        put_rows(
            &engine,
            target_region_id,
            Rows {
                schema: schema.clone(),
                rows: build_rows(6, 7),
            },
        ),
    )
    .await
    .expect("a write after the copy must not wait for a drain that outlived the window");
    assert!(
        target.version_control.current().last_sequence > stalled_sequence,
        "the write after the window must be attributed a sequence above the stalled one, \
         stalled: {stalled_sequence}, last_sequence: {}",
        target.version_control.current().last_sequence
    );
    assert!(
        stalled_sequence > last_sequence_before_copy,
        "the stalled write must be admitted above the published barrier, stalled: {stalled_sequence}, \
         last_sequence_before_copy: {last_sequence_before_copy}"
    );
}
