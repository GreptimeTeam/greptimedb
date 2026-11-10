// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use api::v1::Rows;
use bytes::Bytes;
use common_base::Plugins;
use common_recordbatch::RecordBatches;
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use parquet::file::metadata::ParquetMetaData;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionRequest, RegionTruncateRequest};
use store_api::storage::{FileId, RegionId};

use super::{PublicationOutcome, TableCompactionSnapshot};
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::region_hook::{RegionHook, RegionHookRef, VerifiedSstFileInfo};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::region::MitoRegionRef;
use crate::sst::file::{FileMeta, LEVEL_SHARED, SHARED_LEVEL_SLOT};
use crate::sst::location;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, reopen_region, rows_schema,
};

fn config() -> MitoConfig {
    MitoConfig {
        schedule_compaction_after_edit: false,
        manifest_checkpoint_distance: 0,
        ..Default::default()
    }
}

async fn copy_sst(region: &MitoRegionRef, source: &FileMeta, level: u8) -> FileMeta {
    let mut output = source.clone();
    output.file_id = FileId::random();
    output.level = level;
    let access = &region.access_layer;
    let path = |file: &FileMeta| {
        location::sst_file_path(access.table_dir(), file.file_id(), access.path_type())
    };
    access
        .object_store()
        .copy(&path(source), &path(&output))
        .await
        .unwrap();
    if source.index_file_size > 0 {
        let path = |file: &FileMeta| {
            location::index_file_path(access.table_dir(), file.index_id(), access.path_type())
        };
        access
            .object_store()
            .copy(&path(source), &path(&output))
            .await
            .unwrap();
    }
    output
}

/// Input admission is deliberately separate from the compaction API.
async fn seed(engine: &MitoEngine, region_id: RegionId) -> (TableCompactionSnapshot, FileMeta) {
    let (snapshot, outputs) = seed_shared_inputs(engine, region_id, &[(0, 3)]).await;
    let output = outputs.into_iter().next().unwrap();
    (snapshot, output)
}

/// Writes each row range as a flushed SST, promotes the flushed files to shared
/// level, and returns one region-local output per shared input under the snapshot.
async fn seed_shared_inputs(
    engine: &MitoEngine,
    region_id: RegionId,
    row_ranges: &[(usize, usize)],
) -> (TableCompactionSnapshot, Vec<FileMeta>) {
    let create = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(create.clone()))
        .await
        .unwrap();
    for &(start, end) in row_ranges {
        put_rows(
            engine,
            region_id,
            Rows {
                schema: rows_schema(&create),
                rows: build_rows(start, end),
            },
        )
        .await;
        flush_region(engine, region_id, None).await;
    }
    let region = engine.get_region(region_id).unwrap();
    let originals = region.version().ssts.levels()[0]
        .files()
        .map(|file| file.meta_ref().clone())
        .collect::<Vec<_>>();
    assert_eq!(row_ranges.len(), originals.len());
    let mut shared = Vec::with_capacity(originals.len());
    for original in &originals {
        shared.push(copy_sst(&region, original, LEVEL_SHARED).await);
    }
    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: shared,
                files_to_remove: originals,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let snapshot = engine.table_compaction_snapshot(region_id).unwrap();
    assert_eq!(row_ranges.len(), snapshot.inputs.len());
    let mut outputs = Vec::with_capacity(snapshot.inputs.len());
    for input in &snapshot.inputs {
        outputs.push(copy_sst(&region, input, 0).await);
    }
    (snapshot, outputs)
}

async fn row_count(engine: &MitoEngine, region_id: RegionId) -> usize {
    RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, Default::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .iter()
    .map(|batch| batch.num_rows())
    .sum()
}

#[tokio::test]
async fn test_duplicate_publication_and_later_replacement_do_not_readmit_inputs() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(config()).await;
    let region_id = RegionId::new(1, 1);
    let (snapshot, output) = seed(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let sequence = region.version_control.committed_sequence();
    let (first, second) = tokio::join!(
        engine.publish_table_compaction(region_id, snapshot.clone(), vec![output.clone()]),
        engine.publish_table_compaction(region_id, snapshot.clone(), vec![output.clone()])
    );
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        1,
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublicationOutcome::Applied)
            .count()
    );
    assert_eq!(
        1,
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublicationOutcome::StaleInputs)
            .count()
    );
    assert_eq!(sequence, region.version_control.committed_sequence());
    assert_eq!(sequence, region.version_control.current().last_sequence);
    assert_eq!(3, row_count(&engine, region_id).await);
    let manifest = region.manifest_ctx.manifest().await;
    let encoded = serde_json::to_value(manifest.as_ref()).unwrap();
    assert!(encoded.get("table_compaction").is_none() && encoded.get("extensions").is_none());

    // Publish a later ordinary compaction, then remove the old output object.
    // A late retry must reject its obsolete inputs before trying to verify C.
    let replacement = copy_sst(&region, &output, 0).await;
    let edit = RegionEdit {
        files_to_remove: vec![output.clone()],
        files_to_add: vec![replacement.clone()],
        ..Default::default()
    };
    region
        .manifest_ctx
        .update_manifest_for_compaction(RegionMetaActionList::with_action(RegionMetaAction::Edit(
            edit.clone(),
        )))
        .await
        .unwrap();
    region
        .version_control
        .apply_edit(Some(edit), &[], region.file_purger.clone());
    let path = location::sst_file_path(
        region.access_layer.table_dir(),
        output.file_id(),
        region.path_type(),
    );
    region
        .access_layer
        .object_store()
        .delete(&path)
        .await
        .unwrap();
    assert_eq!(
        PublicationOutcome::StaleInputs,
        engine
            .publish_table_compaction(region_id, snapshot, vec![output.clone()])
            .await
            .unwrap()
    );
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(1, manifest.files.len());
    assert!(manifest.files.contains_key(&replacement.file_id));
    assert!(!manifest.files.contains_key(&output.file_id));
    reopen_region(&engine, region_id, "test".into(), true, Default::default()).await;
    assert_eq!(3, row_count(&engine, region_id).await);
}

#[tokio::test]
async fn test_partial_success_can_retry_both_regions_without_duplicate_files() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(config()).await;
    let a = RegionId::new(1, 1);
    let b = RegionId::new(1, 2);
    let (snapshot_a, output_a) = seed(&engine, a).await;
    let (snapshot_b, output_b) = seed(&engine, b).await;
    let invalid = FileMeta {
        file_size: output_b.file_size + 1,
        ..output_b.clone()
    };
    let (first_a, first_b) = tokio::join!(
        engine.publish_table_compaction(a, snapshot_a.clone(), vec![output_a.clone()]),
        engine.publish_table_compaction(b, snapshot_b.clone(), vec![invalid])
    );
    assert_eq!(PublicationOutcome::Applied, first_a.unwrap());
    assert!(first_b.is_err());
    assert_eq!(
        snapshot_b.inputs,
        engine.table_compaction_snapshot(b).unwrap().inputs
    );
    let (retry_a, retry_b) = tokio::join!(
        engine.publish_table_compaction(a, snapshot_a, vec![output_a.clone()]),
        engine.publish_table_compaction(b, snapshot_b, vec![output_b.clone()])
    );
    assert_eq!(PublicationOutcome::StaleInputs, retry_a.unwrap());
    assert_eq!(PublicationOutcome::Applied, retry_b.unwrap());
    for (id, output) in [(a, output_a), (b, output_b)] {
        let region = engine.get_region(id).unwrap();
        let manifest = region.manifest_ctx.manifest().await;
        assert_eq!(1, manifest.files.len());
        assert_eq!(&output, &manifest.files[&output.file_id]);
        assert_eq!(3, row_count(&engine, id).await);
    }
}

#[tokio::test]
async fn test_invalid_results_preserve_inputs_and_reopen_can_replan() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(config()).await;
    let region_id = RegionId::new(1, 1);
    let (snapshot, output) = seed(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let version = region.manifest_ctx.manifest_version().await;
    // Snapshot-level and footer-derived mutations must all fail output admission
    // with the specific reason, leaving the manifest, inputs and rows untouched.
    #[derive(Clone, Copy)]
    enum ExpectedError {
        InvalidRequest(&'static str),
        OpenDal,
    }
    let statistics_mismatch =
        ExpectedError::InvalidRequest("Output statistics differ from its footer");
    let invalid_output = ExpectedError::InvalidRequest("Invalid region-local compaction output");
    let timestamp_mismatch = ExpectedError::InvalidRequest("Output timestamp range mismatch");
    let cases = [
        (
            FileMeta {
                file_size: output.file_size + 1,
                ..output.clone()
            },
            ExpectedError::InvalidRequest("Output SST size mismatch"),
        ),
        (
            FileMeta {
                num_rows: output.num_rows + 1,
                ..output.clone()
            },
            statistics_mismatch,
        ),
        (
            FileMeta {
                num_row_groups: output.num_row_groups + 1,
                ..output.clone()
            },
            statistics_mismatch,
        ),
        (
            FileMeta {
                max_row_group_uncompressed_size: output.max_row_group_uncompressed_size + 1,
                ..output.clone()
            },
            statistics_mismatch,
        ),
        (
            FileMeta {
                primary_key_min: Some(Bytes::from_static(b"not-the-footer-minimum")),
                ..output.clone()
            },
            statistics_mismatch,
        ),
        (
            FileMeta {
                primary_key_max: Some(Bytes::from_static(b"not-the-footer-maximum")),
                ..output.clone()
            },
            statistics_mismatch,
        ),
        (
            FileMeta {
                time_range: (
                    Timestamp::new(output.time_range.0.value() + 1, output.time_range.0.unit()),
                    output.time_range.1,
                ),
                ..output.clone()
            },
            timestamp_mismatch,
        ),
        (
            FileMeta {
                time_range: (
                    output.time_range.0,
                    Timestamp::new(output.time_range.1.value(), TimeUnit::Second),
                ),
                ..output.clone()
            },
            timestamp_mismatch,
        ),
        (
            FileMeta {
                file_id: FileId::random(),
                ..output.clone()
            },
            ExpectedError::OpenDal,
        ),
        (
            FileMeta {
                sequence: None,
                ..output.clone()
            },
            invalid_output,
        ),
        (
            FileMeta {
                file_id: snapshot.inputs[0].file_id,
                ..output.clone()
            },
            invalid_output,
        ),
    ];
    for (invalid, expected) in cases {
        let error = engine
            .publish_table_compaction(region_id, snapshot.clone(), vec![invalid])
            .await
            .unwrap_err();
        let source = match error {
            crate::error::Error::EditRegion { source, .. } => source,
            other => panic!("unexpected error: {other}"),
        };
        match expected {
            ExpectedError::InvalidRequest(reason) => assert!(
                matches!(source.as_ref(), crate::error::Error::InvalidRequest { reason: actual, .. } if actual == reason),
                "unexpected error: {source}"
            ),
            ExpectedError::OpenDal => assert!(
                matches!(source.as_ref(), crate::error::Error::OpenDal { .. }),
                "unexpected error: {source}"
            ),
        }
        assert_eq!(version, region.manifest_ctx.manifest_version().await);
        assert_eq!(
            snapshot.inputs,
            engine.table_compaction_snapshot(region_id).unwrap().inputs
        );
        assert_eq!(3, row_count(&engine, region_id).await);
    }
    // A real footer written for another region's metadata must reach the footer
    // check; the stale-input shortcut must not mask it.
    let foreign_region_id = RegionId::new(1, 2);
    let (_, foreign_output) = seed(&engine, foreign_region_id).await;
    let foreign_region = engine.get_region(foreign_region_id).unwrap();
    let mut foreign = foreign_output;
    assert_eq!(0, foreign.index_file_size);
    let foreign_path = location::sst_file_path(
        foreign_region.access_layer.table_dir(),
        foreign.file_id(),
        foreign_region.access_layer.path_type(),
    );
    foreign.file_id = FileId::random();
    foreign.region_id = region_id;
    foreign.sequence = output.sequence;
    let target_path = location::sst_file_path(
        region.access_layer.table_dir(),
        foreign.file_id(),
        region.access_layer.path_type(),
    );
    foreign_region
        .access_layer
        .object_store()
        .copy(&foreign_path, &target_path)
        .await
        .unwrap();
    let error = engine
        .publish_table_compaction(region_id, snapshot.clone(), vec![foreign])
        .await
        .unwrap_err();
    let source = match error {
        crate::error::Error::EditRegion { source, .. } => source,
        other => panic!("unexpected error: {other}"),
    };
    assert!(
        matches!(source.as_ref(), crate::error::Error::InvalidRequest { reason, .. }
            if reason == "Output footer schema or partition mismatch"),
        "unexpected error: {source}"
    );
    assert_eq!(version, region.manifest_ctx.manifest_version().await);
    assert_eq!(
        snapshot.inputs,
        engine.table_compaction_snapshot(region_id).unwrap().inputs
    );
    assert_eq!(3, row_count(&engine, region_id).await);
    let mut stale_input = snapshot.clone();
    stale_input.inputs[0].index_version += 1;
    assert_eq!(
        PublicationOutcome::StaleInputs,
        engine
            .publish_table_compaction(region_id, stale_input, vec![output.clone()])
            .await
            .unwrap()
    );
    let mut duplicate = snapshot.clone();
    duplicate.inputs.push(snapshot.inputs[0].clone());
    assert!(
        engine
            .publish_table_compaction(region_id, duplicate, vec![output.clone()])
            .await
            .is_err()
    );
    let mut malformed = snapshot.clone();
    malformed.options = serde_json::json!({ "append_mode": "not-a-bool" });
    let error = engine
        .publish_table_compaction(region_id, malformed, vec![output.clone()])
        .await
        .unwrap_err();
    assert!(matches!(error, crate::error::Error::SerdeJson { .. }));
    assert_eq!(version, region.manifest_ctx.manifest_version().await);
    assert_eq!(
        snapshot.inputs,
        engine.table_compaction_snapshot(region_id).unwrap().inputs
    );
    let mut stale = snapshot;
    stale.options["append_mode"] = serde_json::json!(true);
    assert_eq!(
        PublicationOutcome::StaleInputs,
        engine
            .publish_table_compaction(region_id, stale, vec![output.clone()])
            .await
            .unwrap()
    );
    assert_eq!(version, region.manifest_ctx.manifest_version().await);
    reopen_region(&engine, region_id, "test".into(), true, Default::default()).await;
    // V1 refreshes the snapshot after restart instead of resuming a Prepared task.
    // The public snapshot contract is JSON, so a serde round trip must still publish.
    let fresh = engine.table_compaction_snapshot(region_id).unwrap();
    let fresh: TableCompactionSnapshot =
        serde_json::from_value(serde_json::to_value(&fresh).unwrap()).unwrap();
    assert_eq!(
        PublicationOutcome::Applied,
        engine
            .publish_table_compaction(region_id, fresh, vec![output])
            .await
            .unwrap()
    );
    assert_eq!(3, row_count(&engine, region_id).await);
}

/// A preserve-row-sequence region trusts output markers only when the typed
/// snapshot options and the admitted inputs both carry the marker.
#[tokio::test]
async fn test_preserve_row_sequence_option_drives_output_admission() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(config()).await;
    let region_id = RegionId::new(1, 1);
    let create = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(create.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&create),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    let region = engine.get_region(region_id).unwrap();
    let original = region.version().ssts.levels()[0]
        .files()
        .next()
        .unwrap()
        .meta_ref()
        .clone();
    assert!(original.preserve_row_sequence);
    // A generic import clears the trust marker, so install the admitted shared
    // input directly: this is the shape table compaction receives after admission.
    let shared = copy_sst(&region, &original, LEVEL_SHARED).await;
    let edit = RegionEdit {
        files_to_add: vec![shared],
        files_to_remove: vec![original],
        ..Default::default()
    };
    region
        .manifest_ctx
        .update_manifest_for_compaction(RegionMetaActionList::with_action(RegionMetaAction::Edit(
            edit.clone(),
        )))
        .await
        .unwrap();
    region
        .version_control
        .apply_edit(Some(edit), &[], region.file_purger.clone());
    let snapshot = engine.table_compaction_snapshot(region_id).unwrap();
    assert_eq!(
        Some(true),
        snapshot.options["preserve_row_sequence"].as_bool()
    );
    let output = copy_sst(&region, &snapshot.inputs[0], 0).await;
    assert!(output.preserve_row_sequence);
    let untrusted = FileMeta {
        preserve_row_sequence: false,
        ..output.clone()
    };
    let version = region.manifest_ctx.manifest_version().await;
    let error = engine
        .publish_table_compaction(region_id, snapshot.clone(), vec![untrusted])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::error::Error::EditRegion { source, .. }
            if matches!(source.as_ref(), crate::error::Error::InvalidRequest { reason, .. }
                if reason == "Invalid region-local compaction output")
    ));
    assert_eq!(version, region.manifest_ctx.manifest_version().await);
    assert_eq!(
        snapshot.inputs,
        engine.table_compaction_snapshot(region_id).unwrap().inputs
    );
    assert_eq!(
        PublicationOutcome::Applied,
        engine
            .publish_table_compaction(region_id, snapshot, vec![output.clone()])
            .await
            .unwrap()
    );
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(Some(&output), manifest.files.get(&output.file_id));
    assert_eq!(3, row_count(&engine, region_id).await);
}

#[tokio::test]
async fn test_empty_target_replacement_preserves_other_shared_reference() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(config()).await;
    let source = RegionId::new(1, 1);
    let target = RegionId::new(1, 2);
    let (source_snapshot, _) = seed(&engine, source).await;
    let expr =
        partition::expr::col("tag_0").eq(datatypes::value::Value::String("not-in-source".into()));
    let create = CreateRequestBuilder::new()
        .partition_expr_json(Some(expr.as_json_str().unwrap()))
        .build();
    engine
        .handle_request(target, RegionRequest::Create(create))
        .await
        .unwrap();
    let copy = store_api::region_engine::MitoCopyRegionFromRequest {
        source_region_id: source,
        parallelism: 1,
    };
    engine.copy_region_from(target, copy.clone()).await.unwrap();
    engine.copy_region_from(target, copy).await.unwrap();
    let snapshot = engine.table_compaction_snapshot(target).unwrap();
    assert_eq!(1, snapshot.inputs.len());
    assert_eq!(
        source_snapshot.inputs[0].file_id(),
        snapshot.inputs[0].file_id()
    );
    let region = engine.get_region(target).unwrap();
    assert_eq!(1, region.version_control.current().last_sequence);
    assert_eq!(0, row_count(&engine, target).await);
    assert_eq!(
        PublicationOutcome::Applied,
        engine
            .publish_table_compaction(target, snapshot.clone(), vec![])
            .await
            .unwrap()
    );
    assert_eq!(
        PublicationOutcome::StaleInputs,
        engine
            .publish_table_compaction(target, snapshot, vec![])
            .await
            .unwrap()
    );
    assert_eq!(1, region.version_control.current().last_sequence);
    assert_eq!(0, row_count(&engine, target).await);
    assert_eq!(3, row_count(&engine, source).await);
}

#[tokio::test]
async fn test_staging_rejects_publication_without_changing_files() {
    use store_api::region_request::{EnterStagingRequest, StagingPartitionDirective};
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(config()).await;
    let region_id = RegionId::new(1, 1);
    let (snapshot, output) = seed(&engine, region_id).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let version = region.manifest_ctx.manifest_version().await;
    assert!(
        engine
            .publish_table_compaction(region_id, snapshot, vec![output])
            .await
            .is_err()
    );
    assert!(region.is_staging());
    assert_eq!(version, region.manifest_ctx.manifest_version().await);
    assert_eq!(
        1,
        region.version().ssts.levels()[SHARED_LEVEL_SLOT]
            .files
            .len()
    );
}

#[rstest::rstest]
#[case::before_persist(false)]
#[case::lost_ack(true)]
#[tokio::test]
async fn test_uncertain_write_recovers_files_before_retry(#[case] persisted: bool) {
    use object_store::layers::mock::{
        Buffer, Error, ErrorKind, Metadata, MockLayerBuilder, Result, oio,
    };
    struct FailManifestWrite {
        inner: oio::Writer,
        pending: Vec<Buffer>,
        armed: Arc<AtomicBool>,
        persisted: bool,
    }
    impl oio::Write for FailManifestWrite {
        async fn write(&mut self, bytes: Buffer) -> Result<()> {
            // Buffer at the I/O boundary so the pre-persist case never depends
            // on whether the backing filesystem supports aborting a writer.
            self.pending.push(bytes);
            Ok(())
        }
        async fn close(&mut self) -> Result<Metadata> {
            let fail = self.armed.swap(false, Ordering::SeqCst);
            if fail && !self.persisted {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "injected failure before persistence",
                ));
            }
            for bytes in self.pending.drain(..) {
                self.inner.write(bytes).await?;
            }
            let metadata = self.inner.close().await?;
            if fail {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "lost manifest acknowledgement",
                ))
            } else {
                Ok(metadata)
            }
        }
        async fn abort(&mut self) -> Result<()> {
            self.inner.abort().await
        }
    }
    let armed = Arc::new(AtomicBool::new(false));
    let fault = armed.clone();
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _, inner| {
            if path.contains("/manifest/") {
                Box::new(FailManifestWrite {
                    inner,
                    pending: Vec::new(),
                    armed: fault.clone(),
                    persisted,
                }) as oio::Writer
            } else {
                inner
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(layer);
    let engine = env.create_engine(config()).await;
    let region_id = RegionId::new(1, 1);
    let (snapshot, output) = seed(&engine, region_id).await;
    let access = engine.get_region(region_id).unwrap().access_layer.clone();
    armed.store(true, Ordering::SeqCst);
    assert!(
        engine
            .publish_table_compaction(region_id, snapshot.clone(), vec![output.clone()])
            .await
            .is_err()
    );
    assert!(!armed.load(Ordering::SeqCst));
    assert!(engine.get_region(region_id).is_none());
    let path = location::sst_file_path(access.table_dir(), output.file_id(), access.path_type());
    assert!(access.object_store().exists(&path).await.unwrap());
    reopen_region(&engine, region_id, "test".into(), true, Default::default()).await;
    let fresh = engine.table_compaction_snapshot(region_id).unwrap();
    assert_eq!(!persisted, !fresh.inputs.is_empty());
    if persisted {
        assert_eq!(
            PublicationOutcome::StaleInputs,
            engine
                .publish_table_compaction(region_id, snapshot, vec![output])
                .await
                .unwrap()
        );
    } else {
        assert_eq!(
            PublicationOutcome::Applied,
            engine
                .publish_table_compaction(region_id, fresh, vec![output])
                .await
                .unwrap()
        );
    }
    assert_eq!(3, row_count(&engine, region_id).await);
}

#[derive(Debug)]
struct PublicationBarrier {
    after_commit: bool,
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl PublicationBarrier {
    async fn pause(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
    }
}

#[async_trait::async_trait]
impl RegionHook for PublicationBarrier {
    async fn on_sst_files_verified(
        &self,
        _: RegionId,
        _: &store_api::metadata::RegionMetadataRef,
        _: &[VerifiedSstFileInfo<'_>],
    ) {
        if !self.after_commit {
            self.pause().await;
        }
    }
    async fn on_manifest_updated(
        &self,
        _: RegionId,
        actions: &RegionMetaActionList,
        _: store_api::ManifestVersion,
    ) {
        if self.after_commit && actions.actions.iter().any(|action| matches!(action,
            RegionMetaAction::Edit(edit) if edit.files_to_remove.iter().any(|file| file.level == LEVEL_SHARED))) {
            self.pause().await;
        }
    }
}

#[rstest::rstest]
#[case::verification(false)]
#[case::completion(true)]
#[tokio::test]
async fn test_close_reopen_fences_old_publication(#[case] after_commit: bool) {
    let hook = Arc::new(PublicationBarrier {
        after_commit,
        armed: AtomicBool::new(false),
        entered: Default::default(),
        resume: Default::default(),
    });
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let mut env = TestEnv::new().await;
    let engine = env.create_engine_with_plugins(config(), plugins).await;
    let region_id = RegionId::new(1, 1);
    let (snapshot, output) = seed(&engine, region_id).await;
    hook.armed.store(true, Ordering::SeqCst);
    let (late, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            engine.publish_table_compaction(region_id, snapshot.clone(), vec![output.clone()]),
            async {
                hook.entered.notified().await;
                reopen_region(&engine, region_id, "test".into(), true, Default::default()).await;
                engine
                    .handle_request(
                        region_id,
                        RegionRequest::Truncate(RegionTruncateRequest::All),
                    )
                    .await
                    .unwrap();
                hook.resume.notify_one();
            }
        )
    })
    .await
    .unwrap();
    assert!(late.is_err());
    let region = engine.get_region(region_id).unwrap();
    assert!(region.manifest_ctx.manifest().await.files.is_empty());
    assert!(
        region
            .version()
            .ssts
            .levels()
            .iter()
            .all(|level| level.files.is_empty())
    );
    assert_eq!(
        PublicationOutcome::StaleInputs,
        engine
            .publish_table_compaction(region_id, snapshot, vec![output])
            .await
            .unwrap()
    );
    assert_eq!(0, row_count(&engine, region_id).await);
}

/// Records one notification per verification, keeping verified footers only as
/// weak references, and checks those references again when the publication's
/// manifest update is announced. `on_manifest_updated` runs after the commit, so
/// this catches footers retained across it; it does not claim the hook fires
/// before the commit. Nothing else in the test keeps the footers alive.
#[derive(Debug, Default)]
struct VerificationRecorder {
    verified_files: AtomicUsize,
    notifications: AtomicUsize,
    manifest_updates: AtomicUsize,
    released_by_commit: AtomicBool,
    footers: std::sync::Mutex<Vec<Weak<ParquetMetaData>>>,
}

#[async_trait::async_trait]
impl RegionHook for VerificationRecorder {
    async fn on_sst_files_verified(
        &self,
        _: RegionId,
        _: &store_api::metadata::RegionMetadataRef,
        files: &[VerifiedSstFileInfo<'_>],
    ) {
        let mut footers = self.footers.lock().unwrap();
        footers.extend(files.iter().map(|file| Arc::downgrade(file.footer)));
        self.verified_files.fetch_add(files.len(), Ordering::SeqCst);
        self.notifications.fetch_add(1, Ordering::SeqCst);
    }

    async fn on_manifest_updated(
        &self,
        _: RegionId,
        actions: &RegionMetaActionList,
        _: store_api::ManifestVersion,
    ) {
        let is_compaction = actions.actions.iter().any(|action| matches!(action,
            RegionMetaAction::Edit(edit) if edit.files_to_remove.iter().any(|file| file.level == LEVEL_SHARED)));
        if !is_compaction {
            return;
        }
        let footers = self.footers.lock().unwrap();
        self.released_by_commit.store(
            !footers.is_empty() && footers.iter().all(|footer| footer.upgrade().is_none()),
            Ordering::SeqCst,
        );
        self.manifest_updates.fetch_add(1, Ordering::SeqCst);
    }
}

#[rstest::rstest]
#[case::without_hook(false)]
#[case::with_hook(true)]
#[tokio::test]
async fn test_multiple_outputs_verify_notify_once_and_release_footers(#[case] with_hook: bool) {
    let recorder = Arc::new(VerificationRecorder::default());
    let mut env = TestEnv::new().await;
    let engine = if with_hook {
        let plugins = Plugins::new();
        plugins.insert(recorder.clone() as RegionHookRef);
        env.create_engine_with_plugins(config(), plugins).await
    } else {
        env.create_engine(config()).await
    };
    let region_id = RegionId::new(1, 1);
    let (snapshot, outputs) = seed_shared_inputs(&engine, region_id, &[(0, 3), (3, 6)]).await;
    assert_eq!(
        PublicationOutcome::Applied,
        engine
            .publish_table_compaction(region_id, snapshot, outputs.clone())
            .await
            .unwrap()
    );
    let expected_notifications = usize::from(with_hook);
    assert_eq!(
        expected_notifications * outputs.len(),
        recorder.verified_files.load(Ordering::SeqCst)
    );
    assert_eq!(
        expected_notifications,
        recorder.notifications.load(Ordering::SeqCst)
    );
    if with_hook {
        assert_eq!(1, recorder.manifest_updates.load(Ordering::SeqCst));
        assert!(recorder.released_by_commit.load(Ordering::SeqCst));
        assert!(
            recorder
                .footers
                .lock()
                .unwrap()
                .iter()
                .all(|footer| footer.upgrade().is_none())
        );
    } else {
        assert_eq!(0, recorder.manifest_updates.load(Ordering::SeqCst));
    }
    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(outputs.len(), manifest.files.len());
    for output in &outputs {
        assert_eq!(Some(output), manifest.files.get(&output.file_id));
    }
    assert_eq!(6, row_count(&engine, region_id).await);
}

#[tokio::test]
async fn test_failed_verification_does_not_notify_hook() {
    let recorder = Arc::new(VerificationRecorder::default());
    let plugins = Plugins::new();
    plugins.insert(recorder.clone() as RegionHookRef);
    let mut env = TestEnv::new().await;
    let engine = env.create_engine_with_plugins(config(), plugins).await;
    let region_id = RegionId::new(1, 1);
    let (snapshot, outputs) = seed_shared_inputs(&engine, region_id, &[(0, 3), (3, 6)]).await;
    let region = engine.get_region(region_id).unwrap();
    let version = region.manifest_ctx.manifest_version().await;
    let invalid = FileMeta {
        file_size: outputs[1].file_size + 1,
        ..outputs[1].clone()
    };
    let error = engine
        .publish_table_compaction(
            region_id,
            snapshot.clone(),
            vec![outputs[0].clone(), invalid],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::error::Error::EditRegion { source, .. }
            if matches!(source.as_ref(), crate::error::Error::InvalidRequest { reason, .. }
                if reason == "Output SST size mismatch")
    ));
    assert_eq!(0, recorder.verified_files.load(Ordering::SeqCst));
    assert_eq!(0, recorder.notifications.load(Ordering::SeqCst));
    assert_eq!(version, region.manifest_ctx.manifest_version().await);
    assert_eq!(
        snapshot.inputs,
        engine.table_compaction_snapshot(region_id).unwrap().inputs
    );
    assert_eq!(6, row_count(&engine, region_id).await);
}
