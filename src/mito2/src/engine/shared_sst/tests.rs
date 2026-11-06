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

mod publication;

use std::collections::BTreeMap;

use common_base::cancellation::CancellationHandle;
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use partition::expr::col;
use partition::multi_dim::MultiDimPartitionRule;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    PathType, RegionCloseRequest, RegionOpenRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::ScanRequest;

use super::*;
use crate::access_layer::{Metrics, OperationType, SstWriteRequest, WriteType};
use crate::cache::CacheManager;
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionPlan, MultiRegionTarget, compact_regions,
};
use crate::config::MitoConfig;
use crate::manifest::action::RegionEdit;
use crate::sst::file::RegionFileId;
use crate::sst::parquet::WriteOptions;
use crate::sst::parquet::writer::SstWriteGuard;
use crate::sst::{FlatSchemaOptions, FormatType, to_flat_sst_arrow_schema};
use crate::test_util::sst_util::{
    new_flat_source_from_record_batches, new_record_batch_with_custom_sequence,
};
use crate::test_util::{CreateRequestBuilder, TestEnv};

struct Fixture {
    _env: TestEnv,
    engine: MitoEngine,
    source: FileMeta,
    registrations: Vec<RegisterSharedSst>,
    path_type: PathType,
}

// The fixture runs without GC; durable output protection is the caller's
// integration responsibility, not a mock of the storage operations under test.
struct NoopWriteGuard;
#[async_trait::async_trait]
impl SstWriteGuard for NoopWriteGuard {
    async fn before_create(&self, _: RegionFileId) -> Result<()> {
        Ok(())
    }
}

impl Fixture {
    async fn new() -> Self {
        Self::with_source_tags(&["a", "z"]).await
    }

    async fn with_source_tags(tags: &[&str]) -> Self {
        Self::with_plugins(tags, common_base::Plugins::new()).await
    }

    async fn with_plugins(tags: &[&str], plugins: common_base::Plugins) -> Self {
        Self::with_layout(tags, plugins, PathType::Bare, true).await
    }

    async fn with_layout(
        tags: &[&str],
        plugins: common_base::Plugins,
        path_type: PathType,
        flat_format: bool,
    ) -> Self {
        let mut env = TestEnv::new().await;
        let engine = env
            .create_engine_with_plugins(
                MitoConfig {
                    schedule_compaction_after_edit: false,
                    default_flat_format: flat_format,
                    ..Default::default()
                },
                plugins,
            )
            .await;
        for number in 0..2 {
            let expr = if number == 0 {
                col("tag_0").lt(Value::String("m".into()))
            } else {
                col("tag_0").gt_eq(Value::String("m".into()))
            };
            let mut request = CreateRequestBuilder::new()
                .tag_num(2)
                .field_datatype(ConcreteDataType::uint64_datatype())
                .partition_expr_json(Some(expr.as_json_str().unwrap()))
                .build();
            request.path_type = path_type;
            engine
                .handle_request(RegionId::new(1, number), RegionRequest::Create(request))
                .await
                .unwrap();
        }
        let region = engine.find_region(RegionId::new(1, 0)).unwrap();
        let mut source_metadata = (*region.metadata()).clone();
        // Production bulk files originate in region 0: its native legacy-zero
        // sequence path and the other target's foreign-file path must both work.
        source_metadata.region_id = RegionId::new(1, 0);
        source_metadata.set_partition_expr(None);
        let source = write_sst(
            &engine,
            Arc::new(source_metadata),
            tags,
            LEVEL_SHARED,
            10,
            0,
        )
        .await;
        let mut registrations = Vec::new();
        for number in 0..2 {
            let snapshot = engine
                .shared_sst_snapshot(RegionId::new(1, number))
                .await
                .unwrap();
            registrations.push(RegisterSharedSst {
                metadata: snapshot.metadata,
                manifest_version: snapshot.manifest_version,
                file: source.clone(),
            });
        }
        Self {
            _env: env,
            engine,
            source,
            registrations,
            path_type,
        }
    }

    async fn register(&self, number: usize) -> FileMeta {
        match self
            .engine
            .handle_shared_sst(
                RegionId::new(1, number as u32),
                SharedSstOperation::Register(Box::new(self.registrations[number].clone())),
            )
            .await
            .unwrap()
        {
            SharedSstResponse::Registered(file) => *file,
            other => panic!("Unexpected response: {other:?}"),
        }
    }

    async fn compute(&self) -> Vec<ApplySharedCompaction> {
        let mut targets = BTreeMap::new();
        let mut requests = Vec::new();
        for number in 0..2 {
            let snapshot = self
                .engine
                .shared_sst_snapshot(RegionId::new(1, number))
                .await
                .unwrap();
            targets.insert(
                snapshot.metadata.region_id,
                MultiRegionTarget {
                    metadata: snapshot.metadata.clone(),
                    options: snapshot.options.clone(),
                    inputs: snapshot
                        .files
                        .iter()
                        .map(|f| (f.file_id(), f.clone()))
                        .collect(),
                },
            );
            requests.push(ApplySharedCompaction {
                metadata: snapshot.metadata,
                options: snapshot.options,
                inputs: snapshot.files,
                outputs: Vec::new(),
                window_start: 0,
                window_seconds: 3600,
            });
        }
        let rule = MultiDimPartitionRule::try_new(
            vec!["tag_0".into()],
            vec![0, 1],
            requests
                .iter()
                .map(|r| {
                    crate::region::parse_partition_expr(r.metadata.partition_expr.as_deref())
                        .unwrap()
                        .unwrap()
                })
                .collect(),
            false,
        )
        .unwrap();
        let plan = MultiRegionCompactionPlan {
            partition_rule: Arc::new(rule),
            sources: vec![self.source.clone()],
            targets,
            memory_limit: 256 * 1024 * 1024,
            max_output_file_size: 1024 * 1024,
            window_start: 0,
            window_seconds: 3600,
        };
        let region = self.engine.find_region(RegionId::new(1, 0)).unwrap();
        let mut output = compact_regions(
            plan,
            MultiRegionCompactionContext {
                access_layer: region.access_layer.clone(),
                cache: Arc::new(CacheManager::default()),
                config: self.engine.inner.config.clone(),
                protection: Arc::new(NoopWriteGuard),
                cancellation: Arc::new(CancellationHandle::default()),
            },
        )
        .await
        .unwrap();
        assert_eq!(output.stats.source_files, 1);
        assert_eq!(output.stats.source_rows, self.source.num_rows);
        for request in &mut requests {
            request.outputs = output.files.remove(&request.metadata.region_id).unwrap();
        }
        requests
    }

    async fn apply(&self, request: ApplySharedCompaction) -> Result<SharedSstResponse> {
        self.engine
            .handle_shared_sst(
                request.metadata.region_id,
                SharedSstOperation::Apply(Box::new(request)),
            )
            .await
    }

    async fn query(&self, number: u32) -> String {
        let stream = self
            .engine
            .scan_to_stream(RegionId::new(1, number), ScanRequest::default())
            .await
            .unwrap();
        RecordBatches::try_collect(stream)
            .await
            .unwrap()
            .pretty_print()
            .unwrap()
    }

    async fn reopen(&self, number: u32) {
        let region_id = RegionId::new(1, number);
        self.engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
            .unwrap();
        self.engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    table_dir: "test".into(),
                    options: Default::default(),
                    skip_wal_replay: false,
                    path_type: self.path_type,
                    checkpoint: None,
                    requirements: Default::default(),
                }),
            )
            .await
            .unwrap();
        self.engine
            .set_region_role(region_id, store_api::region_engine::RegionRole::Leader)
            .unwrap();
    }
}

async fn write_sst(
    engine: &MitoEngine,
    metadata: RegionMetadataRef,
    tags: &[&str],
    level: u8,
    field_value: u64,
    row_sequence: u64,
) -> FileMeta {
    let region = engine.find_region(RegionId::new(1, 0)).unwrap();
    let config = &engine.inner.config;
    let batches = tags
        .iter()
        .map(|tag| {
            let batch = new_record_batch_with_custom_sequence(&[tag, "x"], 0, 2, row_sequence);
            let mut columns = batch.columns().to_vec();
            columns[2] = Arc::new(datatypes::arrow::array::UInt64Array::from_value(
                field_value,
                2,
            ));
            datatypes::arrow::record_batch::RecordBatch::try_new(
                to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default()),
                columns,
            )
            .unwrap()
        })
        .collect();
    let info = region
        .access_layer
        .write_sst(
            SstWriteRequest {
                op_type: OperationType::Flush,
                metadata: metadata.clone(),
                source: new_flat_source_from_record_batches(batches),
                cache_manager: Arc::new(CacheManager::default()),
                max_sequence: None,
                sst_write_format: FormatType::Flat,
                preserve_row_sequence: false,
                index_options: Default::default(),
                index_config: config.index.clone(),
                inverted_index_config: config.inverted_index.clone(),
                fulltext_index_config: config.fulltext_index.clone(),
                bloom_filter_index_config: config.bloom_filter_index.clone(),
            },
            &WriteOptions::default(),
            &mut Metrics::new(WriteType::Flush),
        )
        .await
        .unwrap()
        .remove(0);
    let range = info.file_metadata.as_ref().and_then(|footer| {
        crate::sst::parquet::metadata::extract_primary_key_range(footer, &metadata)
    });
    FileMeta {
        region_id: metadata.region_id,
        file_id: info.file_id,
        time_range: info.time_range,
        level,
        file_size: info.file_size,
        num_rows: info.num_rows as u64,
        num_row_groups: info.num_row_groups,
        max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
        num_series: info.num_series,
        primary_key_min: range.as_ref().map(|(min, _)| min.clone()),
        primary_key_max: range.map(|(_, max)| max),
        partition_expr: crate::region::parse_partition_expr(metadata.partition_expr.as_deref())
            .unwrap(),
        available_indexes: info.index_metadata.build_available_indexes(),
        indexes: info.index_metadata.build_indexes(),
        index_file_size: info.index_metadata.file_size,
        ..Default::default()
    }
}

#[tokio::test]
async fn concurrent_registration_allocates_one_visible_sequence() {
    let f = Fixture::new().await;
    let region_id = RegionId::new(1, 0);
    let operation = || SharedSstOperation::Register(Box::new(f.registrations[0].clone()));
    let (first, second) = tokio::join!(
        f.engine.handle_shared_sst(region_id, operation()),
        f.engine.handle_shared_sst(region_id, operation()),
    );
    assert!(first.is_ok() || second.is_ok());
    let accepted = f.register(0).await;
    for response in [first, second].into_iter().flatten() {
        assert_eq!(
            SharedSstResponse::Registered(Box::new(accepted.clone())),
            response
        );
    }
    assert_eq!(
        accepted.sequence.unwrap().get(),
        f.engine
            .find_region(region_id)
            .unwrap()
            .version_control
            .current()
            .last_sequence
    );
}

#[tokio::test]
async fn ordinary_newer_data_wins_after_shared_apply() {
    let f = Fixture::new().await;
    f.register(0).await;
    f.register(1).await;
    let requests = f.compute().await;
    let region_id = RegionId::new(1, 0);
    let region = f.engine.find_region(region_id).unwrap();
    let file = write_sst(&f.engine, region.metadata(), &["a"], 0, 99, 0).await;
    f.engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![file],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let sequence = region.version_control.current().last_sequence;
    let before = f.query(0).await;
    assert!(before.contains("99"));
    f.apply(requests[0].clone()).await.unwrap();
    assert_eq!(sequence, region.version_control.current().last_sequence);
    assert_eq!(before, f.query(0).await);
    f.reopen(0).await;
    assert_eq!(before, f.query(0).await);
}

#[tokio::test]
async fn empty_target_removes_its_shared_reference_without_output() {
    let f = Fixture::with_source_tags(&["a"]).await;
    f.register(0).await;
    f.register(1).await;
    let requests = f.compute().await;
    assert!(requests[1].outputs.is_empty());
    assert_eq!(
        SharedSstResponse::Applied,
        f.apply(requests[1].clone()).await.unwrap()
    );
    f.reopen(1).await;
    assert!(
        f.engine
            .shared_sst_snapshot(RegionId::new(1, 1))
            .await
            .unwrap()
            .files
            .is_empty()
    );
    assert_eq!(
        SharedSstResponse::InputsGone,
        f.apply(requests[1].clone()).await.unwrap()
    );
}

#[tokio::test]
async fn generic_edits_cannot_install_or_remove_shared_references() {
    let f = Fixture::new().await;
    let region_id = RegionId::new(1, 0);
    assert!(
        f.engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_add: vec![f.source.clone()],
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let mut accepted = f.register(0).await;
    // Disguising the existing shared identity as L0 must not bypass the boundary.
    accepted.level = 0;
    assert!(
        f.engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_remove: vec![accepted],
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        1,
        f.engine
            .shared_sst_snapshot(region_id)
            .await
            .unwrap()
            .files
            .len()
    );
}

#[tokio::test]
async fn registration_retry_preserves_sequence_and_truncate_fences_replay() {
    let f = Fixture::new().await;
    let accepted = f.register(0).await;
    let first = f
        .engine
        .shared_sst_snapshot(RegionId::new(1, 0))
        .await
        .unwrap();
    assert_eq!(accepted, f.register(0).await);
    assert_eq!(
        first.manifest_version,
        f.engine
            .shared_sst_snapshot(RegionId::new(1, 0))
            .await
            .unwrap()
            .manifest_version
    );
    f.reopen(0).await;
    assert_eq!(accepted, f.register(0).await);
    f.engine
        .handle_request(
            RegionId::new(1, 0),
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();
    assert!(
        f.engine
            .handle_shared_sst(
                RegionId::new(1, 0),
                SharedSstOperation::Register(Box::new(f.registrations[0].clone()))
            )
            .await
            .is_err()
    );
    f.reopen(0).await;
    assert!(
        f.engine
            .shared_sst_snapshot(RegionId::new(1, 0))
            .await
            .unwrap()
            .files
            .is_empty()
    );
}

#[tokio::test]
async fn multi_region_apply_preserves_queries_and_never_reinstalls_retired_outputs() {
    for (path_type, flat_format) in [(PathType::Bare, true), (PathType::Data, false)] {
        assert_apply_roundtrip(path_type, flat_format).await;
    }
}

async fn assert_apply_roundtrip(path_type: PathType, flat_format: bool) {
    let f = Fixture::with_layout(
        &["a", "z"],
        common_base::Plugins::new(),
        path_type,
        flat_format,
    )
    .await;
    f.register(0).await;
    f.register(1).await;
    let before = [f.query(0).await, f.query(1).await];
    assert!(before[0].contains("a"));
    assert!(before[1].contains("z"));
    let requests = f.compute().await;
    for (number, request) in requests.iter().enumerate() {
        assert_eq!(
            SharedSstResponse::Applied,
            f.apply(request.clone()).await.unwrap()
        );
        assert_eq!(before[number], f.query(number as u32).await);
        f.reopen(number as u32).await;
        assert_eq!(before[number], f.query(number as u32).await);
        assert_eq!(
            SharedSstResponse::InputsGone,
            f.apply(request.clone()).await.unwrap()
        );
    }
    // A later legitimate region operation can retire the published outputs.
    // Retrying the table result must not resurrect them, even after reopen.
    f.engine
        .edit_region(
            RegionId::new(1, 0),
            RegionEdit {
                files_to_remove: requests[0].outputs.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    f.reopen(0).await;
    assert_eq!(
        SharedSstResponse::InputsGone,
        f.apply(requests[0].clone()).await.unwrap()
    );
    assert_eq!(
        0,
        f.engine
            .find_region(RegionId::new(1, 0))
            .unwrap()
            .version_control
            .current()
            .version
            .ssts
            .owned_num_files(RegionId::new(1, 0))
    );
    let path = crate::sst::location::sst_file_path(
        f.engine
            .find_region(RegionId::new(1, 0))
            .unwrap()
            .access_layer
            .table_dir(),
        f.source.file_id(),
        path_type,
    );
    assert!(
        f.engine
            .find_region(RegionId::new(1, 0))
            .unwrap()
            .access_layer
            .object_store()
            .exists(&path)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn invalid_replacements_leave_manifest_and_queries_unchanged() {
    let f = Fixture::new().await;
    f.register(0).await;
    f.register(1).await;
    let requests = f.compute().await;
    let before = f
        .engine
        .shared_sst_snapshot(RegionId::new(1, 0))
        .await
        .unwrap();
    let query = f.query(0).await;
    for mutation in 0..11 {
        let mut request = requests[0].clone();
        match mutation {
            0 => request.outputs[0].level = LEVEL_SHARED,
            1 => request.outputs[0].sequence = None,
            2 => request.outputs[0].num_rows += 1,
            3 => request.inputs[0].file_size += 1,
            4 => request.window_start = 3600,
            5 => request.outputs[0].region_id = RegionId::new(1, 1),
            6 => request.inputs.push(request.inputs[0].clone()),
            7 => {
                let mut missing = request.inputs[0].clone();
                missing.file_id = store_api::storage::FileId::random();
                request.inputs.push(missing);
            }
            8 => request.inputs.clear(),
            9 => request.window_seconds = 0,
            10 => {
                request.window_start = i64::MAX;
                request.window_seconds = 3600;
            }
            _ => unreachable!(),
        }
        assert!(f.apply(request).await.is_err(), "mutation {mutation}");
        assert_eq!(
            before.manifest_version,
            f.engine
                .shared_sst_snapshot(RegionId::new(1, 0))
                .await
                .unwrap()
                .manifest_version
        );
        assert_eq!(query, f.query(0).await);
    }
}
