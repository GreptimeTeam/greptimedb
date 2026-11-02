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

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::OpType;
use datatypes::arrow::array::{
    Array, BinaryArray, BinaryDictionaryBuilder, DictionaryArray, StringArray,
    TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use futures::TryStreamExt;
use partition::expr::col;
use partition::multi_dim::MultiDimPartitionRule;
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_request::PathType;
use store_api::storage::RegionId;

use super::*;
use crate::access_layer::{AccessLayer, Metrics, OperationType, SstWriteRequest, WriteType};
use crate::cache::{CacheManager, CacheStrategy};
use crate::error::InvalidMultiRegionCompactionSnafu;
use crate::read::FlatSource;
use crate::sst::file::{FileHandle, RegionFileId};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::parquet::WriteOptions;
use crate::sst::parquet::flat_format::{
    primary_key_column_index, sequence_column_index, time_index_column_index,
};
use crate::sst::{FlatSchemaOptions, FormatType, to_flat_sst_arrow_schema};
use crate::test_util::scheduler_util::SchedulerEnv;
use crate::test_util::sst_util::{
    new_flat_source_from_record_batches, new_record_batch_with_custom_sequence,
    new_sparse_primary_key, sst_region_metadata_with_encoding,
};

struct Protection {
    layer: AccessLayerRef,
    files: Mutex<HashSet<RegionFileId>>,
    fail_region: Option<RegionId>,
    first_protected: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl SstWriteGuard for Protection {
    async fn before_create(&self, file_id: RegionFileId) -> Result<()> {
        let path = crate::sst::location::sst_file_path(
            self.layer.table_dir(),
            file_id,
            self.layer.path_type(),
        );
        assert!(
            !self.layer.object_store().exists(&path).await.unwrap(),
            "SST already existed before protection"
        );
        if self.fail_region == Some(file_id.region_id()) {
            // Failure must occur after another worker has protected an output;
            // worker scheduling no longer follows region-id order.
            self.first_protected.notified().await;
            return InvalidMultiRegionCompactionSnafu {
                reason: "injected output protection failure",
            }
            .fail();
        }
        assert!(self.files.lock().unwrap().insert(file_id));
        self.first_protected.notify_one();
        Ok(())
    }
}

struct Fixture {
    env: SchedulerEnv,
    layer: AccessLayerRef,
    metadata: RegionMetadataRef,
    source: FileMeta,
    config: Arc<MitoConfig>,
}

impl Fixture {
    async fn new(encoding: PrimaryKeyEncoding, format: FormatType, physical_sequence: u64) -> Self {
        Self::with_source(encoding, format, |metadata| {
            (0..31)
                .map(|series| source_batch(metadata, series, physical_sequence))
                .collect()
        })
        .await
    }

    async fn with_source(
        encoding: PrimaryKeyEncoding,
        format: FormatType,
        batches: impl FnOnce(&RegionMetadataRef) -> Vec<RecordBatch>,
    ) -> Self {
        let env = SchedulerEnv::new().await;
        let aux = env.path.path().join("table-index");
        let layer = Arc::new(AccessLayer::new(
            "table/",
            if encoding == PrimaryKeyEncoding::Sparse {
                PathType::Data
            } else {
                PathType::Bare
            },
            env.access_layer.object_store().clone(),
            PuffinManagerFactory::new(&aux, 1024 * 1024, None, None)
                .await
                .unwrap(),
            IntermediateManager::init_fs(aux.to_str().unwrap())
                .await
                .unwrap(),
        ));
        let mut metadata = sst_region_metadata_with_encoding(encoding);
        metadata.region_id = RegionId::new(1, 0);
        let metadata = Arc::new(metadata);
        let mut config = MitoConfig::default();
        config.index.aux_path = aux.display().to_string();
        let config = Arc::new(config);
        let mut batches = batches(&metadata);
        batches.sort_by_cached_key(|batch| {
            let keys = cast(
                batch.column(primary_key_column_index(batch.num_columns())),
                &DataType::Binary,
            )
            .unwrap();
            keys.as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0)
                .to_vec()
        });
        let source = new_flat_source_from_record_batches(batches);
        let info = layer
            .write_sst(
                write_request(metadata.clone(), source, &config, format),
                &WriteOptions::default(),
                &mut Metrics::new(WriteType::Flush),
            )
            .await
            .unwrap()
            .remove(0);
        let source = FileMeta {
            region_id: metadata.region_id,
            file_id: info.file_id,
            time_range: info.time_range,
            file_size: info.file_size,
            num_rows: info.num_rows as u64,
            num_row_groups: info.num_row_groups,
            max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
            ..Default::default()
        };
        Self {
            env,
            layer,
            metadata,
            source,
            config,
        }
    }

    async fn write_additional_source(&self, batches: Vec<RecordBatch>) -> FileMeta {
        let info = self
            .layer
            .write_sst(
                write_request(
                    self.metadata.clone(),
                    new_flat_source_from_record_batches(batches),
                    &self.config,
                    FormatType::Flat,
                ),
                &WriteOptions::default(),
                &mut Metrics::new(WriteType::Flush),
            )
            .await
            .unwrap()
            .remove(0);
        FileMeta {
            file_id: info.file_id,
            file_size: info.file_size,
            num_rows: info.num_rows as u64,
            num_row_groups: info.num_row_groups,
            max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
            ..self.source.clone()
        }
    }

    fn plan(&self, regions: u32) -> MultiRegionCompactionPlan {
        let mut targets = BTreeMap::new();
        for region in 0..regions {
            let id = RegionId::new(1, region);
            let mut metadata = (*self.metadata).clone();
            metadata.region_id = id;
            let start = format!("{:03}", region * 32 / regions);
            let end = format!("{:03}", (region + 1) * 32 / regions);
            metadata.set_partition_expr(Some(
                col("tag_0")
                    .gt_eq(Value::String(start.into()))
                    .and(col("tag_0").lt(Value::String(end.into())))
                    .as_json_str()
                    .unwrap(),
            ));
            let mut input = self.source.clone();
            input.sequence = NonZeroU64::new(100 + region as u64);
            let options = RegionOptions {
                append_mode: true,
                preserve_row_sequence: true,
                sst_format: Some(FormatType::Flat),
                ..Default::default()
            };
            targets.insert(
                id,
                MultiRegionTarget {
                    metadata: Arc::new(metadata),
                    options,
                    inputs: [(input.file_id(), input)].into_iter().collect(),
                },
            );
        }
        let exprs = targets
            .values()
            .map(|target| {
                crate::region::parse_partition_expr(target.metadata.partition_expr.as_deref())
                    .unwrap()
                    .unwrap()
            })
            .collect();
        let partition_rule = Arc::new(
            MultiDimPartitionRule::try_new(
                vec!["tag_0".into()],
                (0..regions).collect(),
                exprs,
                false,
            )
            .unwrap(),
        );
        MultiRegionCompactionPlan {
            partition_rule,
            sources: vec![self.source.clone()],
            targets,
            memory_limit: 16 * 1024 * 1024,
            max_active_workers: 2,
            spill_limit: 64 * 1024 * 1024,
            max_output_file_size: 1024 * 1024,
            window_seconds: 3600,
        }
    }

    fn context(
        &self,
        fail_region: Option<RegionId>,
    ) -> (MultiRegionCompactionContext, Arc<Protection>) {
        let protection = Arc::new(Protection {
            layer: self.layer.clone(),
            files: Mutex::default(),
            fail_region,
            first_protected: tokio::sync::Notify::new(),
        });
        (
            MultiRegionCompactionContext {
                access_layer: self.layer.clone(),
                cache: Arc::new(CacheManager::default()),
                config: self.config.clone(),
                spill_directory: self.env.path.path().join("spill"),
                protection: protection.clone(),
                cancellation: Arc::new(CancellationHandle::default()),
            },
            protection,
        )
    }

    async fn read_output(
        &self,
        mut file: FileMeta,
        metadata: RegionMetadataRef,
    ) -> Vec<RecordBatch> {
        let expected_region = file.region_id;
        let expected_rows = file.num_rows;
        // Inspect the physical contents: a partition filter or sequence override
        // on read must not hide incorrectly routed/written records.
        file.sequence = None;
        file.preserve_row_sequence = true;
        let mut unfiltered = (*metadata).clone();
        unfiltered.set_partition_expr(None);
        let mut reader = self
            .layer
            .read_sst(FileHandle::new(file, Arc::new(NoopFilePurger)))
            .expected_metadata(Some(Arc::new(unfiltered)))
            .cache(CacheStrategy::Disabled)
            .build()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expected_region, reader.metadata().region_id);
        assert_eq!(
            metadata.column_metadatas,
            reader.metadata().column_metadatas
        );
        assert_eq!(
            expected_rows,
            reader.parquet_metadata().file_metadata().num_rows() as u64
        );
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_record_batch().await.unwrap() {
            batches.push(batch);
        }
        assert_eq!(
            expected_rows,
            batches
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum::<u64>()
        );
        batches
    }
}

fn write_request(
    metadata: RegionMetadataRef,
    source: FlatSource,
    config: &MitoConfig,
    format: FormatType,
) -> SstWriteRequest {
    SstWriteRequest {
        op_type: OperationType::Flush,
        metadata,
        source,
        cache_manager: Arc::new(CacheManager::default()),
        max_sequence: None,
        sst_write_format: format,
        preserve_row_sequence: false,
        index_options: Default::default(),
        index_config: config.index.clone(),
        inverted_index_config: config.inverted_index.clone(),
        fulltext_index_config: config.fulltext_index.clone(),
        bloom_filter_index_config: config.bloom_filter_index.clone(),
    }
}

fn source_batch(metadata: &RegionMetadataRef, series: u32, sequence: u64) -> RecordBatch {
    let tag = format!("{series:03}");
    let batch = new_record_batch_with_custom_sequence(&[&tag, "series"], 0, 4, sequence);
    let mut arrays = batch.columns().to_vec();
    arrays[2] = Arc::new(UInt64Array::from(vec![Some(10), Some(20), None, Some(30)]));
    arrays[3] = Arc::new(TimestampMillisecondArray::from(vec![0, 0, 0, 3_600_000]));
    arrays[6] = Arc::new(UInt8Array::from(vec![
        OpType::Put as u8,
        OpType::Put as u8,
        OpType::Delete as u8,
        OpType::Put as u8,
    ]));
    if metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse {
        let pk = new_sparse_primary_key(
            &[&tag, "series"],
            metadata,
            1000 + series % 2,
            series as u64,
        );
        let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
        for _ in 0..4 {
            keys.append(&pk).unwrap();
        }
        arrays[4] = Arc::new(keys.finish());
        arrays.drain(..2);
    }
    RecordBatch::try_new(
        to_flat_sst_arrow_schema(
            metadata,
            &FlatSchemaOptions::from_encoding(metadata.primary_key_encoding),
        ),
        arrays,
    )
    .unwrap()
}

#[rstest::rstest]
#[case::dense_flat_zero(PrimaryKeyEncoding::Dense, FormatType::Flat, 0)]
#[case::dense_primary_native(PrimaryKeyEncoding::Dense, FormatType::PrimaryKey, 7)]
#[case::sparse_flat_zero(PrimaryKeyEncoding::Sparse, FormatType::Flat, 0)]
#[case::sparse_flat_native(PrimaryKeyEncoding::Sparse, FormatType::Flat, 7)]
#[tokio::test]
async fn single_scan_routes_all_versions_without_folding(
    #[case] encoding: PrimaryKeyEncoding,
    #[case] format: FormatType,
    #[case] sequence: u64,
) {
    let fixture = Fixture::new(encoding, format, sequence).await;
    let mut fetched_bytes = None;
    for regions in [1, 8, 32] {
        let plan = fixture.plan(regions);
        let targets = plan.targets.clone();
        let (context, protected) = fixture.context(None);
        let output = compact_regions(plan, context).await.unwrap();
        assert_eq!(1, output.stats.source_files);
        assert_eq!(124, output.stats.source_rows);
        assert_eq!(124, output.stats.output_rows);
        assert!(output.stats.source_store_page_bytes > 0);
        if let Some(bytes) = fetched_bytes {
            assert_eq!(bytes, output.stats.source_store_page_bytes);
        }
        fetched_bytes = Some(output.stats.source_store_page_bytes);
        let mut total_deletes = 0;
        for (id, files) in output.files {
            assert_eq!(files.len(), output.sst_infos[&id].len());
            for (file, footer) in files.iter().zip(&output.sst_infos[&id]) {
                assert_eq!(file.file_id, footer.file_id);
                assert_eq!(file.file_size, footer.file_size);
                assert_eq!(file.num_rows, footer.num_rows as u64);
            }
            let target = &targets[&id];
            let mut actual_rows = Vec::new();
            if regions == 32 && id.region_sequence() == 31 {
                assert!(files.is_empty());
            }
            for file in files {
                assert!(protected.files.lock().unwrap().contains(&file.file_id()));
                assert_eq!(1, file.level);
                assert_eq!(id, file.region_id);
                assert_eq!(id != fixture.metadata.region_id, file.preserve_row_sequence);
                for batch in fixture.read_output(file, target.metadata.clone()).await {
                    let sequences = batch
                        .column(sequence_column_index(batch.num_columns()))
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    let expected = if id == fixture.metadata.region_id && sequence != 0 {
                        sequence
                    } else {
                        100 + id.region_sequence() as u64
                    };
                    assert!(sequences.values().iter().all(|actual| *actual == expected));
                    let operations = batch
                        .column(batch.num_columns() - 1)
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap();
                    total_deletes += operations
                        .values()
                        .iter()
                        .filter(|op| **op == OpType::Delete as u8)
                        .count();
                    let keys = cast(
                        batch.column(primary_key_column_index(batch.num_columns())),
                        &DataType::Binary,
                    )
                    .unwrap();
                    let keys = keys.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let tags =
                        cast(batch.column_by_name("tag_0").unwrap(), &DataType::Utf8).unwrap();
                    let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
                    let times = crate::read::timestamp_array_to_i64_slice(
                        batch.column(time_index_column_index(batch.num_columns())),
                    );
                    let values = batch
                        .column_by_name("field_0")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    for (row, timestamp) in times.iter().enumerate() {
                        let series: u32 = tags.value(row).parse().unwrap();
                        let expected_key = if encoding == PrimaryKeyEncoding::Sparse {
                            new_sparse_primary_key(
                                &[tags.value(row), "series"],
                                &fixture.metadata,
                                1000 + series % 2,
                                series as u64,
                            )
                        } else {
                            crate::test_util::sst_util::new_primary_key(&[
                                tags.value(row),
                                "series",
                            ])
                        };
                        assert_eq!(expected_key, keys.value(row));
                        actual_rows.push((
                            series,
                            *timestamp,
                            sequences.value(row),
                            operations.value(row),
                            values.is_valid(row).then(|| values.value(row)),
                        ));
                    }
                }
            }
            let expected_sequence = if id == fixture.metadata.region_id && sequence != 0 {
                sequence
            } else {
                100 + id.region_sequence() as u64
            };
            let mut expected_rows = Vec::new();
            for series in (id.region_sequence() * 32 / regions)
                ..(((id.region_sequence() + 1) * 32 / regions).min(31))
            {
                let tied = actual_rows
                    .iter()
                    .filter(|row| row.0 == series && row.1 == 0)
                    .map(|row| (row.3, row.4))
                    .collect::<Vec<_>>();
                assert_eq!(
                    vec![
                        (OpType::Put as u8, Some(10)),
                        (OpType::Put as u8, Some(20)),
                        (OpType::Delete as u8, None)
                    ],
                    tied
                );
                expected_rows.extend([
                    (series, 0, expected_sequence, OpType::Put as u8, Some(10)),
                    (series, 0, expected_sequence, OpType::Put as u8, Some(20)),
                    (series, 0, expected_sequence, OpType::Delete as u8, None),
                    (
                        series,
                        3_600_000,
                        expected_sequence,
                        OpType::Put as u8,
                        Some(30),
                    ),
                ]);
            }
            actual_rows.sort();
            expected_rows.sort();
            assert_eq!(expected_rows, actual_rows);
        }
        assert_eq!(31, total_deletes);
    }
}

#[tokio::test]
async fn failed_target_never_returns_a_partial_table_result() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let (context, protection) = fixture.context(Some(RegionId::new(1, 1)));
    assert!(compact_regions(fixture.plan(8), context).await.is_err());
    assert!(
        !protection.files.lock().unwrap().is_empty(),
        "earlier target wrote protected outputs"
    );
    let path = crate::sst::location::sst_file_path(
        fixture.layer.table_dir(),
        fixture.source.file_id(),
        fixture.layer.path_type(),
    );
    assert!(fixture.layer.object_store().exists(&path).await.unwrap());
    assert_spill_cleaned(&fixture.env.path.path().join("spill")).await;
}

#[rstest::rstest]
#[case(crate::region::options::MergeMode::LastRow)]
#[case(crate::region::options::MergeMode::LastNonNull)]
#[tokio::test]
async fn table_stage_does_not_apply_target_merge_mode(
    #[case] merge_mode: crate::region::options::MergeMode,
) {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(8);
    for target in plan.targets.values_mut() {
        target.options.append_mode = false;
        target.options.preserve_row_sequence = false;
        target.options.merge_mode = Some(merge_mode);
    }
    let output = compact_regions(plan, fixture.context(None).0)
        .await
        .unwrap();
    assert_eq!(124, output.stats.output_rows);
}

#[tokio::test]
async fn routed_runs_spill_and_exhaustion_fails_without_outputs() {
    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        (0..200)
            .map(|series| {
                new_record_batch_with_custom_sequence(
                    &[&format!("{series:03}{}", "x".repeat(96)), "series"],
                    0,
                    512,
                    0,
                )
            })
            .collect()
    })
    .await;
    let mut plan = fixture.plan(1);
    plan.memory_limit = 2 * 1024 * 1024;
    for target in plan.targets.values_mut() {
        Arc::make_mut(&mut target.metadata).set_partition_expr(None);
    }
    let output = compact_regions(plan, fixture.context(None).0)
        .await
        .unwrap();
    assert_eq!(102400, output.stats.output_rows);
    assert!(output.stats.spilled_bytes > 0);
    // Regression: IPC column buffers can share an allocation. Reusing spilled
    // long-tag batches must not exhaust the budget through duplicate charges.
    let mut ordinal = 0;
    for file in output.files.into_values().flatten() {
        for batch in fixture.read_output(file, fixture.metadata.clone()).await {
            let tags = cast(batch.column_by_name("tag_0").unwrap(), &DataType::Utf8).unwrap();
            let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
            let values = batch
                .column_by_name("field_0")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(
                    format!("{:03}{}", ordinal / 512, "x".repeat(96)),
                    tags.value(row)
                );
                assert_eq!(ordinal % 512, values.value(row));
                ordinal += 1;
            }
        }
    }
    assert_eq!(102400, ordinal);
    let mut plan = fixture.plan(1);
    plan.memory_limit = 2 * 1024 * 1024;
    plan.spill_limit = 1;
    for target in plan.targets.values_mut() {
        Arc::make_mut(&mut target.metadata).set_partition_expr(None);
    }
    let (context, protected) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(
            &error,
            crate::error::Error::MultiRegionCompactionResource { reason, .. }
                if reason == "Multi-region temporary disk quota exhausted"
        ),
        "{error:?}"
    );
    assert!(protected.files.lock().unwrap().is_empty());
    assert_spill_cleaned(&fixture.env.path.path().join("spill")).await;
}

#[tokio::test]
async fn schema_defaults_are_materialized_without_losing_old_versions() {
    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::PrimaryKey, 7).await;
    let mut plan = fixture.plan(8);
    for target in plan.targets.values_mut() {
        let mut builder = RegionMetadataBuilder::from_existing((*target.metadata).clone());
        builder.push_column_metadata(ColumnMetadata {
            column_id: 10,
            semantic_type: SemanticType::Field,
            column_schema: ColumnSchema::new("added", ConcreteDataType::uint64_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::UInt64(42))))
                .unwrap(),
        });
        target.metadata = Arc::new(builder.build().unwrap());
    }
    let targets = plan.targets.clone();
    let output = compact_regions(plan, fixture.context(None).0)
        .await
        .unwrap();
    assert_eq!(124, output.stats.output_rows);
    for (region_id, files) in output.files {
        for file in files {
            for batch in fixture
                .read_output(file, targets[&region_id].metadata.clone())
                .await
            {
                let added = batch
                    .column_by_name("added")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                assert_eq!(0, added.null_count());
                assert!(added.values().iter().all(|value| *value == 42));
            }
        }
    }
}

#[tokio::test]
async fn multiple_sources_merge_versions_in_each_target_sequence_domain() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 3).await;
    let batches = (0..31)
        .map(|series| source_batch(&fixture.metadata, series, 7))
        .collect();
    let second = fixture.write_additional_source(batches).await;
    let mut plan = fixture.plan(2);
    plan.sources.push(second.clone());
    for (id, target) in &mut plan.targets {
        let mut input = second.clone();
        input.sequence = NonZeroU64::new(200 + id.region_sequence() as u64);
        target.inputs.insert(input.file_id(), input);
    }
    let targets = plan.targets.clone();
    let output = compact_regions(plan, fixture.context(None).0)
        .await
        .unwrap();
    assert_eq!(2, output.stats.source_files);
    assert_eq!(248, output.stats.source_rows);
    assert_eq!(248, output.stats.output_rows);
    for (id, files) in output.files {
        let expected = if id.region_sequence() == 0 {
            [3, 7]
        } else {
            [101, 201]
        };
        let mut observed = HashSet::new();
        for file in files {
            for batch in fixture
                .read_output(file, targets[&id].metadata.clone())
                .await
            {
                let sequence = batch
                    .column(sequence_column_index(batch.num_columns()))
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                observed.extend(sequence.values().iter().copied());
                let keys = cast(
                    batch.column(primary_key_column_index(batch.num_columns())),
                    &DataType::Binary,
                )
                .unwrap();
                let keys = keys.as_any().downcast_ref::<BinaryArray>().unwrap();
                let times = crate::read::timestamp_array_to_i64_slice(
                    batch.column(time_index_column_index(batch.num_columns())),
                );
                for row in 1..batch.num_rows() {
                    if keys.value(row - 1) == keys.value(row) && times[row - 1] == times[row] {
                        assert!(sequence.value(row - 1) >= sequence.value(row));
                    }
                }
            }
        }
        assert_eq!(expected.into_iter().collect::<HashSet<_>>(), observed);
    }
}

#[tokio::test]
async fn unmatched_partition_values_use_the_table_default_region() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    let exprs = vec![
        col("tag_0").lt(Value::String("010".into())),
        col("tag_0").gt_eq(Value::String("020".into())),
    ];
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(vec!["tag_0".into()], vec![0, 1], exprs.clone(), false)
            .unwrap(),
    );
    for (id, target) in &mut plan.targets {
        Arc::make_mut(&mut target.metadata).set_partition_expr(if id.region_sequence() == 0 {
            None
        } else {
            Some(exprs[1].as_json_str().unwrap())
        });
    }
    let targets = plan.targets.clone();
    let output = compact_regions(plan, fixture.context(None).0)
        .await
        .unwrap();
    assert_eq!(124, output.stats.output_rows);
    for (id, files) in output.files {
        let mut series = HashSet::new();
        for file in files {
            for batch in fixture
                .read_output(file, targets[&id].metadata.clone())
                .await
            {
                let tags = cast(batch.column_by_name("tag_0").unwrap(), &DataType::Utf8).unwrap();
                let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
                series.extend(tags.iter().flatten().map(|tag| tag.parse::<u32>().unwrap()));
            }
        }
        let range = if id.region_sequence() == 0 {
            0..20
        } else {
            20..31
        };
        assert_eq!(range.collect::<HashSet<_>>(), series);
    }
}

#[tokio::test]
async fn nullable_field_partitioning_preserves_delete_records() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    let exprs = vec![
        col("field_0").lt(Value::UInt64(20)),
        col("field_0").gt_eq(Value::UInt64(20)),
    ];
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(vec!["field_0".into()], vec![0, 1], exprs.clone(), false)
            .unwrap(),
    );
    for (id, target) in &mut plan.targets {
        Arc::make_mut(&mut target.metadata).set_partition_expr(if id.region_sequence() == 0 {
            None
        } else {
            Some(exprs[1].as_json_str().unwrap())
        });
    }
    let targets = plan.targets.clone();
    let output = compact_regions(plan, fixture.context(None).0)
        .await
        .unwrap();
    assert_eq!(124, output.stats.output_rows);
    for (id, files) in output.files {
        let mut counts = (0, 0);
        for file in files {
            for batch in fixture
                .read_output(file, targets[&id].metadata.clone())
                .await
            {
                counts.0 += batch.num_rows();
                let values = batch
                    .column_by_name("field_0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                counts.1 += values.null_count();
                for value in values.iter().flatten() {
                    assert_eq!(id.region_sequence() == 0, value < 20);
                }
            }
        }
        assert_eq!(62, counts.0);
        assert_eq!(if id.region_sequence() == 0 { 31 } else { 0 }, counts.1);
    }
}

#[tokio::test]
async fn unknown_partition_target_fails_before_creating_output() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(vec!["tag_0".into()], vec![99], vec![], false).unwrap(),
    );
    let (context, protection) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        format!("{error:?}").contains("Partition rule selected a region outside"),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

/// One decoded batch fans out into 1024 windows, with 32 extra columns
/// materialized by schema compatibility before routing.
async fn wide_window_fixture() -> (Fixture, MultiRegionCompactionPlan) {
    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        let batch = new_record_batch_with_custom_sequence(&["000", "series"], 0, 1024, 5);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..1024).map(|row| (row - 512) * 3_600_000),
        ));
        vec![RecordBatch::try_new(batch.schema(), columns).unwrap()]
    })
    .await;
    let mut plan = fixture.plan(1);
    plan.memory_limit = 4 * 1024 * 1024;
    for target in plan.targets.values_mut() {
        let mut builder = RegionMetadataBuilder::from_existing((*target.metadata).clone());
        for column in 0..32 {
            builder.push_column_metadata(ColumnMetadata {
                column_id: 10 + column,
                semantic_type: SemanticType::Field,
                column_schema: ColumnSchema::new(
                    format!("wide_{column}"),
                    ConcreteDataType::uint64_datatype(),
                    false,
                )
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::UInt64(
                    1000 + column as u64,
                ))))
                .unwrap(),
            });
        }
        target.metadata = Arc::new(builder.build().unwrap());
    }
    (fixture, plan)
}

#[tokio::test]
async fn wide_source_routes_windows_without_retaining_future_fragments() {
    let (fixture, plan) = wide_window_fixture().await;
    let (context, _) = fixture.context(None);
    let counters = Arc::new(SourceCounters::default());
    let source = source::SharedSource::build(
        &plan,
        &context,
        validate_plan(&plan).unwrap(),
        counters.clone(),
    )
    .await
    .unwrap();
    let mut stream = source.batches();
    for row in 0..1024 {
        let Some(source::SourceEvent::Batch {
            region_id,
            window,
            batch,
        }) = stream.try_next().await.unwrap()
        else {
            panic!("source ended before all windows were routed");
        };
        assert_eq!(RegionId::new(1, 0), region_id);
        assert_eq!(row - 512, window);
        assert_eq!(1, batch.num_rows());
        assert_eq!(39, batch.num_columns());
        let keys = batch
            .column(primary_key_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .unwrap();
        // Each eagerly filtered fragment retains the shared dictionary. This
        // checks live ownership while the consumer is paused, not a cache-only
        // counter or process RSS. Allow fixed reader/cursor ownership overhead,
        // but not one reference per future window (1024 in the regression).
        assert!(
            Arc::strong_count(keys.values()) < 16,
            "future fragments retained {} dictionary references",
            Arc::strong_count(keys.values())
        );
        assert_eq!(
            row as u64,
            batch
                .column_by_name("field_0")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0)
        );
        for column in 0..32 {
            let values = batch
                .column_by_name(&format!("wide_{column}"))
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(0, values.null_count());
            assert_eq!(1000 + column as u64, values.value(0));
        }
    }
    assert!(matches!(
        stream.try_next().await.unwrap(),
        Some(source::SourceEvent::SourceEnd)
    ));
    assert!(stream.try_next().await.unwrap().is_none());
    assert_eq!(1, counters.files.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(
        1024,
        counters.rows.load(std::sync::atomic::Ordering::Relaxed)
    );
}

#[tokio::test]
async fn cancelling_between_routed_fragments_stops_the_source() {
    let (fixture, plan) = wide_window_fixture().await;
    let (context, _) = fixture.context(None);
    let source = source::SharedSource::build(
        &plan,
        &context,
        validate_plan(&plan).unwrap(),
        Arc::new(SourceCounters::default()),
    )
    .await
    .unwrap();
    let mut stream = source.batches();
    assert!(matches!(
        stream.try_next().await.unwrap(),
        Some(source::SourceEvent::Batch { .. })
    ));
    context.cancellation.cancel();
    let error = stream
        .try_next()
        .await
        .err()
        .expect("cancelled source yielded another fragment");
    assert!(matches!(
        error,
        crate::error::Error::CompactionCancelled { .. }
    ));
}

#[rstest::rstest]
#[case(1)]
#[case(2)]
#[tokio::test]
async fn many_windows_exhaust_metadata_without_outputs(#[case] workers: usize) {
    let (fixture, mut plan) = wide_window_fixture().await;
    plan.max_active_workers = workers;
    let (context, protected) = fixture.context(None);
    let error = tokio::time::timeout(Duration::from_secs(60), compact_regions(plan, context))
        .await
        .expect("window admission failure stalled the pipeline")
        .err()
        .unwrap();
    assert!(
        matches!(
            &error,
            crate::error::Error::MultiRegionCompactionResource { reason, .. }
                if reason == "Too many regions or time windows"
        ),
        "{error:?}"
    );
    assert!(protected.files.lock().unwrap().is_empty());
    assert_spill_cleaned(&fixture.env.path.path().join("spill")).await;
    let path = crate::sst::location::sst_file_path(
        fixture.layer.table_dir(),
        fixture.source.file_id(),
        fixture.layer.path_type(),
    );
    assert!(fixture.layer.object_store().exists(&path).await.unwrap());
}

#[test]
fn stable_merge_keeps_tied_source_order_across_empty_batches_and_refills() {
    fn tied_batch(start: usize, end: usize) -> RecordBatch {
        let batch = new_record_batch_with_custom_sequence(&["000", "series"], start, end, 7);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(TimestampMillisecondArray::from(vec![0; end - start]));
        RecordBatch::try_new(batch.schema(), columns).unwrap()
    }
    let schema = tied_batch(0, 1).schema();
    let inputs: Vec<crate::memtable::BoxedRecordBatchIterator> = vec![
        Box::new(
            vec![tied_batch(0, 0), tied_batch(0, 2), tied_batch(2, 5)]
                .into_iter()
                .map(Ok),
        ),
        Box::new(
            vec![tied_batch(100, 102), tied_batch(102, 105)]
                .into_iter()
                .map(Ok),
        ),
    ];
    let merge = crate::read::flat_merge::FlatMergeIterator::new_stable(schema, inputs, 3).unwrap();
    let mut payloads = Vec::new();
    for batch in merge {
        let batch = batch.unwrap();
        let values = batch
            .column_by_name("field_0")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        payloads.extend(values.values().iter().copied());
    }
    assert_eq!(vec![0, 1, 2, 3, 4, 100, 101, 102, 103, 104], payloads);
}

/// Five overlapping sources exercise both binary carries and the final merge
/// of unequal levels. Every timestamp has tied puts and a tombstone.
fn overlapping_batches(source: u64) -> Vec<RecordBatch> {
    (0..31)
        .map(|series| {
            let batch = new_record_batch_with_custom_sequence(
                &[&format!("{series:03}"), "series"],
                0,
                512,
                5,
            );
            let mut columns = batch.columns().to_vec();
            columns[2] = Arc::new(UInt64Array::from(
                (0..512)
                    .map(|row| (row % 4 != 2).then_some(source * 1000 + row))
                    .collect::<Vec<_>>(),
            ));
            columns[3] = Arc::new(TimestampMillisecondArray::from(
                (0..512)
                    .map(|row| row / 128 * 3_600_000 + row % 128 / 4)
                    .collect::<Vec<i64>>(),
            ));
            columns[6] = Arc::new(UInt8Array::from(
                (0..512)
                    .map(|row| {
                        if row % 4 == 2 {
                            OpType::Delete as u8
                        } else {
                            OpType::Put as u8
                        }
                    })
                    .collect::<Vec<_>>(),
            ));
            RecordBatch::try_new(batch.schema(), columns).unwrap()
        })
        .collect()
}

async fn overlapping_fixture() -> (Fixture, MultiRegionCompactionPlan) {
    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        overlapping_batches(0)
    })
    .await;
    let mut plan = fixture.plan(3);
    plan.memory_limit = 2 * 1024 * 1024;
    plan.max_output_file_size = 16 * 1024;
    for source in 1..5 {
        let file = fixture
            .write_additional_source(overlapping_batches(source))
            .await;
        for target in plan.targets.values_mut() {
            target.inputs.insert(file.file_id(), file.clone());
        }
        plan.sources.push(file);
    }
    for target in plan.targets.values_mut() {
        for input in target.inputs.values_mut() {
            input.sequence = NonZeroU64::new(777);
        }
    }
    (fixture, plan)
}

#[rstest::rstest]
#[case::range_single_work_slot(1, false)]
#[case::range_parallel_workers(2, false)]
#[case::multi_column_single_work_slot(1, true)]
#[case::multi_column_parallel_workers(2, true)]
#[tokio::test]
async fn bounded_workers_merge_overlapping_sources_without_deadlock_or_reordering(
    #[case] workers: usize,
    #[case] multi_column: bool,
) {
    let (fixture, mut plan) = overlapping_fixture().await;
    plan.max_active_workers = workers;
    if multi_column {
        // A single primary key can route to different targets across sources
        // and versions. Unmatched rows, including tombstones, go to region 0.
        let expressions = vec![
            col("tag_0")
                .lt(Value::String("010".into()))
                .and(col("field_0").gt_eq(Value::UInt64(2000))),
            col("tag_0").gt_eq(Value::String("010".into())).and(
                col("field_0")
                    .gt_eq(Value::UInt64(0))
                    .and(col("field_0").lt(Value::UInt64(2000))),
            ),
        ];
        plan.partition_rule = Arc::new(
            MultiDimPartitionRule::try_new(
                vec!["tag_0".into(), "field_0".into()],
                vec![1, 2],
                expressions.clone(),
                false,
            )
            .unwrap(),
        );
        for (id, target) in &mut plan.targets {
            let expression = id
                .region_sequence()
                .checked_sub(1)
                .map(|index| expressions[index as usize].as_json_str().unwrap());
            Arc::make_mut(&mut target.metadata).set_partition_expr(expression);
        }
    }
    let source_paths: HashSet<_> = plan
        .sources
        .iter()
        .map(|file| {
            crate::sst::location::sst_file_path(
                fixture.layer.table_dir(),
                file.file_id(),
                fixture.layer.path_type(),
            )
        })
        .collect();
    let targets = plan.targets.clone();
    let memory_limit = plan.memory_limit;
    let (context, protection) = fixture.context(None);
    let output = tokio::time::timeout(Duration::from_secs(60), compact_regions(plan, context))
        .await
        .expect("bounded worker pipeline stalled")
        .unwrap();
    assert_eq!(5, output.stats.source_files);
    assert_eq!(5 * 31 * 512, output.stats.source_rows);
    assert_eq!(output.stats.source_rows, output.stats.output_rows);
    assert!(output.stats.spilled_bytes > 0);
    assert!(output.stats.peak_buffered_bytes <= (memory_limit / 2) as u64);
    assert_eq!(
        targets.keys().collect::<Vec<_>>(),
        output.files.keys().collect::<Vec<_>>()
    );
    println!(
        "workers={workers}, multi_column={multi_column}, stats={:?}",
        output.stats
    );
    let mut output_paths = HashSet::new();
    for (id, files) in output.files {
        assert_eq!(files.len(), output.sst_infos[&id].len());
        assert!(
            files.len() >= 4,
            "each target must produce all four windows"
        );
        let file_count = files.len();
        let mut windows = BTreeMap::<i64, Vec<_>>::new();
        for (file, info) in files.into_iter().zip(&output.sst_infos[&id]) {
            assert!(protection.files.lock().unwrap().contains(&file.file_id()));
            assert_eq!(id, file.region_id);
            assert_eq!(1, file.level);
            assert_eq!(info.file_id, file.file_id);
            assert_eq!(info.num_rows as u64, file.num_rows);
            assert_eq!(info.file_size, file.file_size);
            assert_eq!(info.time_range, file.time_range);
            let file_window = file.time_range.0.value().div_euclid(3_600_000);
            assert_eq!(file_window, file.time_range.1.value().div_euclid(3_600_000));
            let path = crate::sst::location::sst_file_path(
                fixture.layer.table_dir(),
                file.file_id(),
                fixture.layer.path_type(),
            );
            assert!(
                !source_paths.contains(&path),
                "output overwrote source {path}"
            );
            assert!(
                output_paths.insert(path.clone()),
                "shared output file {path}"
            );
            println!(
                "region={id}, window={file_window}, rows={}, file={path}",
                file.num_rows
            );
            for batch in fixture
                .read_output(file, targets[&id].metadata.clone())
                .await
            {
                let tags = cast(batch.column_by_name("tag_0").unwrap(), &DataType::Utf8).unwrap();
                let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
                let timestamps = crate::read::timestamp_array_to_i64_slice(
                    batch.column(time_index_column_index(batch.num_columns())),
                );
                let sequences = batch
                    .column(sequence_column_index(batch.num_columns()))
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let operations = batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap();
                let values = batch
                    .column_by_name("field_0")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                for (row, timestamp) in timestamps.iter().copied().enumerate() {
                    assert_eq!(file_window, timestamp.div_euclid(3_600_000));
                    windows.entry(file_window).or_default().push((
                        tags.value(row).parse::<u32>().unwrap(),
                        timestamp,
                        sequences.value(row),
                        operations.value(row),
                        values.is_valid(row).then(|| values.value(row)),
                    ));
                }
            }
        }
        let mut expected = BTreeMap::new();
        for window in 0..4 {
            let mut rows = Vec::new();
            for series in 0..31 {
                for tick in 0..32 {
                    for source in 0..5 {
                        for version in 0..4 {
                            let ordinal = window * 128 + tick * 4 + version;
                            let payload = (version != 2).then_some(source * 1000 + ordinal as u64);
                            // Independent oracle: do not reuse PartitionRule to
                            // compute the expected routing of the physical rows.
                            let owner = if multi_column {
                                if series < 10 && payload.is_some_and(|value| value >= 2000) {
                                    1
                                } else if series >= 10 && payload.is_some_and(|value| value < 2000)
                                {
                                    2
                                } else {
                                    0
                                }
                            } else if series < 10 {
                                0
                            } else if series < 21 {
                                1
                            } else {
                                2
                            };
                            if owner != id.region_sequence() {
                                continue;
                            }
                            rows.push((
                                series,
                                window * 3_600_000 + tick,
                                if id.region_sequence() == 0 { 5 } else { 777 },
                                if version == 2 {
                                    OpType::Delete as u8
                                } else {
                                    OpType::Put as u8
                                },
                                payload,
                            ));
                        }
                    }
                }
            }
            expected.insert(window, rows);
        }
        // Do not sort actual rows: this checks stability across input batches,
        // merge passes, output batches and SST file boundaries, not just a multiset.
        assert_eq!(expected, windows, "incorrect rows or ordering for {id}");
        println!(
            "verified region={id}, files={file_count}, rows={}",
            windows.values().map(Vec::len).sum::<usize>()
        );
    }
    for path in source_paths {
        assert!(fixture.layer.object_store().exists(&path).await.unwrap());
    }
    assert_spill_cleaned(&fixture.env.path.path().join("spill")).await;
}

async fn assert_spill_cleaned(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match tokio::fs::read_dir(path).await {
                Ok(mut entries) => {
                    if entries.next_entry().await.unwrap().is_none() {
                        return;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                Err(error) => panic!("cannot inspect spill cleanup: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("attempt spill resources leaked");
}

struct CancelBeforeCreate(Arc<CancellationHandle>);

#[async_trait::async_trait]
impl SstWriteGuard for CancelBeforeCreate {
    async fn before_create(&self, _file_id: RegionFileId) -> Result<()> {
        self.0.cancel();
        // The coordinator must interrupt a worker waiting on external I/O,
        // rather than waiting for its next cooperative batch check.
        futures::future::pending().await
    }
}

#[tokio::test]
async fn cancelling_a_blocked_writer_cleans_spills_and_preserves_sources() {
    let (fixture, mut plan) = overlapping_fixture().await;
    plan.max_active_workers = 1;
    let sources = plan.sources.clone();
    let (mut context, _) = fixture.context(None);
    context.protection = Arc::new(CancelBeforeCreate(context.cancellation.clone()));
    let error = tokio::time::timeout(Duration::from_secs(60), compact_regions(plan, context))
        .await
        .expect("cancellation did not interrupt the worker")
        .err()
        .unwrap();
    assert!(matches!(
        error,
        crate::error::Error::CompactionCancelled { .. }
    ));
    assert_spill_cleaned(&fixture.env.path.path().join("spill")).await;
    for file in sources {
        let path = crate::sst::location::sst_file_path(
            fixture.layer.table_dir(),
            file.file_id(),
            fixture.layer.path_type(),
        );
        assert!(fixture.layer.object_store().exists(&path).await.unwrap());
    }
}

#[tokio::test]
async fn cancelled_execution_does_not_create_output() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let (context, protection) = fixture.context(None);
    context.cancellation.cancel();
    assert!(matches!(
        compact_regions(fixture.plan(2), context).await,
        Err(crate::error::Error::CompactionCancelled { .. })
    ));
    assert!(protection.files.lock().unwrap().is_empty());
}
