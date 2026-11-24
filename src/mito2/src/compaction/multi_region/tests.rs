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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::{OpType, SemanticType};
use common_error::ext::BoxedError;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryDictionaryBuilder, BooleanArray, DictionaryArray,
    StringArray, StringDictionaryBuilder, TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::{DataType, Field, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::extension::json::{Json2ExtensionType, Json2PhysicalLayout, JsonMetadata};
use datatypes::json::{JsonSettings, JsonTypeHint};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::types::json_type::JsonNativeType;
use datatypes::types::parse_string_to_jsonb;
use datatypes::value::Value;
use datatypes::vectors::json::array::JsonArray;
use datatypes::vectors::json::json2_physical_data_type;
use futures::TryStreamExt;
use futures::io::{self, AsyncReadExt};
use object_store::layers::CapabilityOverrideLayer;
use object_store::layers::mock::oio::Write as _;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use partition::expr::col;
use partition::multi_dim::MultiDimPartitionRule;
use puffin::puffin_manager::{PuffinManager, PuffinReader, PuffinWriter};
use rand::{RngCore, SeedableRng, rngs::SmallRng};
use serde_json::json;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_request::PathType;
use store_api::storage::RegionId;
use tokio::sync::watch;

use super::*;
use crate::access_layer::{AccessLayer, Metrics, OperationType, SstWriteRequest, WriteType};
use crate::cache::{CacheManager, CacheStrategy};
use crate::error::InvalidMultiRegionCompactionSnafu;
use crate::read::FlatSource;
use crate::sst::file::{FileHandle, RegionFileId};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::index::CompletedEntry;
use crate::sst::index::handler::{
    BuildContext, IndexCreator, IndexKindHandler, IndexKindProvider, PlanContext, PlanEntry,
    PlannedIndex,
};
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::kind::{ExtensionIndexKind, IndexKind};
use crate::sst::index::puffin_manager::{PuffinManagerFactory, SstPuffinWriter};
use crate::sst::location;
use crate::sst::parquet::WriteOptions;
use crate::sst::parquet::flat_format::{
    primary_key_column_index, sequence_column_index, time_index_column_index,
};
use crate::sst::{FlatSchemaOptions, FormatType, to_flat_sst_arrow_schema};
use crate::test_util::scheduler_util::SchedulerEnv;
use crate::test_util::sst_util::{
    build_test_binary_test_region_metadata, new_flat_source_from_record_batches,
    new_record_batch_with_binary_values, new_record_batch_with_custom_sequence,
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

type TestRow = (u32, i64, u64, u8, Option<u64>);

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
        let mut fixture = Self::without_source(sst_region_metadata_with_encoding(encoding)).await;
        let mut batches = batches(&fixture.metadata);
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
        let info = fixture
            .write_source(fixture.metadata.clone(), batches, format)
            .await;
        fixture.source = file_meta(info, fixture.metadata.region_id);
        fixture
    }

    async fn without_source(mut metadata: RegionMetadata) -> Self {
        let env = SchedulerEnv::new().await;
        let aux = env.path.path().join("table-index");
        let layer = Arc::new(AccessLayer::new(
            "table/",
            if metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse {
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
        metadata.region_id = RegionId::new(1, 0);
        let mut config = MitoConfig::default();
        config.index.aux_path = aux.display().to_string();
        Self {
            env,
            layer,
            metadata: Arc::new(metadata),
            source: FileMeta::default(),
            config: Arc::new(config),
        }
    }

    async fn write_source(
        &self,
        metadata: RegionMetadataRef,
        batches: Vec<RecordBatch>,
        format: FormatType,
    ) -> SstInfo {
        self.write_source_with_options(metadata, batches, format, &WriteOptions::default())
            .await
    }

    async fn write_source_with_options(
        &self,
        metadata: RegionMetadataRef,
        batches: Vec<RecordBatch>,
        format: FormatType,
        options: &WriteOptions,
    ) -> SstInfo {
        self.layer
            .write_sst(
                write_request(
                    metadata,
                    new_flat_source_from_record_batches(batches),
                    &self.config,
                    format,
                ),
                options,
                &mut Metrics::new(WriteType::Flush),
            )
            .await
            .unwrap()
            .remove(0)
    }

    async fn write_additional_source(&self, batches: Vec<RecordBatch>) -> FileMeta {
        let info = self
            .write_source(self.metadata.clone(), batches, FormatType::Flat)
            .await;
        file_meta(info, self.metadata.region_id)
    }

    fn plan(&self, regions: u32) -> MultiRegionCompactionPlan {
        self.plan_with_sources(regions, vec![self.source.clone()])
    }

    fn plan_with_sources(&self, regions: u32, sources: Vec<FileMeta>) -> MultiRegionCompactionPlan {
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
            let inputs = sources
                .iter()
                .cloned()
                .map(|mut input| {
                    input.sequence = NonZeroU64::new(100 + region as u64);
                    (input.file_id(), input)
                })
                .collect();
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
                    inputs,
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
            sources,
            targets,
            // Includes the fixed Parquet encoder allocations of every resident
            // target, unlike the old run-cache-only budget.
            memory_limit: (128 + regions as usize * 32) * 1024 * 1024,
            max_output_file_size: 1024 * 1024,
            window_start: 0,
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
                index_kind_provider: None,
                access_layer: self.layer.clone(),
                cache: Arc::new(CacheManager::default()),
                config: self.config.clone(),
                protection: protection.clone(),
                cancellation: CancellationToken::new(),
            },
            protection,
        )
    }

    async fn compact(&self, plan: MultiRegionCompactionPlan) -> MultiRegionCompactionOutput {
        compact_regions(plan, self.context(None).0).await.unwrap()
    }

    /// Reads an output exactly as the owning region will once the returned
    /// metadata is published: partition filtering and sequence override are
    /// both derived from that metadata.
    async fn read_published_output(
        &self,
        file: FileMeta,
        metadata: RegionMetadataRef,
    ) -> Vec<RecordBatch> {
        let expected_region = file.region_id;
        let expected_rows = file.num_rows;
        let mut reader = self
            .layer
            .read_sst(FileHandle::new(file, Arc::new(NoopFilePurger)))
            .expected_metadata(Some(metadata.clone()))
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

    async fn read_published_output_rows(
        &self,
        file: FileMeta,
        metadata: RegionMetadataRef,
    ) -> Vec<TestRow> {
        self.decode_base_rows(self.read_published_output(file, metadata).await)
    }

    async fn read_output(
        &self,
        mut file: FileMeta,
        metadata: RegionMetadataRef,
    ) -> Vec<RecordBatch> {
        // Inspect the physical contents: a partition filter or sequence override
        // on read must not hide incorrectly routed/written records.
        file.sequence = None;
        file.preserve_row_sequence = true;
        let mut unfiltered = (*metadata).clone();
        unfiltered.set_partition_expr(None);
        self.read_published_output(file, Arc::new(unfiltered)).await
    }

    async fn read_output_rows(&self, file: FileMeta, metadata: RegionMetadataRef) -> Vec<TestRow> {
        self.decode_base_rows(self.read_output(file, metadata).await)
    }

    fn decode_base_rows(&self, batches: Vec<RecordBatch>) -> Vec<TestRow> {
        let mut decoded = Vec::new();
        for batch in batches {
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
            let keys = cast(
                batch.column(primary_key_column_index(batch.num_columns())),
                &DataType::Binary,
            )
            .unwrap();
            let keys = keys.as_any().downcast_ref::<BinaryArray>().unwrap();
            let tags = cast(batch.column_by_name("tag_0").unwrap(), &DataType::Utf8).unwrap();
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
                let expected_key =
                    if self.metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse {
                        new_sparse_primary_key(
                            &[tags.value(row), "series"],
                            &self.metadata,
                            1000 + series % 2,
                            series as u64,
                        )
                    } else {
                        crate::test_util::sst_util::new_primary_key(&[tags.value(row), "series"])
                    };
                assert_eq!(expected_key, keys.value(row));
                decoded.push((
                    series,
                    *timestamp,
                    sequences.value(row),
                    operations.value(row),
                    values.is_valid(row).then(|| values.value(row)),
                ));
            }
        }
        decoded
    }
}

fn expected_source_rows(regions: u32, id: RegionId, sequence: u64) -> Vec<TestRow> {
    let mut rows = Vec::new();
    for series in
        (id.region_sequence() * 32 / regions)..(((id.region_sequence() + 1) * 32 / regions).min(31))
    {
        rows.extend([
            (series, 0, sequence, OpType::Put as u8, Some(10)),
            (series, 0, sequence, OpType::Put as u8, Some(20)),
            (series, 0, sequence, OpType::Delete as u8, None),
            (series, 1_000, sequence, OpType::Put as u8, Some(30)),
        ]);
    }
    rows
}

fn file_meta(info: SstInfo, region_id: RegionId) -> FileMeta {
    FileMeta {
        region_id,
        file_id: info.file_id,
        time_range: info.time_range,
        file_size: info.file_size,
        num_rows: info.num_rows as u64,
        num_row_groups: info.num_row_groups,
        max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
        ..Default::default()
    }
}

fn write_request(
    metadata: RegionMetadataRef,
    source: FlatSource,
    config: &MitoConfig,
    format: FormatType,
) -> SstWriteRequest {
    SstWriteRequest {
        index_build_inputs: None,
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
    arrays[3] = Arc::new(TimestampMillisecondArray::from(vec![0, 0, 0, 1_000]));
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
async fn test_single_scan_routes_all_versions_without_folding(
    #[case] encoding: PrimaryKeyEncoding,
    #[case] format: FormatType,
    #[case] sequence: u64,
) {
    let fixture = Fixture::new(encoding, format, sequence).await;
    let mut fetched_bytes = None;
    for regions in [1, 8, 32] {
        let mut plan = fixture.plan(regions);
        for target in plan.targets.values_mut() {
            target.options.sst_format = Some(format);
        }
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
                assert!(file.max_row_group_decoded_size > 0);
                assert_eq!(
                    file.max_row_group_decoded_size,
                    footer.max_row_group_decoded_size
                );
            }
            let target = &targets[&id];
            let mut actual_rows = Vec::new();
            if regions == 32 && id.region_sequence() == 31 {
                assert!(files.is_empty());
            }
            for file in files {
                assert!(protected.files.lock().unwrap().contains(&file.file_id()));
                assert_eq!(0, file.level);
                assert_eq!(id, file.region_id);
                assert_eq!(id != fixture.metadata.region_id, file.preserve_row_sequence);
                let rows = fixture
                    .read_output_rows(file, target.metadata.clone())
                    .await;
                let expected = if id == fixture.metadata.region_id && sequence != 0 {
                    sequence
                } else {
                    100 + id.region_sequence() as u64
                };
                assert!(rows.iter().all(|row| row.2 == expected));
                total_deletes += rows
                    .iter()
                    .filter(|row| row.3 == OpType::Delete as u8)
                    .count();
                actual_rows.extend(rows);
            }
            let expected_sequence = if id == fixture.metadata.region_id && sequence != 0 {
                sequence
            } else {
                100 + id.region_sequence() as u64
            };
            let mut expected_rows = expected_source_rows(regions, id, expected_sequence);
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
            }
            actual_rows.sort();
            expected_rows.sort();
            assert_eq!(expected_rows, actual_rows);
        }
        assert_eq!(31, total_deletes);
    }
}

#[tokio::test]
async fn test_failed_target_never_returns_a_partial_table_result() {
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
}

#[rstest::rstest]
#[case::last_row(false, crate::region::options::MergeMode::LastRow)]
#[case::last_non_null(false, crate::region::options::MergeMode::LastNonNull)]
#[case::preserve_row_sequence(true, crate::region::options::MergeMode::LastRow)]
#[tokio::test]
async fn test_table_stage_does_not_fold_versions(
    #[case] preserve_sequence: bool,
    #[case] merge_mode: crate::region::options::MergeMode,
) {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(8);
    for target in plan.targets.values_mut() {
        target.options.append_mode = preserve_sequence;
        target.options.preserve_row_sequence = preserve_sequence;
        target.options.merge_mode = Some(merge_mode);
    }
    let targets = plan.targets.clone();
    let output = fixture.compact(plan).await;
    assert_eq!(124, output.stats.output_rows);
    for (id, files) in output.files {
        let mut rows = Vec::new();
        for file in files {
            assert_eq!(
                preserve_sequence && id != fixture.metadata.region_id,
                file.preserve_row_sequence
            );
            rows.extend(
                fixture
                    .read_output_rows(file, targets[&id].metadata.clone())
                    .await,
            );
        }
        let mut expected = expected_source_rows(8, id, 100 + id.region_sequence() as u64);
        rows.sort();
        expected.sort();
        assert_eq!(expected, rows);
    }
}

#[tokio::test]
async fn test_long_tag_stream_is_lossless_and_insufficient_admission_creates_no_output() {
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
    for target in plan.targets.values_mut() {
        Arc::make_mut(&mut target.metadata).set_partition_expr(None);
    }
    let output = fixture.compact(plan).await;
    assert_eq!(102400, output.stats.output_rows);
    assert!(output.stats.estimated_memory_bytes <= 128 * 1024 * 1024);
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
    plan.memory_limit = 1024 * 1024;
    for target in plan.targets.values_mut() {
        Arc::make_mut(&mut target.metadata).set_partition_expr(None);
    }
    let (context, protected) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(
            &error,
            crate::error::Error::MultiRegionCompactionResource { .. }
        ),
        "{error:?}"
    );
    assert!(protected.files.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_schema_defaults_are_materialized_without_losing_old_versions() {
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
    let output = fixture.compact(plan).await;
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

#[rstest::rstest]
#[case::known_bounds(false)]
#[case::unknown_bound(true)]
#[tokio::test]
async fn test_multiple_sources_merge_versions_in_each_target_sequence_domain(
    #[case] unknown_bound: bool,
) {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 3).await;
    let batches = (0..31)
        .map(|series| source_batch(&fixture.metadata, series, 7))
        .collect();
    let second = fixture.write_additional_source(batches).await;
    let mut plan = fixture.plan(2);
    plan.sources.push(second.clone());
    for (id, target) in &mut plan.targets {
        let mut input = second.clone();
        input.sequence = if unknown_bound && id.region_sequence() == 1 {
            None
        } else {
            NonZeroU64::new(200 + id.region_sequence() as u64)
        };
        target.inputs.insert(input.file_id(), input);
    }
    let targets = plan.targets.clone();
    let output = fixture.compact(plan).await;
    assert_eq!(2, output.stats.source_files);
    assert_eq!(248, output.stats.source_rows);
    assert_eq!(248, output.stats.output_rows);
    for (id, files) in output.files {
        let expected = if id.region_sequence() == 0 {
            [3, 7]
        } else {
            [101, if unknown_bound { 7 } else { 201 }]
        };
        let mut observed = Vec::new();
        let mut published = Vec::new();
        let files_for_publish = files.clone();
        let published_row_count: u64 = files.iter().map(|file| file.num_rows).sum();
        for file in files {
            assert_eq!(
                if unknown_bound && id.region_sequence() == 1 {
                    None
                } else {
                    NonZeroU64::new(200 + id.region_sequence() as u64)
                },
                file.sequence
            );
            assert_eq!(
                id.region_sequence() != 0 && !unknown_bound,
                file.preserve_row_sequence
            );
            observed.extend(
                fixture
                    .read_output_rows(file, targets[&id].metadata.clone())
                    .await,
            );
        }
        assert_eq!(
            expected.into_iter().collect::<HashSet<_>>(),
            observed.iter().map(|row| row.2).collect::<HashSet<_>>()
        );
        for pair in observed.windows(2) {
            if pair[0].0 == pair[1].0 && pair[0].1 == pair[1].1 {
                assert!(pair[0].2 >= pair[1].2);
            }
        }
        // Publishing the returned metadata must not change any complete row,
        // including duplicate versions and their payload association.
        for file in files_for_publish {
            published.extend(
                fixture
                    .read_published_output_rows(file, targets[&id].metadata.clone())
                    .await,
            );
        }
        assert_eq!(
            published_row_count,
            published.len() as u64,
            "published metadata drops rows of region {id}"
        );
        observed.sort();
        published.sort();
        assert_eq!(
            observed, published,
            "published metadata changes rows of region {id}"
        );
    }
}

#[tokio::test]
async fn test_overlapping_partition_masks_are_rejected_before_creating_outputs() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(3);
    // Tags "010".."019" satisfy both predicates. `check_exprs = false` keeps
    // the rule as the caller supplied it, so only the executor can reject it.
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(
            vec!["tag_0".into()],
            vec![1, 2],
            vec![
                col("tag_0").gt_eq(Value::String("010".into())),
                col("tag_0").lt(Value::String("020".into())),
            ],
            false,
        )
        .unwrap(),
    );
    let (context, protection) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(&error, crate::error::Error::InvalidMultiRegionCompaction { reason, .. }
            if reason.contains("matches multiple target partitions")),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

/// Partition rule whose mask length cannot be produced by a real rule.
struct WrongMaskSizeRule;

impl partition::PartitionRule for WrongMaskSizeRule {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn partition_columns(&self) -> &[String] {
        &[]
    }

    fn find_region(
        &self,
        _values: &[Value],
    ) -> partition::error::Result<store_api::storage::RegionNumber> {
        Ok(0)
    }

    fn split_record_batch(
        &self,
        _batch: &RecordBatch,
    ) -> partition::error::Result<
        std::collections::HashMap<
            store_api::storage::RegionNumber,
            partition::partition::RegionMask,
        >,
    > {
        Ok(std::collections::HashMap::from([(
            0,
            partition::partition::RegionMask::new(BooleanArray::from(vec![true]), 1),
        )]))
    }
}

#[tokio::test]
async fn test_wrong_length_partition_mask_is_rejected_before_creating_outputs() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    plan.partition_rule = Arc::new(WrongMaskSizeRule);
    let (context, protection) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(&error, crate::error::Error::InvalidMultiRegionCompaction { reason, .. }
            if reason.contains("wrong row count")),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

struct Json2Column {
    name: &'static str,
    column_id: u32,
    source_layout: JsonSettings,
    target_layout: JsonSettings,
    rows: [serde_json::Value; 2],
}

impl Json2Column {
    fn source_array(&self) -> ArrayRef {
        let jsonb = Arc::new(BinaryArray::from_iter_values(
            self.rows
                .iter()
                .map(|row| parse_string_to_jsonb(&row.to_string()).unwrap())
                .collect::<Vec<_>>()
                .iter()
                .map(|value| value.as_slice()),
        )) as ArrayRef;
        let field = Field::new(self.name, DataType::Binary, true)
            .with_extension_type(Json2ExtensionType::default());
        JsonArray::from(&jsonb)
            .rewrite_to_v2(&field, &self.source_layout, &self.source_layout)
            .unwrap()
    }

    fn expected_rows(&self) -> [serde_json::Value; 2] {
        self.rows.clone().map(|mut row| {
            for hint in self.target_layout.type_hints() {
                row.as_object_mut()
                    .unwrap()
                    .entry(hint.path[0].clone())
                    .or_insert(serde_json::Value::Null);
            }
            row
        })
    }
}

/// Builds dense-encoded metadata with JSON2 fields before `ts`, so the
/// time index stays the last user column as the flat format requires.
fn json2_compaction_metadata(
    columns: &[Json2Column],
    region_id: RegionId,
    target_layout: bool,
) -> RegionMetadataRef {
    let mut metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense);
    metadata.region_id = region_id;
    let json_start = metadata.column_metadatas.len() - 1;
    for (offset, column) in columns.iter().enumerate() {
        let settings = if target_layout {
            &column.target_layout
        } else {
            &column.source_layout
        };
        let mut schema = ColumnSchema::new(
            column.name,
            ConcreteDataType::json2(JsonNativeType::object()),
            true,
        );
        schema.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
            settings.clone(),
        ))));
        metadata.column_metadatas.insert(
            json_start + offset,
            ColumnMetadata {
                column_schema: schema,
                semantic_type: SemanticType::Field,
                column_id: column.column_id,
            },
        );
    }
    Arc::new(
        RegionMetadataBuilder::from_existing(metadata)
            .build()
            .unwrap(),
    )
}

fn json2_settings(hints: Vec<JsonTypeHint>) -> JsonSettings {
    JsonSettings::try_new(hints, Some(0)).unwrap()
}

fn json2_hint(path: &str, data_type: ConcreteDataType) -> JsonTypeHint {
    JsonTypeHint {
        path: vec![path.to_string()],
        data_type,
        inverted_index: false,
        skipping_index: None,
        fulltext_index: None,
    }
}

fn read_json2_row(batch: &RecordBatch, column: &Json2Column) -> serde_json::Value {
    let field = batch.schema_ref().field_with_name(column.name).unwrap();
    assert!(
        Json2PhysicalLayout::try_from_root(field)
            .unwrap()
            .is_version_2()
    );
    assert_eq!(
        &json2_physical_data_type(&column.target_layout),
        field.data_type()
    );
    let projected = JsonArray::from(batch.column_by_name(column.name).unwrap())
        .project_to_v2(field, &DataType::Binary)
        .unwrap();
    assert_eq!(1, projected.len());
    JsonArray::from(&projected).try_get_value(0).unwrap()
}

/// A source SST written with an older JSON2 layout must be rewritten into the
/// target layout, keeping values that lived in the remainder field.
#[tokio::test]
async fn test_json2_rewrite_preserves_values_through_multiregion_compaction() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let json_columns = [
        Json2Column {
            name: "j",
            column_id: 4,
            source_layout: json2_settings(vec![json2_hint(
                "a",
                ConcreteDataType::int64_datatype(),
            )]),
            target_layout: json2_settings(vec![
                json2_hint("a", ConcreteDataType::int64_datatype()),
                json2_hint("b", ConcreteDataType::string_datatype()),
            ]),
            rows: [json!({"a": 1, "b": "x"}), json!({"a": 2})],
        },
        Json2Column {
            name: "k",
            column_id: 5,
            source_layout: json2_settings(vec![json2_hint(
                "left",
                ConcreteDataType::string_datatype(),
            )]),
            target_layout: json2_settings(vec![
                json2_hint("left", ConcreteDataType::string_datatype()),
                json2_hint("right", ConcreteDataType::int64_datatype()),
            ]),
            rows: [
                json!({"left": "first", "right": 7}),
                json!({"left": "second"}),
            ],
        },
    ];
    let source_region = RegionId::new(1, 0);
    let source_metadata = json2_compaction_metadata(&json_columns, source_region, false);
    let base = new_record_batch_with_custom_sequence(&["000", "series"], 0, 2, 100);
    let mut columns = base.columns().to_vec();
    let mut tags = StringDictionaryBuilder::<UInt32Type>::new();
    let mut primary_keys = BinaryDictionaryBuilder::<UInt32Type>::new();
    for tag in ["000", "020"] {
        tags.append_value(tag);
        primary_keys
            .append(crate::test_util::sst_util::new_primary_key(&[
                tag, "series",
            ]))
            .unwrap();
    }
    columns[0] = Arc::new(tags.finish());
    columns[4] = Arc::new(primary_keys.finish());
    let mut options = FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Dense);
    for (index, column) in json_columns.iter().enumerate() {
        let physical = column.source_array();
        options
            .concretized_json_types
            .insert(column.name.to_string(), physical.data_type().clone());
        columns.insert(3 + index, physical);
    }
    let source_batch = RecordBatch::try_new(
        to_flat_sst_arrow_schema(&source_metadata, &options),
        columns,
    )
    .unwrap();
    let info = fixture
        .write_source(source_metadata, vec![source_batch], FormatType::Flat)
        .await;
    let mut source = file_meta(info, source_region);
    source.sequence = NonZeroU64::new(100);

    let mut plan = fixture.plan(2);
    plan.sources = vec![source.clone()];
    for target in plan.targets.values_mut() {
        target.metadata = json2_compaction_metadata(&json_columns, target.metadata.region_id, true);
        target.inputs = [(source.file_id(), source.clone())].into_iter().collect();
    }
    plan.memory_limit = 256 * 1024 * 1024;
    let targets = plan.targets.clone();
    let output = fixture.compact(plan).await;

    for (id, files) in &output.files {
        let mut rows = 0;
        for file in files {
            for batch in fixture
                .read_output(file.clone(), targets[id].metadata.clone())
                .await
            {
                rows += batch.num_rows();
                for column in &json_columns {
                    assert_eq!(
                        column.expected_rows()[id.region_sequence() as usize],
                        read_json2_row(&batch, column)
                    );
                }
            }
        }
        assert_eq!(1, rows, "one row must route to {id}");
    }
    assert_eq!(2, output.stats.output_rows);
}

#[tokio::test]
async fn test_unmatched_partition_values_use_the_table_default_region() {
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
    let output = fixture.compact(plan).await;
    assert_eq!(124, output.stats.output_rows);
    for (id, files) in output.files {
        let mut series = HashSet::new();
        for file in files {
            series.extend(
                fixture
                    .read_output_rows(file, targets[&id].metadata.clone())
                    .await
                    .into_iter()
                    .map(|row| row.0),
            );
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
async fn test_nullable_field_partitioning_preserves_delete_records() {
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
    let output = fixture.compact(plan).await;
    assert_eq!(124, output.stats.output_rows);
    for (id, files) in output.files {
        let mut counts = (0, 0);
        for file in files {
            let rows = fixture
                .read_output_rows(file, targets[&id].metadata.clone())
                .await;
            counts.0 += rows.len();
            counts.1 += rows.iter().filter(|row| row.4.is_none()).count();
            for value in rows.iter().filter_map(|row| row.4) {
                assert_eq!(id.region_sequence() == 0, value < 20);
            }
        }
        assert_eq!(62, counts.0);
        assert_eq!(if id.region_sequence() == 0 { 31 } else { 0 }, counts.1);
    }
}

#[tokio::test]
async fn test_unknown_partition_target_fails_before_creating_output() {
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

/// A version group crossing reader batches with 32 schema-default columns.
async fn wide_source_fixture() -> (Fixture, MultiRegionCompactionPlan) {
    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        let batch = new_record_batch_with_custom_sequence(&["000", "series"], 0, 1024, 5);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(TimestampMillisecondArray::from(vec![0; 1024]));
        vec![RecordBatch::try_new(batch.schema(), columns).unwrap()]
    })
    .await;
    let mut plan = fixture.plan(2);
    let expressions = vec![
        col("field_0").lt(Value::UInt64(512)),
        col("field_0").gt_eq(Value::UInt64(512)),
    ];
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(
            vec!["field_0".into()],
            vec![0, 1],
            expressions.clone(),
            false,
        )
        .unwrap(),
    );
    for (id, target) in &mut plan.targets {
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
        let mut metadata = builder.build().unwrap();
        metadata.set_partition_expr(Some(
            expressions[id.region_sequence() as usize]
                .as_json_str()
                .unwrap(),
        ));
        target.metadata = Arc::new(metadata);
    }
    (fixture, plan)
}

/// Two holders own different halves of one physical SST, and the plan's source
/// comes from one of them. The scan is physical (it must cover the footer), and
/// each target admits only the rows its own reference authorizes.
#[tokio::test]
async fn test_shared_source_scans_the_complete_file_and_authorizes_each_target() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let left = RegionId::new(1, 0);
    let right = RegionId::new(1, 1);
    let file_id = fixture.source.file_id();
    let holder_plan = || {
        let mut plan = fixture.plan(2);
        // Each holder authorizes its own half through its own reference.
        for target in plan.targets.values_mut() {
            let domain =
                crate::region::parse_partition_expr(target.metadata.partition_expr.as_deref())
                    .unwrap()
                    .unwrap();
            target.inputs.get_mut(&file_id).unwrap().owned_range = Some(domain.into());
        }
        // The captured source is one holder's reference, so it authorizes one half.
        let left_domain = crate::region::parse_partition_expr(
            plan.targets[&left].metadata.partition_expr.as_deref(),
        )
        .unwrap()
        .unwrap();
        plan.sources[0].owned_range = Some(left_domain.into());
        plan
    };

    let plan = holder_plan();
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
    // Both halves of the physical file are compacted, each into its own target.
    let mut per_region = HashMap::new();
    let mut delivered = 0usize;
    let mut stream = source.batches();
    while let Some(source::SourceEvent { region_id, batch }) = stream.try_next().await.unwrap() {
        let tags = cast(batch.column_by_name("tag_0").unwrap(), &DataType::Utf8).unwrap();
        let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..batch.num_rows() {
            let tag = tags.value(row);
            let expected = if tag < "016" { left } else { right };
            assert_eq!(expected, region_id, "wrong target for tag {tag}");
            *per_region.entry(region_id).or_insert(0usize) += 1;
            delivered += 1;
        }
    }
    // 31 series of 4 rows each, split at the plan's "016" boundary.
    assert_eq!(124, delivered);
    assert_eq!(
        124,
        counters.rows.load(std::sync::atomic::Ordering::Relaxed)
    );
    assert_eq!(
        124,
        counters
            .physical_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        "every physical row was read exactly once"
    );
    assert_eq!(1, counters.files.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(Some(&64), per_region.get(&left));
    assert_eq!(Some(&60), per_region.get(&right));

    // A target whose own reference is narrower than its domain does not
    // materialize the rest: those rows belong to another reference of the file.
    let mut narrowed = holder_plan();
    narrowed
        .targets
        .get_mut(&left)
        .unwrap()
        .inputs
        .get_mut(&file_id)
        .unwrap()
        .owned_range = Some(col("tag_0").lt(Value::String("008".into())).into());
    let narrowed_counters = Arc::new(SourceCounters::default());
    let source = source::SharedSource::build(
        &narrowed,
        &context,
        validate_plan(&narrowed).unwrap(),
        narrowed_counters.clone(),
    )
    .await
    .unwrap();
    let mut rows = HashMap::new();
    let mut stream = source.batches();
    while let Some(source::SourceEvent { region_id, batch }) = stream.try_next().await.unwrap() {
        *rows.entry(region_id).or_insert(0usize) += batch.num_rows();
    }
    assert_eq!(Some(&32), rows.get(&left));
    assert_eq!(Some(&60), rows.get(&right));
    // The two counters must not be equal: the scan still read every physical row
    // while 32 of them were dropped as invisible through every departing reference.
    assert_eq!(
        124,
        narrowed_counters
            .physical_rows
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    assert_eq!(
        92,
        narrowed_counters
            .rows
            .load(std::sync::atomic::Ordering::Relaxed)
    );
}

/// The counterexample the routing validation exists for: the physical file holds
/// rows above the creator's `k < 016` bound. The left holder is unrestricted and
/// can read them, the right holder routes them but only authorizes the tail, so
/// routing them to the right holder would retire rows the left holder still reads.
#[tokio::test]
async fn test_plan_refuses_a_routed_row_a_departing_reference_can_still_read() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let left = RegionId::new(1, 0);
    let right = RegionId::new(1, 1);
    let file_id = fixture.source.file_id();
    let mut plan = fixture.plan(2);
    // The creator bound is `k < 016`, but the object contains `k >= 016` too, and
    // the left holder's reference authorizes every physical row.
    plan.sources[0].owned_range = None;
    plan.targets
        .get_mut(&left)
        .unwrap()
        .inputs
        .get_mut(&file_id)
        .unwrap()
        .owned_range = None;
    plan.targets
        .get_mut(&right)
        .unwrap()
        .inputs
        .get_mut(&file_id)
        .unwrap()
        .owned_range = Some(col("tag_0").gt_eq(Value::String("024".into())).into());

    let error = validate_plan(&plan).expect_err("the plan must be refused");
    assert!(
        error.to_string().contains("could be retired"),
        "unexpected error: {error}"
    );

    // The whole plan is refused before any output is produced.
    let (context, protection) = fixture.context(None);
    let Err(error) = compact_regions(plan, context).await else {
        panic!("a plan that can lose a row must be refused");
    };
    assert!(
        error.to_string().contains("could be retired"),
        "unexpected error: {error}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_wide_version_group_routes_lazily_without_retaining_fragment_dictionaries() {
    let (fixture, plan) = wide_source_fixture().await;
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
    let mut next_row = 0;
    while let Some(source::SourceEvent { region_id, batch }) = stream.try_next().await.unwrap() {
        assert_eq!(39, batch.num_columns());
        let keys = batch
            .column(primary_key_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .unwrap();
        // Regression R1-C1: a paused consumer must not retain one dictionary
        // reference per future fragment. This is ownership, not an RSS metric.
        assert!(Arc::strong_count(keys.values()) < 16);
        let values = batch
            .column_by_name("field_0")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(next_row, values.value(row));
            assert_eq!((next_row / 512) as u32, region_id.region_sequence());
            for column in 0..32 {
                let defaults = batch
                    .column_by_name(&format!("wide_{column}"))
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                assert_eq!(0, defaults.null_count());
                assert_eq!(1000 + column as u64, defaults.value(row));
            }
            next_row += 1;
        }
    }
    assert_eq!(1024, next_row);
    assert_eq!(1, counters.files.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(
        1024,
        counters.rows.load(std::sync::atomic::Ordering::Relaxed)
    );
}

#[tokio::test]
async fn test_cancelling_between_routed_fragments_stops_the_source() {
    let (fixture, plan) = wide_source_fixture().await;
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
    assert!(stream.try_next().await.unwrap().is_some());
    context.cancellation.cancel();
    assert!(matches!(
        stream.try_next().await,
        Err(crate::error::Error::CompactionCancelled { .. })
    ));
}

#[rstest::rstest]
#[case::metadata(false)]
#[case::actual_rows(true)]
#[tokio::test]
async fn test_cross_window_source_is_rejected_without_filtering_away_rows(
    #[case] stale_metadata: bool,
) {
    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        let batch = new_record_batch_with_custom_sequence(&["000", "series"], 0, 2, 0);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(TimestampMillisecondArray::from(vec![0, 3_600_000]));
        vec![RecordBatch::try_new(batch.schema(), columns).unwrap()]
    })
    .await;
    let mut plan = fixture.plan(1);
    if stale_metadata {
        plan.sources[0].time_range.1 = plan.sources[0].time_range.0;
    }
    let (context, protection) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(
            error,
            crate::error::Error::InvalidMultiRegionCompaction { .. }
        ),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
    let path = crate::sst::location::sst_file_path(
        fixture.layer.table_dir(),
        fixture.source.file_id(),
        fixture.layer.path_type(),
    );
    assert!(fixture.layer.object_store().exists(&path).await.unwrap());
}

#[tokio::test]
async fn test_negative_subsecond_timestamp_stays_in_its_half_open_window() {
    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        let batch = new_record_batch_with_custom_sequence(&["000", "series"], 0, 2, 0);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(TimestampMillisecondArray::from(vec![-3_600_000, -1]));
        vec![RecordBatch::try_new(batch.schema(), columns).unwrap()]
    })
    .await;
    let mut plan = fixture.plan(1);
    plan.window_start = -3600;
    let output = fixture.compact(plan).await;
    assert_eq!(2, output.stats.output_rows);
    let mut times = Vec::new();
    for file in output.files.into_values().flatten() {
        times.extend(
            fixture
                .read_output_rows(file, fixture.metadata.clone())
                .await
                .into_iter()
                .map(|row| row.1),
        );
    }
    assert_eq!(vec![-3_600_000, -1], times);
}

#[tokio::test]
async fn test_equal_key_group_crosses_batches_and_preserves_opposite_target_admission_orders() {
    fn versions(start: usize) -> Vec<RecordBatch> {
        let batch =
            new_record_batch_with_custom_sequence(&["000", "series"], start, start + 256, 0);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(TimestampMillisecondArray::from(vec![0; 256]));
        vec![RecordBatch::try_new(batch.schema(), columns).unwrap()]
    }
    let fixture =
        Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| versions(0)).await;
    let second = fixture.write_additional_source(versions(256)).await;
    let mut plan = fixture.plan(2);
    plan.sources.push(second.clone());
    // Both files put the same PK/time into both targets. Source order is not
    // a valid version order: R0 installs F1 then F2, R1 installs F2 then F1.
    let expressions = vec![
        partition::expr::PartitionExpr::new(
            partition::expr::Operand::Expr(col("field_0").lt(Value::UInt64(128))),
            partition::expr::RestrictedOp::Or,
            partition::expr::Operand::Expr(col("field_0").gt_eq(Value::UInt64(384))),
        ),
        col("field_0")
            .gt_eq(Value::UInt64(128))
            .and(col("field_0").lt(Value::UInt64(384))),
    ];
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(
            vec!["field_0".into()],
            vec![0, 1],
            expressions.clone(),
            false,
        )
        .unwrap(),
    );
    for (id, target) in &mut plan.targets {
        let first_sequence = if id.region_sequence() == 0 { 11 } else { 22 };
        target
            .inputs
            .get_mut(&fixture.source.file_id())
            .unwrap()
            .sequence = NonZeroU64::new(first_sequence);
        let mut input = second.clone();
        input.sequence = NonZeroU64::new(33 - first_sequence);
        target.inputs.insert(input.file_id(), input);
        Arc::make_mut(&mut target.metadata).set_partition_expr(Some(
            expressions[id.region_sequence() as usize]
                .as_json_str()
                .unwrap(),
        ));
    }
    let targets = plan.targets.clone();
    let output = fixture.compact(plan).await;
    assert_eq!(512, output.stats.output_rows);
    for (id, files) in output.files {
        let mut rows = Vec::new();
        for file in files {
            assert_eq!(NonZeroU64::new(22), file.sequence);
            let decoded = fixture
                .read_output_rows(file, targets[&id].metadata.clone())
                .await;
            rows.extend(decoded.into_iter().map(|row| {
                let payload = row.4.unwrap();
                let expected = match (id.region_sequence(), payload < 256) {
                    (0, true) | (1, false) => 11,
                    _ => 22,
                };
                assert_eq!(expected, row.2);
                (row.2, payload)
            }));
        }
        assert!(rows.windows(2).all(|pair| pair[0].0 >= pair[1].0));
        let mut payloads = rows.into_iter().map(|row| row.1).collect::<Vec<_>>();
        payloads.sort();
        let expected = (0..512)
            .filter(|value| {
                if id.region_sequence() == 0 {
                    *value < 128 || *value >= 384
                } else {
                    (128..384).contains(value)
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(expected, payloads);
    }
}

/// Five overlapping sources share one window. Every timestamp has tied puts
/// and a tombstone; source-order tie breaking is deliberately not required.
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
                (0..512).map(|row| row / 4).collect::<Vec<i64>>(),
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
    // Force rolling inside one window; a series must still stay in one SST.
    plan.max_output_file_size = 1;
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
#[case::range(false)]
#[case::multi_column(true)]
#[tokio::test]
async fn test_streaming_merge_preserves_overlapping_versions_and_field_routing(
    #[case] multi_column: bool,
) {
    let (fixture, mut plan) = overlapping_fixture().await;
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
    assert!(output.stats.estimated_memory_bytes <= memory_limit as u64);
    assert!(output.stats.peak_group_bytes <= (memory_limit / 16) as u64);
    assert!(
        output.stats.peak_group_bytes > 0,
        "retained equal-key groups were never charged"
    );
    let expected_output_bytes: u64 = output
        .files
        .values()
        .flatten()
        .map(|file| file.file_size + file.index_file_size)
        .sum();
    assert_eq!(expected_output_bytes, output.stats.output_bytes);
    assert_eq!(
        targets.keys().collect::<Vec<_>>(),
        output.files.keys().collect::<Vec<_>>()
    );
    common_telemetry::info!("multi_column={multi_column}, stats={:?}", output.stats);
    let mut output_paths = HashSet::new();
    for (id, files) in output.files {
        assert_eq!(files.len(), output.sst_infos[&id].len());
        assert!(files.len() > 1, "expected single-window SST rollover");
        for pair in files.windows(2) {
            assert!(
                pair[0].primary_key_max.as_ref().unwrap()
                    < pair[1].primary_key_min.as_ref().unwrap(),
                "SST rollover split a series or reordered primary-key ranges"
            );
        }
        let file_count = files.len();
        let mut actual_rows = Vec::new();
        for (file, info) in files.into_iter().zip(&output.sst_infos[&id]) {
            assert!(protection.files.lock().unwrap().contains(&file.file_id()));
            assert_eq!(id, file.region_id);
            assert_eq!(0, file.level);
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
            common_telemetry::info!(
                "region={id}, window={file_window}, rows={}, file={path}",
                file.num_rows
            );
            let rows = fixture
                .read_output_rows(file, targets[&id].metadata.clone())
                .await;
            assert!(
                rows.iter()
                    .all(|row| row.1.div_euclid(3_600_000) == file_window)
            );
            actual_rows.extend(rows);
        }
        let mut expected = Vec::new();
        for series in 0..31 {
            for tick in 0..128 {
                for source in 0..5 {
                    for version in 0..4 {
                        let ordinal = tick * 4 + version;
                        let payload = (version != 2).then_some(source * 1000 + ordinal as u64);
                        // Independent oracle: do not reuse PartitionRule to
                        // compute the expected routing of the physical rows.
                        let owner = if multi_column {
                            if series < 10 && payload.is_some_and(|value| value >= 2000) {
                                1
                            } else if series >= 10 && payload.is_some_and(|value| value < 2000) {
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
                        expected.push((
                            series,
                            tick,
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
        // Check the actual order before comparing multisets. Equal full keys
        // can interleave sources; unequal keys must remain ordered across SSTs.
        for pair in actual_rows.windows(2) {
            let key = |row: &TestRow| (row.0, row.1, std::cmp::Reverse(row.2));
            assert!(key(&pair[0]) <= key(&pair[1]), "unordered output for {id}");
        }
        actual_rows.sort();
        expected.sort();
        assert_eq!(expected, actual_rows, "missing or changed rows for {id}");
        common_telemetry::info!(
            "verified region={id}, files={file_count}, rows={}",
            actual_rows.len()
        );
    }
    for path in source_paths {
        assert!(fixture.layer.object_store().exists(&path).await.unwrap());
    }
}

struct BlockIndexedSstWrite {
    inner: oio::Writer,
    started: Arc<tokio::sync::Notify>,
}

impl oio::Write for BlockIndexedSstWrite {
    async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
        self.inner.write(buffer).await?;
        self.started.notify_one();
        futures::future::pending().await
    }

    async fn close(&mut self) -> mock::Result<mock::Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> mock::Result<()> {
        self.inner.abort().await
    }
}

fn add_fulltext_column(plan: &mut MultiRegionCompactionPlan) {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{
        ColumnDefaultConstraint, ColumnSchema, FulltextAnalyzer, FulltextBackend, FulltextOptions,
    };
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    let column = ColumnMetadata {
        column_id: 10,
        semantic_type: api::v1::SemanticType::Field,
        column_schema: ColumnSchema::new("text", ConcreteDataType::string_datatype(), false)
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::String(
                "indexed text".into(),
            ))))
            .unwrap()
            .with_fulltext_options(FulltextOptions::new_unchecked(
                true,
                FulltextAnalyzer::English,
                false,
                FulltextBackend::Tantivy,
                1,
                0.01,
            ))
            .unwrap(),
    };
    for target in plan.targets.values_mut() {
        let mut builder = RegionMetadataBuilder::from_existing((*target.metadata).clone());
        builder.push_column_metadata(column.clone());
        target.metadata = Arc::new(builder.build().unwrap());
    }
}

struct FailIndexSibling {
    protection: Arc<Protection>,
    fail: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl SstWriteGuard for FailIndexSibling {
    async fn before_create(&self, file_id: RegionFileId) -> Result<()> {
        if file_id.region_id().region_sequence() == 1 {
            self.fail.notified().await;
            return InvalidMultiRegionCompactionSnafu {
                reason: "injected indexed sibling failure",
            }
            .fail();
        }
        self.protection.before_create(file_id).await
    }
}

#[rstest::rstest]
#[case::cancel(false)]
#[case::sibling_failure(true)]
#[tokio::test]
async fn test_interrupted_indexed_worker_cleans_tantivy_files(#[case] fail_sibling: bool) {
    let mut fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        ["000", "020"]
            .into_iter()
            .map(|tag| {
                let rows = if tag == "000" { 1 } else { 262_144 };
                let batch = new_record_batch_with_custom_sequence(&[tag, "series"], 0, rows, 5);
                let mut columns = batch.columns().to_vec();
                // Exceed both the Arrow writer's internal buffer and the object
                // store chunk, forcing a write before EOF/index finish.
                let mut rng = 0x1234_5678_9abc_def0_u64;
                columns[2] = Arc::new(UInt64Array::from_iter_values((0..rows).map(|_| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng
                })));
                RecordBatch::try_new(batch.schema(), columns).unwrap()
            })
            .collect()
    })
    .await;
    let started = Arc::new(tokio::sync::Notify::new());
    let notifying = started.clone();
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _, inner| {
            if path.contains("1_0000000000/") && path.ends_with(".parquet") {
                Box::new(BlockIndexedSstWrite {
                    inner,
                    started: notifying.clone(),
                }) as oio::Writer
            } else {
                inner
            }
        }))
        .build()
        .unwrap();
    fixture.layer = Arc::new(AccessLayer::new(
        fixture.layer.table_dir(),
        fixture.layer.path_type(),
        fixture.layer.object_store().clone().layer(layer),
        fixture.layer.puffin_manager_factory().clone(),
        fixture.layer.intermediate_manager().clone(),
    ));
    let mut plan = fixture.plan(2);
    // R1 receives its first row before R0 blocks in object storage. This lets
    // the sibling fail independently even though input is now consumed online.
    let expressions = vec![
        col("tag_0").gt_eq(Value::String("010".into())),
        col("tag_0").lt(Value::String("010".into())),
    ];
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(
            vec!["tag_0".into()],
            vec![0, 1],
            expressions.clone(),
            false,
        )
        .unwrap(),
    );
    for (id, target) in &mut plan.targets {
        Arc::make_mut(&mut target.metadata).set_partition_expr(Some(
            expressions[id.region_sequence() as usize]
                .as_json_str()
                .unwrap(),
        ));
    }
    // Admission now includes both resident Tantivy writers and finish space.
    plan.memory_limit = 256 * 1024 * 1024;
    add_fulltext_column(&mut plan);
    let (mut context, protection) = fixture.context(None);
    let cancellation = context.cancellation.clone();
    let fail = Arc::new(tokio::sync::Notify::new());
    context.protection = Arc::new(FailIndexSibling {
        protection: protection.clone(),
        fail: fail.clone(),
    });
    let execution = compact_regions(plan, context);
    tokio::pin!(execution);
    tokio::select! {
        _ = started.notified() => {}
        result = &mut execution => panic!("writer did not block: {}", result.err().map_or("success".into(), |error| error.to_string())),
        _ = tokio::time::sleep(Duration::from_secs(60)) => panic!("indexed writer did not reach object storage"),
    }
    let protected = protection
        .files
        .lock()
        .unwrap()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(1, protected.len());
    let file = protected[0];
    let intm_dir = fixture
        .env
        .path
        .path()
        .join("table-index/__intm")
        .join(file.region_id().as_u64().to_string())
        .join(file.file_id().to_string());
    let mut entries = tokio::fs::read_dir(&intm_dir).await.unwrap();
    let mut tantivy_dirs = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.file_name().to_string_lossy().starts_with("fulltext-") {
            assert!(
                tokio::fs::try_exists(entry.path().join("meta.json"))
                    .await
                    .unwrap()
            );
            tantivy_dirs.push(entry.path());
        }
    }
    assert_eq!(
        1,
        tantivy_dirs.len(),
        "the test must cancel AFTER Tantivy initialization"
    );
    if fail_sibling {
        fail.notify_one();
    } else {
        cancellation.cancel();
    }
    let error = tokio::time::timeout(Duration::from_secs(60), execution)
        .await
        .unwrap()
        .err()
        .unwrap();
    if fail_sibling {
        assert!(
            matches!(&error, crate::error::Error::InvalidMultiRegionCompaction { reason, .. } if reason == "injected indexed sibling failure"),
            "{error:?}"
        );
    } else {
        assert!(matches!(
            error,
            crate::error::Error::CompactionCancelled { .. }
        ));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        for path in &tantivy_dirs {
            while tokio::fs::try_exists(path).await.unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await
    .expect("cancelled worker leaked Tantivy intermediate files");
    // Attempt cleanup must not delete sources or protected SSTs owned by the caller.
    for id in [fixture.source.file_id(), file] {
        let path = crate::sst::location::sst_file_path(
            fixture.layer.table_dir(),
            id,
            fixture.layer.path_type(),
        );
        assert!(fixture.layer.object_store().exists(&path).await.unwrap());
    }
}

struct CancelBeforeCreate(CancellationToken);

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
async fn test_cancelling_a_blocked_writer_preserves_sources() {
    let (fixture, plan) = overlapping_fixture().await;
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
async fn test_private_routing_annotations_do_not_shadow_user_columns() {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    let expressions = vec![
        col("__compaction_region").lt(Value::UInt64(40)),
        col("__compaction_region").gt_eq(Value::UInt64(40)),
    ];
    plan.partition_rule = Arc::new(
        MultiDimPartitionRule::try_new(
            vec!["__compaction_region".into()],
            vec![0, 1],
            expressions.clone(),
            false,
        )
        .unwrap(),
    );
    for (id, target) in &mut plan.targets {
        let mut builder = RegionMetadataBuilder::from_existing((*target.metadata).clone());
        builder.push_column_metadata(ColumnMetadata {
            column_id: 10,
            semantic_type: api::v1::SemanticType::Field,
            column_schema: ColumnSchema::new(
                "__compaction_region",
                ConcreteDataType::uint64_datatype(),
                false,
            )
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::UInt64(42))))
            .unwrap(),
        });
        let mut metadata = builder.build().unwrap();
        metadata.set_partition_expr(Some(
            expressions[id.region_sequence() as usize]
                .as_json_str()
                .unwrap(),
        ));
        target.metadata = Arc::new(metadata);
    }
    let target = plan.targets[&RegionId::new(1, 1)].metadata.clone();
    let output = fixture.compact(plan).await;
    assert!(output.files[&RegionId::new(1, 0)].is_empty());
    assert_eq!(124, output.stats.output_rows);
    for file in output.files[&RegionId::new(1, 1)].iter().cloned() {
        for batch in fixture.read_output(file, target.clone()).await {
            let values = batch
                .column_by_name("__compaction_region")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert!(values.values().iter().all(|value| *value == 42));
        }
    }
}

#[rstest::rstest]
#[case::trusted_zero(true)]
#[case::legacy_override(false)]
#[tokio::test]
async fn test_zero_versions_follow_trust_and_legacy_override(#[case] trusted: bool) {
    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        vec![
            new_record_batch_with_custom_sequence(&["000", "series"], 0, 1, 0),
            new_record_batch_with_custom_sequence(&["020", "series"], 0, 1, 7),
        ]
    })
    .await;
    let mut plan = fixture.plan(2);
    if trusted {
        for target in plan.targets.values_mut() {
            for input in target.inputs.values_mut() {
                input.preserve_row_sequence = true;
            }
        }
    }
    let target = plan.targets[&RegionId::new(1, 0)].clone();
    let input = target.inputs[&fixture.source.file_id()].clone();
    let mut original = fixture
        .layer
        .read_sst(FileHandle::new(input, Arc::new(NoopFilePurger)))
        .expected_metadata(Some(target.metadata.clone()))
        .cache(CacheStrategy::Disabled)
        .build()
        .await
        .unwrap()
        .unwrap();
    let batch = original.next_record_batch().await.unwrap().unwrap();
    let sequences = batch
        .column(sequence_column_index(batch.num_columns()))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(
        0,
        sequences.value(0),
        "the original target really observes version zero"
    );
    if trusted {
        let output = fixture.compact(plan).await;
        let mut rows = Vec::new();
        for file in &output.files[&RegionId::new(1, 0)] {
            assert!(file.preserve_row_sequence);
            rows.extend(
                fixture
                    .read_published_output_rows(file.clone(), target.metadata.clone())
                    .await,
            );
        }
        assert_eq!(
            vec![(0, 0, 0, OpType::Put as u8, Some(0))],
            rows,
            "trusted zero versions must survive publication"
        );
        return;
    }
    let error = compact_regions(plan, fixture.context(None).0)
        .await
        .err()
        .expect("must not publish an output that reads version zero as version100");
    assert!(
        matches!(&error, crate::error::Error::InvalidMultiRegionCompaction { reason, .. } if reason.contains("zero output versions")),
        "{error:?}"
    );
    let path = crate::sst::location::sst_file_path(
        fixture.layer.table_dir(),
        fixture.source.file_id(),
        fixture.layer.path_type(),
    );
    assert!(fixture.layer.object_store().exists(&path).await.unwrap());
}

struct ObserveOnlineOutput {
    protection: Arc<Protection>,
    counters: Arc<SourceCounters>,
    expected_rows: u64,
    observed: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl SstWriteGuard for ObserveOnlineOutput {
    async fn before_create(&self, file: RegionFileId) -> Result<()> {
        use std::sync::atomic::Ordering;
        if !self.observed.swap(true, Ordering::Relaxed) {
            assert_eq!(0, self.counters.files.load(Ordering::Relaxed));
            assert!(self.counters.rows.load(Ordering::Relaxed) < self.expected_rows);
        }
        self.protection.before_create(file).await
    }
}

#[tokio::test]
async fn test_output_starts_before_the_shared_source_is_exhausted() {
    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        vec![new_record_batch_with_custom_sequence(
            &["000", "series"],
            0,
            4096,
            0,
        )]
    })
    .await;
    let plan = fixture.plan(1);
    let (mut context, protection) = fixture.context(None);
    let counters = Arc::new(SourceCounters::default());
    let observer = Arc::new(ObserveOnlineOutput {
        protection,
        counters: counters.clone(),
        expected_rows: fixture.source.num_rows,
        observed: std::sync::atomic::AtomicBool::new(false),
    });
    context.protection = observer.clone();
    let source = source::SharedSource::build(
        &plan,
        &context,
        validate_plan(&plan).unwrap(),
        counters.clone(),
    )
    .await
    .unwrap();
    let output = worker::execute(plan, context, source, counters)
        .await
        .unwrap();
    assert_eq!(4096, output.stats.output_rows);
    assert!(observer.observed.load(std::sync::atomic::Ordering::Relaxed));
}

#[tokio::test]
async fn test_invalid_target_options_are_rejected_before_creating_outputs() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    plan.targets
        .get_mut(&RegionId::new(1, 1))
        .unwrap()
        .options
        .max_row_group_row_count = Some(0);
    let (context, protection) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(error, crate::error::Error::InvalidRegionOptions { .. }),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_all_resident_writers_are_admitted_before_creating_outputs() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(32);
    plan.memory_limit = 128 * 1024 * 1024;
    let (context, protection) = fixture.context(None);
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(&error, crate::error::Error::MultiRegionCompactionResource { reason, .. } if reason.contains("minimum working set")),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

const MIB: usize = 1024 * 1024;
const MULTIPART_CHUNK: usize = 5 * MIB;
const MULTIPART_INPUTS: usize = 8;
const ROWS_PER_INPUT_PER_TARGET: usize = 4096;
const PAYLOAD_BYTES: usize = 640;
const MULTIPART_TAGS: [&str; 2] = ["000", "020"];

#[tokio::test]
async fn test_table_budget_below_legacy_floor_writes_multipart_outputs_losslessly() {
    let metadata = build_test_binary_test_region_metadata();
    let mut fixture = Fixture::without_source((*metadata).clone()).await;
    let row_group_size = 256;
    let options = WriteOptions {
        row_group_size,
        ..Default::default()
    };
    // Each SST has both tags and a disjoint timestamp range: 40 MiB of
    // incompressible payload overall, but only 40 KiB of payload per 64 rows.
    let mut sources = Vec::new();
    for input in 0..MULTIPART_INPUTS {
        let info = fixture
            .write_source_with_options(
                fixture.metadata.clone(),
                multipart_input_batches(input),
                FormatType::Flat,
                &options,
            )
            .await;
        sources.push(info);
    }
    // The final source row group contains the last batches for the second target.
    let tail = Arc::new(InputTailGate::new(
        &fixture,
        sources.last().unwrap(),
        RegionId::new(1, 1),
    ));
    let sources = sources
        .into_iter()
        .map(|info| file_meta(info, fixture.metadata.region_id))
        .collect();
    let mut plan = fixture.plan_with_sources(2, sources);
    // The old per-Arrow-buffer minimum rejected this even at 1 GiB. Each
    // target now gets a separate upload chunk and writes one ~20 MiB SST.
    let budget = 512 * MIB;
    plan.memory_limit = budget;
    plan.max_output_file_size = 32 * MIB;
    for target in plan.targets.values_mut() {
        target.options.max_row_group_row_count = Some(row_group_size);
    }
    let targets = plan.targets.clone();
    let uploads = install_multipart_backend(&mut fixture, MULTIPART_CHUNK, Some(tail.clone()));
    let (mut context, _) = fixture.context(None);
    context.index_kind_provider = Some(Arc::new(UploadProbeIndex));

    let output = tokio::time::timeout(Duration::from_secs(60), compact_regions(plan, context))
        .await
        .expect("the target must upload a part before reading its final input group")
        .unwrap();
    assert!(
        tail.requested.load(Ordering::Relaxed),
        "input never reached the gated row group"
    );
    assert!(output.stats.estimated_memory_bytes <= budget as u64);
    assert_eq!(targets.len(), output.files.len());
    let layer = &fixture.layer;
    for (id, target) in targets {
        let files = &output.files[&id];
        assert_eq!(1, files.len());
        let file = &files[0];
        let path = location::sst_file_path(layer.table_dir(), file.file_id(), layer.path_type());
        uploads.assert_completed(&path, file.file_size);
        let path = location::index_file_path(layer.table_dir(), file.index_id(), layer.path_type());
        uploads.assert_completed(&path, file.index_file_size);
        let puffin = layer
            .build_puffin_manager()
            .reader(&file.index_id())
            .await
            .unwrap();
        assert!(puffin.metadata().await.unwrap().blobs.iter().any(|blob| {
            blob.blob_type == UPLOAD_PROBE_BLOB && blob.length as u64 == UPLOAD_PROBE_BYTES
        }));

        check_multipart_rows(
            fixture
                .read_published_output(file.clone(), target.metadata)
                .await,
            id.region_sequence() as usize,
            MULTIPART_INPUTS * ROWS_PER_INPUT_PER_TARGET,
        );
    }
}

#[rstest::rstest]
#[case::budget_cannot_cover_both_targets(8 * MIB, 92 * MIB)]
#[case::backend_disallows_all_part_sizes(0, 512 * MIB)]
#[tokio::test]
async fn test_multipart_admission_rejects_before_protection_or_write(
    #[case] chunk: usize,
    #[case] budget: usize,
) {
    let mut fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    plan.memory_limit = budget;
    let uploads = install_multipart_backend(&mut fixture, chunk, None);
    let (context, protection) = fixture.context(None);
    let error = compact_regions(plan, context)
        .await
        .err()
        .expect("must reject before writing");
    assert!(
        matches!(
            error,
            crate::error::Error::MultiRegionCompactionResource { .. }
        ),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
    assert!(uploads.files.lock().unwrap().is_empty());
}

fn multipart_input_batches(input: usize) -> Vec<RecordBatch> {
    let start = input * ROWS_PER_INPUT_PER_TARGET;
    let end = start + ROWS_PER_INPUT_PER_TARGET;
    MULTIPART_TAGS
        .iter()
        .enumerate()
        .map(|(target, tag)| {
            let values = (start..end)
                .map(|timestamp| multipart_payload(target, timestamp).to_vec())
                .collect::<Vec<_>>();
            let batch = new_record_batch_with_binary_values(tag, &values);
            let mut columns = batch.columns().to_vec();
            let time_index = time_index_column_index(columns.len());
            columns[time_index] = Arc::new(TimestampMillisecondArray::from_iter_values(
                start as i64..end as i64,
            ));
            RecordBatch::try_new(batch.schema(), columns).unwrap()
        })
        .collect()
}

fn multipart_payload(target: usize, timestamp: usize) -> [u8; PAYLOAD_BYTES] {
    let mut bytes = [0; PAYLOAD_BYTES];
    SmallRng::seed_from_u64((timestamp * MULTIPART_TAGS.len() + target) as u64)
        .fill_bytes(&mut bytes);
    bytes
}

fn check_multipart_rows(batches: Vec<RecordBatch>, target: usize, rows: usize) {
    let mut seen = vec![false; rows];
    // The input owner keeps physical versions; the other region uses its
    // admitted file version (see plan_with_sources).
    let expected_sequence = if target == 0 { 1000 } else { 101 };
    for batch in batches {
        let tags = cast(batch.column_by_name("tag_0").unwrap(), &DataType::Utf8).unwrap();
        let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
        let values = cast(batch.column_by_name("field_0").unwrap(), &DataType::Binary).unwrap();
        let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
        let timestamps =
            crate::read::timestamp_array_to_i64_slice(batch.column_by_name("ts").unwrap());
        let sequences = batch
            .column(sequence_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for (row, &timestamp) in timestamps.iter().enumerate() {
            assert_eq!(expected_sequence, sequences.value(row));
            assert!(tags.is_valid(row) && tags.value(row) == MULTIPART_TAGS[target]);
            let timestamp = timestamp as usize;
            assert!(!seen[timestamp], "duplicate timestamp {timestamp}");
            seen[timestamp] = true;
            assert!(
                values.is_valid(row)
                    && values.value(row) == &multipart_payload(target, timestamp)[..],
                "wrong payload for target {target}, timestamp {timestamp}"
            );
        }
    }
    assert!(
        seen.into_iter().all(|row| row),
        "missing rows in target {target}"
    );
}

/// Withholds the payload of the last input row group until its target's SST starts uploading.
struct InputTailGate {
    source_path: String,
    source_size: usize,
    payload_offset: usize,
    target_dir: String,
    first_part: watch::Sender<bool>,
    requested: AtomicBool,
}

impl InputTailGate {
    fn new(fixture: &Fixture, source: &SstInfo, target: RegionId) -> Self {
        let footer = source.file_metadata.as_ref().unwrap();
        let payload = footer
            .row_groups()
            .last()
            .unwrap()
            .columns()
            .iter()
            .find(|column| column.column_path().string() == "field_0")
            .unwrap();
        let layer = &fixture.layer;
        Self {
            source_path: location::sst_file_path(
                layer.table_dir(),
                RegionFileId::new(fixture.metadata.region_id, source.file_id),
                layer.path_type(),
            ),
            source_size: source.file_size as usize,
            payload_offset: payload.byte_range().0 as usize,
            target_dir: location::region_dir_from_table_dir(
                layer.table_dir(),
                target,
                layer.path_type(),
            ),
            first_part: watch::channel(false).0,
            requested: AtomicBool::new(false),
        }
    }

    async fn wait_for_part(&self, range: mock::BytesRange) -> mock::Result<()> {
        if range
            .to_content_range(self.source_size)?
            .contains(&self.payload_offset)
        {
            self.requested.store(true, Ordering::Relaxed);
            self.first_part
                .subscribe()
                .wait_for(|&started| started)
                .await
                .unwrap();
        }
        Ok(())
    }
}

struct TailReader {
    inner: oio::Reader,
    gate: Arc<InputTailGate>,
}

impl oio::Read for TailReader {
    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> mock::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.gate.wait_for_part(range).await?;
        self.inner.open(range).await
    }

    async fn read(&self, range: mock::BytesRange) -> mock::Result<(mock::RpRead, mock::Buffer)> {
        self.gate.wait_for_part(range).await?;
        self.inner.read(range).await
    }
}

struct MultipartUploads {
    files: Mutex<BTreeMap<String, MultipartUpload>>,
    tail: Option<Arc<InputTailGate>>,
}

#[derive(Default)]
struct MultipartUpload {
    parts: Vec<u64>,
    completed: bool,
}

impl MultipartUploads {
    fn assert_completed(&self, path: &str, size: u64) {
        let uploads = self.files.lock().unwrap();
        let upload = uploads
            .get(path)
            .unwrap_or_else(|| panic!("no upload for {path}"));
        assert!(
            upload.completed,
            "multipart upload did not complete for {path}"
        );
        let (last, full) = upload.parts.split_last().unwrap();
        assert!(
            full.len() >= 2
                && full.iter().all(|&part| part == MULTIPART_CHUNK as u64)
                && (1..=MULTIPART_CHUNK as u64).contains(last),
            "wrong multipart sizes for {path}: {:?}",
            upload.parts
        );
        assert_eq!(size, upload.parts.iter().sum::<u64>(), "{path}");
    }
}

/// Uses the same OpenDAL chunker and multipart state machine as S3. Only the
/// service's part requests are replaced, appending to an FS writer with no
/// whole-file buffer in the fake backend.
fn install_multipart_backend(
    fixture: &mut Fixture,
    chunk: usize,
    tail: Option<Arc<InputTailGate>>,
) -> Arc<MultipartUploads> {
    let mut mock = MockLayerBuilder::default();
    if let Some(gate) = tail.clone() {
        mock.reader_factory(Arc::new(move |path, _, inner| {
            if path == gate.source_path {
                Box::new(TailReader {
                    inner,
                    gate: gate.clone(),
                })
            } else {
                inner
            }
        }));
    }
    let uploads = Arc::new(MultipartUploads {
        files: Mutex::default(),
        tail,
    });
    let layer = &fixture.layer;
    let store = layer.object_store().clone();
    let executor = store.context().executor().clone();
    let observed = uploads.clone();
    let mock = mock
        .writer_factory(Arc::new(move |path, args, inner| {
            assert_eq!(1, args.concurrent(), "multipart concurrency for {path}");
            let previous = observed
                .files
                .lock()
                .unwrap()
                .insert(path.to_string(), MultipartUpload::default());
            assert!(previous.is_none(), "opened multiple writers for {path}");
            Box::new(oio::MultipartWriter::new(
                executor.clone(),
                FsMultipartSink {
                    inner: tokio::sync::Mutex::new(inner),
                    path: path.to_string(),
                    uploads: observed.clone(),
                },
                args.concurrent(),
            )) as oio::Writer
        }))
        .build()
        .unwrap();
    let store = store
        .layer(mock)
        .layer(CapabilityOverrideLayer::new(move |mut caps| {
            caps.write_multi_min_size = Some(chunk);
            caps.write_multi_max_size = Some(chunk);
            caps
        }));
    fixture.layer = Arc::new(AccessLayer::new(
        layer.table_dir(),
        layer.path_type(),
        store,
        layer.puffin_manager_factory().clone(),
        layer.intermediate_manager().clone(),
    ));
    uploads
}

struct FsMultipartSink {
    inner: tokio::sync::Mutex<oio::Writer>,
    path: String,
    uploads: Arc<MultipartUploads>,
}

impl oio::MultipartWrite for FsMultipartSink {
    async fn write_once(&self, _size: u64, body: mock::Buffer) -> mock::Result<mock::Metadata> {
        let mut writer = self.inner.lock().await;
        writer.write(body).await?;
        writer.close().await
    }

    async fn initiate_part(&self) -> mock::Result<String> {
        Ok(self.path.clone())
    }

    async fn write_part(
        &self,
        _: &str,
        part_number: usize,
        size: u64,
        body: mock::Buffer,
    ) -> mock::Result<oio::MultipartPart> {
        {
            let mut files = self.uploads.files.lock().unwrap();
            let parts = &mut files.get_mut(&self.path).unwrap().parts;
            assert_eq!(
                parts.len(),
                part_number,
                "out-of-order part for {}",
                self.path
            );
            parts.push(size);
        }
        if part_number == 0
            && let Some(tail) = &self.uploads.tail
            && self.path.starts_with(&tail.target_dir)
            && self.path.ends_with(".parquet")
        {
            assert!(
                !tail.first_part.send_replace(true),
                "multiple SSTs released the same input tail"
            );
        }
        self.inner.lock().await.write(body).await?;
        Ok(oio::MultipartPart {
            part_number,
            size: Some(size),
            etag: part_number.to_string(),
            checksum: None,
        })
    }

    async fn complete_part(
        &self,
        _: &str,
        _: &[oio::MultipartPart],
    ) -> mock::Result<mock::Metadata> {
        let metadata = self.inner.lock().await.close().await?;
        self.uploads
            .files
            .lock()
            .unwrap()
            .get_mut(&self.path)
            .unwrap()
            .completed = true;
        Ok(metadata)
    }

    async fn abort_part(&self, _: &str) -> mock::Result<()> {
        self.inner.lock().await.abort().await
    }
}

const UPLOAD_PROBE_BLOB: &str = "test.upload-budget";
const UPLOAD_PROBE_BYTES: u64 = (2 * MULTIPART_CHUNK + 123) as u64;
const UPLOAD_PROBE_ENTRY: PlanEntry = PlanEntry {
    column_id: 0,
    kind: IndexKind::Extension(ExtensionIndexKind::Hll),
    format_version: 1,
};

/// An uncompressed streaming blob drives the real Puffin writer across two
/// parts without inflating a built-in index or retaining a large creator state.
#[derive(Debug)]
struct UploadProbeIndex;

impl IndexKindProvider for UploadProbeIndex {
    fn handlers(&self) -> Vec<Arc<dyn IndexKindHandler>> {
        vec![Arc::new(Self)]
    }
}

impl IndexKindHandler for UploadProbeIndex {
    fn kinds(&self) -> &[IndexKind] {
        &[UPLOAD_PROBE_ENTRY.kind]
    }

    fn validate(&self, _: &PlanContext<'_>) -> std::result::Result<(), BoxedError> {
        Ok(())
    }

    fn plan(
        &self,
        _: &PlanContext<'_>,
    ) -> std::result::Result<Option<Arc<dyn PlannedIndex>>, BoxedError> {
        Ok(Some(Arc::new(Self)))
    }
}

#[async_trait::async_trait]
impl PlannedIndex for UploadProbeIndex {
    fn kinds(&self) -> &[IndexKind] {
        IndexKindHandler::kinds(self)
    }

    fn entries(&self) -> &[PlanEntry] {
        &[UPLOAD_PROBE_ENTRY]
    }

    async fn build(
        &self,
        _: &BuildContext<'_>,
    ) -> std::result::Result<Option<Box<dyn IndexCreator>>, BoxedError> {
        Ok(Some(Box::new(Self)))
    }

    fn is_current(&self, _: &FileMeta, _: &PlanContext<'_>) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl IndexCreator for UploadProbeIndex {
    async fn update(&mut self, _: &mut crate::read::Batch) -> std::result::Result<(), BoxedError> {
        Ok(())
    }

    async fn update_flat(&mut self, _: &RecordBatch) -> std::result::Result<(), BoxedError> {
        Ok(())
    }

    async fn finish(
        &mut self,
        writer: &mut SstPuffinWriter,
    ) -> std::result::Result<Vec<CompletedEntry>, BoxedError> {
        writer
            .put_blob(
                UPLOAD_PROBE_BLOB,
                io::repeat(0x5a).take(UPLOAD_PROBE_BYTES),
                Default::default(),
                Default::default(),
            )
            .await
            .map_err(BoxedError::new)?;
        Ok(vec![CompletedEntry {
            column_id: Some(UPLOAD_PROBE_ENTRY.column_id),
            kind: UPLOAD_PROBE_ENTRY.kind,
            format_version: UPLOAD_PROBE_ENTRY.format_version,
        }])
    }

    async fn abort(&mut self) -> std::result::Result<(), BoxedError> {
        Ok(())
    }

    fn memory_usage(&self) -> usize {
        0
    }
}

#[rstest::rstest]
#[case::sync(crate::config::IndexBuildMode::Sync)]
#[case::async_mode(crate::config::IndexBuildMode::Async)]
#[tokio::test]
async fn test_admission_includes_each_tantivy_creator_even_in_async_mode(
    #[case] mode: crate::config::IndexBuildMode,
) {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut baseline = fixture.plan(2);
    baseline.memory_limit = 64 * 1024 * 1024;
    assert_eq!(124, fixture.compact(baseline).await.stats.output_rows);
    let mut plan = fixture.plan(2);
    plan.memory_limit = 64 * 1024 * 1024;
    add_fulltext_column(&mut plan);
    let (mut context, protection) = fixture.context(None);
    Arc::make_mut(&mut context.config).index.build_mode = mode;
    let error = compact_regions(plan, context).await.err().unwrap();
    assert!(
        matches!(&error, crate::error::Error::MultiRegionCompactionResource { reason, .. } if reason.contains("minimum working set")),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_oversized_equal_key_group_fails_without_partial_outputs() {
    let fixture = Fixture::with_source(PrimaryKeyEncoding::Dense, FormatType::Flat, |_| {
        let batch = new_record_batch_with_custom_sequence(&["000", "series"], 0, 262_144, 0);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(TimestampMillisecondArray::from(vec![0; 262_144]));
        vec![RecordBatch::try_new(batch.schema(), columns).unwrap()]
    })
    .await;
    let (context, protection) = fixture.context(None);
    let error = compact_regions(fixture.plan(1), context)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&error, crate::error::Error::MultiRegionCompactionResource { reason, .. } if reason.contains("Equal-key version group")),
        "{error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_target_writer_sizes_row_groups_from_source_size() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let plan = fixture.plan(1);
    let (context, _protection) = fixture.context(None);
    let source = new_flat_source_from_record_batches(vec![source_batch(&fixture.metadata, 0, 100)]);
    let upload = memory::UploadBudget::new(&plan, context.access_layer.object_store()).unwrap();
    let limits = memory::MemoryLimits::new(&plan, source.schema(), upload).unwrap();
    // 64 decoded rows of 4 MiB: 32 rows fill the 128 MiB target. The target's `FileMeta` inputs
    // describe the narrow fixture rows, which would allow the full cap.
    let writer = writer::TargetWriter::new(
        &plan.targets[&RegionId::new(1, 0)],
        &context,
        &limits,
        plan.max_output_file_size,
        (64, 256 << 20),
        Arc::default(),
    );
    assert_eq!(32, writer.options().row_group_size);
}

#[tokio::test]
async fn test_reader_admission_counts_decoded_values() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    // 2048 copies of one 64 KiB value: tiny once dictionary-encoded, 128 MiB decoded.
    let batch = new_record_batch_with_binary_values("a", &vec![vec![7u8; 64 * 1024]; 2048]);
    let info = fixture
        .write_source(
            build_test_binary_test_region_metadata(),
            vec![batch],
            FormatType::Flat,
        )
        .await;
    let footer = info.file_metadata.unwrap();
    assert!(
        footer
            .row_groups()
            .iter()
            .all(|group| group.total_byte_size() < 1 << 20)
    );
    assert!(memory::reader_bytes(&footer) >= 128 << 20);
}

#[rstest::rstest]
#[case::encoder("encoder")]
#[case::metadata("metadata")]
#[case::upload("upload")]
#[tokio::test]
async fn test_limited_writer_fails_instead_of_returning_over_budget_output(
    #[case] kind: &'static str,
) {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let plan = fixture.plan(1);
    let (context, protection) = fixture.context(None);
    let source = new_flat_source_from_record_batches(vec![source_batch(&fixture.metadata, 0, 100)]);
    let upload = memory::UploadBudget::new(&plan, context.access_layer.object_store()).unwrap();
    let mut limits = memory::MemoryLimits::new(&plan, source.schema(), upload).unwrap();
    match kind {
        "encoder" => limits.writer.working_bytes = 1,
        "metadata" => limits.writer.metadata_bytes = 1,
        "upload" => limits.writer.upload_limits.max_object_bytes = 1,
        _ => unreachable!(),
    }
    let error = writer::TargetWriter::new(
        &plan.targets[&RegionId::new(1, 0)],
        &context,
        &limits,
        plan.max_output_file_size,
        (0, 0),
        Arc::default(),
    )
    .write(source)
    .await
    .err()
    .unwrap();
    match error {
        crate::error::Error::MultiRegionCompactionResource { .. } if kind == "upload" => {}
        crate::error::Error::SstWriteMemoryExceeded { kind: actual, .. } if actual == kind => {}
        _ => panic!("unexpected {kind} limit error: {error:?}"),
    }
    assert_eq!(1, protection.files.lock().unwrap().len());
}

#[tokio::test]
async fn test_cancelled_execution_does_not_create_output() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let (context, protection) = fixture.context(None);
    context.cancellation.cancel();
    assert!(matches!(
        compact_regions(fixture.plan(2), context).await,
        Err(crate::error::Error::CompactionCancelled { .. })
    ));
    assert!(protection.files.lock().unwrap().is_empty());
}

/// Mutations applied to a valid plan to reach each `validate_plan` rejection.
#[derive(Clone, Copy, Debug)]
enum InvalidPlan {
    NoSources,
    TinyBudget,
    ZeroFileSize,
    ZeroWindow,
    NegativeWindow,
    OversizedControl,
    NoTargets,
    WindowOverflow,
    UnalignedWindow,
    SourceOutsideWindow,
    DuplicateSource,
    ForeignTable,
    MissingInput,
    InconsistentSchema,
}

#[rstest::rstest]
#[case(InvalidPlan::NoSources, "sources must not be empty")]
#[case(InvalidPlan::TinyBudget, "memory_limit must be at least 1 MiB")]
#[case(
    InvalidPlan::ZeroFileSize,
    "max_output_file_size must be greater than 0"
)]
#[case(InvalidPlan::ZeroWindow, "window_seconds must be greater than 0")]
#[case(InvalidPlan::NegativeWindow, "window_seconds must be greater than 0")]
#[case(InvalidPlan::OversizedControl, "control state exceeds the task budget")]
#[case(InvalidPlan::NoTargets, "No routing targets")]
#[case(InvalidPlan::WindowOverflow, "Task window overflow")]
#[case(InvalidPlan::UnalignedWindow, "not aligned")]
#[case(InvalidPlan::SourceOutsideWindow, "not wholly inside the task window")]
#[case(
    InvalidPlan::DuplicateSource,
    "unique source files from one physical table"
)]
#[case(
    InvalidPlan::ForeignTable,
    "unique source files from one physical table"
)]
#[case(
    InvalidPlan::MissingInput,
    "effective metadata of every selected source"
)]
#[case(InvalidPlan::InconsistentSchema, "consistent physical-table snapshot")]
#[tokio::test]
async fn test_invalid_plan_is_rejected_before_creating_outputs(
    #[case] invalid: InvalidPlan,
    #[case] reason: &'static str,
) {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let mut plan = fixture.plan(2);
    match invalid {
        InvalidPlan::NoSources => plan.sources.clear(),
        InvalidPlan::TinyBudget => plan.memory_limit = 1024 * 1024 - 1,
        InvalidPlan::ZeroFileSize => plan.max_output_file_size = 0,
        InvalidPlan::ZeroWindow => plan.window_seconds = 0,
        InvalidPlan::NegativeWindow => plan.window_seconds = -1,
        InvalidPlan::OversizedControl => {
            plan.memory_limit = 1024 * 1024;
            let source = plan.sources[0].clone();
            let extra = plan.memory_limit / std::mem::size_of::<FileMeta>() + 1;
            plan.sources = vec![source; extra];
        }
        InvalidPlan::NoTargets => plan.targets.clear(),
        InvalidPlan::WindowOverflow => {
            plan.window_start = i64::MAX - plan.window_seconds + 1;
        }
        InvalidPlan::UnalignedWindow => plan.window_start += 1,
        InvalidPlan::SourceOutsideWindow => {
            plan.sources[0].time_range = (
                Timestamp::new_second(plan.window_seconds),
                Timestamp::new_second(plan.window_seconds + 1),
            );
        }
        InvalidPlan::DuplicateSource => {
            let source = plan.sources[0].clone();
            plan.sources.push(source);
        }
        InvalidPlan::ForeignTable => {
            plan.sources[0].region_id = RegionId::new(2, 0);
        }
        InvalidPlan::MissingInput => {
            let target = plan.targets.values_mut().next().unwrap();
            target.inputs.clear();
        }
        InvalidPlan::InconsistentSchema => {
            let target = plan.targets.values_mut().next().unwrap();
            Arc::make_mut(&mut target.metadata).column_metadatas.pop();
        }
    }
    let (context, protection) = fixture.context(None);
    let error = tokio::time::timeout(Duration::from_secs(30), compact_regions(plan, context))
        .await
        .expect("invalid plan hung instead of failing")
        .err()
        .expect("invalid plan must be rejected");
    assert!(
        format!("{error:?}").contains(reason),
        "expected `{reason}`, got {error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

#[derive(Debug)]
struct InvalidOptionalIndexHandler;

impl IndexKindProvider for InvalidOptionalIndexHandler {
    fn handlers(&self) -> Vec<Arc<dyn IndexKindHandler>> {
        vec![Arc::new(Self)]
    }
}

impl IndexKindHandler for InvalidOptionalIndexHandler {
    fn validate(&self, context: &PlanContext<'_>) -> std::result::Result<(), BoxedError> {
        self.plan(context).map(|_| ())
    }

    fn kinds(&self) -> &[IndexKind] {
        &[IndexKind::Extension(ExtensionIndexKind::Hll)]
    }

    fn plan(
        &self,
        context: &PlanContext<'_>,
    ) -> std::result::Result<Option<Arc<dyn PlannedIndex>>, BoxedError> {
        assert!(
            context.metadata.column_metadatas[0]
                .column_schema
                .hll_options()
                .is_err()
        );
        Err(BoxedError::new(
            crate::error::InvalidConfigSnafu {
                reason: "invalid optional index configuration",
            }
            .build(),
        ))
    }
}

#[derive(Debug, Default)]
struct ConflictingRegistrationProvider(std::sync::atomic::AtomicUsize);

impl IndexKindProvider for ConflictingRegistrationProvider {
    fn handlers(&self) -> Vec<Arc<dyn IndexKindHandler>> {
        // Preflight succeeds; source preparation must propagate its own failure too.
        let calls = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let handler: Arc<dyn IndexKindHandler> = Arc::new(InvalidOptionalIndexHandler);
        if calls == 0 {
            vec![handler]
        } else {
            vec![handler.clone(), handler]
        }
    }
}

#[tokio::test]
async fn test_source_index_registration_failure_does_not_fall_back() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    let (mut context, protection) = fixture.context(None);
    context.index_kind_provider = Some(Arc::new(ConflictingRegistrationProvider::default()));
    let error = compact_regions(fixture.plan(2), context)
        .await
        .err()
        .expect("conflicting registration must fail");
    assert!(
        format!("{error:?}").contains("registering index handlers"),
        "unexpected registration error: {error:?}"
    );
    assert!(protection.files.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_invalid_optional_index_options_do_not_block_routed_compaction() {
    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    for invalid_value in ["missing-column", "bad-capacity"] {
        let mut plan = fixture.plan(2);
        for target in plan.targets.values_mut() {
            Arc::make_mut(&mut target.metadata).column_metadatas[0]
                .column_schema
                .mut_metadata()
                .insert(datatypes::schema::HLL_KEY.into(), invalid_value.into());
        }
        let (mut context, _) = fixture.context(None);
        context.index_kind_provider = Some(Arc::new(InvalidOptionalIndexHandler));
        let output = compact_regions(plan, context).await.unwrap();
        assert_eq!(output.stats.source_rows, output.stats.output_rows);
        assert!(output.stats.output_rows > 0);
        assert!(
            output
                .files
                .values()
                .flatten()
                .all(|file| file
                    .indexes
                    .iter()
                    .all(|index| index.created_indexes.iter().all(|kind| !matches!(
                        kind,
                        crate::sst::file::IndexType::HllIndex
                            | crate::sst::file::IndexType::KllIndex
                            | crate::sst::file::IndexType::MgIndex
                    ))))
        );
    }
}

#[tokio::test]
async fn test_missing_index_handler_does_not_block_routed_compaction() {
    use crate::sst::file::{ColumnIndexMetadata, IndexType};

    let fixture = Fixture::new(PrimaryKeyEncoding::Dense, FormatType::Flat, 0).await;
    for required_by in ["schema", "file", "column", "target"] {
        let mut plan = fixture.plan(2);
        match required_by {
            "schema" => {
                for target in plan.targets.values_mut() {
                    Arc::make_mut(&mut target.metadata).column_metadatas[0]
                        .column_schema
                        .set_hll_options(&datatypes::schema::HllOptions::default())
                        .unwrap();
                }
            }
            "file" => plan.sources[0].available_indexes.push(IndexType::HllIndex),
            "column" => plan.sources[0].indexes.push(ColumnIndexMetadata {
                column_id: 0,
                created_indexes: [IndexType::HllIndex].into_iter().collect(),
                ..Default::default()
            }),
            _ => {
                plan.targets
                    .values_mut()
                    .next()
                    .unwrap()
                    .inputs
                    .values_mut()
                    .next()
                    .unwrap()
                    .available_indexes
                    .push(IndexType::HllIndex);
            }
        }
        let (context, _) = fixture.context(None);
        let output = compact_regions(plan, context)
            .await
            .unwrap_or_else(|error| panic!("{required_by}: {error}"));
        assert_eq!(output.stats.source_rows, output.stats.output_rows);
        assert!(output.stats.output_rows > 0);
        assert!(!output.files.is_empty());
        for file in output.files.values().flatten() {
            assert!(file.num_rows > 0);
            assert!(file.available_indexes.iter().all(|kind| !matches!(
                kind,
                IndexType::HllIndex | IndexType::KllIndex | IndexType::MgIndex
            )));
            assert!(
                file.indexes
                    .iter()
                    .all(|index| index.created_indexes.iter().all(|kind| !matches!(
                        kind,
                        IndexType::HllIndex | IndexType::KllIndex | IndexType::MgIndex
                    )))
            );
        }
    }
}

#[rstest::rstest]
#[case::borrowed(PrimaryKeyEncoding::Dense)]
#[case::materialized(PrimaryKeyEncoding::Sparse)]
#[tokio::test]
async fn test_routing_at_source_batch_budget(#[case] encoding: PrimaryKeyEncoding) {
    let fixture = Fixture::new(encoding, FormatType::Flat, 7).await;
    let plan = fixture.plan(2);
    let (context, _) = fixture.context(None);
    let mut source = source::SharedSource::build(
        &plan,
        &context,
        validate_plan(&plan).unwrap(),
        Arc::new(SourceCounters::default()),
    )
    .await
    .unwrap();
    let source = Arc::get_mut(&mut source).unwrap();
    let batch = source_batch(&fixture.metadata, 0, 7);
    source.limits.arrow_batch_bytes = batch.get_array_memory_size();
    let owners = source.row_owners(&batch);
    if encoding == PrimaryKeyEncoding::Dense {
        assert_eq!(
            owners.unwrap(),
            vec![fixture.metadata.region_id; batch.num_rows()]
        );
    } else {
        assert!(matches!(
            owners,
            Err(crate::error::Error::MultiRegionCompactionResource { .. })
        ));
    }
}

// Exercise persisted sparse inputs, schema alignment, routing and output readback
// together: unrelated nullable tags must never become merge columns.
#[rstest::rstest]
#[case(2)]
#[case(40)]
#[case(3600)]
#[tokio::test]
async fn test_sparse_routing_with_wide_target_schema(#[case] target_tag_count: u32) {
    let fixture = Fixture::new(PrimaryKeyEncoding::Sparse, FormatType::Flat, 7).await;
    let mut plan = fixture.plan(8);
    for target in plan.targets.values_mut() {
        let mut keys = target.metadata.primary_key.clone();
        let mut builder = RegionMetadataBuilder::from_existing((*target.metadata).clone());
        // Sparse keys also contain the internal table ID and TSID.
        let existing_tags = keys.len() as u32 - 2;
        for offset in 0..target_tag_count - existing_tags {
            let id = 4 + offset;
            builder.push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    format!("tag_{id}"),
                    datatypes::data_type::ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: id,
            });
            keys.push(id);
        }
        assert_eq!(keys.len(), target_tag_count as usize + 2);
        builder.primary_key(keys);
        target.metadata = Arc::new(builder.build().unwrap());
    }
    let targets = plan.targets.clone();
    let (context, _) = fixture.context(None);
    let metadata = targets.values().next().unwrap().metadata.clone();
    let source = super::source::SharedSource::build(
        &plan,
        &context,
        metadata,
        Arc::new(super::SourceCounters::default()),
    )
    .await
    .unwrap();
    assert!(
        source
            .schema()
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("tag_"))
    );
    drop(source);
    let output = compact_regions(plan, context).await.unwrap();
    assert_eq!(124, output.stats.source_rows);
    assert_eq!(124, output.stats.output_rows);
    for (id, files) in output.files {
        let mut rows = Vec::new();
        for file in files {
            rows.extend(
                fixture
                    .read_output_rows(file, targets[&id].metadata.clone())
                    .await,
            );
        }
        let sequence = if id == fixture.metadata.region_id {
            7
        } else {
            100 + id.region_sequence() as u64
        };
        let mut expected = expected_source_rows(8, id, sequence);
        rows.sort();
        expected.sort();
        assert_eq!(expected, rows);
    }
}
