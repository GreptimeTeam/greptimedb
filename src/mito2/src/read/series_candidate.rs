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

//! Candidate metric-series discovery for the two-stage series scan.

use std::sync::Arc;
use std::time::Instant;

use async_stream::try_stream;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use datatypes::arrow::array::{Array, BinaryArray, BinaryBuilder};
use datatypes::arrow::compute::SortOptions;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use mito_codec::row_converter::{PrimaryKeyFilter, SparsePrimaryKeyCodec};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::region_engine::PartitionRange;
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, ReservedColumnId};
use tokio::sync::Semaphore;

use crate::error::{
    InvalidRequestSnafu, MergeCandidateSeriesSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::read::BoxedRecordBatchStream;
use crate::read::pruner::{PartitionPruner, Pruner};
use crate::read::range::RowGroupIndex;
use crate::read::range_cache::{
    build_candidate_range_cache_key, cache_flat_range_stream, cached_flat_range_stream,
};
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::{
    PartitionMetrics, maybe_scan_flat_other_ranges, new_filter_metrics, scan_flat_mem_ranges,
};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::format::PrimaryKeyArray;
use crate::sst::parquet::prefilter::{
    CachedPrimaryKeyFilter, build_primary_key_filter, prefilter_flat_batch_by_primary_key,
};
use crate::sst::parquet::reader::ReaderMetrics;
use crate::sst::parquet::row_group::ParquetFetchMetrics;

const CANDIDATE_SERIES_BATCH_SIZE: usize = 500;

/// Identifies one series in a physical metric region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetricSeriesId {
    pub(crate) table_id: u32,
    pub(crate) tsid: u64,
}

pub(crate) type MetricSeriesIdStream = BoxStream<'static, Result<Vec<MetricSeriesId>>>;

/// Builds candidate metric series from the ranges assigned to a [`SeriesScan`](super::series_scan::SeriesScan).
#[allow(dead_code)]
pub(crate) struct SeriesCandidateScanner {
    stream_ctx: Arc<StreamContext>,
    partitions: Vec<Vec<PartitionRange>>,
    pruner: Arc<Pruner>,
    source_semaphore: Arc<Semaphore>,
    memory_pool: Arc<dyn MemoryPool>,
    metrics_set: ExecutionPlanMetricsSet,
    part_metrics: PartitionMetrics,
}

#[allow(dead_code)]
impl SeriesCandidateScanner {
    pub(crate) fn try_new(
        stream_ctx: Arc<StreamContext>,
        partitions: Vec<Vec<PartitionRange>>,
        pruner: Arc<Pruner>,
        source_semaphore: Arc<Semaphore>,
        memory_pool: Arc<dyn MemoryPool>,
        metrics_set: ExecutionPlanMetricsSet,
        part_metrics: PartitionMetrics,
    ) -> Result<Self> {
        validate_metric_metadata(&stream_ctx)?;
        Ok(Self {
            stream_ctx,
            partitions,
            pruner,
            source_semaphore,
            memory_pool,
            metrics_set,
            part_metrics,
        })
    }

    /// Builds a globally sorted stream of candidate metric-series IDs.
    pub(crate) async fn build_stream(&self) -> Result<MetricSeriesIdStream> {
        let all_ranges = self
            .partitions
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        self.pruner.add_partition_ranges(&all_ranges);
        let partition_pruner = Arc::new(PartitionPruner::new(self.pruner.clone(), &all_ranges));

        let mut range_streams = Vec::with_capacity(all_ranges.len());
        for (range_idx, part_range) in all_ranges.iter().enumerate() {
            let stream = self
                .build_range_stream(*part_range, partition_pruner.clone(), range_idx)
                .await?;
            range_streams.push(stream);
        }

        let merged = merge_primary_key_streams(
            range_streams,
            self.memory_pool.clone(),
            &self.metrics_set,
            self.partitions.len(),
            "SeriesCandidateScanner::final_merge",
        )?;
        decode_metric_series(merged, self.stream_ctx.input.region_metadata().clone())
    }

    async fn build_range_stream(
        &self,
        part_range: PartitionRange,
        partition_pruner: Arc<PartitionPruner>,
        merge_partition: usize,
    ) -> Result<BoxedRecordBatchStream> {
        let cache_key = build_candidate_range_cache_key(&self.stream_ctx, &part_range);
        if let Some(key) = cache_key.as_ref() {
            if let Some(value) = self.stream_ctx.input.cache_strategy.get_range_result(key) {
                self.part_metrics.inc_range_cache_hit();
                return Ok(cached_flat_range_stream(value));
            }
            self.part_metrics.inc_range_cache_miss();
        }

        let range_meta = &self.stream_ctx.ranges[part_range.identifier];
        let mut sources = Vec::with_capacity(range_meta.row_group_indices.len());
        for index in &range_meta.row_group_indices {
            let source = self
                .build_source(
                    *index,
                    range_meta.time_range,
                    &part_range,
                    partition_pruner.clone(),
                )
                .await?;
            if let Some(source) = source {
                sources.push(source);
            }
        }

        let sources = self.stream_ctx.input.create_parallel_flat_sources(
            sources,
            self.source_semaphore.clone(),
            2,
        )?;
        let stream = merge_primary_key_streams(
            sources,
            self.memory_pool.clone(),
            &self.metrics_set,
            merge_partition,
            "SeriesCandidateScanner::range_merge",
        )?;

        Ok(match cache_key {
            Some(key) => cache_flat_range_stream(
                stream,
                self.stream_ctx.input.cache_strategy.clone(),
                key,
                self.part_metrics.clone(),
            ),
            None => stream,
        })
    }

    async fn build_source(
        &self,
        index: RowGroupIndex,
        time_range: crate::sst::file::FileTimeRange,
        part_range: &PartitionRange,
        partition_pruner: Arc<PartitionPruner>,
    ) -> Result<Option<BoxedRecordBatchStream>> {
        let metadata = self.stream_ctx.input.region_metadata().clone();
        if self.stream_ctx.is_mem_range_index(index) {
            let raw = scan_flat_mem_ranges(
                self.stream_ctx.clone(),
                self.part_metrics.clone(),
                index,
                time_range,
            );
            let filter = build_primary_key_filter(
                &metadata,
                self.stream_ctx.input.predicate_group().predicate(),
            );
            return Ok(Some(candidate_primary_key_stream(Box::pin(raw), filter)));
        }

        if self.stream_ctx.is_file_range_index(index) {
            if partition_pruner.try_skip_manifest_pruned_file_range(index, &self.part_metrics) {
                return Ok(None);
            }

            let mut reader_metrics = ReaderMetrics {
                filter_metrics: new_filter_metrics(self.part_metrics.explain_verbose()),
                ..Default::default()
            };
            let ranges = partition_pruner
                .build_file_ranges(index, &self.part_metrics, &mut reader_metrics)
                .await?;
            self.part_metrics.inc_num_file_ranges(ranges.len());
            self.part_metrics
                .merge_reader_metrics(&reader_metrics, None);

            // Reuse the exact encoded-PK filter selected by the file's reader plan, but
            // execute it in `candidate_primary_key_stream` after the PK-only read.
            let filter = ranges.first().and_then(|range| range.primary_key_filter());
            let part_metrics = self.part_metrics.clone();
            let raw = Box::pin(try_stream! {
                let fetch_metrics = part_metrics
                    .explain_verbose()
                    .then(|| Arc::new(ParquetFetchMetrics::default()));
                let mut reader_metrics = ReaderMetrics {
                    fetch_metrics: fetch_metrics.clone(),
                    ..Default::default()
                };
                for range in ranges {
                    let build_start = Instant::now();
                    let Some(mut reader) = range
                        .primary_key_reader(fetch_metrics.as_deref())
                        .await?
                    else {
                        continue;
                    };
                    reader_metrics.build_cost += build_start.elapsed();

                    let scan_start = Instant::now();
                    while let Some(batch) = reader.try_next().await? {
                        reader_metrics.num_record_batches += 1;
                        reader_metrics.num_batches += 1;
                        reader_metrics.num_rows += batch.num_rows();
                        yield batch;
                    }
                    reader_metrics.scan_cost += scan_start.elapsed();
                }
                reader_metrics.observe_rows("candidate_series");
                part_metrics.merge_reader_metrics(&reader_metrics, None);
            });
            return Ok(Some(candidate_primary_key_stream(raw, filter)));
        }

        let raw = maybe_scan_flat_other_ranges(
            &self.stream_ctx,
            index,
            &self.part_metrics,
            self.stream_ctx.range_pre_filter_mode(part_range),
        )
        .await?;
        let filter = build_primary_key_filter(
            &metadata,
            self.stream_ctx.input.predicate_group().predicate(),
        );
        Ok(Some(candidate_primary_key_stream(raw, filter)))
    }
}

fn validate_metric_metadata(stream_ctx: &StreamContext) -> Result<()> {
    let metadata = stream_ctx.input.region_metadata();
    let valid_prefix = metadata
        .primary_key
        .starts_with(&[ReservedColumnId::table_id(), ReservedColumnId::tsid()]);
    let valid_types = metadata
        .column_by_id(ReservedColumnId::table_id())
        .zip(metadata.column_by_id(ReservedColumnId::tsid()))
        .is_some_and(|(table_id, tsid)| {
            table_id.column_schema.data_type == ConcreteDataType::uint32_datatype()
                && tsid.column_schema.data_type == ConcreteDataType::uint64_datatype()
        });
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse && valid_prefix && valid_types,
        InvalidRequestSnafu {
            region_id: metadata.region_id,
            reason: "candidate-series scan requires sparse (__table_id, __tsid) primary keys",
        }
    );
    Ok(())
}

fn primary_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        PRIMARY_KEY_COLUMN_NAME,
        DataType::Binary,
        false,
    )]))
}

/// Filters a source by encoded-primary-key predicates and emits one binary row per local key.
fn candidate_primary_key_stream(
    mut input: BoxedRecordBatchStream,
    mut filter: Option<CachedPrimaryKeyFilter>,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let mut last_primary_key = Vec::new();
        let mut has_last = false;
        while let Some(batch) = input.try_next().await? {
            if let Some(batch) = normalize_candidate_batch(
                batch,
                filter.as_mut(),
                &mut last_primary_key,
                &mut has_last,
            )? {
                yield batch;
            }
        }
    })
}

fn normalize_candidate_batch(
    mut batch: RecordBatch,
    filter: Option<&mut CachedPrimaryKeyFilter>,
    last_primary_key: &mut Vec<u8>,
    has_last: &mut bool,
) -> Result<Option<RecordBatch>> {
    let pk_idx = batch
        .schema()
        .column_with_name(PRIMARY_KEY_COLUMN_NAME)
        .map(|(idx, _)| idx)
        .context(UnexpectedSnafu {
            reason: "candidate source does not contain __primary_key",
        })?;
    if let Some(filter) = filter {
        let Some(filtered) = prefilter_flat_batch_by_primary_key(
            batch,
            pk_idx,
            filter as &mut dyn PrimaryKeyFilter,
        )?
        else {
            return Ok(None);
        };
        batch = filtered;
    }

    let pk_column = batch.column(pk_idx);
    let mut builder = BinaryBuilder::new();
    if let Some(array) = pk_column.as_any().downcast_ref::<PrimaryKeyArray>() {
        let values = array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(UnexpectedSnafu {
                reason: "dictionary primary-key values are not binary",
            })?;
        for key in array.keys().values() {
            append_unique_primary_key(
                values.value(*key as usize),
                &mut builder,
                last_primary_key,
                has_last,
            );
        }
    } else if let Some(array) = pk_column.as_any().downcast_ref::<BinaryArray>() {
        for value in array.iter().flatten() {
            append_unique_primary_key(value, &mut builder, last_primary_key, has_last);
        }
    } else {
        return UnexpectedSnafu {
            reason: format!(
                "primary-key column is neither binary nor dictionary, got {:?}",
                pk_column.data_type()
            ),
        }
        .fail();
    }

    let array = builder.finish();
    if array.is_empty() {
        return Ok(None);
    }
    let batch = RecordBatch::try_new(primary_key_schema(), vec![Arc::new(array)])
        .context(NewRecordBatchSnafu)?;
    Ok(Some(batch))
}

fn append_unique_primary_key(
    value: &[u8],
    builder: &mut BinaryBuilder,
    last_primary_key: &mut Vec<u8>,
    has_last: &mut bool,
) {
    if !*has_last || last_primary_key != value {
        builder.append_value(value);
        last_primary_key.clear();
        last_primary_key.extend_from_slice(value);
        *has_last = true;
    }
}

fn merge_primary_key_streams(
    sources: Vec<BoxedRecordBatchStream>,
    memory_pool: Arc<dyn MemoryPool>,
    metrics_set: &ExecutionPlanMetricsSet,
    partition: usize,
    consumer_name: &'static str,
) -> Result<BoxedRecordBatchStream> {
    if sources.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    if sources.len() == 1 {
        return Ok(sources.into_iter().next().unwrap());
    }

    let schema = primary_key_schema();
    let df_sources = sources
        .into_iter()
        .map(|source| {
            let stream = source.map_err(|error| DataFusionError::External(Box::new(error)));
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream)) as _
        })
        .collect();
    let ordering = LexOrdering::new([PhysicalSortExpr {
        expr: Arc::new(Column::new(PRIMARY_KEY_COLUMN_NAME, 0)),
        options: SortOptions {
            descending: false,
            nulls_first: false,
        },
    }])
    .unwrap();
    let reservation = MemoryConsumer::new(consumer_name).register(&memory_pool);
    let mut merged = StreamingMergeBuilder::new()
        .with_streams(df_sources)
        .with_schema(schema)
        .with_expressions(&ordering)
        .with_metrics(BaselineMetrics::new(metrics_set, partition))
        .with_batch_size(DEFAULT_READ_BATCH_SIZE)
        .with_reservation(reservation)
        .build()
        .context(MergeCandidateSeriesSnafu)?;

    Ok(Box::pin(try_stream! {
        while let Some(batch) = merged.next().await {
            yield batch.context(MergeCandidateSeriesSnafu)?;
        }
    }))
}

fn decode_metric_series(
    mut input: BoxedRecordBatchStream,
    metadata: store_api::metadata::RegionMetadataRef,
) -> Result<MetricSeriesIdStream> {
    let codec = SparsePrimaryKeyCodec::new(&metadata);
    Ok(Box::pin(try_stream! {
        let mut last_primary_key = Vec::new();
        let mut last_series = None;
        let mut output = Vec::with_capacity(CANDIDATE_SERIES_BATCH_SIZE);
        while let Some(batch) = input.try_next().await? {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(UnexpectedSnafu {
                    reason: "merged candidate primary key is not binary",
                })?;
            for primary_key in array.iter().flatten() {
                if last_primary_key == primary_key {
                    continue;
                }
                last_primary_key.clear();
                last_primary_key.extend_from_slice(primary_key);

                let (table_id, tsid) = codec.decode_internal(primary_key).context(crate::error::DecodeSnafu)?;
                let series = MetricSeriesId { table_id, tsid };
                if last_series == Some(series) {
                    continue;
                }
                last_series = Some(series);
                output.push(series);
                if output.len() == CANDIDATE_SERIES_BATCH_SIZE {
                    yield std::mem::replace(
                        &mut output,
                        Vec::with_capacity(CANDIDATE_SERIES_BATCH_SIZE),
                    );
                }
            }
        }
        if !output.is_empty() {
            yield output;
        }
    }))
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{ArrayRef, DictionaryArray, UInt32Array};
    use datatypes::arrow::datatypes::UInt32Type;
    use futures::TryStreamExt;
    use store_api::codec::PrimaryKeyEncoding;
    use table::predicate::Predicate;

    use super::*;
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;

    fn binary_batch(values: &[&[u8]]) -> RecordBatch {
        RecordBatch::try_new(
            primary_key_schema(),
            vec![Arc::new(BinaryArray::from_iter_values(
                values.iter().copied(),
            ))],
        )
        .unwrap()
    }

    fn dictionary_batch(values: &[&[u8]], keys: &[u32]) -> RecordBatch {
        let dict_values: ArrayRef = Arc::new(BinaryArray::from_iter_values(values.iter().copied()));
        let dict =
            DictionaryArray::<UInt32Type>::try_new(UInt32Array::from(keys.to_vec()), dict_values)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            DataType::UInt32,
            DataType::Binary,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap()
    }

    #[tokio::test]
    async fn candidate_stream_normalizes_and_deduplicates_primary_keys() {
        let input = Box::pin(futures::stream::iter(vec![
            Ok(dictionary_batch(&[b"a", b"b"], &[0, 0, 1])),
            Ok(binary_batch(&[b"b", b"c", b"c"])),
        ]));
        let batches = candidate_primary_key_stream(input, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let actual = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[tokio::test]
    async fn candidate_stream_filters_primary_keys_before_merge() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut table_1 = Vec::new();
        let mut table_2 = Vec::new();
        codec.encode_internal(1, 10, &mut table_1).unwrap();
        codec.encode_internal(2, 20, &mut table_2).unwrap();

        let predicate = Predicate::new(vec![
            col(store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME).eq(lit(1_u32)),
        ]);
        let filter = build_primary_key_filter(&metadata, Some(&predicate));
        let input = Box::pin(futures::stream::iter(vec![Ok(dictionary_batch(
            &[table_1.as_slice(), table_2.as_slice()],
            &[0, 1],
        ))]));
        let batches = candidate_primary_key_stream(input, filter)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(array.len(), 1);
        assert_eq!(array.value(0), table_1);
    }

    #[tokio::test]
    async fn decode_metric_series_yields_groups_of_500() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let primary_keys = (0..501_u64)
            .map(|tsid| {
                let mut primary_key = Vec::new();
                codec.encode_internal(1, tsid, &mut primary_key).unwrap();
                primary_key
            })
            .collect::<Vec<_>>();
        let batch = binary_batch(&primary_keys.iter().map(Vec::as_slice).collect::<Vec<_>>());
        let source = Box::pin(futures::stream::iter(vec![Ok(batch)]));
        let metrics = ExecutionPlanMetricsSet::new();
        let pool = Arc::new(UnboundedMemoryPool::default());
        let merged =
            merge_primary_key_streams(vec![source], pool, &metrics, 0, "candidate-test").unwrap();
        let groups = decode_metric_series(merged, metadata)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), [500, 1]);
        assert_eq!(
            groups[0][0],
            MetricSeriesId {
                table_id: 1,
                tsid: 0
            }
        );
        assert_eq!(
            groups[1][0],
            MetricSeriesId {
                table_id: 1,
                tsid: 500
            }
        );
    }

    #[tokio::test]
    async fn merge_primary_keys_globally_sorts_and_deduplicates_series() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let encode = |tsid| {
            let mut primary_key = Vec::new();
            codec.encode_internal(1, tsid, &mut primary_key).unwrap();
            primary_key
        };
        let keys_1 = [encode(1), encode(3)];
        let keys_2 = [encode(1), encode(2)];
        let sources = vec![
            Box::pin(futures::stream::iter(vec![Ok(binary_batch(
                &keys_1.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            ))])) as BoxedRecordBatchStream,
            Box::pin(futures::stream::iter(vec![Ok(binary_batch(
                &keys_2.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            ))])),
        ];

        let merged = merge_primary_key_streams(
            sources,
            Arc::new(UnboundedMemoryPool::default()),
            &ExecutionPlanMetricsSet::new(),
            0,
            "candidate-merge-test",
        )
        .unwrap();
        let groups = decode_metric_series(merged, metadata)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            groups,
            vec![vec![
                MetricSeriesId {
                    table_id: 1,
                    tsid: 1,
                },
                MetricSeriesId {
                    table_id: 1,
                    tsid: 2,
                },
                MetricSeriesId {
                    table_id: 1,
                    tsid: 3,
                },
            ]]
        );
    }
}
