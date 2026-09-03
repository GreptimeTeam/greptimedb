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

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use async_stream::try_stream;
use bytes::Bytes;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{Expr, col, lit};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt32Array, UInt64Array};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use datatypes::value::timestamp_to_scalar_value;
use object_store::ObjectStore;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use table::predicate::Predicate;

use crate::error::{
    InvalidRecordBatchSnafu, OpenDalSnafu, ReadParquetSnafu, RecordBatchSnafu, Result,
    UnexpectedSnafu,
};
use crate::series_index::{
    MAX_TS_COLUMN, METRIC_SERIES_ID_BATCH_SIZE, MIN_TS_COLUMN, MetricSeriesId,
    MetricSeriesIdStream, ROW_COUNT_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN, series_index_schema,
};
use crate::sst::parquet::format::{column_null_counts, column_values_by_type};
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::prefilter::simple_tag_filters;
use crate::sst::parquet::reader::MetadataCacheMetrics;

#[derive(Clone)]
struct SeriesIndexRangeFetcher {
    object_store: ObjectStore,
}

impl SeriesIndexRangeFetcher {
    async fn fetch(&self, path: &str, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        fetch_byte_ranges(path, self.object_store.clone(), ranges)
            .await
            .context(OpenDalSnafu)
    }
}

#[derive(Clone)]
struct SeriesIndexMetadataProvider {
    object_store: ObjectStore,
}

impl SeriesIndexMetadataProvider {
    async fn load(&self, path: &str) -> Result<Arc<ParquetMetaData>> {
        let mut metrics = MetadataCacheMetrics::default();
        MetadataLoader::new(self.object_store.clone(), path, 0)
            .load(&mut metrics)
            .await
            .map(Arc::new)
    }
}

/// Searches a series-index file for metric series matching query predicates.
pub struct SeriesIndexSearcher {
    range_fetcher: SeriesIndexRangeFetcher,
    metadata_provider: SeriesIndexMetadataProvider,
    filters: Vec<(Expr, SimpleFilterEvaluator)>,
    time_range: Option<TimestampRange>,
    empty_time_range: bool,
}

impl SeriesIndexSearcher {
    /// Creates a searcher reusable across series-index files of `metadata`.
    /// Time-range predicates are built per file from the unit recorded in each
    /// file's schema, so files written before a time index unit widening keep
    /// being interpreted in their own unit.
    pub fn try_new(
        metadata: RegionMetadataRef,
        object_store: ObjectStore,
        predicate: Option<&Predicate>,
        time_range: Option<TimestampRange>,
    ) -> Result<Self> {
        // Keep search-time metadata validation identical to the writer.
        series_index_schema(&metadata)?;

        let filters = simple_tag_filters(&metadata, None, predicate);
        let empty_time_range = time_range.as_ref().is_some_and(TimestampRange::is_empty);

        Ok(Self {
            range_fetcher: SeriesIndexRangeFetcher {
                object_store: object_store.clone(),
            },
            metadata_provider: SeriesIndexMetadataProvider { object_store },
            filters,
            time_range,
            empty_time_range,
        })
    }

    /// Searches `path` and returns sorted batches of matching metric-series IDs.
    pub async fn search(&self, path: &str) -> Result<MetricSeriesIdStream> {
        if self.empty_time_range {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let parquet_metadata = self.metadata_provider.load(path).await?;
        let arrow_metadata =
            ArrowReaderMetadata::try_new(parquet_metadata, ArrowReaderOptions::new())
                .with_context(|_| ReadParquetSnafu {
                    path: path.to_string(),
                })?;
        let unit = validate_index_schema(arrow_metadata.schema())?;

        // An older index file may not contain tags added by schema evolution.
        // Ignore filters on those tags to preserve a conservative candidate set.
        let mut filters = self.filters.clone();
        for expr in time_range_exprs(unit, self.time_range.as_ref()) {
            let filter = SimpleFilterEvaluator::try_new(&expr).context(UnexpectedSnafu {
                reason: "failed to build an internal series-index time filter",
            })?;
            filters.push((expr, filter));
        }
        let (pruning_predicate, filters) = filters_for_schema(arrow_metadata.schema(), &filters);
        let row_groups = row_groups_to_read(
            arrow_metadata.metadata().row_groups(),
            arrow_metadata.schema().clone(),
            &pruning_predicate,
        );
        let projection = projection_mask(
            arrow_metadata.parquet_schema(),
            arrow_metadata.schema(),
            &filters,
        )?;
        let mut decoder = ParquetPushDecoderBuilder::new_with_metadata(arrow_metadata)
            .with_row_groups(row_groups)
            .with_projection(projection)
            .build()
            .with_context(|_| ReadParquetSnafu {
                path: path.to_string(),
            })?;
        let path = path.to_string();
        let range_fetcher = self.range_fetcher.clone();

        Ok(Box::pin(try_stream! {
            let mut last_series = None;
            let mut output = Vec::with_capacity(METRIC_SERIES_ID_BATCH_SIZE);
            loop {
                let batch = match decoder
                    .try_decode()
                    .with_context(|_| ReadParquetSnafu { path: path.clone() })?
                {
                    DecodeResult::NeedsData(ranges) => {
                        let data = range_fetcher.fetch(&path, &ranges).await?;
                        decoder
                            .push_ranges(ranges, data)
                            .with_context(|_| ReadParquetSnafu { path: path.clone() })?;
                        continue;
                    }
                    DecodeResult::Data(batch) => batch,
                    DecodeResult::Finished => break,
                };

                let mut mask = BooleanBuffer::new_set(batch.num_rows());
                for filter in &filters {
                    let column = column(&batch, filter.column_name())?;
                    let evaluated = filter.evaluate_array(column).context(RecordBatchSnafu)?;
                    mask = &mask & &evaluated;
                }

                let table_ids = column(&batch, TABLE_ID_COLUMN)?
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .context(InvalidRecordBatchSnafu {
                        reason: "series index __table_id is not UInt32",
                    })?;
                let tsids = column(&batch, TSID_COLUMN)?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .context(InvalidRecordBatchSnafu {
                        reason: "series index __tsid is not UInt64",
                    })?;

                for (row, matched) in mask.iter().enumerate() {
                    if !matched {
                        continue;
                    }
                    let series = MetricSeriesId {
                        table_id: table_ids.value(row),
                        tsid: tsids.value(row),
                    };
                    if last_series == Some(series) {
                        continue;
                    }
                    last_series = Some(series);
                    output.push(series);
                    if output.len() == METRIC_SERIES_ID_BATCH_SIZE {
                        yield std::mem::replace(
                            &mut output,
                            Vec::with_capacity(METRIC_SERIES_ID_BATCH_SIZE),
                        );
                    }
                }
            }
            if !output.is_empty() {
                yield output;
            }
        }))
    }
}

fn filters_for_schema(
    schema: &SchemaRef,
    filters: &[(Expr, SimpleFilterEvaluator)],
) -> (Predicate, Vec<SimpleFilterEvaluator>) {
    let (exprs, filters): (Vec<_>, Vec<_>) = filters
        .iter()
        .filter(|(_, filter)| schema.field_with_name(filter.column_name()).is_ok())
        .cloned()
        .unzip();
    (Predicate::new(exprs), filters)
}

// Builds `__series_min_ts`/`__series_max_ts` predicates in `unit`, the unit
// recorded in the file being searched: its raw i64 bounds were written in that
// unit even if the region's time index has since been widened.
fn time_range_exprs(unit: TimeUnit, time_range: Option<&TimestampRange>) -> Vec<Expr> {
    let Some(time_range) = time_range else {
        return Vec::new();
    };
    if time_range.is_empty() {
        return Vec::new();
    }
    let mut exprs = Vec::with_capacity(2);
    // A series overlaps [start, end) only if its maximum is at least start.
    // Round start up so a series ending before an unaligned start is pruned.
    if let Some(start) = time_range
        .start()
        .and_then(|start| start.convert_to_ceil(unit))
    {
        exprs.push(
            col(MAX_TS_COLUMN).gt_eq(lit(timestamp_to_scalar_value(unit, Some(start.value())))),
        );
    }
    // A series overlaps [start, end) only if its minimum is less than end.
    // Round the exclusive end up to avoid pruning the containing unit interval.
    if let Some(end) = time_range.end().and_then(|end| end.convert_to_ceil(unit)) {
        exprs.push(col(MIN_TS_COLUMN).lt(lit(timestamp_to_scalar_value(unit, Some(end.value())))));
    }
    exprs
}

/// Validates an index file's schema and returns the time unit of its
/// `__series_min_ts`/`__series_max_ts` columns. They are native
/// `Timestamp(unit)` columns, so the unit is part of the datatype and each
/// file is interpreted in the unit it was written with.
fn validate_index_schema(schema: &SchemaRef) -> Result<TimeUnit> {
    for (name, data_type) in [
        (ROW_COUNT_COLUMN, DataType::UInt64),
        (TABLE_ID_COLUMN, DataType::UInt32),
        (TSID_COLUMN, DataType::UInt64),
    ] {
        let field = schema
            .field_with_name(name)
            .ok()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("series index is missing internal column {name}"),
            })?;
        ensure!(
            field.data_type() == &data_type && !field.is_nullable(),
            InvalidRecordBatchSnafu {
                reason: format!(
                    "series index internal column {name} must be non-nullable {data_type:?}, got {:?}",
                    field.data_type()
                ),
            }
        );
    }
    let unit = |name: &str| {
        let field = schema
            .field_with_name(name)
            .ok()
            .context(InvalidRecordBatchSnafu {
                reason: format!("series index is missing internal column {name}"),
            })?;
        ensure!(
            !field.is_nullable(),
            InvalidRecordBatchSnafu {
                reason: format!("series index column {name} must be non-nullable"),
            }
        );
        match field.data_type() {
            DataType::Timestamp(unit, _) => Ok(unit.into()),
            data_type => InvalidRecordBatchSnafu {
                reason: format!(
                    "series index column {name} must be a Timestamp, got {data_type:?}"
                ),
            }
            .fail(),
        }
    };
    let min_unit = unit(MIN_TS_COLUMN)?;
    let max_unit = unit(MAX_TS_COLUMN)?;
    ensure!(
        min_unit == max_unit,
        InvalidRecordBatchSnafu {
            reason: format!(
                "series index columns {MIN_TS_COLUMN} and {MAX_TS_COLUMN} have different time units"
            ),
        }
    );
    Ok(min_unit)
}

fn projection_mask(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    arrow_schema: &SchemaRef,
    filters: &[SimpleFilterEvaluator],
) -> Result<ProjectionMask> {
    let mut indices = HashSet::new();
    for name in [TABLE_ID_COLUMN, TSID_COLUMN] {
        let index = arrow_schema
            .index_of(name)
            .ok()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("series index is missing internal column {name}"),
            })?;
        indices.insert(index);
    }
    for filter in filters {
        let index = arrow_schema
            .index_of(filter.column_name())
            .ok()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "series index is missing predicate column {}",
                    filter.column_name()
                ),
            })?;
        indices.insert(index);
    }
    Ok(ProjectionMask::roots(parquet_schema, indices))
}

fn column<'a>(
    batch: &'a datatypes::arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<&'a ArrayRef> {
    let index = batch
        .schema()
        .index_of(name)
        .ok()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!("series index batch is missing column {name}"),
        })?;
    Ok(batch.column(index))
}

struct SeriesIndexPruningStats<'a> {
    row_groups: &'a [RowGroupMetaData],
    schema: SchemaRef,
}

impl PruningStatistics for SeriesIndexPruningStats<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.column_values(column, true)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.column_values(column, false)
    }

    fn num_containers(&self) -> usize {
        self.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_index = self.schema.index_of(&column.name).ok()?;
        column_null_counts(self.row_groups, column_index)
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

impl SeriesIndexPruningStats<'_> {
    fn column_values(&self, column: &Column, is_min: bool) -> Option<ArrayRef> {
        let column_index = self.schema.index_of(&column.name).ok()?;
        let data_type = self.schema.field(column_index).data_type();
        let values = column_values_by_type(self.row_groups, data_type, column_index, is_min)?;
        if values.data_type() == data_type {
            Some(values)
        } else {
            // Parquet timestamp statistics surface as raw Int64; reinterpret
            // them in the column's Timestamp type so pruning compares
            // like-typed values. A cast failure yields no stats and keeps the
            // row group (conservative).
            datatypes::arrow::compute::cast(&values, data_type).ok()
        }
    }
}

fn row_groups_to_read(
    row_groups: &[RowGroupMetaData],
    schema: SchemaRef,
    predicate: &Predicate,
) -> Vec<usize> {
    let stats = SeriesIndexPruningStats { row_groups, schema };
    predicate
        .prune_with_stats(&stats, &stats.schema)
        .into_iter()
        .enumerate()
        .filter_map(|(row_group, keep)| keep.then_some(row_group))
        .collect()
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{
        BinaryArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    };
    use datatypes::arrow::datatypes::{Field, Schema, TimeUnit as ArrowTimeUnit};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use futures::TryStreamExt;
    use object_store::services::Memory;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;
    use crate::series_index::{SeriesIndexWriter, SeriesIndexWriterOptions};
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn flat_batch_with_time_unit(
        primary_keys: &[Vec<u8>],
        timestamps: &[i64],
        unit: ArrowTimeUnit,
    ) -> RecordBatch {
        let ts_column = match unit {
            ArrowTimeUnit::Second => {
                Arc::new(TimestampSecondArray::from(timestamps.to_vec())) as ArrayRef
            }
            ArrowTimeUnit::Millisecond => {
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())) as ArrayRef
            }
            ArrowTimeUnit::Microsecond => {
                Arc::new(TimestampMicrosecondArray::from(timestamps.to_vec())) as ArrayRef
            }
            ArrowTimeUnit::Nanosecond => {
                Arc::new(TimestampNanosecondArray::from(timestamps.to_vec())) as ArrayRef
            }
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(unit, None), false),
            Field::new("__primary_key", DataType::Binary, false),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                ts_column,
                Arc::new(BinaryArray::from_iter_values(
                    primary_keys.iter().map(Vec::as_slice),
                )),
                Arc::new(UInt64Array::from(vec![1; timestamps.len()])),
                Arc::new(UInt8Array::from(vec![0; timestamps.len()])),
            ],
        )
        .unwrap()
    }

    async fn write_index(
        metadata: RegionMetadataRef,
        object_store: ObjectStore,
        path: &str,
        rows: &[(u32, u64, &str, &str, i64)],
        row_group_size: usize,
    ) {
        write_index_with_time_unit(
            metadata,
            object_store,
            path,
            rows,
            row_group_size,
            ArrowTimeUnit::Millisecond,
        )
        .await
    }

    async fn write_index_with_time_unit(
        metadata: RegionMetadataRef,
        object_store: ObjectStore,
        path: &str,
        rows: &[(u32, u64, &str, &str, i64)],
        row_group_size: usize,
        unit: ArrowTimeUnit,
    ) {
        let primary_keys = rows
            .iter()
            .map(|(table_id, tsid, tag_0, tag_1, _)| {
                new_sparse_primary_key(&[*tag_0, *tag_1], &metadata, *table_id, *tsid)
            })
            .collect::<Vec<_>>();
        let timestamps = rows.iter().map(|row| row.4).collect::<Vec<_>>();
        let mut writer = SeriesIndexWriter::try_new(
            metadata,
            object_store,
            path,
            SeriesIndexWriterOptions { row_group_size },
        )
        .await
        .unwrap();
        writer
            .write(&flat_batch_with_time_unit(&primary_keys, &timestamps, unit))
            .await
            .unwrap();
        writer.finish().await.unwrap();
    }

    async fn collect_ids(stream: MetricSeriesIdStream) -> Vec<MetricSeriesId> {
        stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    #[tokio::test]
    async fn search_applies_candidate_tag_filters_and_time_overlap() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let object_store = object_store();
        let path = "search.parquet";
        write_index(
            metadata.clone(),
            object_store.clone(),
            path,
            &[
                (1, 10, "a", "x", 10),
                (1, 20, "b", "x", 20),
                (1, 30, "a", "y", 30),
            ],
            2,
        )
        .await;

        // The field filter is not available in the series index and is ignored,
        // matching candidate-primary-key filter behavior.
        let predicate = Predicate::new(vec![
            col("tag_0").eq(lit("a")),
            col("field_0").gt(lit(0_u64)),
        ]);
        let time_range = TimestampRange::new(
            common_time::Timestamp::new_millisecond(20),
            common_time::Timestamp::new_millisecond(31),
        )
        .unwrap();
        let searcher = SeriesIndexSearcher::try_new(
            metadata.clone(),
            object_store.clone(),
            Some(&predicate),
            Some(time_range),
        )
        .unwrap();
        let ids = collect_ids(searcher.search(path).await.unwrap()).await;
        assert_eq!(
            ids,
            vec![MetricSeriesId {
                table_id: 1,
                tsid: 30
            }]
        );

        // Both bounds fall between millisecond ticks. Rounding the inclusive
        // start and exclusive end upward leaves only the 30 ms series.
        let time_range = TimestampRange::new(
            common_time::Timestamp::new_microsecond(20_001),
            common_time::Timestamp::new_microsecond(30_001),
        )
        .unwrap();
        let searcher = SeriesIndexSearcher::try_new(
            metadata.clone(),
            object_store.clone(),
            None,
            Some(time_range),
        )
        .unwrap();
        let ids = collect_ids(searcher.search(path).await.unwrap()).await;
        assert_eq!(
            ids,
            vec![MetricSeriesId {
                table_id: 1,
                tsid: 30
            }]
        );

        // The stored maximum equal to the query start intersects, while the
        // stored minimum equal to the exclusive query end does not.
        let time_range = TimestampRange::new(
            common_time::Timestamp::new_millisecond(20),
            common_time::Timestamp::new_millisecond(30),
        )
        .unwrap();
        let searcher =
            SeriesIndexSearcher::try_new(metadata, object_store, None, Some(time_range)).unwrap();
        let ids = collect_ids(searcher.search(path).await.unwrap()).await;
        assert_eq!(
            ids,
            vec![MetricSeriesId {
                table_id: 1,
                tsid: 20
            }]
        );
    }

    #[tokio::test]
    async fn search_skips_filters_for_columns_missing_from_older_index() {
        let old_metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let object_store = object_store();
        let path = "schema-evolution.parquet";
        write_index(
            old_metadata.clone(),
            object_store.clone(),
            path,
            &[
                (1, 10, "a", "x", 10),
                (1, 20, "b", "x", 20),
                (1, 30, "a", "y", 30),
            ],
            2,
        )
        .await;

        let mut builder = RegionMetadataBuilder::from_existing(old_metadata.as_ref().clone());
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("tag_2", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 4,
        });
        let mut primary_key = old_metadata.primary_key.clone();
        primary_key.push(4);
        builder.primary_key(primary_key);
        let current_metadata = Arc::new(builder.build().unwrap());

        let predicate =
            Predicate::new(vec![col("tag_0").eq(lit("a")), col("tag_2").eq(lit("new"))]);
        let searcher =
            SeriesIndexSearcher::try_new(current_metadata, object_store, Some(&predicate), None)
                .unwrap();
        let ids = collect_ids(searcher.search(path).await.unwrap()).await;
        assert_eq!(
            ids,
            vec![
                MetricSeriesId {
                    table_id: 1,
                    tsid: 10,
                },
                MetricSeriesId {
                    table_id: 1,
                    tsid: 30,
                },
            ]
        );
    }

    #[tokio::test]
    async fn search_uses_file_time_unit_after_time_index_widen() {
        // The index is written while the region's time index is milliseconds.
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let object_store = object_store();
        let path = "widen.parquet";
        write_index(
            metadata.clone(),
            object_store.clone(),
            path,
            &[
                (1, 10, "a", "x", 10),
                (1, 20, "b", "x", 20),
                (1, 30, "c", "x", 30),
            ],
            2,
        )
        .await;

        // The region's time index is then widened to microseconds.
        let mut widened = (*metadata).clone();
        for column in &mut widened.column_metadatas {
            if column.column_schema.name == "ts" {
                column.column_schema.data_type = ConcreteDataType::timestamp_microsecond_datatype();
            }
        }
        let widened = Arc::new(widened);

        // A query range of [10.001ms, 25ms) expressed in microseconds must be
        // compared against the file's millisecond bounds (ceil: [11ms,
        // 25ms)): only the 20ms series intersects. Interpreting the file in
        // microseconds instead would match nothing.
        let time_range = TimestampRange::new(
            common_time::Timestamp::new_microsecond(10_001),
            common_time::Timestamp::new_microsecond(25_000),
        )
        .unwrap();
        let searcher =
            SeriesIndexSearcher::try_new(widened, object_store.clone(), None, Some(time_range))
                .unwrap();
        let ids = collect_ids(searcher.search(path).await.unwrap()).await;
        assert_eq!(
            ids,
            vec![MetricSeriesId {
                table_id: 1,
                tsid: 20
            }]
        );

        // A millisecond-unit range behaves identically to the pre-widen
        // searcher: [20ms, 30ms) keeps only the 20ms series.
        let time_range = TimestampRange::new(
            common_time::Timestamp::new_millisecond(20),
            common_time::Timestamp::new_millisecond(30),
        )
        .unwrap();
        let searcher =
            SeriesIndexSearcher::try_new(metadata, object_store, None, Some(time_range)).unwrap();
        let ids = collect_ids(searcher.search(path).await.unwrap()).await;
        assert_eq!(
            ids,
            vec![MetricSeriesId {
                table_id: 1,
                tsid: 20
            }]
        );
    }

    #[tokio::test]
    async fn search_reads_each_file_in_its_recorded_unit() {
        // The first file is written while the region's time index is
        // milliseconds; its series sit at 10ms and 20ms.
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let object_store = object_store();
        write_index(
            metadata.clone(),
            object_store.clone(),
            "old_ms.parquet",
            &[(1, 10, "a", "x", 10), (1, 20, "b", "x", 20)],
            2,
        )
        .await;

        // The region's time index is then widened to microseconds and a
        // second file is written; its series sit at 15_000µs and 15µs.
        let mut widened = (*metadata).clone();
        for column in &mut widened.column_metadatas {
            if column.column_schema.name == "ts" {
                column.column_schema.data_type = ConcreteDataType::timestamp_microsecond_datatype();
            }
        }
        let widened = Arc::new(widened);
        write_index_with_time_unit(
            widened.clone(),
            object_store.clone(),
            "new_us.parquet",
            &[(1, 30, "c", "x", 15_000), (1, 40, "d", "x", 15)],
            2,
            ArrowTimeUnit::Microsecond,
        )
        .await;

        // One searcher over the widened region must interpret each file in
        // the unit it was written with. For [10.5ms, 25ms):
        // - the millisecond file (ceil: [11ms, 25ms)) keeps only the 20ms
        //   series; reading it in microseconds would match nothing;
        // - the microsecond file keeps only the 15_000µs series; reading it
        //   in milliseconds would also keep the 15µs series.
        let time_range = TimestampRange::new(
            common_time::Timestamp::new_microsecond(10_500),
            common_time::Timestamp::new_microsecond(25_000),
        )
        .unwrap();
        let searcher =
            SeriesIndexSearcher::try_new(widened, object_store, None, Some(time_range)).unwrap();
        let ids = collect_ids(searcher.search("old_ms.parquet").await.unwrap()).await;
        assert_eq!(
            ids,
            vec![MetricSeriesId {
                table_id: 1,
                tsid: 20
            }]
        );
        let ids = collect_ids(searcher.search("new_us.parquet").await.unwrap()).await;
        assert_eq!(
            ids,
            vec![MetricSeriesId {
                table_id: 1,
                tsid: 30
            }]
        );
    }

    fn ts_field(name: &str, unit: Option<ArrowTimeUnit>) -> Field {
        let data_type = match unit {
            Some(unit) => DataType::Timestamp(unit, None),
            None => DataType::Int64,
        };
        Field::new(name, data_type, false)
    }

    fn index_file_schema(
        min_unit: Option<ArrowTimeUnit>,
        max_unit: Option<ArrowTimeUnit>,
    ) -> SchemaRef {
        Arc::new(Schema::new(vec![
            ts_field(MIN_TS_COLUMN, min_unit),
            ts_field(MAX_TS_COLUMN, max_unit),
            Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
            Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
            Field::new(TSID_COLUMN, DataType::UInt64, false),
        ]))
    }

    #[test]
    fn validate_index_schema_rejects_unusable_columns() {
        // Min/max columns that are not Timestamps are rejected.
        let err = validate_index_schema(&index_file_schema(None, None))
            .unwrap_err()
            .to_string();
        assert!(err.contains("must be a Timestamp, got Int64"), "{err}");

        // The min and max columns must agree on the unit.
        let err = validate_index_schema(&index_file_schema(
            Some(ArrowTimeUnit::Millisecond),
            Some(ArrowTimeUnit::Microsecond),
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("have different time units"), "{err}");

        assert_eq!(
            TimeUnit::Nanosecond,
            validate_index_schema(&index_file_schema(
                Some(ArrowTimeUnit::Nanosecond),
                Some(ArrowTimeUnit::Nanosecond),
            ))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn search_streams_fixed_size_batches() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let object_store = object_store();
        let rows = (0..501_u64)
            .map(|tsid| (1, tsid, "a", "x", tsid as i64))
            .collect::<Vec<_>>();
        write_index(
            metadata.clone(),
            object_store.clone(),
            "batching.parquet",
            &rows,
            100,
        )
        .await;

        let searcher = SeriesIndexSearcher::try_new(metadata, object_store, None, None).unwrap();
        let batches = searcher
            .search("batching.parquet")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [500, 1]);
        assert_eq!(
            batches[0][0],
            MetricSeriesId {
                table_id: 1,
                tsid: 0
            }
        );
        assert_eq!(
            batches[1][0],
            MetricSeriesId {
                table_id: 1,
                tsid: 500
            }
        );
    }

    #[tokio::test]
    async fn search_prunes_row_groups_and_empty_ranges() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let object_store = object_store();
        let path = "pruning.parquet";
        write_index(
            metadata.clone(),
            object_store.clone(),
            path,
            &[
                (1, 0, "a", "x", 0),
                (1, 1, "b", "x", 1),
                (1, 2, "m", "x", 2),
                (1, 3, "m", "x", 3),
                (1, 4, "y", "x", 4),
                (1, 5, "z", "x", 5),
            ],
            2,
        )
        .await;

        let predicate = Predicate::new(vec![col("tag_0").eq(lit("m"))]);
        let searcher = SeriesIndexSearcher::try_new(
            metadata.clone(),
            object_store.clone(),
            Some(&predicate),
            None,
        )
        .unwrap();
        let parquet_metadata = searcher.metadata_provider.load(path).await.unwrap();
        let arrow_metadata =
            ArrowReaderMetadata::try_new(parquet_metadata, ArrowReaderOptions::new()).unwrap();
        let (pruning_predicate, _) = filters_for_schema(arrow_metadata.schema(), &searcher.filters);
        assert_eq!(
            row_groups_to_read(
                arrow_metadata.metadata().row_groups(),
                arrow_metadata.schema().clone(),
                &pruning_predicate,
            ),
            vec![1]
        );

        // Time-range predicates prune row groups in the file's own unit:
        // [2ms, 4ms) keeps only the row group holding the 2ms and 3ms series.
        let time_range = TimestampRange::new(
            common_time::Timestamp::new_millisecond(2),
            common_time::Timestamp::new_millisecond(4),
        )
        .unwrap();
        let unit = validate_index_schema(arrow_metadata.schema()).unwrap();
        let filters = searcher
            .filters
            .clone()
            .into_iter()
            .chain(
                time_range_exprs(unit, Some(&time_range))
                    .into_iter()
                    .map(|expr| {
                        (
                            expr.clone(),
                            SimpleFilterEvaluator::try_new(&expr)
                                .context(UnexpectedSnafu {
                                    reason: "failed to build an internal series-index time filter",
                                })
                                .unwrap(),
                        )
                    }),
            )
            .collect::<Vec<_>>();
        let (pruning_predicate, _) = filters_for_schema(arrow_metadata.schema(), &filters);
        assert_eq!(
            row_groups_to_read(
                arrow_metadata.metadata().row_groups(),
                arrow_metadata.schema().clone(),
                &pruning_predicate,
            ),
            vec![1]
        );

        let empty = SeriesIndexSearcher::try_new(
            metadata,
            object_store,
            None,
            Some(TimestampRange::empty()),
        )
        .unwrap();
        assert!(
            empty
                .search("does-not-need-to-exist.parquet")
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }
}
