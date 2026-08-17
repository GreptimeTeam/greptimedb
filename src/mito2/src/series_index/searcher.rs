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
use std::sync::Arc;

use api::v1::SemanticType;
use async_stream::try_stream;
use common_datasource::file_format::parquet::LazyParquetFileReader;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::range::TimestampRange;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{Expr, col, lit};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt32Array, UInt64Array};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use table::predicate::Predicate;

use crate::error::{
    InvalidMetaSnafu, InvalidRecordBatchSnafu, ReadParquetSnafu, RecordBatchSnafu, Result,
    UnexpectedSnafu,
};
use crate::series_index::{
    MAX_TS_COLUMN, METRIC_SERIES_ID_BATCH_SIZE, MIN_TS_COLUMN, MetricSeriesId,
    MetricSeriesIdStream, ROW_COUNT_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN, series_index_schema,
};
use crate::sst::parquet::prefilter::simple_tag_filters;

/// Searches a series-index file for metric series matching query predicates.
pub struct SeriesIndexSearcher {
    object_store: ObjectStore,
    filters: Vec<SimpleFilterEvaluator>,
    pruning_predicate: Predicate,
    empty_time_range: bool,
}

impl SeriesIndexSearcher {
    /// Creates a searcher reusable across series-index files of `metadata`.
    pub fn try_new(
        metadata: RegionMetadataRef,
        object_store: ObjectStore,
        predicate: Option<&Predicate>,
        time_range: Option<TimestampRange>,
    ) -> Result<Self> {
        // Keep search-time metadata validation identical to the writer.
        series_index_schema(&metadata)?;

        let mut filters = simple_tag_filters(&metadata, None, predicate);
        let (empty_time_range, time_exprs) = time_range_filters(&metadata, time_range)?;
        for expr in time_exprs {
            let filter = SimpleFilterEvaluator::try_new(&expr).context(UnexpectedSnafu {
                reason: "failed to build an internal series-index time filter",
            })?;
            filters.push((expr, filter));
        }
        let (exprs, filters): (Vec<_>, Vec<_>) = filters.into_iter().unzip();

        Ok(Self {
            object_store,
            filters,
            pruning_predicate: Predicate::new(exprs),
            empty_time_range,
        })
    }

    /// Searches `path` and returns sorted batches of matching metric-series IDs.
    pub async fn search(&self, path: &str) -> Result<MetricSeriesIdStream> {
        if self.empty_time_range {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let reader = LazyParquetFileReader::new(self.object_store.clone(), path.to_string(), None);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .with_context(|_| ReadParquetSnafu {
                path: path.to_string(),
            })?;
        validate_index_schema(builder.schema())?;

        let row_groups = row_groups_to_read(
            builder.metadata().row_groups(),
            builder.schema().clone(),
            &self.pruning_predicate,
        );
        let projection =
            projection_mask(builder.parquet_schema(), builder.schema(), &self.filters)?;
        let mut input = builder
            .with_row_groups(row_groups)
            .with_projection(projection)
            .build()
            .with_context(|_| ReadParquetSnafu {
                path: path.to_string(),
            })?;
        let path = path.to_string();
        let filters = self.filters.clone();

        Ok(Box::pin(try_stream! {
            let mut last_series = None;
            let mut output = Vec::with_capacity(METRIC_SERIES_ID_BATCH_SIZE);
            while let Some(batch) = input
                .try_next()
                .await
                .with_context(|_| ReadParquetSnafu { path: path.clone() })?
            {
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

fn time_range_filters(
    metadata: &RegionMetadataRef,
    time_range: Option<TimestampRange>,
) -> Result<(bool, Vec<Expr>)> {
    let Some(time_range) = time_range else {
        return Ok((false, Vec::new()));
    };
    if time_range.is_empty() {
        return Ok((true, Vec::new()));
    }

    let time_index = metadata.time_index_column();
    ensure!(
        time_index.semantic_type == SemanticType::Timestamp,
        InvalidMetaSnafu {
            reason: "series index metadata has no timestamp time index",
        }
    );
    let timestamp_type =
        time_index
            .column_schema
            .data_type
            .as_timestamp()
            .context(InvalidMetaSnafu {
                reason: "series index time index is not a timestamp",
            })?;
    let unit = timestamp_type.unit();
    let mut exprs = Vec::with_capacity(2);
    if let Some(start) = time_range
        .start()
        .and_then(|start| start.convert_to_ceil(unit))
    {
        exprs.push(col(MAX_TS_COLUMN).gt_eq(lit(start.value())));
    }
    if let Some(end) = time_range.end().and_then(|end| end.convert_to_ceil(unit)) {
        exprs.push(col(MIN_TS_COLUMN).lt(lit(end.value())));
    }
    Ok((false, exprs))
}

fn validate_index_schema(schema: &SchemaRef) -> Result<()> {
    for (name, data_type) in [
        (MIN_TS_COLUMN, DataType::Int64),
        (MAX_TS_COLUMN, DataType::Int64),
        (ROW_COUNT_COLUMN, DataType::UInt64),
        (TABLE_ID_COLUMN, DataType::UInt32),
        (TSID_COLUMN, DataType::UInt64),
    ] {
        let field = schema
            .field_with_name(name)
            .ok()
            .context(InvalidRecordBatchSnafu {
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
    Ok(())
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
            .context(InvalidRecordBatchSnafu {
                reason: format!("series index is missing internal column {name}"),
            })?;
        indices.insert(index);
    }
    for filter in filters {
        let index =
            arrow_schema
                .index_of(filter.column_name())
                .ok()
                .context(InvalidRecordBatchSnafu {
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
        .context(InvalidRecordBatchSnafu {
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
        let null_counts = self
            .row_groups
            .iter()
            .map(|row_group| {
                row_group
                    .column(column_index)
                    .statistics()?
                    .null_count_opt()
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Arc::new(UInt64Array::from(null_counts)))
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
        let null_scalar: ScalarValue = data_type.try_into().ok()?;
        let values = self
            .row_groups
            .iter()
            .map(|row_group| {
                let stats = row_group.column(column_index).statistics()?;
                stats_scalar_value(stats, data_type, is_min)
            })
            .map(|value| value.unwrap_or_else(|| null_scalar.clone()))
            .collect::<Vec<_>>();
        ScalarValue::iter_to_array(values).ok()
    }
}

fn stats_scalar_value(
    stats: &Statistics,
    data_type: &DataType,
    is_min: bool,
) -> Option<ScalarValue> {
    macro_rules! value {
        ($stats:expr) => {
            if is_min {
                *$stats.min_opt()?
            } else {
                *$stats.max_opt()?
            }
        };
    }

    match (stats, data_type) {
        (Statistics::Boolean(stats), DataType::Boolean) => {
            Some(ScalarValue::Boolean(Some(value!(stats))))
        }
        (Statistics::Int32(stats), DataType::Int32) => {
            Some(ScalarValue::Int32(Some(value!(stats))))
        }
        (Statistics::Int32(stats), DataType::UInt32) => {
            Some(ScalarValue::UInt32(Some(value!(stats) as u32)))
        }
        (Statistics::Int64(stats), DataType::Int64) => {
            Some(ScalarValue::Int64(Some(value!(stats))))
        }
        (Statistics::Int64(stats), DataType::UInt64) => {
            Some(ScalarValue::UInt64(Some(value!(stats) as u64)))
        }
        (Statistics::Float(stats), DataType::Float32) => {
            Some(ScalarValue::Float32(Some(value!(stats))))
        }
        (Statistics::Double(stats), DataType::Float64) => {
            Some(ScalarValue::Float64(Some(value!(stats))))
        }
        (Statistics::ByteArray(stats), DataType::Utf8) => {
            let bytes = if is_min {
                stats.min_bytes_opt()?
            } else {
                stats.max_bytes_opt()?
            };
            Some(ScalarValue::Utf8(String::from_utf8(bytes.to_vec()).ok()))
        }
        _ => None,
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
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{BinaryArray, TimestampMillisecondArray, UInt8Array};
    use datatypes::arrow::datatypes::{Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use object_store::services::Memory;
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::series_index::{SeriesIndexWriter, SeriesIndexWriterOptions};
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn flat_batch(primary_keys: &[Vec<u8>], timestamps: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("__primary_key", DataType::Binary, false),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
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
            .write(&flat_batch(&primary_keys, &timestamps))
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
        let builder = ParquetRecordBatchStreamBuilder::new(LazyParquetFileReader::new(
            object_store.clone(),
            path.to_string(),
            None,
        ))
        .await
        .unwrap();
        assert_eq!(
            row_groups_to_read(
                builder.metadata().row_groups(),
                builder.schema().clone(),
                &searcher.pruning_predicate,
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
