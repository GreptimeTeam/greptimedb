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

use api::v1::SemanticType;
use async_stream::try_stream;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::range::TimestampRange;
use datafusion_expr::{Expr, col, lit};
use datatypes::arrow::array::{ArrayRef, UInt32Array, UInt64Array};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use table::predicate::Predicate;

use crate::error::{
    InvalidMetaSnafu, InvalidRecordBatchSnafu, RecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::series_index::{
    MAX_TS_COLUMN, METRIC_SERIES_ID_BATCH_SIZE, MIN_TS_COLUMN, MetricSeriesId,
    MetricSeriesIdStream, ROW_COUNT_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN, series_index_schema,
};
use crate::sst::parquet::index_reader::ParquetIndexReader;
use crate::sst::parquet::prefilter::simple_tag_filters;

/// Searches a series-index file for metric series matching query predicates.
pub struct SeriesIndexSearcher {
    object_store: ObjectStore,
    filters: Vec<(Expr, SimpleFilterEvaluator)>,
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

        Ok(Self {
            object_store,
            filters,
            empty_time_range,
        })
    }

    /// Searches `path` and returns sorted batches of matching metric-series IDs.
    pub async fn search(&self, path: &str) -> Result<MetricSeriesIdStream> {
        if self.empty_time_range {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let reader = ParquetIndexReader::open(self.object_store.clone(), path).await?;
        validate_index_schema(reader.schema())?;

        // An older index file may not contain tags added by schema evolution.
        // Ignore filters on those tags to preserve a conservative candidate set.
        let (pruning_predicate, filters) = self.filters_for_schema(reader.schema());
        let mut projection_columns = Vec::with_capacity(filters.len() + 2);
        projection_columns.extend([TABLE_ID_COLUMN, TSID_COLUMN]);
        projection_columns.extend(filters.iter().map(SimpleFilterEvaluator::column_name));
        let mut batches = reader.read(&pruning_predicate, &projection_columns)?;

        Ok(Box::pin(try_stream! {
            let mut last_series = None;
            let mut output = Vec::with_capacity(METRIC_SERIES_ID_BATCH_SIZE);
            while let Some(batch) = batches.try_next().await? {
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

    fn filters_for_schema(&self, schema: &SchemaRef) -> (Predicate, Vec<SimpleFilterEvaluator>) {
        let (exprs, filters): (Vec<_>, Vec<_>) = self
            .filters
            .iter()
            .filter(|(_, filter)| schema.field_with_name(filter.column_name()).is_ok())
            .cloned()
            .unzip();
        (Predicate::new(exprs), filters)
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
    // A series overlaps [start, end) only if its maximum is at least start.
    // Round start up so a series ending before an unaligned start is pruned.
    if let Some(start) = time_range
        .start()
        .and_then(|start| start.convert_to_ceil(unit))
    {
        exprs.push(col(MAX_TS_COLUMN).gt_eq(lit(start.value())));
    }
    // A series overlaps [start, end) only if its minimum is less than end.
    // Round the exclusive end up to avoid pruning the containing unit interval.
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
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{BinaryArray, TimestampMillisecondArray, UInt8Array};
    use datatypes::arrow::datatypes::{Field, Schema};
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
        let reader = ParquetIndexReader::open(object_store.clone(), path)
            .await
            .unwrap();
        let (pruning_predicate, _) = searcher.filters_for_schema(reader.schema());
        assert_eq!(reader.row_groups_to_read(&pruning_predicate), vec![1]);

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
