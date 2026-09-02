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

use std::cmp::Ordering;
use std::ops::Range;

use datafusion_expr::{Expr, col, lit};
use datatypes::arrow::array::{Int64Array, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::{OptionExt, ensure};
use table::predicate::Predicate;

use crate::error::{InvalidRecordBatchSnafu, Result, UnexpectedSnafu};
use crate::series_index::MetricSeriesId;
use crate::sst::parquet::index_reader::ParquetIndexReader;
use crate::sst::range_index::{
    END_COLUMN, ROW_GROUP_ID_COLUMN, START_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN,
};

/// Searches per-SST range-index files for the rows of candidate metric series.
pub struct SstRangeIndexSearcher {
    reader: ParquetIndexReader,
}

impl SstRangeIndexSearcher {
    /// Opens the range-index file at `path` and loads its Parquet metadata.
    pub async fn open(object_store: ObjectStore, path: &str) -> Result<Self> {
        let reader = ParquetIndexReader::open(object_store, path).await?;
        validate_index_schema(reader.schema())?;
        Ok(Self { reader })
    }

    /// Returns the row ranges for `series` in one source SST row group.
    ///
    /// `series` is one batch emitted by a
    /// [`MetricSeriesIdStream`](crate::series_index::MetricSeriesIdStream). The
    /// returned half-open ranges are relative to the start of `row_group_id`,
    /// sorted, non-overlapping, and coalesced when adjacent.
    pub async fn search(
        &self,
        row_group_id: u32,
        series: &[MetricSeriesId],
    ) -> Result<Vec<Range<usize>>> {
        if series.is_empty() {
            return Ok(Vec::new());
        }

        validate_sorted_series(series)?;
        let predicate = search_predicate(row_group_id, series)?;
        let mut batches = self.reader.read(
            &predicate,
            &[
                ROW_GROUP_ID_COLUMN,
                TABLE_ID_COLUMN,
                TSID_COLUMN,
                START_COLUMN,
                END_COLUMN,
            ],
        )?;
        let mut merge = RangeMergeState::new(row_group_id, series);

        while let Some(batch) = batches.try_next().await? {
            if merge.append_batch(&batch)? {
                break;
            }
        }

        Ok(merge.finish())
    }
}

fn validate_sorted_series(series: &[MetricSeriesId]) -> Result<()> {
    if let Some(pair) = series.windows(2).find(|pair| pair[0] > pair[1]) {
        return InvalidRecordBatchSnafu {
            reason: format!(
                "range index search series are not sorted: {:?} appears before {:?}",
                pair[0], pair[1]
            ),
        }
        .fail();
    }
    Ok(())
}

fn search_predicate(row_group_id: u32, series: &[MetricSeriesId]) -> Result<Predicate> {
    let mut table_exprs = Vec::new();
    let mut table_start = 0;
    while table_start < series.len() {
        let table_id = series[table_start].table_id;
        let mut table_end = table_start;
        let mut last_tsid = None;
        let mut tsids = Vec::new();
        while table_end < series.len() && series[table_end].table_id == table_id {
            let tsid = series[table_end].tsid;
            if last_tsid != Some(tsid) {
                tsids.push(lit(tsid));
                last_tsid = Some(tsid);
            }
            table_end += 1;
        }
        table_exprs.push(
            col(TABLE_ID_COLUMN)
                .eq(lit(table_id))
                .and(col(TSID_COLUMN).in_list(tsids, false)),
        );
        table_start = table_end;
    }

    let series_expr = table_exprs
        .into_iter()
        .reduce(Expr::or)
        .context(UnexpectedSnafu {
            reason: "cannot build a range-index predicate for an empty series set",
        })?;

    Ok(Predicate::new(vec![
        col(ROW_GROUP_ID_COLUMN).eq(lit(row_group_id)),
        series_expr,
    ]))
}

fn validate_index_schema(schema: &SchemaRef) -> Result<()> {
    for (name, data_type) in [
        (ROW_GROUP_ID_COLUMN, DataType::UInt32),
        (TABLE_ID_COLUMN, DataType::UInt32),
        (TSID_COLUMN, DataType::UInt64),
        (START_COLUMN, DataType::Int64),
        (END_COLUMN, DataType::Int64),
    ] {
        let field = schema
            .field_with_name(name)
            .ok()
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("range index is missing column {name}"),
            })?;
        ensure!(
            field.data_type() == &data_type && !field.is_nullable(),
            InvalidRecordBatchSnafu {
                reason: format!(
                    "range index column {name} must be non-nullable {data_type:?}, got {:?}",
                    field.data_type()
                ),
            }
        );
    }
    Ok(())
}

struct RangeMergeState<'a> {
    row_group_id: u32,
    series: &'a [MetricSeriesId],
    series_index: usize,
    last_index_key: Option<(u32, MetricSeriesId)>,
    ranges: Vec<Range<usize>>,
}

impl<'a> RangeMergeState<'a> {
    fn new(row_group_id: u32, series: &'a [MetricSeriesId]) -> Self {
        Self {
            row_group_id,
            series,
            series_index: 0,
            last_index_key: None,
            ranges: Vec::new(),
        }
    }

    /// Appends matches from `batch` and returns whether the merge is complete.
    fn append_batch(
        &mut self,
        batch: &datatypes::arrow::record_batch::RecordBatch,
    ) -> Result<bool> {
        let row_group_ids = typed_column::<UInt32Array>(batch, ROW_GROUP_ID_COLUMN, "UInt32")?;
        let table_ids = typed_column::<UInt32Array>(batch, TABLE_ID_COLUMN, "UInt32")?;
        let tsids = typed_column::<UInt64Array>(batch, TSID_COLUMN, "UInt64")?;
        let starts = typed_column::<Int64Array>(batch, START_COLUMN, "Int64")?;
        let ends = typed_column::<Int64Array>(batch, END_COLUMN, "Int64")?;

        for row in 0..batch.num_rows() {
            let index_series = MetricSeriesId {
                table_id: table_ids.value(row),
                tsid: tsids.value(row),
            };
            let index_key = (row_group_ids.value(row), index_series);
            ensure!(
                self.last_index_key.is_none_or(|last| last < index_key),
                InvalidRecordBatchSnafu {
                    reason: format!(
                        "range index rows are not strictly sorted: {index_key:?} follows {:?}",
                        self.last_index_key
                    ),
                }
            );
            self.last_index_key = Some(index_key);

            match index_key.0.cmp(&self.row_group_id) {
                Ordering::Less => continue,
                Ordering::Greater => return Ok(true),
                Ordering::Equal => {}
            }

            while self.series_index < self.series.len()
                && self.series[self.series_index] < index_series
            {
                self.advance_series();
            }
            if self.series_index == self.series.len() {
                return Ok(true);
            }

            match self.series[self.series_index].cmp(&index_series) {
                Ordering::Less => {
                    return UnexpectedSnafu {
                        reason: "range-index merge cursor did not advance past a smaller series",
                    }
                    .fail();
                }
                Ordering::Greater => continue,
                Ordering::Equal => {
                    self.append_range(starts.value(row), ends.value(row), row)?;
                    self.advance_series();
                    if self.series_index == self.series.len() {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn advance_series(&mut self) {
        let current = self.series[self.series_index];
        while self.series_index < self.series.len() && self.series[self.series_index] == current {
            self.series_index += 1;
        }
    }

    fn append_range(&mut self, start: i64, end: i64, row: usize) -> Result<()> {
        let start = usize::try_from(start).map_err(|_| {
            InvalidRecordBatchSnafu {
                reason: format!("range index contains negative start offset at row {row}"),
            }
            .build()
        })?;
        let end = usize::try_from(end).map_err(|_| {
            InvalidRecordBatchSnafu {
                reason: format!("range index contains negative end offset at row {row}"),
            }
            .build()
        })?;
        ensure!(
            start < end,
            InvalidRecordBatchSnafu {
                reason: format!("range index contains invalid range {start}..{end} at row {row}"),
            }
        );

        if let Some(last) = self.ranges.last_mut() {
            ensure!(
                start >= last.end,
                InvalidRecordBatchSnafu {
                    reason: format!(
                        "range index contains overlapping or unsorted range {start}..{end} after {}..{}",
                        last.start, last.end
                    ),
                }
            );
            if start == last.end {
                last.end = end;
                return Ok(());
            }
        }
        self.ranges.push(start..end);
        Ok(())
    }

    fn finish(self) -> Vec<Range<usize>> {
        self.ranges
    }
}

fn typed_column<'a, T: 'static>(
    batch: &'a datatypes::arrow::record_batch::RecordBatch,
    name: &str,
    data_type: &str,
) -> Result<&'a T> {
    let index = batch
        .schema()
        .index_of(name)
        .ok()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!("range index batch is missing column {name}"),
        })?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!("range index column {name} is not {data_type}"),
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{ArrayRef, BinaryArray};
    use datatypes::arrow::datatypes::{Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use object_store::services::Memory;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::RegionMetadataRef;
    use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

    use super::*;
    use crate::sst::range_index::{
        SstRangeIndexWriter, SstRangeIndexWriterOptions, range_index_schema,
    };
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn series(table_id: u32, tsid: u64) -> MetricSeriesId {
        MetricSeriesId { table_id, tsid }
    }

    fn primary_key_batch(metadata: &RegionMetadataRef, ids: &[(u32, u64)]) -> RecordBatch {
        let primary_keys = ids
            .iter()
            .map(|(table_id, tsid)| new_sparse_primary_key(&["a", "x"], metadata, *table_id, *tsid))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![Field::new(
            PRIMARY_KEY_COLUMN_NAME,
            DataType::Binary,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_iter_values(
                primary_keys.iter().map(Vec::as_slice),
            ))],
        )
        .unwrap()
    }

    async fn write_index(store: &ObjectStore, path: &str) {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let mut writer = SstRangeIndexWriter::try_new(
            metadata.clone(),
            store.clone(),
            path,
            SstRangeIndexWriterOptions {
                index_row_group_size: 2,
            },
        )
        .await
        .unwrap();
        writer
            .write(
                0,
                &primary_key_batch(
                    &metadata,
                    &[(1, 10), (1, 10), (1, 20), (2, 10), (2, 20), (2, 20)],
                ),
            )
            .await
            .unwrap();
        writer
            .write(1, &primary_key_batch(&metadata, &[(2, 20), (2, 20)]))
            .await
            .unwrap();
        writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn search_filters_exact_series_pairs_and_coalesces_ranges() {
        let store = object_store();
        let path = "range-search.parquet";
        write_index(&store, path).await;
        let searcher = SstRangeIndexSearcher::open(store, path).await.unwrap();

        let ranges = searcher
            .search(0, &[series(1, 10), series(2, 20)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![0..2, 4..6]);

        let ranges = searcher
            .search(0, &[series(1, 10), series(1, 20)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![0..3]);

        let ranges = searcher
            .search(1, &[series(2, 20), series(2, 20)])
            .await
            .unwrap();
        assert_eq!(ranges, vec![0..2]);

        assert!(searcher.search(0, &[]).await.unwrap().is_empty());

        let error = searcher
            .search(0, &[series(2, 20), series(1, 10)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not sorted"), "{error}");
    }

    #[tokio::test]
    async fn opening_a_missing_index_fails() {
        assert!(
            SstRangeIndexSearcher::open(object_store(), "does-not-exist.parquet")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn pruning_uses_the_source_row_group_and_series_pairs() {
        let store = object_store();
        let path = "range-pruning.parquet";
        write_index(&store, path).await;
        let reader = ParquetIndexReader::open(store, path).await.unwrap();
        let predicate = search_predicate(1, &[series(2, 20)]).unwrap();

        assert_eq!(reader.row_groups_to_read(&predicate), vec![2]);
    }

    #[test]
    fn validates_schema_and_range_offsets() {
        let nullable_schema = Arc::new(Schema::new(vec![
            Field::new(ROW_GROUP_ID_COLUMN, DataType::UInt32, false),
            Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
            Field::new(TSID_COLUMN, DataType::UInt64, false),
            Field::new(START_COLUMN, DataType::Int64, true),
            Field::new(END_COLUMN, DataType::Int64, false),
        ]));
        assert!(validate_index_schema(&nullable_schema).is_err());

        let batch = RecordBatch::try_new(
            range_index_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(UInt64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![-1])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let selected = [series(1, 10)];
        let mut merge = RangeMergeState::new(0, &selected);
        assert!(merge.append_batch(&batch).is_err());

        let unsorted_batch = RecordBatch::try_new(
            range_index_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![1, 1])),
                Arc::new(UInt64Array::from(vec![20, 10])),
                Arc::new(Int64Array::from(vec![0, 1])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let selected = [series(1, 20), series(1, 30)];
        let mut merge = RangeMergeState::new(0, &selected);
        assert!(merge.append_batch(&unsorted_batch).is_err());

        let make_batch = |tsid, start, end| {
            RecordBatch::try_new(
                range_index_schema(),
                vec![
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(UInt32Array::from(vec![1])),
                    Arc::new(UInt64Array::from(vec![tsid])),
                    Arc::new(Int64Array::from(vec![start])),
                    Arc::new(Int64Array::from(vec![end])),
                ],
            )
            .unwrap()
        };
        let selected = [series(1, 10), series(1, 20)];
        let mut merge = RangeMergeState::new(0, &selected);
        assert!(!merge.append_batch(&make_batch(10, 0, 1)).unwrap());
        assert!(merge.append_batch(&make_batch(20, 1, 2)).unwrap());
        assert_eq!(merge.finish(), vec![0..2]);
    }
}
