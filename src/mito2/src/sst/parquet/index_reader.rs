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

//! Reader for standalone Parquet index files.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{ArrayRef, BooleanArray};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use snafu::{OptionExt, ResultExt};
use table::predicate::Predicate;

use crate::error::{InvalidRecordBatchSnafu, OpenDalSnafu, ReadParquetSnafu, Result};
use crate::sst::parquet::format::{column_null_counts, column_values_by_type};
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::reader::MetadataCacheMetrics;

/// Reads a standalone index file stored in Parquet format.
pub(crate) struct ParquetIndexReader {
    object_store: ObjectStore,
    path: String,
    arrow_metadata: ArrowReaderMetadata,
}

impl ParquetIndexReader {
    /// Opens `path` and loads its Parquet metadata.
    pub(crate) async fn open(object_store: ObjectStore, path: &str) -> Result<Self> {
        let mut metrics = MetadataCacheMetrics::default();
        let parquet_metadata = MetadataLoader::new(object_store.clone(), path, 0)
            .load(&mut metrics)
            .await?;
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::new(parquet_metadata), ArrowReaderOptions::new())
                .with_context(|_| ReadParquetSnafu {
                    path: path.to_string(),
                })?;

        Ok(Self {
            object_store,
            path: path.to_string(),
            arrow_metadata,
        })
    }

    /// Returns the Arrow schema of the index file.
    pub(crate) fn schema(&self) -> &SchemaRef {
        self.arrow_metadata.schema()
    }

    /// Returns row groups that may match `predicate`.
    pub(crate) fn row_groups_to_read(&self, predicate: &Predicate) -> Vec<usize> {
        let stats = IndexRowGroupPruningStats {
            row_groups: self.arrow_metadata.metadata().row_groups(),
            schema: self.arrow_metadata.schema(),
        };
        predicate
            .prune_with_stats(&stats, stats.schema)
            .into_iter()
            .enumerate()
            .filter_map(|(row_group, keep)| keep.then_some(row_group))
            .collect()
    }

    /// Returns a stream of projected batches from row groups matching `predicate`.
    pub(crate) fn read(
        &self,
        predicate: &Predicate,
        projection_columns: &[&str],
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let projection = self.projection_mask(projection_columns)?;
        let row_groups = self.row_groups_to_read(predicate);
        if row_groups.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }

        let mut decoder = ParquetPushDecoderBuilder::new_with_metadata(self.arrow_metadata.clone())
            .with_row_groups(row_groups)
            .with_projection(projection)
            .build()
            .with_context(|_| ReadParquetSnafu {
                path: self.path.clone(),
            })?;
        let path = self.path.clone();
        let object_store = self.object_store.clone();

        Ok(async_stream::try_stream! {
            loop {
                match decoder
                    .try_decode()
                    .with_context(|_| ReadParquetSnafu { path: path.clone() })?
                {
                    DecodeResult::NeedsData(ranges) => {
                        let data = fetch_byte_ranges(&path, object_store.clone(), &ranges)
                            .await
                            .context(OpenDalSnafu)?;
                        decoder
                            .push_ranges(ranges, data)
                            .with_context(|_| ReadParquetSnafu { path: path.clone() })?;
                    }
                    DecodeResult::Data(batch) => yield batch,
                    DecodeResult::Finished => break,
                }
            }
        }
        .boxed())
    }

    fn projection_mask(&self, projection_columns: &[&str]) -> Result<ProjectionMask> {
        let mut indices = HashSet::with_capacity(projection_columns.len());
        for name in projection_columns {
            let index = self
                .arrow_metadata
                .schema()
                .index_of(name)
                .ok()
                .with_context(|| InvalidRecordBatchSnafu {
                    reason: format!("Parquet index is missing projected column {name}"),
                })?;
            indices.insert(index);
        }
        Ok(ProjectionMask::roots(
            self.arrow_metadata.parquet_schema(),
            indices,
        ))
    }
}

struct IndexRowGroupPruningStats<'a> {
    row_groups: &'a [RowGroupMetaData],
    schema: &'a SchemaRef,
}

impl PruningStatistics for IndexRowGroupPruningStats<'_> {
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

impl IndexRowGroupPruningStats<'_> {
    fn column_values(&self, column: &Column, is_min: bool) -> Option<ArrayRef> {
        let column_index = self.schema.index_of(&column.name).ok()?;
        let data_type = self.schema.field(column_index).data_type();
        column_values_by_type(self.row_groups, data_type, column_index, is_min)
    }
}
