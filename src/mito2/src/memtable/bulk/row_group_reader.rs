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

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow::array::RecordBatch;
use datatypes::arrow::error::ArrowError;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroups, RowSelection};
use parquet::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use parquet::column::page::{PageIterator, PageReader};
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;

use crate::error;
use crate::error::ReadDataPartSnafu;
use crate::memtable::bulk::context::BulkIterContextRef;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::{RowGroupReaderBase, RowGroupReaderContext};
use crate::sst::parquet::row_group::{ColumnChunkIterator, RowGroupBase};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

/// Helper for reading specific row group inside Memtable Parquet parts.
// This is similar to [mito2::sst::parquet::row_group::InMemoryRowGroup] since
// it's a workaround for lacking of keyword generics.
pub struct MemtableRowGroupPageFetcher<'a> {
    /// Shared structs for reading row group.
    base: RowGroupBase<'a>,
    bytes: Bytes,
}

impl<'a> MemtableRowGroupPageFetcher<'a> {
    pub(crate) fn create(
        row_group_idx: usize,
        parquet_meta: &'a ParquetMetaData,
        bytes: Bytes,
    ) -> Self {
        let metadata = parquet_meta.row_group(row_group_idx);
        let row_count = metadata.num_rows() as usize;
        let page_locations = parquet_meta
            .offset_index()
            .map(|x| x[row_group_idx].as_slice());

        Self {
            base: RowGroupBase {
                metadata,
                page_locations,
                row_count,
                column_chunks: vec![None; metadata.columns().len()],
                // the cached `column_uncompressed_pages` would never be used in Memtable readers.
                column_uncompressed_pages: vec![None; metadata.columns().len()],
            },
            bytes,
        }
    }

    /// Fetches column pages from memory file.
    pub(crate) fn fetch(&mut self, projection: &ProjectionMask, selection: Option<&RowSelection>) {
        if let Some((selection, page_locations)) = selection.zip(self.base.page_locations) {
            // Selection provided.
            let (fetch_ranges, page_start_offsets) =
                self.base
                    .calc_sparse_read_ranges(projection, page_locations, selection);
            if fetch_ranges.is_empty() {
                return;
            }
            let chunk_data = self.fetch_bytes(&fetch_ranges);

            self.base
                .assign_sparse_chunk(projection, chunk_data, page_start_offsets);
        } else {
            let fetch_ranges = self.base.calc_dense_read_ranges(projection);
            if fetch_ranges.is_empty() {
                // Nothing to fetch.
                return;
            }
            let chunk_data = self.fetch_bytes(&fetch_ranges);
            self.base.assign_dense_chunk(projection, chunk_data);
        }
    }

    fn fetch_bytes(&self, ranges: &[Range<u64>]) -> Vec<Bytes> {
        ranges
            .iter()
            .map(|range| self.bytes.slice(range.start as usize..range.end as usize))
            .collect()
    }

    /// Creates a page reader to read column at `i`.
    fn column_page_reader(&self, i: usize) -> parquet::errors::Result<Box<dyn PageReader>> {
        let reader = self.base.column_reader(i)?;
        Ok(Box::new(reader))
    }
}

impl RowGroups for MemtableRowGroupPageFetcher<'_> {
    fn num_rows(&self) -> usize {
        self.base.row_count
    }

    fn column_chunks(&self, i: usize) -> parquet::errors::Result<Box<dyn PageIterator>> {
        Ok(Box::new(ColumnChunkIterator {
            reader: Some(self.column_page_reader(i)),
        }))
    }
}

impl RowGroupReaderContext for BulkIterContextRef {
    fn map_result(
        &self,
        result: Result<Option<RecordBatch>, ArrowError>,
    ) -> error::Result<Option<RecordBatch>> {
        result.context(error::DecodeArrowRowGroupSnafu)
    }

    fn read_format(&self) -> &ReadFormat {
        self.as_ref().read_format()
    }
}

pub(crate) type MemtableRowGroupReader = RowGroupReaderBase<BulkIterContextRef>;

pub(crate) struct MemtableRowGroupReaderBuilder {
    context: BulkIterContextRef,
    projection: ProjectionMask,
    parquet_metadata: Arc<ParquetMetaData>,
    field_levels: FieldLevels,
    data: Bytes,
}

impl MemtableRowGroupReaderBuilder {
    pub(crate) fn try_new(
        context: BulkIterContextRef,
        projection: ProjectionMask,
        parquet_metadata: Arc<ParquetMetaData>,
        data: Bytes,
    ) -> error::Result<Self> {
        let parquet_schema_desc = parquet_metadata.file_metadata().schema_descr();
        let hint = Some(context.read_format().arrow_schema().fields());
        let field_levels =
            parquet_to_arrow_field_levels(parquet_schema_desc, projection.clone(), hint)
                .context(ReadDataPartSnafu)?;
        Ok(Self {
            context,
            projection,
            parquet_metadata,
            field_levels,
            data,
        })
    }

    /// Builds a reader to read the row group at `row_group_idx` from memory.
    pub(crate) fn build_row_group_reader(
        &self,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> error::Result<MemtableRowGroupReader> {
        let mut row_group = MemtableRowGroupPageFetcher::create(
            row_group_idx,
            &self.parquet_metadata,
            self.data.clone(),
        );
        // Fetches data from memory part. Currently, row selection is not supported.
        row_group.fetch(&self.projection, row_selection.as_ref());

        // Builds the parquet reader.
        // Now the row selection is None.
        let reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &row_group,
            DEFAULT_READ_BATCH_SIZE,
            row_selection,
        )
        .context(ReadDataPartSnafu)?;
        Ok(MemtableRowGroupReader::create(self.context.clone(), reader))
    }
}
