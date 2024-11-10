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
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroups, RowSelection};
use parquet::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use parquet::column::page::{PageIterator, PageReader};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::SerializedPageReader;
use snafu::ResultExt;

use crate::error;
use crate::error::ReadDataPartSnafu;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::page_reader::RowGroupCachedReader;
use crate::sst::parquet::row_group::{ColumnChunkIterator, RowGroupBase};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

/// Helper for reading specific row group inside Memtable Parquet parts.
// This is similar to [mito2::sst::parquet::row_group::InMemoryRowGroup] since
// it's a workaround for lacking of keyword generics.
pub struct MemtableRowGroupReader<'a> {
    /// Shared structs for reading row group.
    base: RowGroupBase<'a>,
    bytes: Bytes,
}

impl<'a> MemtableRowGroupReader<'a> {
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
                column_chunks: vec![None; metadata.columns().len()],
                row_count,
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
        if let Some(cached_pages) = &self.base.column_uncompressed_pages[i] {
            debug_assert!(!cached_pages.row_group.is_empty());
            // Hits the row group level page cache.
            return Ok(Box::new(RowGroupCachedReader::new(&cached_pages.row_group)));
        }

        let page_reader = match &self.base.column_chunks[i] {
            None => {
                return Err(ParquetError::General(format!(
                    "Invalid column index {i}, column was not fetched"
                )))
            }
            Some(data) => {
                let page_locations = self.base.page_locations.map(|index| index[i].clone());
                SerializedPageReader::new(
                    data.clone(),
                    self.base.metadata.column(i),
                    self.base.row_count,
                    page_locations,
                )?
            }
        };

        // This column don't cache uncompressed pages.
        Ok(Box::new(page_reader))
    }
}

impl<'a> RowGroups for MemtableRowGroupReader<'a> {
    fn num_rows(&self) -> usize {
        self.base.row_count
    }

    fn column_chunks(&self, i: usize) -> parquet::errors::Result<Box<dyn PageIterator>> {
        Ok(Box::new(ColumnChunkIterator {
            reader: Some(self.column_page_reader(i)),
        }))
    }
}

pub(crate) struct MemtableRowGroupReaderBuilder {
    read_format: ReadFormat,
    projection: ProjectionMask,
    parquet_metadata: Arc<ParquetMetaData>,
    field_levels: FieldLevels,
    bytes: Bytes,
}

impl MemtableRowGroupReaderBuilder {
    /// Builds a [ParquetRecordBatchReader] to read the row group at `row_group_idx` from memory.
    pub(crate) async fn build_parquet_reader(
        &self,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> error::Result<ParquetRecordBatchReader> {
        let parquet_schema_desc = self.parquet_metadata.file_metadata().schema_descr();
        let hint = Some(self.read_format.arrow_schema().fields());
        let field_levels =
            parquet_to_arrow_field_levels(parquet_schema_desc, self.projection.clone(), hint)
                .context(ReadDataPartSnafu)?;

        let mut row_group = MemtableRowGroupReader::create(
            row_group_idx,
            &self.parquet_metadata,
            self.bytes.clone(),
        );
        // Fetches data from memory part. Currently, row selection is not supported.
        row_group.fetch(&self.projection, row_selection.as_ref());

        // Builds the parquet reader.
        // Now the row selection is None.
        ParquetRecordBatchReader::try_new_with_row_groups(
            &field_levels,
            &row_group,
            DEFAULT_READ_BATCH_SIZE,
            row_selection,
        )
        .context(ReadDataPartSnafu)
    }
}
