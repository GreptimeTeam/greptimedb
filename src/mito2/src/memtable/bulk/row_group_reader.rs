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
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::error;
use crate::sst::parquet::row_group::{ColumnChunkData, RowGroupBase};

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
    pub(crate) fn fetch(
        &mut self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
    ) -> error::Result<()> {
        if let Some((selection, page_locations)) = selection.zip(self.base.page_locations) {
            let (fetch_ranges, page_start_offsets) =
                self.base
                    .calc_sparse_read_ranges(projection, page_locations, selection);

            let chunk_data = self.fetch_bytes(&fetch_ranges);

            self.base
                .assign_sparse_chunk(projection, chunk_data, page_start_offsets);
        } else {
            let fetch_ranges = self.base.calc_dense_read_ranges(projection);
            if fetch_ranges.is_empty() {
                // Nothing to fetch.
                return Ok(());
            }
            let chunk_data = self.fetch_bytes(&fetch_ranges);

            let mut chunk_data = chunk_data.into_iter();
            for (idx, (chunk, row_group_pages)) in self
                .base
                .column_chunks
                .iter_mut()
                .zip(&self.base.column_uncompressed_pages)
                .enumerate()
            {
                if chunk.is_some() || !projection.leaf_included(idx) || row_group_pages.is_some() {
                    continue;
                }

                // Get the fetched page.
                let Some(data) = chunk_data.next() else {
                    continue;
                };

                let column = self.base.metadata.column(idx);
                *chunk = Some(Arc::new(ColumnChunkData::Dense {
                    offset: column.byte_range().0 as usize,
                    data,
                }));
            }
        }

        Ok(())
    }

    fn fetch_bytes(&self, ranges: &[Range<u64>]) -> Vec<Bytes> {
        ranges
            .iter()
            .map(|range| self.bytes.slice(range.start as usize..range.end as usize))
            .collect()
    }
}
