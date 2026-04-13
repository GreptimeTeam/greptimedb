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

use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;

use crate::error;
use crate::error::ReadDataPartSnafu;
use crate::memtable::bulk::chunk_reader::MemtableChunkReader;
use crate::memtable::bulk::context::BulkIterContextRef;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

pub(crate) struct MemtableRowGroupReaderBuilder {
    projection: ProjectionMask,
    arrow_metadata: ArrowReaderMetadata,
    data: Bytes,
}

impl MemtableRowGroupReaderBuilder {
    pub(crate) fn try_new(
        context: &BulkIterContextRef,
        projection: ProjectionMask,
        parquet_metadata: Arc<ParquetMetaData>,
        data: Bytes,
    ) -> error::Result<Self> {
        // Create ArrowReaderMetadata for building the reader.
        let arrow_reader_options =
            ArrowReaderOptions::new().with_schema(context.read_format().arrow_schema().clone());
        let arrow_metadata =
            ArrowReaderMetadata::try_new(parquet_metadata.clone(), arrow_reader_options)
                .context(ReadDataPartSnafu)?;
        Ok(Self {
            projection,
            arrow_metadata,
            data,
        })
    }

    /// Builds a reader to read the row group at `row_group_idx` from memory.
    pub(crate) fn build_row_group_reader(
        &self,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> error::Result<ParquetRecordBatchReader> {
        let chunk_reader = MemtableChunkReader::new(self.data.clone());

        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            chunk_reader,
            self.arrow_metadata.clone(),
        )
        .with_row_groups(vec![row_group_idx])
        .with_projection(self.projection.clone())
        .with_batch_size(DEFAULT_READ_BATCH_SIZE);

        if let Some(selection) = row_selection {
            builder = builder.with_row_selection(selection);
        }

        builder.build().context(ReadDataPartSnafu)
    }

    /// Computes whether to skip field filters for a specific row group based on PreFilterMode.
    pub(crate) fn compute_skip_fields(
        &self,
        context: &BulkIterContextRef,
        _row_group_idx: usize,
    ) -> bool {
        use crate::sst::parquet::file_range::PreFilterMode;

        match context.pre_filter_mode() {
            PreFilterMode::All => false,
            PreFilterMode::SkipFields => true,
        }
    }
}
