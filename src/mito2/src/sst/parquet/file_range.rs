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

//! Structs and functions for reading ranges from a parquet file. A file range
//! is usually a row group in a parquet file.

use std::ops::BitAnd;
use std::sync::Arc;

use api::v1::SemanticType;
use datatypes::arrow::array::BooleanArray;
use datatypes::arrow::buffer::BooleanBuffer;
use parquet::arrow::arrow_reader::RowSelection;
use snafu::ResultExt;
use store_api::storage::TopHint;

use crate::cache::CacheManagerRef;
use crate::error::{FieldTypeMismatchSnafu, FilterRecordBatchSnafu, Result};
use crate::read::compat::CompatBatch;
use crate::read::Batch;
use crate::row_converter::{McmpRowCodec, RowCodec};
use crate::sst::file::{FileHandle, FileId};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::{RowGroupReader, RowGroupReaderBuilder, SimpleFilterContext};

/// A range of a parquet SST. Now it is a row group.
/// We can read different file ranges in parallel.
#[derive(Clone)]
pub struct FileRange {
    /// Shared context.
    context: FileRangeContextRef,
    /// Index of the row group in the SST.
    row_group_idx: usize,
    /// Row selection for the row group. `None` means all rows.
    row_selection: Option<RowSelection>,
}

impl FileRange {
    /// Creates a new [FileRange].
    pub(crate) fn new(
        context: FileRangeContextRef,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> Self {
        Self {
            context,
            row_group_idx,
            row_selection,
        }
    }

    /// Returns a reader to read the [FileRange].
    pub(crate) async fn reader(&self, mut top_hint: Option<TopHint>) -> Result<RowGroupReader> {
        let row_selection = if top_hint.is_some() && self.context.cache_manager().is_some() {
            let top_selection = self
                .context
                .cache_manager()
                .unwrap()
                .get_top_rows(&(self.file_handle().file_id(), self.row_group_idx));
            if let Some(top_select) = top_selection {
                top_hint = None;
                // common_telemetry::info!(
                //     "file {}, row group {}, top cache hit, top_select: {:?}",
                //     self.file_handle().file_id(),
                //     self.row_group_idx,
                //     top_select.selection
                // );
                self.row_selection
                    .as_ref()
                    .map(|row_select| row_select.and_then(&top_select.selection))
            } else {
                self.row_selection.clone()
            }
        } else {
            self.row_selection.clone()
        };

        let parquet_reader = self
            .context
            .reader_builder
            .build(self.row_group_idx, row_selection)
            .await?;

        Ok(
            RowGroupReader::new(self.row_group_idx, self.context.clone(), parquet_reader)
                .with_top_hint(top_hint),
        )
    }

    /// Returns the helper to compat batches.
    pub(crate) fn compat_batch(&self) -> Option<&CompatBatch> {
        self.context.compat_batch()
    }

    /// Returns the file handle of the file range.
    pub(crate) fn file_handle(&self) -> &FileHandle {
        self.context.reader_builder.file_handle()
    }
}

/// Context shared by ranges of the same parquet SST.
pub(crate) struct FileRangeContext {
    /// Row group reader builder for the file.
    reader_builder: RowGroupReaderBuilder,
    /// Base of the context.
    base: RangeBase,
}

pub(crate) type FileRangeContextRef = Arc<FileRangeContext>;

impl FileRangeContext {
    /// Creates a new [FileRangeContext].
    pub(crate) fn new(
        reader_builder: RowGroupReaderBuilder,
        filters: Vec<SimpleFilterContext>,
        read_format: ReadFormat,
        codec: McmpRowCodec,
    ) -> Self {
        Self {
            reader_builder,
            base: RangeBase {
                filters,
                read_format,
                codec,
                compat_batch: None,
            },
        }
    }

    /// Returns the path of the file to read.
    pub(crate) fn file_path(&self) -> &str {
        self.reader_builder.file_path()
    }

    /// Returns the file id of the file to read.
    pub(crate) fn file_id(&self) -> FileId {
        self.reader_builder.file_handle().file_id()
    }

    /// Returns filters pushed down.
    pub(crate) fn filters(&self) -> &[SimpleFilterContext] {
        &self.base.filters
    }

    /// Returns the format helper.
    pub(crate) fn read_format(&self) -> &ReadFormat {
        &self.base.read_format
    }

    /// Returns the reader builder.
    pub(crate) fn reader_builder(&self) -> &RowGroupReaderBuilder {
        &self.reader_builder
    }

    /// Returns the helper to compat batches.
    pub(crate) fn compat_batch(&self) -> Option<&CompatBatch> {
        self.base.compat_batch.as_ref()
    }

    /// Sets the `CompatBatch` to the context.
    pub(crate) fn set_compat_batch(&mut self, compat: Option<CompatBatch>) {
        self.base.compat_batch = compat;
    }

    /// TRY THE BEST to perform pushed down predicate precisely on the input batch.
    /// Return the filtered batch. If the entire batch is filtered out, return None.
    pub(crate) fn precise_filter(&self, input: Batch) -> Result<Option<Batch>> {
        self.base.precise_filter(input)
    }

    /// Returns the cache manager.
    pub(crate) fn cache_manager(&self) -> Option<&CacheManagerRef> {
        self.reader_builder.cache_manager()
    }
}

/// Common fields for a range to read and filter batches.
pub(crate) struct RangeBase {
    /// Filters pushed down.
    pub(crate) filters: Vec<SimpleFilterContext>,
    /// Helper to read the SST.
    pub(crate) read_format: ReadFormat,
    /// Decoder for primary keys
    pub(crate) codec: McmpRowCodec,
    /// Optional helper to compat batches.
    pub(crate) compat_batch: Option<CompatBatch>,
}

impl RangeBase {
    /// TRY THE BEST to perform pushed down predicate precisely on the input batch.
    /// Return the filtered batch. If the entire batch is filtered out, return None.
    ///
    /// Supported filter expr type is defined in [SimpleFilterEvaluator].
    ///
    /// When a filter is referencing primary key column, this method will decode
    /// the primary key and put it into the batch.
    pub(crate) fn precise_filter(&self, mut input: Batch) -> Result<Option<Batch>> {
        let mut mask = BooleanBuffer::new_set(input.num_rows());

        // Run filter one by one and combine them result
        // TODO(ruihang): run primary key filter first. It may short circuit other filters
        for filter in &self.filters {
            let result = match filter.semantic_type() {
                SemanticType::Tag => {
                    let pk_values = if let Some(pk_values) = input.pk_values() {
                        pk_values
                    } else {
                        input.set_pk_values(self.codec.decode(input.primary_key())?);
                        input.pk_values().unwrap()
                    };
                    // Safety: this is a primary key
                    let pk_index = self
                        .read_format
                        .metadata()
                        .primary_key_index(filter.column_id())
                        .unwrap();
                    let pk_value = pk_values[pk_index]
                        .try_to_scalar_value(filter.data_type())
                        .context(FieldTypeMismatchSnafu)?;
                    if filter
                        .filter()
                        .evaluate_scalar(&pk_value)
                        .context(FilterRecordBatchSnafu)?
                    {
                        continue;
                    } else {
                        // PK not match means the entire batch is filtered out.
                        return Ok(None);
                    }
                }
                SemanticType::Field => {
                    let Some(field_index) = self.read_format.field_index_by_id(filter.column_id())
                    else {
                        continue;
                    };
                    let field_col = &input.fields()[field_index].data;
                    filter
                        .filter()
                        .evaluate_vector(field_col)
                        .context(FilterRecordBatchSnafu)?
                }
                SemanticType::Timestamp => filter
                    .filter()
                    .evaluate_vector(input.timestamps())
                    .context(FilterRecordBatchSnafu)?,
            };

            mask = mask.bitand(&result);
        }

        input.filter(&BooleanArray::from(mask).into())?;

        Ok(Some(input))
    }
}
