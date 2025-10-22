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

use std::collections::HashMap;
use std::ops::BitAnd;
use std::sync::Arc;

use api::v1::{OpType, SemanticType};
use common_telemetry::error;
use datatypes::arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, UInt32Array};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec};
use parquet::arrow::arrow_reader::RowSelection;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, TimeSeriesRowSelector};

use crate::error::{
    ComputeArrowSnafu, DataTypeMismatchSnafu, DecodeSnafu, DecodeStatsSnafu,
    InvalidRecordBatchSnafu, RecordBatchSnafu, Result, StatsNotPresentSnafu,
};
use crate::read::Batch;
use crate::read::compat::CompatBatch;
use crate::read::last_row::RowGroupLastRowCachedReader;
use crate::read::prune::{FlatPruneReader, PruneReader};
use crate::sst::file::FileHandle;
use crate::sst::parquet::flat_format::{
    DecodedPrimaryKeys, decode_primary_keys, primary_key_column_index,
};
use crate::sst::parquet::format::{PrimaryKeyArray, ReadFormat};
use crate::sst::parquet::reader::{
    FlatRowGroupReader, MaybeFilter, RowGroupReader, RowGroupReaderBuilder, SimpleFilterContext,
};
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

    /// Returns true if [FileRange] selects all rows in row group.
    fn select_all(&self) -> bool {
        let rows_in_group = self
            .context
            .reader_builder
            .parquet_metadata()
            .row_group(self.row_group_idx)
            .num_rows();

        let Some(row_selection) = &self.row_selection else {
            return true;
        };
        row_selection.row_count() == rows_in_group as usize
    }

    /// Returns a reader to read the [FileRange].
    pub(crate) async fn reader(
        &self,
        selector: Option<TimeSeriesRowSelector>,
    ) -> Result<PruneReader> {
        let parquet_reader = self
            .context
            .reader_builder
            .build(self.row_group_idx, self.row_selection.clone())
            .await?;

        let use_last_row_reader = if selector
            .map(|s| s == TimeSeriesRowSelector::LastRow)
            .unwrap_or(false)
        {
            // Only use LastRowReader if row group does not contain DELETE
            // and all rows are selected.
            let put_only = !self
                .context
                .contains_delete(self.row_group_idx)
                .inspect_err(|e| {
                    error!(e; "Failed to decode min value of op_type, fallback to RowGroupReader");
                })
                .unwrap_or(true);
            put_only && self.select_all()
        } else {
            // No selector provided, use RowGroupReader
            false
        };

        let prune_reader = if use_last_row_reader {
            // Row group is PUT only, use LastRowReader to skip unnecessary rows.
            let reader = RowGroupLastRowCachedReader::new(
                self.file_handle().file_id().file_id(),
                self.row_group_idx,
                self.context.reader_builder.cache_strategy().clone(),
                RowGroupReader::new(self.context.clone(), parquet_reader),
            );
            PruneReader::new_with_last_row_reader(self.context.clone(), reader)
        } else {
            // Row group contains DELETE, fallback to default reader.
            PruneReader::new_with_row_group_reader(
                self.context.clone(),
                RowGroupReader::new(self.context.clone(), parquet_reader),
            )
        };

        Ok(prune_reader)
    }

    /// Creates a flat reader that returns RecordBatch.
    pub(crate) async fn flat_reader(&self) -> Result<FlatPruneReader> {
        let parquet_reader = self
            .context
            .reader_builder
            .build(self.row_group_idx, self.row_selection.clone())
            .await?;

        let flat_row_group_reader = FlatRowGroupReader::new(self.context.clone(), parquet_reader);
        let flat_prune_reader =
            FlatPruneReader::new_with_row_group_reader(self.context.clone(), flat_row_group_reader);

        Ok(flat_prune_reader)
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
        codec: Arc<dyn PrimaryKeyCodec>,
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

    /// Filters the input RecordBatch by the pushed down predicate and returns RecordBatch.
    pub(crate) fn precise_filter_flat(&self, input: RecordBatch) -> Result<Option<RecordBatch>> {
        // Filters by the table id in the primary key column first.
        let Some(input) = self.base.filter_sparse_table_id(input)? else {
            return Ok(None);
        };

        self.base.precise_filter_flat(input)
    }

    //// Decodes parquet metadata and finds if row group contains delete op.
    pub(crate) fn contains_delete(&self, row_group_index: usize) -> Result<bool> {
        let metadata = self.reader_builder.parquet_metadata();
        let row_group_metadata = &metadata.row_groups()[row_group_index];

        // safety: The last column of SST must be op_type
        let column_metadata = &row_group_metadata.columns().last().unwrap();
        let stats = column_metadata.statistics().context(StatsNotPresentSnafu {
            file_path: self.reader_builder.file_path(),
        })?;
        stats
            .min_bytes_opt()
            .context(StatsNotPresentSnafu {
                file_path: self.reader_builder.file_path(),
            })?
            .try_into()
            .map(i32::from_le_bytes)
            .map(|min_op_type| min_op_type == OpType::Delete as i32)
            .ok()
            .context(DecodeStatsSnafu {
                file_path: self.reader_builder.file_path(),
            })
    }
}

/// Common fields for a range to read and filter batches.
pub(crate) struct RangeBase {
    /// Filters pushed down.
    pub(crate) filters: Vec<SimpleFilterContext>,
    /// Helper to read the SST.
    pub(crate) read_format: ReadFormat,
    /// Decoder for primary keys
    pub(crate) codec: Arc<dyn PrimaryKeyCodec>,
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
        for filter_ctx in &self.filters {
            let filter = match filter_ctx.filter() {
                MaybeFilter::Filter(f) => f,
                // Column matches.
                MaybeFilter::Matched => continue,
                // Column doesn't match, filter the entire batch.
                MaybeFilter::Pruned => return Ok(None),
            };
            let result = match filter_ctx.semantic_type() {
                SemanticType::Tag => {
                    let pk_values = if let Some(pk_values) = input.pk_values() {
                        pk_values
                    } else {
                        input.set_pk_values(
                            self.codec
                                .decode(input.primary_key())
                                .context(DecodeSnafu)?,
                        );
                        input.pk_values().unwrap()
                    };
                    let pk_value = match pk_values {
                        CompositeValues::Dense(v) => {
                            // Safety: this is a primary key
                            let pk_index = self
                                .read_format
                                .metadata()
                                .primary_key_index(filter_ctx.column_id())
                                .unwrap();
                            v[pk_index]
                                .1
                                .try_to_scalar_value(filter_ctx.data_type())
                                .context(DataTypeMismatchSnafu)?
                        }
                        CompositeValues::Sparse(v) => {
                            let v = v.get_or_null(filter_ctx.column_id());
                            v.try_to_scalar_value(filter_ctx.data_type())
                                .context(DataTypeMismatchSnafu)?
                        }
                    };
                    if filter
                        .evaluate_scalar(&pk_value)
                        .context(RecordBatchSnafu)?
                    {
                        continue;
                    } else {
                        // PK not match means the entire batch is filtered out.
                        return Ok(None);
                    }
                }
                SemanticType::Field => {
                    // Safety: Input is Batch so we are using primary key format.
                    let Some(field_index) = self
                        .read_format
                        .as_primary_key()
                        .unwrap()
                        .field_index_by_id(filter_ctx.column_id())
                    else {
                        continue;
                    };
                    let field_col = &input.fields()[field_index].data;
                    filter
                        .evaluate_vector(field_col)
                        .context(RecordBatchSnafu)?
                }
                SemanticType::Timestamp => filter
                    .evaluate_vector(input.timestamps())
                    .context(RecordBatchSnafu)?,
            };

            mask = mask.bitand(&result);
        }

        input.filter(&BooleanArray::from(mask).into())?;

        Ok(Some(input))
    }

    /// Filters the input RecordBatch by the pushed down predicate and returns RecordBatch.
    ///
    /// It assumes all necessary tags are already decoded from the primary key.
    pub(crate) fn precise_filter_flat(&self, input: RecordBatch) -> Result<Option<RecordBatch>> {
        let mask = self.compute_filter_mask_flat(&input)?;

        // If mask is None, the entire batch is filtered out
        let Some(mask) = mask else {
            return Ok(None);
        };

        let filtered_batch =
            datatypes::arrow::compute::filter_record_batch(&input, &BooleanArray::from(mask))
                .context(ComputeArrowSnafu)?;

        if filtered_batch.num_rows() > 0 {
            Ok(Some(filtered_batch))
        } else {
            Ok(None)
        }
    }

    /// Computes the filter mask for the input RecordBatch based on pushed down predicates.
    ///
    /// Returns `None` if the entire batch is filtered out, otherwise returns the boolean mask.
    pub(crate) fn compute_filter_mask_flat(&self, input: &RecordBatch) -> Result<Option<BooleanBuffer>> {
        let mut mask = BooleanBuffer::new_set(input.num_rows());

        let flat_format = self
            .read_format
            .as_flat()
            .context(crate::error::UnexpectedSnafu {
                reason: "Expected flat format for precise_filter_flat",
            })?;

        // Decodes primary keys once if we have any tag filters not in projection
        let mut decoded_pks: Option<DecodedPrimaryKeys> = None;
        // Cache decoded tag arrays by column id to avoid redundant decoding
        let mut decoded_tag_cache: HashMap<ColumnId, ArrayRef> = HashMap::new();

        // Run filter one by one and combine them result
        for filter_ctx in &self.filters {
            let filter = match filter_ctx.filter() {
                MaybeFilter::Filter(f) => f,
                // Column matches.
                MaybeFilter::Matched => continue,
                // Column doesn't match, filter the entire batch.
                MaybeFilter::Pruned => return Ok(None),
            };

            // Get the column directly by its projected index
            let column_idx = flat_format.projected_index_by_id(filter_ctx.column_id());
            if let Some(idx) = column_idx {
                let column = &input.columns()[idx];
                let result = filter.evaluate_array(column).context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            } else if filter_ctx.semantic_type() == SemanticType::Tag {
                // Column not found in projection, it may be a tag column.
                // Decodes primary keys if not already decoded.
                if decoded_pks.is_none() {
                    decoded_pks = Some(decode_primary_keys(self.codec.as_ref(), input)?);
                }

                let metadata = flat_format.metadata();
                let column_id = filter_ctx.column_id();

                // Check cache first
                let tag_column = if let Some(cached_column) = decoded_tag_cache.get(&column_id) {
                    cached_column.clone()
                } else {
                    // For dense encoding, we need pk_index. For sparse encoding, pk_index is None.
                    let pk_index = if self.codec.encoding() == PrimaryKeyEncoding::Sparse {
                        None
                    } else {
                        metadata.primary_key_index(column_id)
                    };
                    let column_index = metadata.column_index_by_id(column_id);

                    if let (Some(column_index), Some(decoded)) =
                        (column_index, decoded_pks.as_ref())
                    {
                        let column_metadata = &metadata.column_metadatas[column_index];
                        let tag_column = decoded.get_tag_column(
                            column_id,
                            pk_index,
                            &column_metadata.column_schema.data_type,
                        )?;
                        // Cache the decoded tag column
                        decoded_tag_cache.insert(column_id, tag_column.clone());
                        tag_column
                    } else {
                        continue;
                    }
                };

                let result = filter
                    .evaluate_array(&tag_column)
                    .context(RecordBatchSnafu)?;
                mask = mask.bitand(&result);
            }
            // Non-tag column not found in projection.
        }

        Ok(Some(mask))
    }

    /// Filters the input RecordBatch by table id filters if the encoding is sparse.
    pub(crate) fn filter_sparse_table_id(&self, input: RecordBatch) -> Result<Option<RecordBatch>> {
        if self.codec.encoding() != PrimaryKeyEncoding::Sparse {
            return Ok(Some(input));
        }

        let table_id_column_id = ReservedColumnId::table_id();
        let has_table_id_filter = self
            .filters
            .iter()
            .any(|filter_ctx| filter_ctx.column_id() == table_id_column_id);

        // No table_id filters, returns the input as-is
        if !has_table_id_filter {
            return Ok(Some(input));
        }

        let primary_key_index = primary_key_column_index(input.num_columns());
        let pk_dict_array = input
            .column(primary_key_index)
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .context(InvalidRecordBatchSnafu {
                reason: "Primary key column is not a dictionary array",
            })?;
        let pk_values_array = pk_dict_array
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        // Decodes leftmost value (table_id) for each row.
        let num_pk_values = pk_dict_array.len();
        let mut decoded_table_ids = Vec::with_capacity(num_pk_values);
        for i in 0..num_pk_values {
            // Primary key array should not contain null values.
            let key = pk_dict_array.keys().value(i) as usize;
            let pk_bytes = pk_values_array.value(key);
            let table_id_opt = self.codec.decode_leftmost(pk_bytes).context(DecodeSnafu)?;
            // Extract u32 value from Value::UInt32
            let table_id = match table_id_opt {
                Some(Value::UInt32(val)) => val,
                _ => panic!("Expect table id, found: {:?}", table_id_opt),
            };
            decoded_table_ids.push(table_id);
        }
        let table_id_array = Arc::new(UInt32Array::from(decoded_table_ids)) as _;

        // Build a boolean mask by applying table_id filters
        let mut mask = BooleanBuffer::new_set(input.num_rows());
        for filter_ctx in &self.filters {
            // Skip non-table_id filters
            if filter_ctx.column_id() != table_id_column_id {
                continue;
            }

            let filter = match filter_ctx.filter() {
                MaybeFilter::Filter(f) => f,
                // Column matches.
                MaybeFilter::Matched => continue,
                // Column doesn't match, filter the entire batch.
                MaybeFilter::Pruned => return Ok(None),
            };

            // Evaluate filter on the table_id array
            let result = filter
                .evaluate_array(&table_id_array)
                .context(RecordBatchSnafu)?;

            mask = mask.bitand(&result);
        }

        // Filter the batch by the mask
        let filtered_batch =
            datatypes::arrow::compute::filter_record_batch(&input, &BooleanArray::from(mask))
                .context(ComputeArrowSnafu)?;

        if filtered_batch.num_rows() > 0 {
            Ok(Some(filtered_batch))
        } else {
            Ok(None)
        }
    }
}
