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

use std::collections::VecDeque;
use std::ops::BitAnd;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow::array::{BooleanArray, Scalar, UInt64Array};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;
use store_api::storage::SequenceNumber;

use crate::error::{self, ComputeArrowSnafu};
use crate::memtable::bulk::context::BulkIterContextRef;
use crate::memtable::bulk::row_group_reader::{
    MemtableRowGroupReader, MemtableRowGroupReaderBuilder,
};
use crate::read::Batch;
use crate::sst::parquet::reader::MaybeFilter;

/// Iterator for reading data inside a bulk part.
pub struct EncodedBulkPartIter {
    row_groups_to_read: VecDeque<usize>,
    current_reader: Option<PruneReader>,
    builder: MemtableRowGroupReaderBuilder,
    sequence: Option<SequenceNumber>,
}

impl EncodedBulkPartIter {
    /// Creates a new [BulkPartIter].
    pub(crate) fn try_new(
        context: BulkIterContextRef,
        mut row_groups_to_read: VecDeque<usize>,
        parquet_meta: Arc<ParquetMetaData>,
        data: Bytes,
        sequence: Option<SequenceNumber>,
    ) -> error::Result<Self> {
        let projection_mask = ProjectionMask::roots(
            parquet_meta.file_metadata().schema_descr(),
            context.read_format().projection_indices().iter().copied(),
        );

        let builder = MemtableRowGroupReaderBuilder::try_new(
            context.clone(),
            projection_mask,
            parquet_meta,
            data,
        )?;

        let init_reader = row_groups_to_read
            .pop_front()
            .map(|first_row_group| builder.build_row_group_reader(first_row_group, None))
            .transpose()?
            .map(|r| PruneReader::new(context, r));
        Ok(Self {
            row_groups_to_read,
            current_reader: init_reader,
            builder,
            sequence,
        })
    }

    pub(crate) fn next_batch(&mut self) -> error::Result<Option<Batch>> {
        let Some(current) = &mut self.current_reader else {
            // All row group exhausted.
            return Ok(None);
        };

        if let Some(mut batch) = current.next_batch()? {
            batch.filter_by_sequence(self.sequence)?;
            return Ok(Some(batch));
        }

        // Previous row group exhausted, read next row group
        while let Some(next_row_group) = self.row_groups_to_read.pop_front() {
            current.reset(self.builder.build_row_group_reader(next_row_group, None)?);
            if let Some(mut next_batch) = current.next_batch()? {
                next_batch.filter_by_sequence(self.sequence)?;
                return Ok(Some(next_batch));
            }
        }
        Ok(None)
    }
}

impl Iterator for EncodedBulkPartIter {
    type Item = error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

struct PruneReader {
    context: BulkIterContextRef,
    row_group_reader: MemtableRowGroupReader,
}

//todo(hl): maybe we also need to support lastrow mode here.
impl PruneReader {
    fn new(context: BulkIterContextRef, reader: MemtableRowGroupReader) -> Self {
        Self {
            context,
            row_group_reader: reader,
        }
    }

    /// Iterates current inner reader until exhausted.
    fn next_batch(&mut self) -> error::Result<Option<Batch>> {
        while let Some(b) = self.row_group_reader.next_inner()? {
            match self.prune(b)? {
                Some(b) => {
                    return Ok(Some(b));
                }
                None => {
                    continue;
                }
            }
        }
        Ok(None)
    }

    /// Prunes batch according to filters.
    fn prune(&mut self, batch: Batch) -> error::Result<Option<Batch>> {
        //todo(hl): add metrics.

        // fast path
        if self.context.base.filters.is_empty() {
            return Ok(Some(batch));
        }

        let Some(batch_filtered) = self.context.base.precise_filter(batch)? else {
            // the entire batch is filtered out
            return Ok(None);
        };
        if !batch_filtered.is_empty() {
            Ok(Some(batch_filtered))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self, reader: MemtableRowGroupReader) {
        self.row_group_reader = reader;
    }
}

/// Iterator that reads data from a single RecordBatch.
/// Supports projection, pruning, and sequence filtering similar to BulkPartIter.
pub struct RecordBatchIter {
    /// The RecordBatch to read from
    record_batch: Option<RecordBatch>,
    /// Iterator context for filtering and conversion
    context: BulkIterContextRef,
    /// Buffer for converted batches
    batch_buffer: VecDeque<Batch>,
    /// Sequence number filter
    sequence: Option<SequenceNumber>,
}

impl RecordBatchIter {
    /// Creates a new RecordBatchIter from a single RecordBatch.
    ///
    /// # Arguments
    /// * `record_batch` - The RecordBatch to iterate over
    /// * `context` - Context for filtering and conversion
    /// * `sequence` - Optional sequence filter
    pub fn new(
        record_batch: RecordBatch,
        context: BulkIterContextRef,
        sequence: Option<SequenceNumber>,
    ) -> Self {
        assert!(context.read_format().as_primary_key().is_some());

        Self {
            record_batch: Some(record_batch),
            context,
            batch_buffer: VecDeque::new(),
            sequence,
        }
    }

    /// Applies projection to the RecordBatch if needed.
    fn apply_projection(&self, record_batch: RecordBatch) -> error::Result<RecordBatch> {
        // Check if projection is needed
        let projection_indices = self.context.read_format().projection_indices();
        if projection_indices.len() == record_batch.num_columns() {
            // No projection needed if all columns are selected
            return Ok(record_batch);
        }

        // Apply projection by selecting only the required columns
        record_batch
            .project(projection_indices)
            .context(ComputeArrowSnafu)
    }

    /// Prunes batch according to filters, similar to PruneReader.
    fn prune(&self, batch: Batch) -> error::Result<Option<Batch>> {
        // Fast path: if no filters are configured, return the batch as-is
        if self.context.base.filters.is_empty() {
            return Ok(Some(batch));
        }

        // Apply precise filtering using the context's base filters
        let batch_filtered = self.context.base.precise_filter(batch)?;

        match batch_filtered {
            Some(batch) if !batch.is_empty() => Ok(Some(batch)),
            _ => Ok(None), // Entire batch filtered out or empty
        }
    }

    /// Processes the RecordBatch and fills the batch buffer.
    fn process_record_batch(&mut self, record_batch: RecordBatch) -> error::Result<()> {
        if record_batch.num_rows() == 0 {
            return Ok(());
        }

        // Applies projection
        let record_batch = self.apply_projection(record_batch)?;

        // Converts to Batch format and apply pruning and sequence filtering
        // Converts RecordBatch to Batch format using the context's read format
        let mut batches = VecDeque::new();
        // Safety: We checked the format type in new().
        self.context
            .read_format()
            .as_primary_key()
            .unwrap()
            .convert_record_batch(&record_batch, None, &mut batches)?;

        // Applies pruning and sequence filtering to each converted batch
        for batch in batches {
            // Applies precise filtering (pruning) first, similar to PruneReader
            let pruned_batch = self.prune(batch)?;

            if let Some(mut batch) = pruned_batch {
                // Applies sequence filtering after pruning
                if let Some(sequence) = self.sequence {
                    batch.filter_by_sequence(Some(sequence))?;
                }

                // Only adds non-empty batches
                if batch.num_rows() > 0 {
                    self.batch_buffer.push_back(batch);
                }
            }
        }

        Ok(())
    }
}

impl Iterator for RecordBatchIter {
    type Item = error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        // Returns buffered batch if available
        if let Some(batch) = self.batch_buffer.pop_front() {
            return Some(Ok(batch));
        }

        // Processes RecordBatch if not yet processed
        if let Some(record_batch) = self.record_batch.take() {
            match self.process_record_batch(record_batch) {
                Ok(()) => {
                    // Tries to return a batch from the buffer
                    if let Some(batch) = self.batch_buffer.pop_front() {
                        return Some(Ok(batch));
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // No more batches available
        None
    }
}

/// Iterator similar to RecordBatchIter but returns RecordBatch directly instead of Batch.
/// Uses non-encoded primary key columns for filtering instead of encoded ones.
pub struct BulkPartRecordBatchIter {
    /// The RecordBatch to read from
    record_batch: Option<RecordBatch>,
    /// Iterator context for filtering
    context: BulkIterContextRef,
    /// Sequence number filter.
    sequence: Option<Scalar<UInt64Array>>,
}

impl BulkPartRecordBatchIter {
    /// Creates a new [BulkPartRecordBatchIter] from a RecordBatch and non-encoded primary key columns.
    ///
    /// # Arguments
    /// * `record_batch` - The RecordBatch to iterate over
    /// * `context` - Context for filtering
    /// * `sequence` - Optional sequence filter
    pub fn new(
        record_batch: RecordBatch,
        context: BulkIterContextRef,
        sequence: Option<SequenceNumber>,
    ) -> Self {
        assert!(context.read_format().as_flat().is_some());

        let sequence = sequence.map(|seq| UInt64Array::new_scalar(seq));

        Self {
            record_batch: Some(record_batch),
            context,
            sequence,
        }
    }

    /// Applies projection to the RecordBatch if needed.
    fn apply_projection(&self, record_batch: RecordBatch) -> error::Result<RecordBatch> {
        let projection_indices = self.context.read_format().projection_indices();
        if projection_indices.len() == record_batch.num_columns() {
            return Ok(record_batch);
        }

        record_batch
            .project(projection_indices)
            .context(ComputeArrowSnafu)
    }

    /// Applies both predicate filtering and sequence filtering in a single pass.
    /// Returns None if the filtered batch is empty.
    fn apply_combined_filters(
        &self,
        record_batch: RecordBatch,
    ) -> error::Result<Option<RecordBatch>> {
        let num_rows = record_batch.num_rows();
        let mut combined_filter = None;

        // First, apply predicate filters.
        if !self.context.base.filters.is_empty() {
            let num_rows = record_batch.num_rows();
            let mut mask = BooleanBuffer::new_set(num_rows);

            // Run filter one by one and combine them result, similar to RangeBase::precise_filter
            for filter_ctx in &self.context.base.filters {
                let filter = match filter_ctx.filter() {
                    MaybeFilter::Filter(f) => f,
                    // Column matches.
                    MaybeFilter::Matched => continue,
                    // Column doesn't match, filter the entire batch.
                    MaybeFilter::Pruned => return Ok(None),
                };

                // Safety: We checked the format type in new().
                let Some(column_index) = self
                    .context
                    .read_format()
                    .as_flat()
                    .unwrap()
                    .sst_column_index_by_id(filter_ctx.column_id())
                else {
                    continue;
                };
                let array = record_batch.column(column_index);
                let result = filter
                    .evaluate_array(&array)
                    .context(crate::error::RecordBatchSnafu)?;

                mask = mask.bitand(&result);
            }
            // Convert the mask to BooleanArray
            combined_filter = Some(BooleanArray::from(mask));
        }

        // Filters rows by the given `sequence`. Only preserves rows with sequence less than or equal to `sequence`.
        if let Some(sequence) = &self.sequence {
            let sequence_filter =
                datatypes::arrow::compute::kernels::cmp::lt_eq(sequence, sequence)
                    .context(ComputeArrowSnafu)?;
            // Combine with existing filter using AND operation
            combined_filter = match combined_filter {
                None => Some(sequence_filter),
                Some(existing_filter) => {
                    let and_result =
                        datatypes::arrow::compute::and(&existing_filter, &sequence_filter)
                            .context(ComputeArrowSnafu)?;
                    Some(and_result)
                }
            };
        }

        // Apply the combined filter if any filters were applied
        let Some(filter_array) = combined_filter else {
            // No filters applied, return original batch
            return Ok(Some(record_batch));
        };
        let select_count = filter_array.true_count();
        if select_count == 0 {
            return Ok(None);
        }
        if select_count == num_rows {
            return Ok(Some(record_batch));
        }
        let filtered_batch =
            datatypes::arrow::compute::filter_record_batch(&record_batch, &filter_array)
                .context(ComputeArrowSnafu)?;

        if filtered_batch.num_rows() == 0 {
            return Ok(None);
        }

        Ok(Some(filtered_batch))
    }

    fn process_batch(&mut self, record_batch: RecordBatch) -> error::Result<Option<RecordBatch>> {
        // Apply combined filtering (both predicate and sequence filters)
        let Some(filtered_batch) = self.apply_combined_filters(record_batch)? else {
            return Ok(None);
        };
        // Apply projection last
        let projected_batch = self.apply_projection(filtered_batch)?;

        Ok(Some(projected_batch))
    }
}

impl Iterator for BulkPartRecordBatchIter {
    type Item = error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let record_batch = self.record_batch.take()?;

        self.process_batch(record_batch).transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array, UInt8Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;
    use table::predicate::Predicate;

    use super::*;
    use crate::memtable::bulk::context::BulkIterContext;

    #[test]
    fn test_record_batch_iter_basic() {
        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));

        // Create test data
        let field1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let timestamp = Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000, 3000],
        ));

        // Create primary key dictionary array
        use datatypes::arrow::array::{BinaryArray, DictionaryArray, UInt32Array};
        let values = Arc::new(BinaryArray::from_iter_values([b"key1", b"key2", b"key3"]));
        let keys = UInt32Array::from(vec![0, 1, 2]);
        let primary_key = Arc::new(DictionaryArray::new(keys, values));

        let sequence = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1])); // PUT operations

        let record_batch = RecordBatch::try_new(
            schema,
            vec![field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap();

        // Create a minimal region metadata for testing
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);

        let region_metadata = builder.build().unwrap();

        // Create context
        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata),
            &None, // No projection
            None,  // No predicate
            false,
        ));

        // Create iterator
        let mut iter = RecordBatchIter::new(record_batch, context, None);

        // Test iteration
        let batch = iter.next();
        assert!(batch.is_some(), "Should return at least one batch");

        let batch = batch.unwrap().unwrap();
        assert!(batch.num_rows() > 0, "Batch should have rows");
    }

    #[test]
    fn test_record_batch_iter_with_sequence_filter() {
        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));

        // Create test data with different sequence numbers
        let field1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let timestamp = Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000, 3000],
        ));

        // Create primary key dictionary array
        use datatypes::arrow::array::{BinaryArray, DictionaryArray, UInt32Array};
        let values = Arc::new(BinaryArray::from_iter_values([b"key1", b"key2", b"key3"]));
        let keys = UInt32Array::from(vec![0, 1, 2]);
        let primary_key = Arc::new(DictionaryArray::new(keys, values));

        let sequence = Arc::new(UInt64Array::from(vec![1, 5, 10])); // Different sequences
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1])); // PUT operations

        let record_batch = RecordBatch::try_new(
            schema,
            vec![field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap();

        // Create a minimal region metadata for testing
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);

        let region_metadata = builder.build().unwrap();

        // Create context
        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata),
            &None, // No projection
            None,  // No predicate
            false,
        ));

        // Create iterator with sequence filter (only include sequences <= 5)
        let mut iter = RecordBatchIter::new(record_batch, context, Some(5));

        // Test iteration - should filter out sequence 10
        let mut total_rows = 0;
        while let Some(batch_result) = iter.next() {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        // Should have fewer rows due to sequence filtering
        assert!(
            total_rows <= 3,
            "Should filter out some rows based on sequence"
        );
    }

    #[test]
    fn test_record_batch_iter_with_pruning() {
        use datafusion_common::ScalarValue;
        use datafusion_expr::{col, lit, Expr};
        use table::predicate::Predicate;

        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));

        // Create test data where field1 has values 1, 2, 3
        let field1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let timestamp = Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000, 3000],
        ));

        // Create primary key dictionary array
        use datatypes::arrow::array::{BinaryArray, DictionaryArray, UInt32Array};
        let values = Arc::new(BinaryArray::from_iter_values([b"key1", b"key2", b"key3"]));
        let keys = UInt32Array::from(vec![0, 1, 2]);
        let primary_key = Arc::new(DictionaryArray::new(keys, values));

        let sequence = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1])); // PUT operations

        let record_batch = RecordBatch::try_new(
            schema,
            vec![field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap();

        // Create a minimal region metadata for testing
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);

        let region_metadata = builder.build().unwrap();

        // Create a predicate: field1 > 1 (should filter out the first row)
        let predicate_expr: Expr = col("field1").gt(lit(ScalarValue::Int64(Some(1))));
        let predicate = Predicate::new(vec![predicate_expr]);

        // Create context with predicate
        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata),
            &None,           // No projection
            Some(predicate), // With predicate
            false,
        ));

        // Create iterator
        let mut iter = RecordBatchIter::new(record_batch, context, None);

        // Test iteration - should have fewer rows due to pruning
        let mut total_rows = 0;
        while let Some(batch_result) = iter.next() {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        // Should have fewer than 3 rows due to pruning (field1 > 1 filters out first row)
        assert!(
            total_rows < 3,
            "Should filter out some rows based on predicate, got {} rows",
            total_rows
        );
    }

    #[test]
    fn test_bulk_part_record_batch_iter() {
        // Create a simple schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("key1", DataType::Utf8, false),
            Field::new("field1", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "__primary_key",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
                false,
            ),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]));

        // Create test data
        let key1 = Arc::new(StringArray::from_iter_values(["key1", "key2", "key3"]));
        let field1 = Arc::new(Int64Array::from(vec![11, 12, 13]));
        let timestamp = Arc::new(datatypes::arrow::array::TimestampMillisecondArray::from(
            vec![1000, 2000, 3000],
        ));

        // Create primary key dictionary array
        use datatypes::arrow::array::{BinaryArray, DictionaryArray, UInt32Array};
        let values = Arc::new(BinaryArray::from_iter_values([b"key1", b"key2", b"key3"]));
        let keys = UInt32Array::from(vec![0, 1, 2]);
        let primary_key = Arc::new(DictionaryArray::new(keys, values));

        let sequence = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1])); // PUT operations

        let record_batch = RecordBatch::try_new(
            schema,
            vec![key1, field1, timestamp, primary_key, sequence, op_type],
        )
        .unwrap();

        // Create a minimal region metadata for testing
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "key1",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::int64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![0]);

        let region_metadata = builder.build().unwrap();

        // Create context
        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata.clone()),
            &None, // No projection
            None,  // No predicate
            true,
        ));
        // Iterates all rows.
        let iter = BulkPartRecordBatchIter::new(record_batch.clone(), context.clone(), None);
        let result: Vec<_> = iter.map(|rb| rb.unwrap()).collect();
        assert_eq!(1, result.len());
        assert_eq!(3, result[0].num_rows());
        assert_eq!(5, result[0].num_columns(),);

        // Creates iter with sequence filter (only include sequences <= 2)
        let iter = BulkPartRecordBatchIter::new(record_batch.clone(), context, Some(2));
        let result: Vec<_> = iter.map(|rb| rb.unwrap()).collect();
        assert_eq!(1, result.len());
        assert_eq!(2, result[0].num_rows());
        let expect_sequence = Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef;
        assert_eq!(
            &expect_sequence,
            result[0].column(result[0].num_columns() - 2)
        );
        assert_eq!(5, result[0].num_columns());

        let context = Arc::new(BulkIterContext::new(
            Arc::new(region_metadata),
            &Some(&[1, 2]),
            Some(Predicate::new(vec![col("key1").eq(lit("key2"))])),
            true,
        ));
        // Creates iter with projection and predicate.
        let iter = BulkPartRecordBatchIter::new(record_batch.clone(), context.clone(), None);
        let result: Vec<_> = iter.map(|rb| rb.unwrap()).collect();
        assert_eq!(1, result.len());
        assert_eq!(1, result[0].num_rows());
        assert_eq!(4, result[0].num_columns());
        let expect_field = Arc::new(Int64Array::from(vec![12])) as ArrayRef;
        assert_eq!(&expect_field, result[0].column(result[0].num_columns() - 2));
    }
}
