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
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;
use store_api::storage::SequenceNumber;

use crate::error::{self, ComputeArrowSnafu, DecodeArrowRowGroupSnafu};
use crate::memtable::bulk::context::{BulkIterContext, BulkIterContextRef};
use crate::memtable::bulk::row_group_reader::MemtableRowGroupReaderBuilder;
use crate::sst::parquet::flat_format::sequence_column_index;
use crate::sst::parquet::reader::{MaybeFilter, RowGroupReaderContext};

/// Iterator for reading data inside a bulk part.
pub struct EncodedBulkPartIter {
    context: BulkIterContextRef,
    row_groups_to_read: VecDeque<usize>,
    current_reader: Option<ParquetRecordBatchReader>,
    builder: MemtableRowGroupReaderBuilder,
    /// Sequence number filter.
    sequence: Option<Scalar<UInt64Array>>,
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
        assert!(context.read_format().as_flat().is_some());

        let sequence = sequence.map(UInt64Array::new_scalar);

        let projection_mask = ProjectionMask::roots(
            parquet_meta.file_metadata().schema_descr(),
            context.read_format().projection_indices().iter().copied(),
        );
        let builder =
            MemtableRowGroupReaderBuilder::try_new(&context, projection_mask, parquet_meta, data)?;

        let init_reader = row_groups_to_read
            .pop_front()
            .map(|first_row_group| builder.build_row_group_reader(first_row_group, None))
            .transpose()?;
        Ok(Self {
            context,
            row_groups_to_read,
            current_reader: init_reader,
            builder,
            sequence,
        })
    }

    /// Fetches next non-empty record batch.
    pub(crate) fn next_record_batch(&mut self) -> error::Result<Option<RecordBatch>> {
        let Some(current) = &mut self.current_reader else {
            // All row group exhausted.
            return Ok(None);
        };

        for batch in current {
            let batch = batch.context(DecodeArrowRowGroupSnafu)?;
            if let Some(batch) = apply_combined_filters(&self.context, &self.sequence, batch)? {
                return Ok(Some(batch));
            }
        }

        // Previous row group exhausted, read next row group
        while let Some(next_row_group) = self.row_groups_to_read.pop_front() {
            let next_reader = self.builder.build_row_group_reader(next_row_group, None)?;
            let current = self.current_reader.insert(next_reader);

            for batch in current {
                let batch = batch.context(DecodeArrowRowGroupSnafu)?;
                if let Some(batch) = apply_combined_filters(&self.context, &self.sequence, batch)? {
                    return Ok(Some(batch));
                }
            }
        }

        Ok(None)
    }
}

impl Iterator for EncodedBulkPartIter {
    type Item = error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_record_batch().transpose()
    }
}

/// Iterator for a record batch in a bulk part.
pub struct BulkPartRecordBatchIter {
    /// The RecordBatch to read from
    record_batch: Option<RecordBatch>,
    /// Iterator context for filtering
    context: BulkIterContextRef,
    /// Sequence number filter.
    sequence: Option<Scalar<UInt64Array>>,
}

impl BulkPartRecordBatchIter {
    /// Creates a new [BulkPartRecordBatchIter] from a RecordBatch.
    pub fn new(
        record_batch: RecordBatch,
        context: BulkIterContextRef,
        sequence: Option<SequenceNumber>,
    ) -> Self {
        assert!(context.read_format().as_flat().is_some());

        let sequence = sequence.map(UInt64Array::new_scalar);

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

    fn process_batch(&mut self, record_batch: RecordBatch) -> error::Result<Option<RecordBatch>> {
        // Apply projection first.
        let projected_batch = self.apply_projection(record_batch)?;
        // Apply combined filtering (both predicate and sequence filters)
        let Some(filtered_batch) =
            apply_combined_filters(&self.context, &self.sequence, projected_batch)?
        else {
            return Ok(None);
        };

        Ok(Some(filtered_batch))
    }
}

impl Iterator for BulkPartRecordBatchIter {
    type Item = error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let record_batch = self.record_batch.take()?;

        self.process_batch(record_batch).transpose()
    }
}

/// Applies both predicate filtering and sequence filtering in a single pass.
/// Returns None if the filtered batch is empty.
///
/// # Panics
/// Panics if the format is not flat.
fn apply_combined_filters(
    context: &BulkIterContext,
    sequence: &Option<Scalar<UInt64Array>>,
    record_batch: RecordBatch,
) -> error::Result<Option<RecordBatch>> {
    // Converts the format to the flat format first.
    let format = context.read_format().as_flat().unwrap();
    let record_batch = format.convert_batch(record_batch, None)?;

    let num_rows = record_batch.num_rows();
    let mut combined_filter = None;

    // First, apply predicate filters.
    if !context.base.filters.is_empty() {
        let num_rows = record_batch.num_rows();
        let mut mask = BooleanBuffer::new_set(num_rows);

        // Run filter one by one and combine them result, similar to RangeBase::precise_filter
        for filter_ctx in &context.base.filters {
            let filter = match filter_ctx.filter() {
                MaybeFilter::Filter(f) => f,
                // Column matches.
                MaybeFilter::Matched => continue,
                // Column doesn't match, filter the entire batch.
                MaybeFilter::Pruned => return Ok(None),
            };

            // Safety: We checked the format type in new().
            let Some(column_index) = context
                .read_format()
                .as_flat()
                .unwrap()
                .projected_index_by_id(filter_ctx.column_id())
            else {
                continue;
            };
            let array = record_batch.column(column_index);
            let result = filter
                .evaluate_array(array)
                .context(crate::error::RecordBatchSnafu)?;

            mask = mask.bitand(&result);
        }
        // Convert the mask to BooleanArray
        combined_filter = Some(BooleanArray::from(mask));
    }

    // Filters rows by the given `sequence`. Only preserves rows with sequence less than or equal to `sequence`.
    if let Some(sequence) = sequence {
        let sequence_column =
            record_batch.column(sequence_column_index(record_batch.num_columns()));
        let sequence_filter =
            datatypes::arrow::compute::kernels::cmp::lt_eq(sequence_column, sequence)
                .context(ComputeArrowSnafu)?;
        // Combine with existing filter using AND operation
        combined_filter = match combined_filter {
            None => Some(sequence_filter),
            Some(existing_filter) => {
                let and_result = datatypes::arrow::compute::and(&existing_filter, &sequence_filter)
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

    Ok(Some(filtered_batch))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datafusion_expr::{col, lit};
    use datatypes::arrow::array::{ArrayRef, Int64Array, StringArray, UInt8Array, UInt64Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;
    use table::predicate::Predicate;

    use super::*;
    use crate::memtable::bulk::context::BulkIterContext;

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
            vec![
                key1,
                field1,
                timestamp,
                primary_key.clone(),
                sequence,
                op_type,
            ],
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
        let context = Arc::new(
            BulkIterContext::new(
                Arc::new(region_metadata.clone()),
                None, // No projection
                None, // No predicate
                false,
            )
            .unwrap(),
        );
        // Iterates all rows.
        let iter = BulkPartRecordBatchIter::new(record_batch.clone(), context.clone(), None);
        let result: Vec<_> = iter.map(|rb| rb.unwrap()).collect();
        assert_eq!(1, result.len());
        assert_eq!(3, result[0].num_rows());
        assert_eq!(6, result[0].num_columns(),);

        // Creates iter with sequence filter (only include sequences <= 2)
        let iter = BulkPartRecordBatchIter::new(record_batch.clone(), context, Some(2));
        let result: Vec<_> = iter.map(|rb| rb.unwrap()).collect();
        assert_eq!(1, result.len());
        let expect_sequence = Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef;
        assert_eq!(
            &expect_sequence,
            result[0].column(result[0].num_columns() - 2)
        );
        assert_eq!(6, result[0].num_columns());

        let context = Arc::new(
            BulkIterContext::new(
                Arc::new(region_metadata),
                Some(&[0, 2]),
                Some(Predicate::new(vec![col("key1").eq(lit("key2"))])),
                false,
            )
            .unwrap(),
        );
        // Creates iter with projection and predicate.
        let iter = BulkPartRecordBatchIter::new(record_batch.clone(), context.clone(), None);
        let result: Vec<_> = iter.map(|rb| rb.unwrap()).collect();
        assert_eq!(1, result.len());
        assert_eq!(1, result[0].num_rows());
        assert_eq!(5, result[0].num_columns());
        let expect_sequence = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        assert_eq!(
            &expect_sequence,
            result[0].column(result[0].num_columns() - 2)
        );
    }
}
