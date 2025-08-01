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

//! Batch without an encoded primary key.

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::OpType;
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt64Array, UInt8Array};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::{
    ComputeArrowSnafu, CreateDefaultSnafu, InvalidRequestSnafu, NewRecordBatchSnafu, Result,
    UnexpectedSnafu,
};

/// Number of columns that have fixed positions.
///
/// Contains all internal columns.
pub(crate) const PLAIN_FIXED_POS_COLUMN_NUM: usize = 2;

/// [PlainBatch] represents a batch of rows.
/// It is a wrapper around [RecordBatch].
///
/// The columns order is the same as the order of the columns read from the SST.
/// It always contains two internal columns now. We may change modify this behavior
/// in the future.
#[derive(Debug)]
pub struct PlainBatch {
    /// The original record batch.
    record_batch: RecordBatch,
}

impl PlainBatch {
    /// Creates a new [PlainBatch] from a [RecordBatch].
    pub fn new(record_batch: RecordBatch) -> Self {
        assert!(
            record_batch.num_columns() >= 2,
            "record batch missing internal columns, num_columns: {}",
            record_batch.num_columns()
        );

        Self { record_batch }
    }

    /// Returns a new [PlainBatch] with the given columns.
    pub fn with_new_columns(&self, columns: Vec<ArrayRef>) -> Result<Self> {
        let record_batch = RecordBatch::try_new(self.record_batch.schema(), columns)
            .context(NewRecordBatchSnafu)?;
        Ok(Self::new(record_batch))
    }

    /// Returns the number of columns in the batch.
    pub fn num_columns(&self) -> usize {
        self.record_batch.num_columns()
    }

    /// Returns the number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.record_batch.num_rows()
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Returns all columns.
    pub fn columns(&self) -> &[ArrayRef] {
        self.record_batch.columns()
    }

    /// Returns the array of column at index `idx`.
    pub fn column(&self, idx: usize) -> &ArrayRef {
        self.record_batch.column(idx)
    }

    /// Returns the slice of internal columns.
    pub fn internal_columns(&self) -> &[ArrayRef] {
        &self.record_batch.columns()[self.record_batch.num_columns() - PLAIN_FIXED_POS_COLUMN_NUM..]
    }

    /// Returns the inner record batch.
    pub fn as_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    /// Converts this batch into a record batch.
    pub fn into_record_batch(self) -> RecordBatch {
        self.record_batch
    }

    /// Filters this batch by the boolean array.
    pub fn filter(&self, predicate: &BooleanArray) -> Result<Self> {
        let record_batch =
            filter_record_batch(&self.record_batch, predicate).context(ComputeArrowSnafu)?;
        Ok(Self::new(record_batch))
    }

    /// Returns the column index of the sequence column.
    #[allow(dead_code)]
    pub(crate) fn sequence_column_index(&self) -> usize {
        self.record_batch.num_columns() - PLAIN_FIXED_POS_COLUMN_NUM
    }
}

/// Helper struct to fill default values and internal columns.
pub struct ColumnFiller<'a> {
    /// Region metadata information
    metadata: &'a RegionMetadata,
    /// Schema for the output record batch
    schema: SchemaRef,
    /// Map of column names to indices in the input record batch
    name_to_index: HashMap<String, usize>,
}

impl<'a> ColumnFiller<'a> {
    /// Creates a new ColumnFiller
    /// The `schema` is the sst schema of the `metadata`.
    pub fn new(
        metadata: &'a RegionMetadata,
        schema: SchemaRef,
        record_batch: &RecordBatch,
    ) -> Self {
        debug_assert_eq!(metadata.column_metadatas.len() + 2, schema.fields().len());

        // Pre-construct the name to index map
        let name_to_index: HashMap<_, _> = record_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| (field.name().clone(), i))
            .collect();

        Self {
            metadata,
            schema,
            name_to_index,
        }
    }

    /// Fills default values and internal columns for a [RecordBatch].
    pub fn fill_missing_columns(
        &self,
        record_batch: &RecordBatch,
        sequence: SequenceNumber,
        op_type: OpType,
    ) -> Result<RecordBatch> {
        let num_rows = record_batch.num_rows();
        let mut new_columns =
            Vec::with_capacity(record_batch.num_columns() + PLAIN_FIXED_POS_COLUMN_NUM);

        // Fills default values.
        // Implementation based on `WriteRequest::fill_missing_columns()`.
        for column in &self.metadata.column_metadatas {
            let array = match self.name_to_index.get(&column.column_schema.name) {
                Some(index) => record_batch.column(*index).clone(),
                None => match op_type {
                    OpType::Put => {
                        // For put requests, we use the default value from column schema.
                        fill_column_put_default(self.metadata.region_id, column, num_rows)?
                    }
                    OpType::Delete => {
                        // For delete requests, we need default value for padding.
                        fill_column_delete_default(column, num_rows)?
                    }
                },
            };

            new_columns.push(array);
        }

        // Adds internal columns.
        // Adds the sequence number.
        let sequence_array = Arc::new(UInt64Array::from(vec![sequence; num_rows]));
        // Adds the op type.
        let op_type_array = Arc::new(UInt8Array::from(vec![op_type as u8; num_rows]));
        new_columns.push(sequence_array);
        new_columns.push(op_type_array);

        RecordBatch::try_new(self.schema.clone(), new_columns).context(NewRecordBatchSnafu)
    }
}

fn fill_column_put_default(
    region_id: RegionId,
    column: &ColumnMetadata,
    num_rows: usize,
) -> Result<ArrayRef> {
    if column.column_schema.is_default_impure() {
        return UnexpectedSnafu {
            reason: format!(
                "unexpected impure default value with region_id: {}, column: {}, default_value: {:?}",
                region_id,
                column.column_schema.name,
                column.column_schema.default_constraint(),
            ),
        }
        .fail();
    }
    let vector = column
        .column_schema
        .create_default_vector(num_rows)
        .context(CreateDefaultSnafu {
            region_id,
            column: &column.column_schema.name,
        })?
        // This column doesn't have default value.
        .with_context(|| InvalidRequestSnafu {
            region_id,
            reason: format!(
                "column {} does not have default value",
                column.column_schema.name
            ),
        })?;
    Ok(vector.to_arrow_array())
}

fn fill_column_delete_default(column: &ColumnMetadata, num_rows: usize) -> Result<ArrayRef> {
    // For delete requests, we need a default value for padding
    let vector = column
        .column_schema
        .create_default_vector_for_padding(num_rows);
    Ok(vector.to_arrow_array())
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::arrow::array::{
        Float64Array, Int32Array, StringArray, TimestampMillisecondArray,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datatypes::schema::constraint::ColumnDefaultConstraint;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME};
    use store_api::storage::{ConcreteDataType, RegionId};

    use super::*;
    use crate::sst::to_plain_sst_arrow_schema;

    /// Creates a test region metadata with schema: k0(string), ts(timestamp), v1(float64)
    fn create_test_region_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(100, 200));
        builder
            // Add string key column
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false)
                    .with_default_constraint(None)
                    .unwrap(),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            // Add timestamp column
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true)
                .with_default_constraint(None)
                .unwrap(),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            // Add float value column with default
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true)
                    .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Float64(
                        datatypes::value::OrderedFloat::from(42.0),
                    ))))
                    .unwrap(),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .primary_key(vec![0]);

        builder.build().unwrap()
    }

    #[test]
    fn test_column_filler_put() {
        let region_metadata = create_test_region_metadata();
        let output_schema = to_plain_sst_arrow_schema(&region_metadata);

        // Create input record batch with only k0 and ts columns (v1 is missing)
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let k0_values: ArrayRef = Arc::new(StringArray::from(vec!["key1", "key2"]));
        let ts_values: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1000, 2000]));

        let input_batch =
            RecordBatch::try_new(input_schema, vec![k0_values.clone(), ts_values.clone()]).unwrap();

        // Create column filler
        let filler = ColumnFiller::new(&region_metadata, output_schema.clone(), &input_batch);

        // Fill missing columns with OpType::Put
        let result = filler
            .fill_missing_columns(&input_batch, 100, OpType::Put)
            .unwrap();

        // Verify the result
        // Create an expected record batch to compare against
        let expected_columns = vec![
            k0_values.clone(),
            ts_values.clone(),
            Arc::new(Float64Array::from(vec![42.0, 42.0])),
            Arc::new(UInt64Array::from(vec![100, 100])),
            Arc::new(UInt8Array::from(vec![OpType::Put as u8, OpType::Put as u8])),
        ];
        let expected_batch = RecordBatch::try_new(output_schema.clone(), expected_columns).unwrap();
        assert_eq!(expected_batch, result);
    }

    #[test]
    fn test_column_filler_delete() {
        let region_metadata = create_test_region_metadata();
        let output_schema = to_plain_sst_arrow_schema(&region_metadata);

        // Create input record batch with only k0 and ts columns (v1 is missing)
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let k0_values: ArrayRef = Arc::new(StringArray::from(vec!["key1", "key2"]));
        let ts_values: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1000, 2000]));

        let input_batch =
            RecordBatch::try_new(input_schema, vec![k0_values.clone(), ts_values.clone()]).unwrap();

        // Create column filler
        let filler = ColumnFiller::new(&region_metadata, output_schema.clone(), &input_batch);

        // Fill missing columns with OpType::Delete
        let result = filler
            .fill_missing_columns(&input_batch, 200, OpType::Delete)
            .unwrap();

        // Verify the result by creating an expected record batch to compare against
        let v1_default = Arc::new(Float64Array::from(vec![None, None]));
        let expected_columns = vec![
            k0_values.clone(),
            ts_values.clone(),
            v1_default,
            Arc::new(UInt64Array::from(vec![200, 200])),
            Arc::new(UInt8Array::from(vec![
                OpType::Delete as u8,
                OpType::Delete as u8,
            ])),
        ];
        let expected_batch = RecordBatch::try_new(output_schema.clone(), expected_columns).unwrap();
        assert_eq!(expected_batch, result);
    }

    fn create_test_record_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Utf8, false),
            Field::new(SEQUENCE_COLUMN_NAME, DataType::UInt64, false),
            Field::new(OP_TYPE_COLUMN_NAME, DataType::UInt8, false),
        ]));

        let col1 = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let col2 = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let sequence = Arc::new(UInt64Array::from(vec![100, 101, 102]));
        let op_type = Arc::new(UInt8Array::from(vec![1, 1, 1]));

        RecordBatch::try_new(schema, vec![col1, col2, sequence, op_type]).unwrap()
    }

    #[test]
    fn test_plain_batch_basic_methods() {
        let record_batch = create_test_record_batch();
        let plain_batch = PlainBatch::new(record_batch.clone());

        // Test basic properties
        assert_eq!(plain_batch.num_columns(), 4);
        assert_eq!(plain_batch.num_rows(), 3);
        assert!(!plain_batch.is_empty());
        assert_eq!(plain_batch.columns().len(), 4);

        // Test internal columns access
        let internal_columns = plain_batch.internal_columns();
        assert_eq!(internal_columns.len(), PLAIN_FIXED_POS_COLUMN_NUM);
        assert_eq!(internal_columns[0].len(), 3);
        assert_eq!(internal_columns[1].len(), 3);

        // Test column access
        let col1 = plain_batch.column(0);
        assert_eq!(col1.len(), 3);
        assert_eq!(
            col1.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            1
        );

        // Test sequence column index
        assert_eq!(plain_batch.sequence_column_index(), 2);

        // Test to record batch.
        assert_eq!(record_batch, *plain_batch.as_record_batch());
        assert_eq!(record_batch, plain_batch.into_record_batch());
    }

    #[test]
    fn test_with_new_columns() {
        let record_batch = create_test_record_batch();
        let plain_batch = PlainBatch::new(record_batch);

        // Create new columns
        let col1 = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let col2 = Arc::new(StringArray::from(vec!["x", "y", "z"]));
        let sequence = Arc::new(UInt64Array::from(vec![200, 201, 202]));
        let op_type = Arc::new(UInt8Array::from(vec![0, 0, 0]));

        let new_batch = plain_batch
            .with_new_columns(vec![col1, col2, sequence, op_type])
            .unwrap();

        assert_eq!(new_batch.num_columns(), 4);
        assert_eq!(new_batch.num_rows(), 3);
        assert_eq!(
            new_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            new_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "x"
        );
    }

    #[test]
    fn test_filter() {
        let record_batch = create_test_record_batch();
        let plain_batch = PlainBatch::new(record_batch);

        // Create a predicate that selects the first and third rows
        let predicate = BooleanArray::from(vec![true, false, true]);

        let filtered_batch = plain_batch.filter(&predicate).unwrap();

        assert_eq!(filtered_batch.num_rows(), 2);
        assert_eq!(
            filtered_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            filtered_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            3
        );
    }
}
