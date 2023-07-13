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

pub mod codec;
mod compat;

use std::collections::HashMap;

use common_recordbatch::RecordBatch;
use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::vectors::VectorRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{OpType, WriteRequest};

use crate::error::{
    BatchMissingColumnSnafu, CreateDefaultSnafu, CreateRecordBatchSnafu, Error, HasNullSnafu,
    MoreColumnThanExpectedSnafu, RequestTooLargeSnafu, Result, TypeMismatchSnafu,
    UnequalLengthsSnafu, UnknownColumnSnafu,
};

/// Max number of updates in a write batch.
pub(crate) const MAX_BATCH_SIZE: usize = 1_000_000;

/// Data of [WriteBatch].
///
/// We serialize this struct to the WAL instead of the whole `WriteBatch` to avoid
/// storing unnecessary information.
#[derive(Debug, PartialEq)]
pub struct Payload {
    /// Schema of the payload.
    ///
    /// This schema doesn't contain internal columns.
    pub schema: SchemaRef,
    pub mutations: Vec<Mutation>,
}

impl Payload {
    /// Creates a new payload with given `schema`.
    fn new(schema: SchemaRef) -> Payload {
        Payload {
            schema,
            mutations: Vec::new(),
        }
    }

    /// Returns true if there is no mutation in the payload.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

/// A write operation to the region.
#[derive(Debug, PartialEq)]
pub struct Mutation {
    /// Type of the mutation.
    pub op_type: OpType,
    /// Data of the mutation.
    pub record_batch: RecordBatch,
}

/// Implementation of [WriteRequest].
#[derive(Debug)]
pub struct WriteBatch {
    payload: Payload,
    /// Number of rows this batch need to mutate (put, delete, etc).
    ///
    /// We use it to check whether this batch is too large.
    num_rows_to_mutate: usize,
    /// The ending index of row key columns.
    ///
    /// The `WriteBatch` use this index to locate all row key columns from
    /// the schema.
    row_key_end: usize,
}

impl WriteRequest for WriteBatch {
    type Error = Error;

    fn put(&mut self, data: HashMap<String, VectorRef>) -> Result<()> {
        let data = NameToVector::new(data)?;
        if data.is_empty() {
            return Ok(());
        }

        let record_batch = self.process_put_data(data)?;

        self.add_num_rows_to_mutate(record_batch.num_rows())?;
        self.payload.mutations.push(Mutation {
            op_type: OpType::Put,
            record_batch,
        });

        Ok(())
    }

    fn delete(&mut self, keys: HashMap<String, VectorRef>) -> Result<()> {
        let data = NameToVector::new(keys)?;
        if data.is_empty() {
            return Ok(());
        }

        let record_batch = self.process_delete_data(data)?;

        self.add_num_rows_to_mutate(record_batch.num_rows())?;
        self.payload.mutations.push(Mutation {
            op_type: OpType::Delete,
            record_batch,
        });

        Ok(())
    }
}

// WriteBatch pub methods.
impl WriteBatch {
    /// Creates a new `WriteBatch`.
    ///
    /// The `schema` is the user schema of the region (no internal columns) and
    /// the `row_key_end` is the ending index of row key columns.
    ///
    /// # Panics
    /// Panics if `row_key_end <= schema.num_columns()`.
    pub fn new(schema: SchemaRef, row_key_end: usize) -> Self {
        assert!(row_key_end <= schema.num_columns());

        Self {
            payload: Payload::new(schema),
            num_rows_to_mutate: 0,
            row_key_end,
        }
    }

    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.payload.schema
    }

    #[inline]
    pub fn payload(&self) -> &Payload {
        &self.payload
    }
}

impl WriteBatch {
    /// Validates `data` and converts it into a [RecordBatch].
    ///
    /// It fills missing columns by schema's default values.
    fn process_put_data(&self, data: NameToVector) -> Result<RecordBatch> {
        let num_rows = data.num_rows();
        let mut columns = Vec::with_capacity(self.schema().num_columns());

        for column_schema in self.schema().column_schemas() {
            match data.0.get(&column_schema.name) {
                Some(col) => {
                    validate_column(column_schema, col)?;
                    columns.push(col.clone());
                }
                None => {
                    // If column is not provided, fills it by default value.
                    let col = new_column_with_default_value(column_schema, num_rows)?;
                    columns.push(col);
                }
            }
        }

        // Check all columns in data also exists in schema, which means we
        // are not inserting unknown columns.
        for name in data.0.keys() {
            ensure!(
                self.schema().contains_column(name),
                UnknownColumnSnafu { name }
            );
        }

        RecordBatch::new(self.schema().clone(), columns).context(CreateRecordBatchSnafu)
    }

    /// Validates `data` and converts it into a [RecordBatch].
    ///
    /// It fills value columns by null, ignoring whether the column is nullable as the contents
    /// of value columns won't be read.
    fn process_delete_data(&self, data: NameToVector) -> Result<RecordBatch> {
        // Ensure row key columns are provided.
        for column_schema in self.row_key_column_schemas() {
            ensure!(
                data.0.contains_key(&column_schema.name),
                BatchMissingColumnSnafu {
                    column: &column_schema.name,
                }
            );
        }
        // Ensure only provides row key columns.
        ensure!(
            data.0.len() == self.row_key_column_schemas().len(),
            MoreColumnThanExpectedSnafu
        );

        let num_rows = data.num_rows();
        let mut columns = Vec::with_capacity(self.schema().num_columns());
        for column_schema in self.schema().column_schemas() {
            match data.0.get(&column_schema.name) {
                Some(col) => {
                    validate_column(column_schema, col)?;
                    columns.push(col.clone());
                }
                None => {
                    // Fills value columns by default value, these columns are just placeholders to ensure
                    // the schema of the record batch is correct.
                    let col = column_schema.create_default_vector_for_padding(num_rows);
                    columns.push(col);
                }
            }
        }

        RecordBatch::new(self.schema().clone(), columns).context(CreateRecordBatchSnafu)
    }

    fn add_num_rows_to_mutate(&mut self, len: usize) -> Result<()> {
        let num_rows = self.num_rows_to_mutate + len;
        ensure!(
            num_rows <= MAX_BATCH_SIZE,
            RequestTooLargeSnafu { num_rows }
        );
        self.num_rows_to_mutate = num_rows;
        Ok(())
    }

    /// Returns all row key columns in the schema.
    fn row_key_column_schemas(&self) -> &[ColumnSchema] {
        &self.payload.schema.column_schemas()[..self.row_key_end]
    }
}

/// Returns the length of the first vector in `data`.
fn first_vector_len(data: &HashMap<String, VectorRef>) -> usize {
    data.values().next().map(|col| col.len()).unwrap_or(0)
}

/// Checks whether `col` matches given `column_schema`.
fn validate_column(column_schema: &ColumnSchema, col: &VectorRef) -> Result<()> {
    if !col.data_type().is_null() {
        // This allow us to use NullVector for columns that only have null value.
        // TODO(yingwen): Let NullVector supports different logical type so we could
        // check data type directly.
        ensure!(
            col.data_type() == column_schema.data_type,
            TypeMismatchSnafu {
                name: &column_schema.name,
                expect: column_schema.data_type.clone(),
                given: col.data_type(),
            }
        );
    }

    ensure!(
        column_schema.is_nullable() || col.null_count() == 0,
        HasNullSnafu {
            name: &column_schema.name,
        }
    );

    Ok(())
}

/// Creates a new column and fills it by default value.
///
/// `num_rows` MUST be greater than 0. This function will also validate the schema.
pub(crate) fn new_column_with_default_value(
    column_schema: &ColumnSchema,
    num_rows: usize,
) -> Result<VectorRef> {
    // If column is not provided, fills it by default value.
    let vector = column_schema
        .create_default_vector(num_rows)
        .context(CreateDefaultSnafu {
            name: &column_schema.name,
        })?
        .context(BatchMissingColumnSnafu {
            column: &column_schema.name,
        })?;

    validate_column(column_schema, &vector)?;

    Ok(vector)
}

/// Vectors in [NameToVector] have same length.
///
/// MUST construct it via [`NameToVector::new()`] to ensure the vector lengths are validated.
struct NameToVector(HashMap<String, VectorRef>);

impl NameToVector {
    fn new(data: HashMap<String, VectorRef>) -> Result<NameToVector> {
        let num_rows = first_vector_len(&data);
        for (name, vector) in &data {
            ensure!(
                num_rows == vector.len(),
                UnequalLengthsSnafu {
                    name,
                    expect: num_rows,
                    given: vector.len(),
                }
            );
        }

        Ok(NameToVector(data))
    }

    fn num_rows(&self) -> usize {
        first_vector_len(&self.0)
    }

    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

#[cfg(test)]
pub(crate) fn new_test_batch() -> WriteBatch {
    use datatypes::type_id::LogicalTypeId;

    use crate::test_util::write_batch_util;

    write_batch_util::new_write_batch(
        &[
            ("k1", LogicalTypeId::UInt64, false),
            ("ts", LogicalTypeId::TimestampMillisecond, false),
            ("v1", LogicalTypeId::Boolean, true),
        ],
        Some(1),
        2,
    )
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use datatypes::prelude::ScalarVector;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::{
        BooleanVector, Int32Vector, Int64Vector, TimestampMillisecondVector, UInt64Vector,
    };

    use super::*;
    use crate::test_util::write_batch_util;

    #[test]
    fn test_name_to_vector_basic() {
        let columns = NameToVector::new(HashMap::new()).unwrap();
        assert!(columns.is_empty());

        let vector1 = Arc::new(Int32Vector::from_slice([1, 2, 3, 4, 5])) as VectorRef;

        let put_data = HashMap::from([
            ("k1".to_string(), vector1.clone()),
            ("v1".to_string(), vector1),
        ]);

        let columns = NameToVector::new(put_data).unwrap();
        assert_eq!(5, columns.num_rows());
        assert!(!columns.is_empty());
    }

    #[test]
    fn test_name_to_vector_empty_vector() {
        let vector1 = Arc::new(Int32Vector::from_slice([])) as VectorRef;
        let put_data = HashMap::from([("k1".to_string(), vector1)]);

        let columns = NameToVector::new(put_data).unwrap();
        assert_eq!(0, columns.num_rows());
        assert!(columns.is_empty());
    }

    #[test]
    fn test_write_batch_put() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice([0, 0, 0])) as VectorRef;
        let put_data = HashMap::from([
            ("k1".to_string(), intv),
            ("v1".to_string(), boolv),
            ("ts".to_string(), tsv),
        ]);

        let mut batch = new_test_batch();
        batch.put(put_data).unwrap();
        assert!(!batch.payload().is_empty());

        let mutation = &batch.payload().mutations[0];
        assert_eq!(3, mutation.record_batch.num_rows());
    }

    fn check_err(err: Error, msg: &str) {
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(
            err.to_string().contains(msg),
            "<{err}> does not contain {msg}",
        );
    }

    #[test]
    fn test_write_batch_too_large() {
        let boolv = Arc::new(BooleanVector::from_iterator(
            iter::repeat(true).take(MAX_BATCH_SIZE + 1),
        )) as VectorRef;
        let put_data = HashMap::from([("k1".to_string(), boolv)]);

        let mut batch =
            write_batch_util::new_write_batch(&[("k1", LogicalTypeId::Boolean, false)], None, 1);
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Request is too large");
    }

    #[test]
    fn test_put_data_different_len() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice([0, 0])) as VectorRef;
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let put_data = HashMap::from([
            ("k1".to_string(), intv),
            ("v1".to_string(), boolv),
            ("ts".to_string(), tsv),
        ]);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "not equals to other columns");
    }

    #[test]
    fn test_put_type_mismatch() {
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let tsv = Arc::new(Int64Vector::from_slice([0, 0, 0])) as VectorRef;
        let put_data = HashMap::from([("k1".to_string(), boolv), ("ts".to_string(), tsv)]);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Type of column k1 does not match");
    }

    #[test]
    fn test_put_type_has_null() {
        let intv = Arc::new(UInt64Vector::from(vec![Some(1), None, Some(3)])) as VectorRef;
        let tsv = Arc::new(Int64Vector::from_slice([0, 0, 0])) as VectorRef;
        let put_data = HashMap::from([("k1".to_string(), intv), ("ts".to_string(), tsv)]);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Column k1 is not null");
    }

    #[test]
    fn test_put_missing_column() {
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice([0, 0, 0])) as VectorRef;
        let put_data = HashMap::from([("v1".to_string(), boolv), ("ts".to_string(), tsv)]);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Missing column k1");
    }

    #[test]
    fn test_put_unknown_column() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice([0, 0, 0])) as VectorRef;
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let put_data = HashMap::from([
            ("k1".to_string(), intv.clone()),
            ("v1".to_string(), boolv.clone()),
            ("ts".to_string(), tsv),
            ("v2".to_string(), boolv),
        ]);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        assert_eq!(StatusCode::TableColumnNotFound, err.status_code());
    }

    #[test]
    fn test_put_empty() {
        let mut batch = new_test_batch();
        batch.put(HashMap::new()).unwrap();
        assert!(batch.payload().is_empty());
    }

    #[test]
    fn test_delete_empty() {
        let mut batch = new_test_batch();
        batch.delete(HashMap::new()).unwrap();
        assert!(batch.payload().is_empty());
    }

    #[test]
    fn test_write_batch_delete() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice([0, 0, 0])) as VectorRef;
        let keys = HashMap::from([("k1".to_string(), intv), ("ts".to_string(), tsv)]);

        let mut batch = new_test_batch();
        batch.delete(keys).unwrap();

        let record_batch = &batch.payload().mutations[0].record_batch;
        assert_eq!(3, record_batch.num_rows());
        assert_eq!(3, record_batch.num_columns());
        let v1 = record_batch.column_by_name("v1").unwrap();
        assert!(v1.only_null());
    }

    #[test]
    fn test_delete_missing_column() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let keys = HashMap::from([("k1".to_string(), intv)]);

        let mut batch = new_test_batch();
        let err = batch.delete(keys).unwrap_err();
        check_err(err, "Missing column ts");
    }

    #[test]
    fn test_delete_columns_more_than_row_key() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice([0, 0, 0])) as VectorRef;
        let keys = HashMap::from([
            ("k1".to_string(), intv.clone()),
            ("ts".to_string(), tsv),
            ("v2".to_string(), intv),
        ]);

        let mut batch = new_test_batch();
        let err = batch.delete(keys).unwrap_err();
        check_err(err, "More columns than expected");
    }

    #[test]
    fn test_delete_type_mismatch() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let keys = HashMap::from([("k1".to_string(), intv.clone()), ("ts".to_string(), boolv)]);

        let mut batch = new_test_batch();
        let err = batch.delete(keys).unwrap_err();
        check_err(err, "Type of column ts does not match");
    }

    #[test]
    fn test_delete_non_null_value() {
        let intv = Arc::new(UInt64Vector::from_slice([1, 2, 3])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice([0, 0, 0])) as VectorRef;
        let keys = HashMap::from([("k1".to_string(), intv.clone()), ("ts".to_string(), tsv)]);

        let mut batch = write_batch_util::new_write_batch(
            &[
                ("k1", LogicalTypeId::UInt64, false),
                ("ts", LogicalTypeId::TimestampMillisecond, false),
                ("v1", LogicalTypeId::Boolean, false),
            ],
            Some(1),
            2,
        );
        batch.delete(keys).unwrap();
    }
}
