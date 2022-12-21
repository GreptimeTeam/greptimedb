// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
    LenNotEqualsSnafu, RequestTooLargeSnafu, Result, TypeMismatchSnafu, UnknownColumnSnafu,
};

/// Max number of updates of a write batch.
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
    /// Create a new payload with given `schema`.
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
    row_index: usize,
}

impl WriteRequest for WriteBatch {
    type Error = Error;

    fn put(&mut self, data: HashMap<String, VectorRef>) -> Result<()> {
        let data = NameToVector::new(data)?;
        if data.is_empty() {
            return Ok(());
        }

        let record_batch = self.process_put_data(data)?;

        self.add_row_index(record_batch.num_rows())?;
        self.payload.mutations.push(Mutation {
            op_type: OpType::Put,
            record_batch,
        });

        Ok(())
    }
}

// WriteBatch pub methods.
impl WriteBatch {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            payload: Payload::new(schema),
            row_index: 0,
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
    /// Validate `data` and converting it into a [RecordBatch].
    ///
    /// This will fill missing columns by schema's default values.
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

    fn add_row_index(&mut self, len: usize) -> Result<()> {
        let num_rows = self.row_index + len;
        ensure!(
            num_rows <= MAX_BATCH_SIZE,
            RequestTooLargeSnafu { num_rows }
        );
        self.row_index = num_rows;
        Ok(())
    }
}

/// Returns the length of the first vector in `data`.
fn first_vector_len(data: &HashMap<String, VectorRef>) -> usize {
    data.values().next().map(|col| col.len()).unwrap_or(0)
}

/// Check whether `col` matches given `column_schema`.
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

/// Create a new column and fill it by default value.
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
struct NameToVector(HashMap<String, VectorRef>);

impl NameToVector {
    fn new(data: HashMap<String, VectorRef>) -> Result<NameToVector> {
        let num_rows = first_vector_len(&data);
        for (name, vector) in &data {
            ensure!(
                num_rows == vector.len(),
                LenNotEqualsSnafu {
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
    use store_api::storage::consts;

    use crate::test_util::write_batch_util;

    write_batch_util::new_write_batch(
        &[
            ("k1", LogicalTypeId::UInt64, false),
            (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
            ("ts", LogicalTypeId::TimestampMillisecond, false),
            ("v1", LogicalTypeId::Boolean, true),
        ],
        Some(2),
    )
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use common_error::prelude::*;
    use datatypes::prelude::ScalarVector;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::{
        BooleanVector, Int32Vector, Int64Vector, TimestampMillisecondVector, UInt64Vector,
    };
    use store_api::storage::consts;

    use super::*;
    use crate::test_util::write_batch_util;

    #[test]
    fn test_name_to_vector_basic() {
        let columns = NameToVector::new(HashMap::new()).unwrap();
        assert!(columns.is_empty());

        let vector1 = Arc::new(Int32Vector::from_slice(&[1, 2, 3, 4, 5])) as VectorRef;
        let vector2 = Arc::new(UInt64Vector::from_slice(&[0, 2, 4, 6, 8])) as VectorRef;

        let mut put_data = HashMap::with_capacity(3);
        put_data.insert("k1".to_string(), vector1.clone());
        put_data.insert(consts::VERSION_COLUMN_NAME.to_string(), vector2);
        put_data.insert("v1".to_string(), vector1);

        let columns = NameToVector::new(put_data).unwrap();
        assert_eq!(5, columns.num_rows());
        assert!(!columns.is_empty());
    }

    #[test]
    fn test_name_to_vector_empty_vector() {
        let vector1 = Arc::new(Int32Vector::from_slice(&[])) as VectorRef;
        let mut put_data = HashMap::new();
        put_data.insert("k1".to_string(), vector1);

        let columns = NameToVector::new(put_data).unwrap();
        assert_eq!(0, columns.num_rows());
        assert!(columns.is_empty());
    }

    #[test]
    fn test_write_batch_put() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3])) as VectorRef;
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice(&[0, 0, 0])) as VectorRef;

        let mut put_data = HashMap::with_capacity(4);
        put_data.insert("k1".to_string(), intv.clone());
        put_data.insert(consts::VERSION_COLUMN_NAME.to_string(), intv);
        put_data.insert("v1".to_string(), boolv);
        put_data.insert("ts".to_string(), tsv);

        let mut batch = new_test_batch();
        assert!(batch.payload().is_empty());
        batch.put(put_data).unwrap();
        assert!(!batch.payload().is_empty());

        let mutation = &batch.payload().mutations[0];
        assert_eq!(3, mutation.record_batch.num_rows());
    }

    fn check_err(err: Error, msg: &str) {
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(err.backtrace_opt().is_some());
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

        let mut put_data = HashMap::new();
        put_data.insert("k1".to_string(), boolv);

        let mut batch =
            write_batch_util::new_write_batch(&[("k1", LogicalTypeId::Boolean, false)], None);
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Request is too large");
    }

    #[test]
    fn test_put_data_different_len() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice(&[0, 0])) as VectorRef;
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;

        let mut put_data = HashMap::new();
        put_data.insert("k1".to_string(), intv.clone());
        put_data.insert(consts::VERSION_COLUMN_NAME.to_string(), intv);
        put_data.insert("v1".to_string(), boolv.clone());
        put_data.insert("ts".to_string(), tsv);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "not equals to other columns");
    }

    #[test]
    fn test_put_type_mismatch() {
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let tsv = Arc::new(Int64Vector::from_slice(&[0, 0, 0])) as VectorRef;

        let mut put_data = HashMap::new();
        put_data.insert("k1".to_string(), boolv);
        put_data.insert("ts".to_string(), tsv);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Type of column k1 does not match");
    }

    #[test]
    fn test_put_type_has_null() {
        let intv = Arc::new(UInt64Vector::from(vec![Some(1), None, Some(3)])) as VectorRef;
        let tsv = Arc::new(Int64Vector::from_slice(&[0, 0, 0])) as VectorRef;

        let mut put_data = HashMap::new();
        put_data.insert("k1".to_string(), intv);
        put_data.insert("ts".to_string(), tsv);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Column k1 is not null");
    }

    #[test]
    fn test_put_missing_column() {
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;
        let tsv = Arc::new(Int64Vector::from_slice(&[0, 0, 0])) as VectorRef;

        let mut put_data = HashMap::new();
        put_data.insert("v1".to_string(), boolv);
        put_data.insert("ts".to_string(), tsv);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        check_err(err, "Missing column k1");
    }

    #[test]
    fn test_put_unknown_column() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3])) as VectorRef;
        let tsv = Arc::new(TimestampMillisecondVector::from_slice(&[0, 0, 0])) as VectorRef;
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true])) as VectorRef;

        let mut put_data = HashMap::new();
        put_data.insert("k1".to_string(), intv.clone());
        put_data.insert(consts::VERSION_COLUMN_NAME.to_string(), intv);
        put_data.insert("v1".to_string(), boolv.clone());
        put_data.insert("ts".to_string(), tsv);
        put_data.insert("v2".to_string(), boolv);

        let mut batch = new_test_batch();
        let err = batch.put(put_data).unwrap_err();
        assert_eq!(StatusCode::TableColumnNotFound, err.status_code());
    }
}
