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

use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::slice;
use std::sync::Arc;
use std::time::Duration;

use common_recordbatch::RecordBatch;
use common_time::timestamp_millis::BucketAligned;
use common_time::RangeMillis;
use datatypes::arrow::error::ArrowError;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::{ScalarVector, Value};
use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::vectors::{
    Int64Vector, TimestampMillisecondVector, UInt64Vector, UInt8Vector, VectorRef,
};
use prost::{DecodeError, EncodeError};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{consts, OpType, WriteRequest};

use crate::error::{self, Error, Result};
use crate::proto;
use crate::schema::StoreSchemaRef;

// // TODO(yingwen): Remove unused errors.
// #[derive(Debug, Snafu)]
// pub enum Error {
//     // #[snafu(display("Duplicate column {} in same request", name))]
//     // DuplicateColumn { name: String, backtrace: Backtrace },

//     #[snafu(display("Missing column {} in request", name))]
//     MissingColumn { name: String, backtrace: Backtrace },

//     #[snafu(display(
//         "Type of column {} does not match type in schema, expect {:?}, given {:?}",
//         name,
//         expect,
//         given
//     ))]
//     TypeMismatch {
//         name: String,
//         expect: ConcreteDataType,
//         given: ConcreteDataType,
//         backtrace: Backtrace,
//     },

//     #[snafu(display("Column {} is not null but input has null", name))]
//     HasNull { name: String, backtrace: Backtrace },

//     #[snafu(display("Unknown column {}", name))]
//     UnknownColumn { name: String, backtrace: Backtrace },

//     #[snafu(display(
//         "Length of column {} not equals to other columns, expect {}, given {}",
//         name,
//         expect,
//         given
//     ))]
//     LenNotEquals {
//         name: String,
//         expect: usize,
//         given: usize,
//         backtrace: Backtrace,
//     },

//     #[snafu(display(
//         "Request is too large, max is {}, current is {}",
//         MAX_BATCH_SIZE,
//         num_rows
//     ))]
//     RequestTooLarge {
//         num_rows: usize,
//         backtrace: Backtrace,
//     },

//     #[snafu(display("Cannot align timestamp: {}", ts))]
//     TimestampOverflow { ts: i64 },

//     #[snafu(display("Failed to encode, source: {}", source))]
//     EncodeArrow {
//         backtrace: Backtrace,
//         source: ArrowError,
//     },

//     #[snafu(display("Failed to decode, source: {}", source))]
//     DecodeArrow {
//         backtrace: Backtrace,
//         source: ArrowError,
//     },

//     #[snafu(display("Failed to encode into protobuf, source: {}", source))]
//     EncodeProtobuf {
//         backtrace: Backtrace,
//         source: EncodeError,
//     },

//     #[snafu(display("Failed to decode from protobuf, source: {}", source))]
//     DecodeProtobuf {
//         backtrace: Backtrace,
//         source: DecodeError,
//     },

//     #[snafu(display("Failed to parse schema, source: {}", source))]
//     ParseSchema {
//         backtrace: Backtrace,
//         source: datatypes::error::Error,
//     },

//     #[snafu(display("Failed to decode, corrupted data {}", message))]
//     DataCorrupted {
//         message: String,
//         backtrace: Backtrace,
//     },

//     #[snafu(display("Failed to decode vector, source {}", source))]
//     DecodeVector {
//         backtrace: Backtrace,
//         source: datatypes::error::Error,
//     },

//     #[snafu(display("Failed to convert into protobuf struct, source {}", source))]
//     ToProtobuf {
//         source: proto::write_batch::Error,
//         backtrace: Backtrace,
//     },

//     #[snafu(display("Failed to convert from protobuf struct, source {}", source))]
//     FromProtobuf {
//         source: proto::write_batch::Error,
//         backtrace: Backtrace,
//     },

//     #[snafu(display(
//         "Failed to create default value for column {}, source: {}",
//         name,
//         source
//     ))]
//     CreateDefault {
//         name: String,
//         #[snafu(backtrace)]
//         source: datatypes::error::Error,
//     },

//     #[snafu(display("Failed to create record batch for write batch, source:{}", source))]
//     CreateRecordBatch {
//         #[snafu(backtrace)]
//         source: common_recordbatch::error::Error,
//     },
// }

// pub type Result<T> = std::result::Result<T, Error>;

/// Max number of updates of a write batch.
pub(crate) const MAX_BATCH_SIZE: usize = 1_000_000;

// impl ErrorExt for Error {
//     fn status_code(&self) -> StatusCode {
//         StatusCode::InvalidArguments
//     }

//     fn backtrace_opt(&self) -> Option<&Backtrace> {
//         ErrorCompat::backtrace(self)
//     }

//     fn as_any(&self) -> &dyn Any {
//         self
//     }
// }

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

    // fn put(&mut self, mut data: PutData) -> Result<()> {
    //     if data.is_empty() {
    //         return Ok(());
    //     }

    //     self.preprocess_put_data(&mut data)?;

    //     self.add_num_rows(data.num_rows())?;
    //     self.mutations.push(Mutation::Put(data));

    //     Ok(())
    // }

    // /// Aligns timestamps in write batch specified by schema to durations.
    // ///
    // /// A negative timestamp means "before Unix epoch".
    // /// Valid timestamp range is `[i64::MIN + duration, i64::MAX-(i64::MAX%duration))`.
    // fn time_ranges(&self, duration: Duration) -> Result<Vec<RangeMillis>> {
    //     let ts_col_name = match self.schema.timestamp_column() {
    //         None => {
    //             // write batch does not have a timestamp column
    //             return Ok(Vec::new());
    //         }
    //         Some(ts_col) => &ts_col.name,
    //     };
    //     let durations_millis = duration.as_millis() as i64;
    //     let mut aligned_timestamps: BTreeSet<i64> = BTreeSet::new();
    //     for m in &self.mutations {
    //         match m {
    //             Mutation::Put(put_data) => {
    //                 let column = put_data
    //                     .column_by_name(ts_col_name)
    //                     .unwrap_or_else(|| panic!("Cannot find column by name: {}", ts_col_name));
    //                 if column.is_const() {
    //                     let ts = match column.get(0) {
    //                         Value::Timestamp(ts) => ts,
    //                         _ => unreachable!(),
    //                     };
    //                     let aligned = align_timestamp(ts.value(), durations_millis)
    //                         .context(TimestampOverflowSnafu { ts: ts.value() })?;

    //                     aligned_timestamps.insert(aligned);
    //                 } else {
    //                     match column.data_type() {
    //                         ConcreteDataType::Timestamp(_) => {
    //                             let ts_vector = column
    //                                 .as_any()
    //                                 .downcast_ref::<TimestampMillisecondVector>()
    //                                 .unwrap();
    //                             for ts in ts_vector.iter_data().flatten() {
    //                                 let aligned = align_timestamp(ts.into(), durations_millis)
    //                                     .context(TimestampOverflowSnafu { ts: i64::from(ts) })?;
    //                                 aligned_timestamps.insert(aligned);
    //                             }
    //                         }
    //                         ConcreteDataType::Int64(_) => {
    //                             let ts_vector =
    //                                 column.as_any().downcast_ref::<Int64Vector>().unwrap();
    //                             for ts in ts_vector.iter_data().flatten() {
    //                                 let aligned = align_timestamp(ts, durations_millis)
    //                                     .context(TimestampOverflowSnafu { ts })?;
    //                                 aligned_timestamps.insert(aligned);
    //                             }
    //                         }
    //                         _ => unreachable!(),
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     let ranges = aligned_timestamps
    //         .iter()
    //         .map(|t| RangeMillis::new(*t, *t + durations_millis).unwrap())
    //         .collect::<Vec<_>>();

    //     Ok(ranges)
    // }
}

// /// Aligns timestamp to nearest time interval.
// /// Negative ts means a timestamp before Unix epoch.
// /// If arithmetic overflows, this function returns None.
// /// So timestamp within `[i64::MIN, i64::MIN + duration)` or
// /// `[i64::MAX-(i64::MAX%duration), i64::MAX]` is not a valid input.
// fn align_timestamp(ts: i64, duration: i64) -> Option<i64> {
//     let aligned = ts.align_by_bucket(duration)?.as_i64();
//     // Also ensure end timestamp won't overflow.
//     aligned.checked_add(duration)?;
//     Some(aligned)
// }

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

// /// Enum to wrap different operations.
// pub enum Mutation {
//     Put(PutData),
// }

// #[derive(Default, Debug)]
// pub struct PutData {
//     columns: HashMap<String, VectorRef>,
// }

// impl PutData {
//     pub(crate) fn new() -> PutData {
//         PutData::default()
//     }

//     pub(crate) fn with_num_columns(num_columns: usize) -> PutData {
//         PutData {
//             columns: HashMap::with_capacity(num_columns),
//         }
//     }

//     fn add_column_by_name(&mut self, name: &str, vector: VectorRef) -> Result<()> {
//         ensure!(
//             !self.columns.contains_key(name),
//             DuplicateColumnSnafu { name }
//         );

//         if let Some(col) = self.columns.values().next() {
//             ensure!(
//                 col.len() == vector.len(),
//                 LenNotEqualsSnafu {
//                     name,
//                     expect: col.len(),
//                     given: vector.len(),
//                 }
//             );
//         }

//         self.columns.insert(name.to_string(), vector);

//         Ok(())
//     }

//     /// Add columns by its default value.
//     fn add_default_by_name(&mut self, column_schema: &ColumnSchema) -> Result<()> {
//         let num_rows = self.num_rows();

//         // If column is not provided, fills it by default value.
//         let vector = column_schema
//             .create_default_vector(num_rows)
//             .context(CreateDefaultSnafu {
//                 name: &column_schema.name,
//             })?
//             .context(MissingColumnSnafu {
//                 name: &column_schema.name,
//             })?;

//         validate_column(column_schema, &vector)?;

//         self.add_column_by_name(&column_schema.name, vector)
//     }
// }

// impl PutOperation for PutData {
//     type Error = Error;

//     fn add_key_column(&mut self, name: &str, vector: VectorRef) -> Result<()> {
//         self.add_column_by_name(name, vector)
//     }

//     fn add_version_column(&mut self, vector: VectorRef) -> Result<()> {
//         self.add_column_by_name(consts::VERSION_COLUMN_NAME, vector)
//     }

//     fn add_value_column(&mut self, name: &str, vector: VectorRef) -> Result<()> {
//         self.add_column_by_name(name, vector)
//     }
// }

// // PutData pub methods.
// impl PutData {
//     pub fn column_by_name(&self, name: &str) -> Option<&VectorRef> {
//         self.columns.get(name)
//     }

//     /// Returns number of columns in data.
//     pub fn num_columns(&self) -> usize {
//         self.columns.len()
//     }

//     /// Returns number of rows in data.
//     pub fn num_rows(&self) -> usize {
//         self.columns
//             .values()
//             .next()
//             .map(|col| col.len())
//             .unwrap_or(0)
//     }

//     /// Returns true if no rows in data.
//     ///
//     /// `PutData` with empty column will also be considered as empty.
//     pub fn is_empty(&self) -> bool {
//         self.num_rows() == 0
//     }

//     /// Returns slice of [PutData] in range `[start, end)`.
//     ///
//     /// # Panics
//     /// Panics if `start > end`.
//     pub fn slice(&self, start: usize, end: usize) -> PutData {
//         assert!(start <= end);

//         let columns = self
//             .columns
//             .iter()
//             .map(|(k, v)| (k.clone(), v.slice(start, end - start)))
//             .collect();

//         PutData { columns }
//     }
// }

impl WriteBatch {
    // /// Create [WriteBatch] with `schema` and `mutations`.
    // ///
    // /// This method won't validating the schema and is designed for the wal decoder
    // /// to assemble the batch from raw parts.
    // fn with_mutations(schema: SchemaRef, mutations: Vec<Mutation>) -> WriteBatch {
    //     let row_index = mutations.iter().map(|m| m.record_batch.num_rows()).sum();
    //     WriteBatch {
    //         schema,
    //         mutations,
    //         row_index,
    //     }
    // }

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
                    let col = new_column_with_default_value(&column_schema, num_rows)?;
                    columns.push(col);
                }
            }
        }

        // Check all columns in data also exists in schema, which means we
        // are not inserting unknown columns.
        for name in data.0.keys() {
            ensure!(
                self.schema().contains_column(name),
                error::UnknownColumnSnafu { name }
            );
        }

        RecordBatch::new(self.schema().clone(), columns).context(error::CreateRecordBatchSnafu)
    }

    // /// Initialize internal columns and push into `columns`.
    // ///
    // /// Since the actual sequence isn't available during constructing the write batch, so we use
    // /// the row indices in this batch to filled the sequence column.
    // fn push_internal_columns(&self, op_type: OpType, num_rows: usize, columns: &mut Vec<VectorRef>) {
    //     // Use row index as sequence during this phase.
    //     let sequences = UInt64Vector::from_values((self.row_index..self.row_index + num_rows).map(|v| v as u64));
    //     let op_types = UInt8Vector::from_value(op_type.as_u8(), num_rows);

    //     debug_assert_eq!(self.schema.sequence_index(), columns.len());
    //     columns.push(Arc::new(sequences));
    //     debug_assert_eq!(self.schema.op_type_index(), columns.len());
    //     columns.push(Arc::new(op_types));
    // }

    // /// Validate [PutData] and fill missing columns by default value.
    // fn preprocess_put_data(&self, data: &mut PutData) -> Result<()> {
    //     for column_schema in self.schema.column_schemas() {
    //         match data.column_by_name(&column_schema.name) {
    //             Some(col) => {
    //                 validate_column(column_schema, col)?;
    //             }
    //             None => {
    //                 // If column is not provided, fills it by default value.
    //                 data.add_default_by_name(column_schema)?;
    //             }
    //         }
    //     }

    //     // Check all columns in data also exists in schema.
    //     for name in data.columns.keys() {
    //         ensure!(
    //             self.schema.contains_column(name).is_some(),
    //             UnknownColumnSnafu { name }
    //         );
    //     }

    //     Ok(())
    // }

    fn add_row_index(&mut self, len: usize) -> Result<()> {
        let num_rows = self.row_index + len;
        ensure!(
            num_rows <= MAX_BATCH_SIZE,
            error::RequestTooLargeSnafu { num_rows }
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
            error::TypeMismatchSnafu {
                name: &column_schema.name,
                expect: column_schema.data_type.clone(),
                given: col.data_type(),
            }
        );
    }

    ensure!(
        column_schema.is_nullable() || col.null_count() == 0,
        error::HasNullSnafu {
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
        .context(error::CreateDefaultSnafu {
            name: &column_schema.name,
        })?
        .context(error::BatchMissingColumnSnafu {
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
                error::LenNotEqualsSnafu {
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

// impl<'a> IntoIterator for &'a WriteBatch {
//     type Item = &'a Mutation;
//     type IntoIter = slice::Iter<'a, Mutation>;

//     fn into_iter(self) -> slice::Iter<'a, Mutation> {
//         self.iter()
//     }
// }

#[cfg(test)]
pub(crate) fn new_test_batch() -> WriteBatch {
    use datatypes::type_id::LogicalTypeId;

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
    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::{
        BooleanVector, ConstantVector, Int32Vector, Int64Vector, TimestampMillisecondVector,
        UInt64Vector,
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
            "<{}> does not contain {}",
            err,
            msg
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
        check_err(err, "Unknown column v2");
    }
}
