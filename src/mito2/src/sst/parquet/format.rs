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

//! Format to store in parquet.
//!
//! We store three internal columns in parquet:
//! - `__primary_key`, the primary key of the row (tags).
//! - `__sequence`, the sequence number of a row.
//! - `__op_type`, the op type of the row.
//!
//! The schema of a parquet file is:
//! ```text
//! field 0, field 1, ..., field N, time index, primary key, sequence, op type
//! ```

use std::sync::Arc;

use api::v1::SemanticType;
use datatypes::arrow::array::{ArrayRef, BinaryArray, DictionaryArray, UInt16Array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::Vector;
use snafu::{ResultExt, ensure};
use store_api::storage::consts::{
    OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};
use store_api::storage::ColumnId;

use crate::error::{NewRecordBatchSnafu, InvalidRecordBatchSnafu, Result};
use crate::metadata::RegionMetadata;
use crate::read::Batch;

/// Number of internal columns.
const INTERNAL_COLUMN_NUM: usize = 3;
/// Number of columns that have fixed positions.
///
/// Contains: time index and internal columns.
const FIXED_POS_COLUMN_NUM: usize = 4;

/// Gets the arrow schema to store in parquet.
pub(crate) fn to_sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .zip(&metadata.column_metadatas)
            .filter_map(|(field, column_meta)| {
                // If the column is a tag column, we already store it in the primary key so we
                // can ignore it.
                if column_meta.semantic_type == SemanticType::Tag {
                    None
                } else {
                    Some(field.clone())
                }
            })
            .chain([metadata.time_index_field()])
            .chain(internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Gets the arrow record batch to store in parquet.
///
/// The `arrow_schema` is constructed by [to_sst_arrow_schema].
pub(crate) fn to_sst_record_batch(batch: &Batch, arrow_schema: &SchemaRef) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(batch.fields().len() + FIXED_POS_COLUMN_NUM);

    // Store all fields first.
    for column in batch.fields() {
        columns.push(column.data.to_arrow_array());
    }
    // Add time index column.
    columns.push(batch.timestamps().to_arrow_array());
    // Add internal columns: primary key, sequences, op types.
    columns.push(new_primary_key_array(batch.primary_key(), batch.num_rows()));
    columns.push(batch.sequences().to_arrow_array());
    columns.push(batch.op_types().to_arrow_array());

    RecordBatch::try_new(arrow_schema.clone(), columns).context(NewRecordBatchSnafu)
}

/// Gets projection indices to read `columns` from parquet files.
///
/// This function ignores columns not in `metadata` to for compatibility between
/// different schemas.
pub(crate) fn to_sst_projection_indices(
    metadata: &RegionMetadata,
    columns: impl IntoIterator<Item = ColumnId>,
) -> Vec<usize> {
    columns
        .into_iter()
        .filter_map(|column_id| metadata.column_index_by_id(column_id))
        .collect()
}

/// Convert a arrow record batch into `batches`.
pub(crate) fn from_sst_record_batch(
    metadata: &RegionMetadata,
    record_batch: &RecordBatch,
    batches: &mut Vec<Batch>,
) -> Result<()> {
    // The record batch must has time index and internal columns.
    ensure!(record_batch.num_columns() > INTERNAL_COLUMN_NUM, InvalidRecordBatchSnafu {
        reason: format!("record batch only has {} columns", record_batch.num_columns()),
    });
    // Convert time index.
    let num_cols = record_batch.num_columns();
    let ts_array = record_batch.column(num_cols - FIXED_POS_COLUMN_NUM);
    //

    unimplemented!()
}

/// Key type for arrow dictionary.
const fn dictionary_key_type() -> DataType {
    DataType::UInt16
}

/// Value type of the primary key.
const fn pk_value_type() -> DataType {
    DataType::Binary
}

/// Fields for internal columns.
fn internal_fields() -> [FieldRef; 3] {
    // Internal columns are always not null.
    [
        Arc::new(Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            dictionary_key_type(),
            pk_value_type(),
            false,
        )),
        Arc::new(Field::new(SEQUENCE_COLUMN_NAME, DataType::UInt64, false)),
        Arc::new(Field::new(OP_TYPE_COLUMN_NAME, DataType::UInt8, false)),
    ]
}

/// Creates a new array for specific `primary_key`.
fn new_primary_key_array(primary_key: &[u8], num_rows: usize) -> ArrayRef {
    let values = Arc::new(BinaryArray::from_iter_values([primary_key]));
    let keys = UInt16Array::from_value(0, num_rows);

    // Safety: The key index is valid.
    Arc::new(DictionaryArray::new(keys, values))
}
