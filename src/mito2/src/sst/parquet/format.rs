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
use datatypes::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Schema, SchemaRef, UInt16Type,
};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::{Helper, Vector};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::consts::{
    OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};
use store_api::storage::ColumnId;

use crate::error::{ConvertVectorSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result};
use crate::metadata::RegionMetadata;
use crate::read::{Batch, BatchBuilder, BatchColumn};

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
    debug_assert!(batches.is_empty());

    // The record batch must has time index and internal columns.
    ensure!(
        record_batch.num_columns() > INTERNAL_COLUMN_NUM,
        InvalidRecordBatchSnafu {
            reason: format!(
                "record batch only has {} columns",
                record_batch.num_columns()
            ),
        }
    );

    let mut fixed_pos_columns = record_batch
        .columns()
        .iter()
        .rev()
        .take(FIXED_POS_COLUMN_NUM);
    // Safety: We have checked the column number.
    let op_type_array = fixed_pos_columns.next().unwrap();
    let sequence_array = fixed_pos_columns.next().unwrap();
    let pk_array = fixed_pos_columns.next().unwrap();
    let ts_array = fixed_pos_columns.next().unwrap();
    let num_cols = record_batch.num_columns();
    let field_vectors = record_batch
        .columns()
        .iter()
        .zip(record_batch.schema().fields())
        .take(num_cols - FIXED_POS_COLUMN_NUM) // Take all field columns.
        .map(|(array, field)| {
            let vector = Helper::try_into_vector(array.clone()).context(ConvertVectorSnafu)?;
            let column =
                metadata
                    .column_by_name(field.name())
                    .with_context(|| InvalidRecordBatchSnafu {
                        reason: format!("column {} not found in metadata", field.name()),
                    })?;

            Ok(BatchColumn {
                column_id: column.column_id,
                data: vector,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Compute primary key offsets.
    let pk_dict_array = pk_array
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!("primary key array should not be {:?}", pk_array.data_type()),
        })?;
    let offsets = primary_key_offsets(pk_dict_array)?;
    if offsets.is_empty() {
        return Ok(());
    }

    // Split record batch according to pk offsets.
    let keys = pk_dict_array.keys();
    let pk_values = pk_dict_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!(
                "values of primary key array should not be {:?}",
                pk_dict_array.values().data_type()
            ),
        })?;
    for (i, start) in offsets[..offsets.len() - 1].iter().enumerate() {
        let end = offsets[i + 1];
        let rows_in_batch = end - start;

        let dict_key = keys.value(*start);
        let primary_key = pk_values.value(dict_key.into()).to_vec();

        let mut builder = BatchBuilder::new(primary_key);
        builder
            .timestamps_array(ts_array.slice(*start, rows_in_batch))?
            .sequences_array(sequence_array.slice(*start, rows_in_batch))?
            .op_types_array(op_type_array.slice(*start, rows_in_batch))?;
        // Push all fields
        for field_vector in &field_vectors {
            builder.push_field(BatchColumn {
                column_id: field_vector.column_id,
                data: field_vector.data.slice(*start, rows_in_batch),
            });
        }

        let batch = builder.build()?;
        batches.push(batch);
    }

    Ok(())
}

/// Compute offsets of different primary keys in the array.
fn primary_key_offsets(pk_dict_array: &DictionaryArray<UInt16Type>) -> Result<Vec<usize>> {
    if pk_dict_array.is_empty() {
        return Ok(Vec::new());
    }

    // Init offsets.
    let mut offsets = vec![0];
    let keys = pk_dict_array.keys();
    // We know that primary keys are always not null so we iterate `keys.values()` directly.
    let pk_indices = &keys.values()[..keys.len() - 1];
    for (i, key) in pk_indices.iter().enumerate() {
        // Compare each key with next key
        if *key != pk_indices[i + 1] {
            // We meet a new key, push the next index as end of the offset.
            offsets.push(i + 1);
        }
    }
    offsets.push(keys.len());

    Ok(offsets)
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
