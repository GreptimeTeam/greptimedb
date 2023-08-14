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
//! We append three internal columns to record batches in parquet:
//! - `__sequence`, the sequence number of a row.
//! - `__op_type`, the op type of the row.
//! - `__tsid`, the time series id of the row.

use std::sync::Arc;

use datatypes::arrow::array::{ArrayRef, UInt64Array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use snafu::ResultExt;
use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME, TSID_COLUMN_NAME};
use store_api::storage::{ColumnId, Tsid};

use crate::error::{NewRecordBatchSnafu, Result};
use crate::metadata::{ColumnMetadata, RegionMetadata};
use api::v1::SemanticType;
use crate::read::Batch;

/// Number of internal columns.
const INTERNAL_COLUMN_NUM: usize = 3;

/// Gets the arrow schema to store in parquet.
pub(crate) fn to_sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .zip(&metadata.column_metadatas)
            .map(|(field, column_meta)| to_sst_field(column_meta, field))
            .chain(internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Gets the arrow record batch to store in parquet.
///
/// The `arrow_schema` is constructed by [to_sst_arrow_schema].
pub(crate) fn to_sst_record_batch(batch: &Batch, arrow_schema: &SchemaRef) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(batch.columns().len() + INTERNAL_COLUMN_NUM);
    debug_assert_eq!(columns.len(), arrow_schema.fields().len());
    for column in batch.columns() {
        columns.push(column.data.to_arrow_array());
    }
    // Add internal columns.
    columns.push(batch.sequences().to_arrow_array());
    columns.push(batch.op_types().to_arrow_array());
    columns.push(new_tsid_array(batch.tsid(), batch.num_rows()));

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

// FIXME(yingwen): Need to split by time series.
/// Convert the arrow record batch to a [Batch].
pub(crate) fn from_sst_record_batch(metadata: &RegionMetadata, record_batch: &RecordBatch) -> Batch {
    unimplemented!()
}

/// Returns the field type to store this column.
fn to_sst_field(column_meta: &ColumnMetadata, field: &FieldRef) -> FieldRef {
    // If the column is a tag column and it has string type, store
    // it in dictionary type.
    if column_meta.semantic_type == SemanticType::Tag {
        if let ConcreteDataType::String(_) = &column_meta.column_schema.data_type {
            return Arc::new(Field::new_dictionary(
                field.name(),
                dictionary_key_type(),
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
    }

    // Otherwise, store as the original type.
    field.clone()
}

/// Key type for arrow dictionary.
const fn dictionary_key_type() -> DataType {
    DataType::UInt16
}

/// Fields for internal columns.
fn internal_fields() -> [FieldRef; 3] {
    [
        Arc::new(Field::new(SEQUENCE_COLUMN_NAME, DataType::UInt64, false)),
        Arc::new(Field::new(OP_TYPE_COLUMN_NAME, DataType::UInt8, false)),
        Arc::new(Field::new(TSID_COLUMN_NAME, DataType::UInt64, false)),
    ]
}

/// Returns an arrary with `count` element for the tsid.
fn new_tsid_array(tsid: Tsid, count: usize) -> ArrayRef {
    let tsids = UInt64Array::from_value(tsid, count);
    Arc::new(tsids)
}
