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

use std::sync::Arc;

use datatypes::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME, TSID_COLUMN_NAME};

use crate::metadata::{ColumnMetadata, RegionMetadata, SemanticType};
use crate::read::Batch;

/// Get the arrow schema to store in parquet.
pub fn sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .zip(&metadata.column_metadatas)
            .map(|(field, column_meta)| sst_field(column_meta, field))
            .chain(internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Get the arrow record batch to store in parquet.
pub fn sst_record(_batch: &Batch) -> RecordBatch {
    //
    todo!()
}

/// Returns the field type to store this column.
fn sst_field(column_meta: &ColumnMetadata, field: &FieldRef) -> FieldRef {
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
