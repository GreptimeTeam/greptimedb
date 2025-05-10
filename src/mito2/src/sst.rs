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

//! Sorted strings tables.

use std::sync::Arc;

use api::v1::SemanticType;
use common_base::readable_size::ReadableSize;
use datatypes::arrow::datatypes::{
    DataType as ArrowDataType, Field, FieldRef, Fields, Schema, SchemaRef,
};
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::{
    OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};

pub mod file;
pub mod file_purger;
pub mod index;
pub mod location;
pub mod parquet;
pub(crate) mod version;

/// Default write buffer size, it should be greater than the default minimum upload part of S3 (5mb).
pub const DEFAULT_WRITE_BUFFER_SIZE: ReadableSize = ReadableSize::mb(8);

/// Default number of concurrent write, it only works on object store backend(e.g., S3).
pub const DEFAULT_WRITE_CONCURRENCY: usize = 8;

/// Gets the arrow schema to store in parquet.
pub fn to_sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .zip(&metadata.column_metadatas)
            .filter_map(|(field, column_meta)| {
                if column_meta.semantic_type == SemanticType::Field {
                    Some(field.clone())
                } else {
                    // We have fixed positions for tags (primary key) and time index.
                    None
                }
            })
            .chain([metadata.time_index_field()])
            .chain(internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Fields for internal columns.
fn internal_fields() -> [FieldRef; 3] {
    // Internal columns are always not null.
    [
        Arc::new(Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            ArrowDataType::UInt32,
            ArrowDataType::Binary,
            false,
        )),
        Arc::new(Field::new(
            SEQUENCE_COLUMN_NAME,
            ArrowDataType::UInt64,
            false,
        )),
        Arc::new(Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false)),
    ]
}

/// Gets the arrow schema to store in parquet.
pub fn to_plain_sst_arrow_schema(metadata: &RegionMetadata) -> SchemaRef {
    let fields = Fields::from_iter(
        metadata
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .cloned()
            .chain(plain_internal_fields()),
    );

    Arc::new(Schema::new(fields))
}

/// Fields for internal columns.
fn plain_internal_fields() -> [FieldRef; 2] {
    // Internal columns are always not null.
    [
        Arc::new(Field::new(
            SEQUENCE_COLUMN_NAME,
            ArrowDataType::UInt64,
            false,
        )),
        Arc::new(Field::new(OP_TYPE_COLUMN_NAME, ArrowDataType::UInt8, false)),
    ]
}
