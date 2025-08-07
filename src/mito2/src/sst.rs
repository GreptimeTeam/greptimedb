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
use store_api::codec::PrimaryKeyEncoding;
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

/// Options of flat schema.
pub struct FlatSchemaOptions {
    /// Whether to store primary key columns additionally instead of an encoded column.
    pub raw_pk_columns: bool,
    /// Whether to use dictionary encoding for string primary key columns
    /// when storing primary key columns.
    /// Only takes effect when `raw_pk_columns` is true.
    pub string_pk_use_dict: bool,
}

impl Default for FlatSchemaOptions {
    fn default() -> Self {
        Self {
            raw_pk_columns: true,
            string_pk_use_dict: true,
        }
    }
}

impl FlatSchemaOptions {
    /// Creates a options according to the primary key encoding.
    pub fn from_encoding(encoding: PrimaryKeyEncoding) -> Self {
        if encoding == PrimaryKeyEncoding::Dense {
            Self::default()
        } else {
            Self {
                raw_pk_columns: false,
                string_pk_use_dict: false,
            }
        }
    }
}

/// Gets the arrow schema to store in parquet.
///
/// The schema is:
/// ```text
/// primary key columns, field columns, time index, __prmary_key, __sequence, __op_type
/// ```
///
/// # Panics
/// Panics if the metadata is invalid.
pub fn to_flat_sst_arrow_schema(
    metadata: &RegionMetadata,
    options: &FlatSchemaOptions,
) -> SchemaRef {
    let num_fields = if options.raw_pk_columns {
        metadata.column_metadatas.len() + 3
    } else {
        metadata.column_metadatas.len() + 3 - metadata.primary_key.len()
    };
    let mut fields = Vec::with_capacity(num_fields);
    let schema = metadata.schema.arrow_schema();
    if options.raw_pk_columns {
        for pk_id in &metadata.primary_key {
            let pk_index = metadata.column_index_by_id(*pk_id).unwrap();
            if options.string_pk_use_dict
                && metadata.column_metadatas[pk_index]
                    .column_schema
                    .data_type
                    .is_string()
            {
                let field = &schema.fields[pk_index];
                let field = Arc::new(Field::new_dictionary(
                    field.name(),
                    datatypes::arrow::datatypes::DataType::UInt32,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
                fields.push(field);
            } else {
                fields.push(schema.fields[pk_index].clone());
            }
        }
    }
    let remaining_fields = schema
        .fields()
        .iter()
        .zip(&metadata.column_metadatas)
        .filter_map(|(field, column_meta)| {
            if column_meta.semantic_type == SemanticType::Field {
                Some(field.clone())
            } else {
                None
            }
        })
        .chain([metadata.time_index_field()])
        .chain(internal_fields());
    for field in remaining_fields {
        fields.push(field);
    }

    Arc::new(Schema::new(fields))
}

/// Fields for internal columns.
pub(crate) fn internal_fields() -> [FieldRef; 3] {
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
