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

//! Utilities to read data with different schema revision.

use std::sync::Arc;

use datatypes::arrow::datatypes::{Field, FieldRef, Schema};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::VectorRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    NewDefaultVectorSnafu, NewRecordBatchSnafu, NoDefaultSnafu, Result, ToFieldSnafu,
};
use crate::metadata::{ColumnMetadata, RegionMetadata};

/// Compatibility adapter for data with different schema.
pub struct SchemaCompat {
    /// Default vectors for padding.
    // TODO(yingwen): If we know the batch size, we can create a default vector
    // with that size first and reuse it.
    default_vectors: Vec<Option<VectorRef>>,
    /// Fields to add.
    fields_to_add: Vec<FieldRef>,
}

impl SchemaCompat {
    /// Creates a [SchemaCompat] to adapts record batches from files with `old_meta`.
    fn new(old_meta: &RegionMetadata, columns_to_read: &[ColumnMetadata]) -> Result<SchemaCompat> {
        let mut default_vectors = Vec::with_capacity(columns_to_read.len());
        let mut fields_to_add = Vec::new();
        for column in columns_to_read {
            if old_meta.column_by_id(column.column_id).is_some() {
                default_vectors.push(None);
            } else {
                // No such column in old meta, need to fill default value.
                let vector = column
                    .column_schema
                    .create_default_vector(1)
                    .context(NewDefaultVectorSnafu {
                        column: &column.column_schema.name,
                    })?
                    .context(NoDefaultSnafu {
                        column: &column.column_schema.name,
                    })?;
                default_vectors.push(Some(vector));

                let field = Arc::new(Field::try_from(&column.column_schema).context(
                    ToFieldSnafu {
                        column: &column.column_schema.name,
                    },
                )?);
                fields_to_add.push(field);
            }
        }

        Ok(SchemaCompat {
            default_vectors,
            fields_to_add,
        })
    }

    /// Compat record batch.
    ///
    /// The `record_batch` must be read by the `old_meta`
    /// and `columns_to_read` that builds this struct.
    fn compat_record_batch(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        let mut columns = record_batch.columns().iter();
        let old_schema = record_batch.schema();
        let mut fields = old_schema.fields().iter();
        let num_rows = record_batch.num_rows();
        let mut num_field_added = 0;
        let mut new_columns =
            Vec::with_capacity(record_batch.num_columns() + self.fields_to_add.len());
        let mut new_fields =
            Vec::with_capacity(record_batch.num_columns() + self.fields_to_add.len());

        for default_vec in &self.default_vectors {
            if let Some(vector) = default_vec {
                let array = if num_rows == 0 {
                    vector.to_arrow_array().slice(0, 0)
                } else {
                    vector.replicate(&[num_rows]).to_arrow_array()
                };
                new_columns.push(array);
                new_fields.push(self.fields_to_add[num_field_added].clone());
                num_field_added += 1;
            } else {
                new_columns.push(columns.next().unwrap().clone());
                new_fields.push(fields.next().unwrap().clone());
            }
        }

        let schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(schema, new_columns).context(NewRecordBatchSnafu)
    }
}
