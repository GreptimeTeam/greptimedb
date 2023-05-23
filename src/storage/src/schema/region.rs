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

use std::fmt;
use std::sync::Arc;

use common_error::prelude::*;
use datatypes::schema::{Schema, SchemaBuilder, SchemaRef};

use crate::metadata::{self, ColumnMetadata, ColumnsMetadata, ColumnsMetadataRef, Result};
use crate::schema::{StoreSchema, StoreSchemaRef};

/// Schema of region.
///
/// The `RegionSchema` has the knowledge of reserved and internal columns.
/// Reserved columns are columns that their names, ids are reserved by the storage
/// engine, and could not be used by the user. Reserved columns usually have
/// special usage. Reserved columns expect the version columns are also
/// called internal columns (though the version could also be thought as a
/// special kind of internal column), are not visible to user, such as our
/// internal sequence, op_type columns.
///
/// The user schema is the schema that only contains columns that user could visit,
/// as well as what the schema user created.
#[derive(PartialEq, Eq)]
pub struct RegionSchema {
    /// Schema that only contains columns that user defined, excluding internal columns
    /// that are reserved and used by the storage engine.
    ///
    /// Holding a [SchemaRef] to allow converting into `SchemaRef`/`arrow::SchemaRef`
    /// conveniently. The fields order in `SchemaRef` **must** be consistent with
    /// columns order in [ColumnsMetadata] to ensure the projection index of a field
    /// is correct.
    user_schema: SchemaRef,
    /// store schema contains all columns of the region, including all internal columns.
    store_schema: StoreSchemaRef,
    /// Metadata of columns.
    columns: ColumnsMetadataRef,
}

impl fmt::Debug for RegionSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RegionSchema")
            .field("columns", &self.columns)
            .finish()
    }
}

impl RegionSchema {
    pub fn new(columns: ColumnsMetadataRef, version: u32) -> Result<RegionSchema> {
        let user_schema = Arc::new(build_user_schema(&columns, version)?);
        let store_schema = Arc::new(StoreSchema::from_columns_metadata(&columns, version)?);

        debug_assert_eq!(user_schema.version(), store_schema.version());
        debug_assert_eq!(version, user_schema.version());

        Ok(RegionSchema {
            user_schema,
            store_schema,
            columns,
        })
    }

    /// Returns the schema of the region, excluding internal columns that used by
    /// the storage engine.
    #[inline]
    pub fn user_schema(&self) -> &SchemaRef {
        &self.user_schema
    }

    /// Returns the schema actually stores, which would also contains all internal columns.
    #[inline]
    pub fn store_schema(&self) -> &StoreSchemaRef {
        &self.store_schema
    }

    #[inline]
    pub fn row_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter_row_key_columns()
    }

    #[inline]
    pub fn field_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter_field_columns()
    }

    #[inline]
    pub fn num_row_key_columns(&self) -> usize {
        self.columns.num_row_key_columns()
    }

    #[inline]
    pub fn num_field_columns(&self) -> usize {
        self.columns.num_field_columns()
    }

    #[inline]
    pub fn version(&self) -> u32 {
        self.user_schema.version()
    }

    #[inline]
    pub(crate) fn row_key_end(&self) -> usize {
        self.columns.row_key_end()
    }

    #[inline]
    pub(crate) fn sequence_index(&self) -> usize {
        self.store_schema.sequence_index()
    }

    #[inline]
    pub(crate) fn op_type_index(&self) -> usize {
        self.store_schema.op_type_index()
    }

    #[inline]
    pub(crate) fn row_key_indices(&self) -> impl Iterator<Item = usize> {
        self.store_schema.row_key_indices()
    }

    #[inline]
    pub(crate) fn timestamp_index(&self) -> usize {
        self.store_schema.timestamp_index()
    }

    #[inline]
    pub(crate) fn value_indices(&self) -> impl Iterator<Item = usize> {
        self.store_schema.value_indices()
    }

    #[inline]
    pub(crate) fn column_metadata(&self, idx: usize) -> &ColumnMetadata {
        self.columns.column_metadata(idx)
    }

    #[cfg(test)]
    pub(crate) fn columns(&self) -> &[ColumnMetadata] {
        self.columns.columns()
    }
}

pub type RegionSchemaRef = Arc<RegionSchema>;

// Now user schema don't have extra metadata like store schema.
fn build_user_schema(columns: &ColumnsMetadata, version: u32) -> Result<Schema> {
    let column_schemas: Vec<_> = columns
        .iter_user_columns()
        .map(|col| col.desc.to_column_schema())
        .collect();

    SchemaBuilder::try_from(column_schemas)
        .context(metadata::ConvertSchemaSnafu)?
        .version(version)
        .build()
        .context(metadata::InvalidSchemaSnafu)
}

#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;

    use super::*;
    use crate::test_util::schema_util;

    #[test]
    fn test_region_schema() {
        let region_schema = Arc::new(schema_util::new_region_schema(123, 1));

        let expect_schema = schema_util::new_schema_with_version(
            &[
                ("k0", LogicalTypeId::Int64, false),
                ("timestamp", LogicalTypeId::TimestampMillisecond, false),
                ("v0", LogicalTypeId::Int64, true),
            ],
            Some(1),
            123,
        );

        assert_eq!(expect_schema, **region_schema.user_schema());

        // Checks row key column.
        let mut row_keys = region_schema.row_key_columns();
        assert_eq!("k0", row_keys.next().unwrap().desc.name);
        assert_eq!("timestamp", row_keys.next().unwrap().desc.name);
        assert_eq!(None, row_keys.next());
        assert_eq!(2, region_schema.num_row_key_columns());

        // Checks value column.
        let mut values = region_schema.field_columns();
        assert_eq!("v0", values.next().unwrap().desc.name);
        assert_eq!(None, values.next());
        assert_eq!(1, region_schema.num_field_columns());

        // Checks version.
        assert_eq!(123, region_schema.version());
    }
}
