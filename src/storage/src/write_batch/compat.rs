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

use datatypes::schema::{ColumnSchema, SchemaRef};
use snafu::{ensure, ResultExt};

use crate::error::{self, Result};
use crate::schema::compat::CompatWrite;
use crate::write_batch::{Mutation, PutData, WriteBatch};

impl CompatWrite for WriteBatch {
    fn compat_write(&mut self, dest_schema: &SchemaRef) -> Result<()> {
        let (data_version, schema_version) = (dest_schema.version(), self.schema.version());
        // Fast path, nothing to do if schema version of the write batch is equal to version
        // of destination.
        if data_version == schema_version {
            debug_assert_eq!(dest_schema.column_schemas(), self.schema.column_schemas());

            return Ok(());
        }

        ensure!(
            data_version > schema_version,
            error::WriteToOldVersionSnafu {
                data_version,
                schema_version,
            }
        );

        // For columns not in schema, returns error instead of discarding the column silently.
        let column_not_in = column_not_in_schema(dest_schema, self.schema.column_schemas());
        ensure!(
            column_not_in.is_none(),
            error::NotInSchemaToCompatSnafu {
                column: column_not_in.unwrap(),
                version: data_version,
            }
        );

        for m in &mut self.mutations {
            match m {
                Mutation::Put(put_data) => {
                    put_data.compat_write(dest_schema)?;
                }
            }
        }

        // Change schema to `dest_schema`.
        self.schema = dest_schema.clone();

        Ok(())
    }
}

impl CompatWrite for PutData {
    fn compat_write(&mut self, dest_schema: &SchemaRef) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        for column_schema in dest_schema.column_schemas() {
            if self.column_by_name(&column_schema.name).is_none() {
                // We need to fill the column by null or its default value.
                self.add_default_by_name(column_schema)
                    .context(error::AddDefaultSnafu {
                        column: &column_schema.name,
                    })?;
            }
        }

        Ok(())
    }
}

fn column_not_in_schema(schema: &SchemaRef, column_schemas: &[ColumnSchema]) -> Option<String> {
    column_schemas.iter().find_map(|col| {
        if schema.column_schema_by_name(&col.name).is_none() {
            Some(col.name.clone())
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, SchemaBuilder};
    use datatypes::vectors::{Int32Vector, TimestampMillisecondVector};
    use store_api::storage::{PutOperation, WriteRequest};

    use super::*;
    use crate::error::Error;

    fn new_test_schema_builder(
        v0_constraint: Option<Option<ColumnDefaultConstraint>>,
    ) -> SchemaBuilder {
        let mut column_schemas = vec![
            ColumnSchema::new("k0", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];

        if let Some(v0_constraint) = v0_constraint {
            column_schemas.push(
                ColumnSchema::new("v0", ConcreteDataType::int32_datatype(), true)
                    .with_default_constraint(v0_constraint)
                    .unwrap(),
            );
        }

        SchemaBuilder::try_from(column_schemas).unwrap()
    }

    fn new_test_schema(v0_constraint: Option<Option<ColumnDefaultConstraint>>) -> SchemaRef {
        let schema = new_test_schema_builder(v0_constraint).build().unwrap();

        Arc::new(schema)
    }

    fn new_put_data() -> PutData {
        let mut put_data = PutData::new();
        let k0 = Arc::new(Int32Vector::from_slice(&[1, 2, 3]));
        let ts = Arc::new(TimestampMillisecondVector::from_values([11, 12, 13]));

        put_data.add_key_column("k0", k0).unwrap();
        put_data.add_key_column("ts", ts).unwrap();

        put_data
    }

    #[test]
    fn test_put_data_compat_write() {
        let mut put_data = new_put_data();
        let schema = new_test_schema(Some(Some(ColumnDefaultConstraint::null_value())));
        put_data.compat_write(&schema).unwrap();
        let v0 = put_data.column_by_name("v0").unwrap();
        assert!(v0.only_null());
    }

    #[test]
    fn test_write_batch_compat_write() {
        let schema_old = new_test_schema(None);
        let mut batch = WriteBatch::new(schema_old);
        let put_data = new_put_data();
        batch.put(put_data).unwrap();

        let schema_new = Arc::new(
            new_test_schema_builder(Some(Some(ColumnDefaultConstraint::null_value())))
                .version(1)
                .build()
                .unwrap(),
        );
        batch.compat_write(&schema_new).unwrap();
        assert_eq!(schema_new, *batch.schema());
        let Mutation::Put(put_data) = batch.iter().next().unwrap();
        put_data.column_by_name("v0").unwrap();
    }

    #[test]
    fn test_write_batch_compat_to_old() {
        let schema_old = new_test_schema(None);
        let schema_new = Arc::new(
            new_test_schema_builder(None)
                .version(1) // Bump the version
                .build()
                .unwrap(),
        );

        let mut batch = WriteBatch::new(schema_new);
        let err = batch.compat_write(&schema_old).unwrap_err();
        assert!(
            matches!(err, Error::WriteToOldVersion { .. }),
            "err {err} is not WriteToOldVersion",
        );
    }

    #[test]
    fn test_write_batch_skip_compat() {
        let schema = new_test_schema(None);
        let mut batch = WriteBatch::new(schema.clone());
        batch.compat_write(&schema).unwrap();
    }

    #[test]
    fn test_write_batch_compat_columns_not_in_schema() {
        let schema_has_column = new_test_schema(Some(None));
        let mut batch = WriteBatch::new(schema_has_column);

        let schema_no_column = Arc::new(new_test_schema_builder(None).version(1).build().unwrap());
        let err = batch.compat_write(&schema_no_column).unwrap_err();
        assert!(
            matches!(err, Error::NotInSchemaToCompat { .. }),
            "err {err} is not NotInSchemaToCompat",
        );
    }
}
