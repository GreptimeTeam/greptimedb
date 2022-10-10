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

        // For columns not in schema, returns error instead of dropping it silently.
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
