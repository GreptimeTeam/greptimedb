//! Utilities for resolving schema compatibility problems.

use datatypes::schema::ColumnSchema;
use datatypes::vectors::VectorRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::schema::StoreSchema;
use crate::write_batch::{Mutation, PutData, WriteBatch};

/// Make schema of `write_batch` compatible with `dest_schema`.
///
/// For column in `dest_schema` but not in `write_batch`, this method would insert a
/// vector with default value to the `write_batch`.
///
/// If there are columns not in `dest_schema`, an error would be returned.
pub fn compat_write(dest_schema: &StoreSchema, write_batch: &mut WriteBatch) -> Result<()> {
    let (data_version, schema_version) = (dest_schema.version(), write_batch.schema().version());
    // Fast path, nothing to do if schema version of the write batch is equal to version
    // of destination.
    if data_version == schema_version {
        debug_assert_eq!(
            dest_schema.user_columns(),
            write_batch.schema().column_schemas()
        );

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
    let column_not_in = column_not_in_schema(dest_schema, write_batch.schema().column_schemas());
    ensure!(
        column_not_in.is_none(),
        error::NotInSchemaToCompatSnafu {
            column: column_not_in.unwrap(),
            version: data_version,
        }
    );

    for m in write_batch.iter_mut() {
        match m {
            Mutation::Put(put_data) => {
                compat_put_data(dest_schema, put_data)?;
            }
        }
    }

    Ok(())
}

fn compat_put_data(dest_schema: &StoreSchema, put_data: &mut PutData) -> Result<()> {
    if put_data.is_empty() {
        return Ok(());
    }

    // put_data is not empty, so num_rows must greater than 0.
    let num_rows = put_data.num_rows();

    for column_schema in dest_schema.user_columns() {
        if put_data.column_by_name(&column_schema.name).is_none() {
            // We need to fill the column by null or its default value.
            let vector = try_create_default_vector(column_schema, num_rows)?;

            put_data
                .add_column_by_name(&column_schema.name, vector)
                .context(error::AddDefaultSnafu {
                    column: &column_schema.name,
                })?;
        }
    }

    Ok(())
}

fn column_not_in_schema(schema: &StoreSchema, column_schemas: &[ColumnSchema]) -> Option<String> {
    column_schemas.iter().find_map(|col| {
        if !schema.contains_column(&col.name) {
            Some(col.name.clone())
        } else {
            None
        }
    })
}

fn try_create_default_vector(column_schema: &ColumnSchema, num_rows: usize) -> Result<VectorRef> {
    column_schema
        .create_default_vector(num_rows)
        .context(error::CreateDefaultSnafu {
            column: &column_schema.name,
        })?
        .context(error::NoDefaultSnafu {
            column: &column_schema.name,
        })
}
