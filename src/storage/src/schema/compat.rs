//! Utilities for resolving schema compatibility problems.

use crate::error::Result;
use crate::schema::StoreSchema;
use crate::write_batch::{Mutation, PutData, WriteBatch};

/// Make schema of `write_batch` compatible with `dest_schema`.
///
/// Returns error if the compatibility issue couldn't be resolved.
fn compat_write(dest_schema: &StoreSchema, write_batch: &mut WriteBatch) -> Result<()> {
    let dest_version = dest_schema.version();
    // Nothing to do if schema version of the write batch is equal to version of destination.
    if dest_version == write_batch.schema().version() {
        // TODO(yingwen): Add a debug assert to check schema?
        return Ok(());
    }

    for m in write_batch.iter_mut() {
        match m {
            Mutation::Put(put_data) => {
                compat_put_data(dest_schema, put_data)?;
            }
        }
    }

    unimplemented!()
}

fn compat_put_data(dest_schema: &StoreSchema, put_data: &mut PutData) -> Result<()> {
    if put_data.is_empty() {
        return Ok(());
    }

    for col in dest_schema.row_key_columns() {
        if put_data.column_by_name(&col.name).is_none() {
            // We need to fill the column by null or its default value.
            unimplemented!();
        }
    }

    unimplemented!()
}
