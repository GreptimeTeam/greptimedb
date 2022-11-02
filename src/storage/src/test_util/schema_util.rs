use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder, SchemaRef};

/// Column definition: (name, datatype, is_nullable)
pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);

pub fn new_schema(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> Schema {
    new_schema_with_version(column_defs, timestamp_index, 0)
}

pub fn new_schema_with_version(
    column_defs: &[ColumnDef],
    timestamp_index: Option<usize>,
    version: u32,
) -> Schema {
    let column_schemas: Vec<_> = column_defs
        .iter()
        .map(|column_def| {
            let datatype = column_def.1.data_type();
            ColumnSchema::new(column_def.0, datatype, column_def.2)
        })
        .collect();

    SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .timestamp_index(timestamp_index)
        .version(version)
        .build()
        .unwrap()
}

pub fn new_schema_ref(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> SchemaRef {
    Arc::new(new_schema(column_defs, timestamp_index))
}
