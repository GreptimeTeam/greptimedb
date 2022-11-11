use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder, SchemaRef};

/// Column definition: (name, datatype, is_nullable)
pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);

pub fn new_schema(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> Schema {
    let column_schemas: Vec<_> = column_defs
        .iter()
        .enumerate()
        .map(|(index, column_def)| {
            let datatype = column_def.1.data_type();
            if let Some(timestamp_index) = timestamp_index {
                ColumnSchema::new(column_def.0, datatype, column_def.2)
                    .with_time_index(index == timestamp_index)
            } else {
                ColumnSchema::new(column_def.0, datatype, column_def.2)
            }
        })
        .collect();

    SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .build()
        .unwrap()
}

pub fn new_schema_ref(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> SchemaRef {
    Arc::new(new_schema(column_defs, timestamp_index))
}
