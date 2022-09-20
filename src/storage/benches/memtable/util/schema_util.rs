use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder, SchemaRef};

/// Column definition: (name, datatype, is_nullable)
pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);

pub fn new_schema(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> Schema {
    let column_schemas = column_defs
        .iter()
        .map(|column_def| {
            let datatype = column_def.1.data_type();
            ColumnSchema::new(column_def.0, datatype, column_def.2)
        })
        .collect();

    if let Some(index) = timestamp_index {
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .timestamp_index(index)
            .build()
            .unwrap()
    } else {
        Schema::try_new(column_schemas).unwrap()
    }
}

pub fn new_schema_ref(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> SchemaRef {
    Arc::new(new_schema(column_defs, timestamp_index))
}
