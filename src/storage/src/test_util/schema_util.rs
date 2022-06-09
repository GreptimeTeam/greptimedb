use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};

/// Column definition: (name, datatype, is_nullable)
pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);

pub fn new_schema(column_defs: &[ColumnDef]) -> Schema {
    let column_schemas = column_defs
        .iter()
        .map(|column_def| {
            let datatype = column_def.1.data_type();
            ColumnSchema::new(column_def.0, datatype, column_def.2)
        })
        .collect();

    Schema::new(column_schemas)
}

pub fn new_schema_ref(column_defs: &[ColumnDef]) -> SchemaRef {
    Arc::new(new_schema(column_defs))
}
