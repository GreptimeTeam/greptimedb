use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};

/// Column definition: (name, datatype, is_nullable)
pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);

pub fn new_schema(column_defs: &[ColumnDef]) -> Schema {
    let column_schemas = column_defs
        .into_iter()
        .map(|column_def| {
            let datatype = logical_type_id_to_concrete_type(column_def.1);
            ColumnSchema::new(column_def.0, datatype, column_def.2)
        })
        .collect();

    Schema::new(column_schemas)
}

pub fn new_schema_ref(column_defs: &[ColumnDef]) -> SchemaRef {
    Arc::new(new_schema(column_defs))
}

pub fn logical_type_id_to_concrete_type(type_id: LogicalTypeId) -> ConcreteDataType {
    match type_id {
        LogicalTypeId::Null => ConcreteDataType::null_datatype(),
        LogicalTypeId::Boolean => ConcreteDataType::boolean_datatype(),
        LogicalTypeId::Int8 => ConcreteDataType::int8_datatype(),
        LogicalTypeId::Int16 => ConcreteDataType::int16_datatype(),
        LogicalTypeId::Int32 => ConcreteDataType::int32_datatype(),
        LogicalTypeId::Int64 => ConcreteDataType::int64_datatype(),
        LogicalTypeId::UInt8 => ConcreteDataType::uint8_datatype(),
        LogicalTypeId::UInt16 => ConcreteDataType::uint16_datatype(),
        LogicalTypeId::UInt32 => ConcreteDataType::uint32_datatype(),
        LogicalTypeId::UInt64 => ConcreteDataType::uint64_datatype(),
        LogicalTypeId::Float32 => ConcreteDataType::float32_datatype(),
        LogicalTypeId::Float64 => ConcreteDataType::float64_datatype(),
        LogicalTypeId::String => ConcreteDataType::string_datatype(),
        LogicalTypeId::Binary => ConcreteDataType::binary_datatype(),
        LogicalTypeId::Date | LogicalTypeId::DateTime => {
            unimplemented!("Unimplemented logical type")
        }
    }
}
