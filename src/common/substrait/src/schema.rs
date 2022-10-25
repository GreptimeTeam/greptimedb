use datatypes::schema::{ColumnSchema, Schema};
use substrait_proto::protobuf::r#type::{Nullability, Struct as SubstraitStruct};
use substrait_proto::protobuf::NamedStruct;

use crate::error::Result;
use crate::types::{from_concrete_type, to_concrete_type};

pub fn to_schema(named_struct: NamedStruct) -> Result<Schema> {
    if named_struct.r#struct.is_none() {
        return Ok(Schema::new(vec![]));
    }

    let column_schemas = named_struct
        .r#struct
        .unwrap()
        .types
        .into_iter()
        .zip(named_struct.names.into_iter())
        .map(|(ty, name)| {
            let (concrete_type, is_nullable) = to_concrete_type(&ty)?;
            let column_schema = ColumnSchema::new(name, concrete_type, is_nullable);
            Ok(column_schema)
        })
        .collect::<Result<_>>()?;

    Ok(Schema::new(column_schemas))
}

pub fn from_schema(schema: &Schema) -> Result<NamedStruct> {
    let mut names = Vec::with_capacity(schema.num_columns());
    let mut types = Vec::with_capacity(schema.num_columns());

    for column_schema in schema.column_schemas() {
        names.push(column_schema.name.clone());
        let substrait_type = from_concrete_type(
            column_schema.data_type.clone(),
            Some(column_schema.is_nullable()),
        )?;
        types.push(substrait_type);
    }

    // TODO(ruihang): `type_variation_reference` and `nullability` are unspecified.
    let substrait_struct = SubstraitStruct {
        types,
        type_variation_reference: 0,
        nullability: Nullability::Unspecified as _,
    };

    Ok(NamedStruct {
        names,
        r#struct: Some(substrait_struct),
    })
}
