use std::collections::HashMap;

use api::v1::{column::Values, Column, InsertBatch, InsertExpr};
use datatypes::{data_type::ConcreteDataType, value::Value, vectors::VectorBuilder};
use query::catalog::schema::SchemaProviderRef;
use snafu::OptionExt;
use table::requests::InsertRequest;

use crate::error::{ColumnNotFoundSnafu, Result, TableNotFoundSnafu};

pub fn insert_to_request(
    schema_provider: SchemaProviderRef,
    insert: InsertExpr,
) -> Result<InsertRequest> {
    let table_name = insert.table_name;
    let table = schema_provider
        .table(&table_name)
        .context(TableNotFoundSnafu {
            table_name: table_name.clone(),
        })?;
    let schema = table.schema();

    let mut columns_values = HashMap::new();

    for value in insert.values {
        if let Ok(InsertBatch { columns, row_count }) = value.try_into() {
            for Column {
                column_name,
                values,
                null_mask,
                ..
            } in columns
            {
                let column_schema =
                    schema
                        .column_schema_by_name(&column_name)
                        .with_context(|| ColumnNotFoundSnafu {
                            column_name: column_name.clone(),
                            table_name: table_name.clone(),
                        })?;
                let data_type = &column_schema.data_type;

                let mut vector_builder =
                    VectorBuilder::with_capacity(data_type.clone(), row_count as usize);

                add_values_to_builder(
                    &mut vector_builder,
                    data_type,
                    values,
                    row_count as usize,
                    null_mask,
                );

                columns_values.insert(column_name, vector_builder.finish());
            }
        }
    }

    Ok(InsertRequest {
        table_name,
        columns_values,
    })
}

fn add_values_to_builder(
    builder: &mut VectorBuilder,
    data_type: &ConcreteDataType,
    values: Option<Values>,
    row_count: usize,
    null_masks: Vec<u8>,
) {
    let vals = if let Some(vals) = values {
        vals
    } else {
        return;
    };
    match data_type {
        ConcreteDataType::Int64(_) => {
            let mut i = 0;
            for val in vals.i64_values {
                if get_bit(&null_masks, row_count, i) {
                    builder.push(&Value::Null);
                    i += 1;
                }
                builder.push(&Value::Int64(val));
                i += 1;
            }
        }
        ConcreteDataType::Float64(_) => {
            let mut i = 0;
            for val in vals.f64_values {
                if get_bit(&null_masks, row_count, i) {
                    builder.push(&Value::Null);
                    i += 1;
                }
                builder.push(&Value::Float64(val.into()));
                i += 1;
            }
        }
        ConcreteDataType::String(_) => {
            let mut i = 0;
            for val in vals.string_values {
                if get_bit(&null_masks, row_count, i) {
                    builder.push(&Value::Null);
                    i += 1;
                }
                builder.push(&Value::String(val.into()));
                i += 1;
            }
        }
        _ => unimplemented!(),
    }
}

fn get_bit(data: &[u8], bit_len: usize, idx: usize) -> bool {
    assert!(idx < bit_len);

    let byte_idx = idx >> 3;
    let bit_idx = idx & 7;
    (data[byte_idx] >> bit_idx) & 1 != 0
}

#[cfg(test)]
mod tests {
    use crate::server::grpc::insert::get_bit;

    #[test]
    fn test_get_bit_right() {
        let data: Vec<u8> = vec![1, 8];
        let bit_len = 16;
        assert_eq!(get_bit(&data, bit_len, 0), true);
        assert_eq!(get_bit(&data, bit_len, 1), false);
        assert_eq!(get_bit(&data, bit_len, 10), false);
        assert_eq!(get_bit(&data, bit_len, 11), true);
        assert_eq!(get_bit(&data, bit_len, 12), false);
    }

    #[should_panic]
    #[test]
    fn test_get_bit_wrong() {
        let data: Vec<u8> = vec![1, 8];
        let bit_len = 16;
        assert_eq!(get_bit(&data, bit_len, 16), true);
    }
}
