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

    let insert_batches = insert_batches(insert.values);

    for InsertBatch { columns, row_count } in insert_batches {
        for Column {
            column_name,
            values,
            null_mask,
            ..
        } in columns
        {
            let column_schema = schema
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
                &null_mask,
            );

            columns_values.insert(column_name, vector_builder.finish());
        }
    }

    Ok(InsertRequest {
        table_name,
        columns_values,
    })
}

fn insert_batches(bytes_vec: Vec<Vec<u8>>) -> Vec<InsertBatch> {
    let mut insert_batches = Vec::new();

    for bytes in bytes_vec {
        if let Ok(insert_batch) = bytes.try_into() {
            insert_batches.push(insert_batch);
        }
    }
    insert_batches
}

fn add_values_to_builder(
    builder: &mut VectorBuilder,
    data_type: &ConcreteDataType,
    values: Option<Values>,
    row_count: usize,
    null_mask: &[u8],
) {
    if let Some(vals) = values {
        let mut idx = 0;

        convert_values(data_type, vals).iter().for_each(|val| {
            if get_bit(null_mask, row_count, idx) {
                builder.push(&Value::Null);
                idx += 1;
            }
            builder.push(val);
            idx += 1;
        })
    }
}

fn convert_values(data_type: &ConcreteDataType, values: Values) -> Vec<Value> {
    let mut vals: Vec<Value> = Vec::new();
    match data_type {
        ConcreteDataType::Int64(_) => {
            values.i64_values.into_iter().for_each(|val| {
                vals.push(val.into());
            });
        }
        ConcreteDataType::Float64(_) => {
            values.f64_values.into_iter().for_each(|val| {
                vals.push(val.into());
            });
        }
        ConcreteDataType::String(_) => {
            values.string_values.into_iter().for_each(|val| {
                vals.push(val.into());
            });
        }
        _ => unimplemented!(),
    }
    vals
}

fn get_bit(data: &[u8], bit_len: usize, idx: usize) -> bool {
    assert!(idx < bit_len);

    let byte_idx = idx >> 3;
    let bit_idx = idx & 7;
    (data[byte_idx] >> bit_idx) & 1 != 0
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use datatypes::{data_type::ConcreteDataType, value::Value};

    use crate::server::grpc::insert::{convert_values, get_bit};

    #[test]
    fn test_convert_values() {
        let data_type = ConcreteDataType::float64_datatype();
        let values = Values {
            f64_values: vec![0.1, 0.2, 0.3],
            ..Default::default()
        };

        let result = convert_values(&data_type, values);

        assert_eq!(
            vec![
                Value::Float64(0.1.into()),
                Value::Float64(0.2.into()),
                Value::Float64(0.3.into())
            ],
            result
        );
    }

    #[test]
    fn test_get_bit_right() {
        let null_mask = vec![1, 8];
        let bit_len = 16;

        assert_eq!(get_bit(&null_mask, bit_len, 0), true);
        assert_eq!(get_bit(&null_mask, bit_len, 1), false);
        assert_eq!(get_bit(&null_mask, bit_len, 10), false);
        assert_eq!(get_bit(&null_mask, bit_len, 11), true);
        assert_eq!(get_bit(&null_mask, bit_len, 12), false);
    }

    #[should_panic]
    #[test]
    fn test_get_bit_wrong() {
        let null_mask = vec![1, 8];
        let bit_len = 16;

        assert_eq!(get_bit(&null_mask, bit_len, 16), true);
    }
}
