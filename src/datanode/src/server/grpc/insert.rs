use std::collections::HashMap;

use api::v1::{column::Values, Column, InsertBatch, InsertExpr};
use datatypes::{data_type::ConcreteDataType, value::Value, vectors::VectorBuilder};
use query::catalog::schema::SchemaProviderRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::requests::InsertRequest;

use crate::error::{
    ColumnNotFoundSnafu, DecodeInsertSnafu, IllegalInsertDataSnafu, Result, TableNotFoundSnafu,
};

pub fn insertion_expr_to_request(
    schema_provider: SchemaProviderRef,
    insert: InsertExpr,
) -> Result<InsertRequest> {
    let table_name = insert.table_name;
    let table = schema_provider
        .table(&table_name)
        .context(TableNotFoundSnafu {
            table_name: &table_name,
        })?;
    let schema = table.schema();

    let mut columns_builders = HashMap::with_capacity(schema.column_schemas().len());
    let insert_batches = insert_batches(insert.values)?;

    for InsertBatch { columns, row_count } in insert_batches {
        for Column {
            column_name,
            values,
            null_mask,
            ..
        } in columns
        {
            let values = match values {
                Some(vals) => vals,
                None => continue,
            };

            let column_schema = schema
                .column_schema_by_name(&column_name)
                .with_context(|| ColumnNotFoundSnafu {
                    column_name: &column_name,
                    table_name: &table_name,
                })?;
            let data_type = &column_schema.data_type;

            let vector_builder = columns_builders.entry(column_name).or_insert_with(|| {
                VectorBuilder::with_capacity(data_type.clone(), row_count as usize)
            });

            add_values_to_builder(
                vector_builder,
                data_type,
                values,
                row_count as usize,
                null_mask,
            )?;
        }
    }
    let columns_values = columns_builders
        .into_iter()
        .map(|(column_name, mut vector_builder)| (column_name, vector_builder.finish()))
        .collect();

    Ok(InsertRequest {
        table_name,
        columns_values,
    })
}

fn insert_batches(bytes_vec: Vec<Vec<u8>>) -> Result<Vec<InsertBatch>> {
    let mut insert_batches = Vec::with_capacity(bytes_vec.len());

    for bytes in bytes_vec {
        insert_batches.push(bytes.try_into().context(DecodeInsertSnafu)?);
    }
    Ok(insert_batches)
}

fn add_values_to_builder(
    builder: &mut VectorBuilder,
    data_type: &ConcreteDataType,
    values: Values,
    row_count: usize,
    null_mask: impl Into<BitSet>,
) -> Result<()> {
    let null_mask = null_mask.into();
    let values = convert_values(data_type, values);

    ensure!(
        null_mask.set_count() + values.len() == row_count,
        IllegalInsertDataSnafu
    );

    let mut idx_of_values = 0;
    for idx in 0..row_count {
        if is_null(&null_mask, idx) {
            builder.push(&Value::Null);
        } else {
            builder.push(&values[idx_of_values]);
            idx_of_values += 1;
        }
    }
    Ok(())
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

fn is_null(null_mask: &BitSet, idx: usize) -> bool {
    debug_assert!(idx < null_mask.len, "idx should be less than null_mask.len");

    matches!(null_mask.get_bit(idx), Some(true))
}

struct BitSet {
    buffer: Vec<u8>,
    len: usize,
}

impl BitSet {
    fn set_count(&self) -> usize {
        (0..self.len)
            .into_iter()
            .filter(|&i| matches!(self.get_bit(i), Some(true)))
            .count()
    }

    fn get_bit(&self, idx: usize) -> Option<bool> {
        if idx >= self.len {
            return None;
        }

        let byte_idx = idx >> 3;
        let bit_idx = idx & 7;
        Some((self.buffer[byte_idx] >> bit_idx) & 1 != 0)
    }
}

impl From<Vec<u8>> for BitSet {
    fn from(data: Vec<u8>) -> Self {
        BitSet {
            len: data.len() * 8,
            buffer: data,
        }
    }
}

impl From<&[u8]> for BitSet {
    fn from(data: &[u8]) -> Self {
        BitSet {
            buffer: data.into(),
            len: data.len() * 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use datatypes::{data_type::ConcreteDataType, value::Value};

    use crate::server::grpc::insert::{convert_values, is_null, BitSet};

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
    fn test_is_null() {
        let null_mask: BitSet = vec![0b0000_0001, 0b0000_1000].into();

        assert!(is_null(&null_mask, 0));
        assert!(!is_null(&null_mask, 1));
        assert!(!is_null(&null_mask, 10));
        assert!(is_null(&null_mask, 11));
        assert!(!is_null(&null_mask, 12));
    }

    #[test]
    fn test_bit_set() {
        let bit_set: BitSet = vec![0b0000_0001, 0b0000_1000].into();

        assert!(bit_set.get_bit(0).unwrap());
        assert!(!bit_set.get_bit(1).unwrap());
        assert!(!bit_set.get_bit(10).unwrap());
        assert!(bit_set.get_bit(11).unwrap());
        assert!(!bit_set.get_bit(12).unwrap());

        assert!(bit_set.get_bit(16).is_none());

        assert_eq!(2, bit_set.set_count());

        let bit_set: BitSet = vec![0b0000_0000, 0b0000_0000].into();
        assert_eq!(0, bit_set.set_count());

        let bit_set: BitSet = vec![0b1111_1111, 0b1111_1111].into();
        assert_eq!(16, bit_set.set_count());
    }
}
