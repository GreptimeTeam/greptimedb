#![allow(clippy::all)]

use std::sync::Arc;

use datatypes::{
    data_type::ConcreteDataType,
    prelude::{ScalarVector, ScalarVectorBuilder},
    vectors::{
        BinaryVector, BinaryVectorBuilder, BooleanVector, BooleanVectorBuilder, Float32Vector,
        Float32VectorBuilder, Float64Vector, Float64VectorBuilder, Int16Vector, Int16VectorBuilder,
        Int32Vector, Int32VectorBuilder, Int64Vector, Int64VectorBuilder, Int8Vector,
        Int8VectorBuilder, StringVector, StringVectorBuilder, UInt16Vector, UInt16VectorBuilder,
        UInt32Vector, UInt32VectorBuilder, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder,
        Vector, VectorRef,
    },
};

use crate::write_batch as wb;

tonic::include_proto!("greptime.storage.wal.v1");

impl From<&ConcreteDataType> for DataType {
    fn from(data_type: &ConcreteDataType) -> Self {
        match data_type {
            ConcreteDataType::Boolean(_) => DataType::Boolean,
            ConcreteDataType::Int8(_) => DataType::Int8,
            ConcreteDataType::Int16(_) => DataType::Int16,
            ConcreteDataType::Int32(_) => DataType::Int32,
            ConcreteDataType::Int64(_) => DataType::Int64,
            ConcreteDataType::UInt8(_) => DataType::Uint8,
            ConcreteDataType::UInt16(_) => DataType::Uint16,
            ConcreteDataType::UInt32(_) => DataType::Uint32,
            ConcreteDataType::UInt64(_) => DataType::Uint64,
            ConcreteDataType::Float32(_) => DataType::Float64,
            ConcreteDataType::Float64(_) => DataType::Float64,
            ConcreteDataType::String(_) => DataType::String,
            ConcreteDataType::Null(_) => DataType::Null,
            ConcreteDataType::Binary(_) => DataType::Binary,
            _ => unreachable!(),
        }
    }
}

impl Into<ConcreteDataType> for DataType {
    fn into(self) -> ConcreteDataType {
        match self {
            DataType::Boolean => ConcreteDataType::boolean_datatype(),
            DataType::Int8 => ConcreteDataType::int8_datatype(),
            DataType::Int16 => ConcreteDataType::int16_datatype(),
            DataType::Int32 => ConcreteDataType::int32_datatype(),
            DataType::Int64 => ConcreteDataType::int64_datatype(),
            DataType::Uint8 => ConcreteDataType::uint8_datatype(),
            DataType::Uint16 => ConcreteDataType::uint16_datatype(),
            DataType::Uint32 => ConcreteDataType::uint32_datatype(),
            DataType::Uint64 => ConcreteDataType::uint64_datatype(),
            DataType::Float32 => ConcreteDataType::float32_datatype(),
            DataType::Float64 => ConcreteDataType::float64_datatype(),
            DataType::String => ConcreteDataType::string_datatype(),
            DataType::Binary => ConcreteDataType::binary_datatype(),
            DataType::Null => ConcreteDataType::null_datatype(),
        }
    }
}

#[macro_export]
macro_rules! gen_columns {
    ($name:tt, $vec_ty: ty, $vari:ident, $cast: expr, $field: tt) => {
        pub fn $name(vector: &VectorRef) -> Column {
            let mut column = Column::default();
            let mut values = Values::default();

            let vector_ref = vector
                .as_any()
                .downcast_ref::<$vec_ty>()
                .expect("downcast failed");

            let mut value_null_mask = bit_vec::BitVec::from_elem(vector_ref.len(), false);

            vector_ref
                .iter_data()
                .enumerate()
                .for_each(|(i, value)| match value {
                    Some($vari) => values.$field.push($cast),
                    None => value_null_mask.set(i, true),
                });
            column.value_null_mask = value_null_mask.to_bytes();
            column.values = Some(values);
            column
        }
    };
}

gen_columns!(gen_columns_i8, Int8Vector, value, value as i32, i8_values);
gen_columns!(
    gen_columns_i16,
    Int16Vector,
    value,
    value as i32,
    i16_values
);
gen_columns!(
    gen_columns_i32,
    Int32Vector,
    value,
    value as i32,
    i32_values
);
gen_columns!(
    gen_columns_i64,
    Int64Vector,
    value,
    value as i64,
    i64_values
);
gen_columns!(gen_columns_u8, UInt8Vector, value, value as u32, u8_values);
gen_columns!(
    gen_columns_u16,
    UInt16Vector,
    value,
    value as u32,
    u16_values
);
gen_columns!(
    gen_columns_u32,
    UInt32Vector,
    value,
    value as u32,
    u32_values
);
gen_columns!(
    gen_columns_u64,
    UInt32Vector,
    value,
    value as u64,
    u64_values
);
gen_columns!(
    gen_columns_string,
    StringVector,
    value,
    value.to_string(),
    string_values
);
gen_columns!(gen_columns_bool, BooleanVector, value, value, bool_values);
gen_columns!(gen_columns_f64, Float64Vector, value, value, f64_values);
gen_columns!(gen_columns_f32, Float32Vector, value, value, f32_values);
gen_columns!(
    gen_columns_bytes,
    BinaryVector,
    value,
    value.to_vec(),
    binary_values
);

#[macro_export]
macro_rules! gen_put_data {
    ($name:tt,$builder_type:ty,$field:tt,$v:ident,$cast:expr) => {
        pub fn $name(num_rows: usize, column: &Column) -> VectorRef {
            let values = match &column.values {
                Some(values) => values,
                None => unreachable!(),
            };
            let mut vector_iter = values.$field.iter();
            let mut builder = <$builder_type>::with_capacity(num_rows);
            bit_vec::BitVec::from_bytes(&column.value_null_mask)
                .iter()
                .take(num_rows)
                .for_each(|mask| {
                    if mask == false {
                        let v = match vector_iter.next() {
                            Some($v) => Some($cast),
                            None => None,
                        };
                        builder.push(v);
                    } else {
                        builder.push(None);
                    }
                });
            Arc::new(builder.finish())
        }
    };
}

gen_put_data!(gen_put_data_i8, Int8VectorBuilder, i8_values, v, *v as i8);
gen_put_data!(
    gen_put_data_i16,
    Int16VectorBuilder,
    i16_values,
    v,
    *v as i16
);
gen_put_data!(gen_put_data_i32, Int32VectorBuilder, i32_values, v, *v);
gen_put_data!(gen_put_data_i64, Int64VectorBuilder, i64_values, v, *v);
gen_put_data!(gen_put_data_u8, UInt8VectorBuilder, u8_values, v, *v as u8);
gen_put_data!(
    gen_put_data_u16,
    UInt16VectorBuilder,
    u16_values,
    v,
    *v as u16
);
gen_put_data!(
    gen_put_data_u32,
    UInt32VectorBuilder,
    u32_values,
    v,
    *v as u32
);
gen_put_data!(
    gen_put_data_u64,
    UInt64VectorBuilder,
    u64_values,
    v,
    *v as u64
);
gen_put_data!(
    gen_put_data_f32,
    Float32VectorBuilder,
    f32_values,
    v,
    *v as f32
);
gen_put_data!(
    gen_put_data_f64,
    Float64VectorBuilder,
    f64_values,
    v,
    *v as f64
);
gen_put_data!(
    gen_put_data_string,
    StringVectorBuilder,
    string_values,
    v,
    v.as_str()
);
gen_put_data!(gen_put_data_bool, BooleanVectorBuilder, bool_values, v, *v);
gen_put_data!(
    gen_put_data_bytes,
    BinaryVectorBuilder,
    binary_values,
    v,
    v.as_slice()
);

pub fn gen_columns(data_type: ConcreteDataType, vector: &VectorRef) -> Column {
    let column = match data_type {
        ConcreteDataType::Boolean(_) => gen_columns_bool(vector),
        ConcreteDataType::Int8(_) => gen_columns_i8(vector),
        ConcreteDataType::Int16(_) => gen_columns_i16(vector),
        ConcreteDataType::Int32(_) => gen_columns_i32(vector),
        ConcreteDataType::Int64(_) => gen_columns_i64(vector),
        ConcreteDataType::UInt8(_) => gen_columns_u8(vector),
        ConcreteDataType::UInt16(_) => gen_columns_u16(vector),
        ConcreteDataType::UInt32(_) => gen_columns_u32(vector),
        ConcreteDataType::UInt64(_) => gen_columns_u64(vector),
        ConcreteDataType::Float32(_) => gen_columns_f32(vector),
        ConcreteDataType::Float64(_) => gen_columns_f64(vector),
        ConcreteDataType::Binary(_) => gen_columns_bytes(vector),
        ConcreteDataType::String(_) => gen_columns_string(vector),
        _ => {
            unreachable!()
        }
    };
    column
}

pub fn gen_put_data_vector(
    data_type: ConcreteDataType,
    num_rows: usize,
    column: &Column,
) -> VectorRef {
    match data_type {
        ConcreteDataType::Boolean(_) => gen_put_data_bool(num_rows, column),
        ConcreteDataType::Int8(_) => gen_put_data_i8(num_rows, column),
        ConcreteDataType::Int16(_) => gen_put_data_i16(num_rows, column),
        ConcreteDataType::Int32(_) => gen_put_data_i32(num_rows, column),
        ConcreteDataType::Int64(_) => gen_put_data_i64(num_rows, column),
        ConcreteDataType::UInt8(_) => gen_put_data_u8(num_rows, column),
        ConcreteDataType::UInt16(_) => gen_put_data_u16(num_rows, column),
        ConcreteDataType::UInt32(_) => gen_put_data_u32(num_rows, column),
        ConcreteDataType::UInt64(_) => gen_put_data_u64(num_rows, column),
        ConcreteDataType::Float32(_) => gen_put_data_f32(num_rows, column),
        ConcreteDataType::Float64(_) => gen_put_data_f64(num_rows, column),
        ConcreteDataType::Binary(_) => gen_put_data_bytes(num_rows, column),
        ConcreteDataType::String(_) => gen_put_data_string(num_rows, column),
        _ => unreachable!(),
    }
}
pub fn gen_mutation_extras(write_batch: &wb::WriteBatch) -> Vec<MutationExtra> {
    let column_schemas = write_batch.schema().column_schemas();
    write_batch
        .iter()
        .map(|m| match m {
            wb::Mutation::Put(put) => {
                if put.num_columns() == column_schemas.len() {
                    MutationExtra {
                        mutation_type: MutationType::Put.into(),
                        column_null_mask: Default::default(),
                    }
                } else {
                    let mut column_null_mask =
                        bit_vec::BitVec::from_elem(column_schemas.len(), false);
                    for (i, cs) in column_schemas.iter().enumerate() {
                        if put.column_by_name(&cs.name).is_none() {
                            column_null_mask.set(i, true);
                        }
                    }
                    MutationExtra {
                        mutation_type: MutationType::Put.into(),
                        column_null_mask: column_null_mask.to_bytes(),
                    }
                }
            }
        })
        .collect::<Vec<_>>()
}

impl WalHeader {
    pub fn with_last_manifest_version(last_manifest_version: u64) -> Self {
        Self {
            last_manifest_version,
            ..Default::default()
        }
    }
}
