#![allow(clippy::all)]

<<<<<<< HEAD
=======
use datatypes::{
    arrow::array::{Int32Vec, Int64Vec, UInt64Vec, UInt8Vec},
    data_type::ConcreteDataType,
    prelude::ScalarVector,
    vectors::{
        BinaryVector, BooleanVector, Float32Vector, Float64Vector, Int16Vector, Int32Vector,
        Int64Vector, Int8Vector, StringVector, UInt16Vector, UInt32Vector, UInt8Vector, Vector,
        VectorRef,
    },
};

>>>>>>> 3e9d60a (feat/wal/protobuf)
use crate::write_batch as wb;

tonic::include_proto!("greptime.storage.wal.v1");

<<<<<<< HEAD
=======
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
        pub fn $name(vector: &VectorRef, columns: &mut Vec<Column>) {
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
            columns.push(column);
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

>>>>>>> 3e9d60a (feat/wal/protobuf)
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
