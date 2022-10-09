#![allow(clippy::all)]
tonic::include_proto!("greptime.storage.write_batch.v1");

use std::sync::Arc;

use common_base::BitVec;
use common_error::prelude::*;
use datatypes::schema;
use datatypes::{
    data_type::ConcreteDataType,
    prelude::{ScalarVector, ScalarVectorBuilder},
    vectors::{
        BinaryVector, BinaryVectorBuilder, BooleanVector, BooleanVectorBuilder, Float32Vector,
        Float32VectorBuilder, Float64Vector, Float64VectorBuilder, Int16Vector, Int16VectorBuilder,
        Int32Vector, Int32VectorBuilder, Int64Vector, Int64VectorBuilder, Int8Vector,
        Int8VectorBuilder, StringVector, StringVectorBuilder, TimestampVector,
        TimestampVectorBuilder, UInt16Vector, UInt16VectorBuilder, UInt32Vector,
        UInt32VectorBuilder, UInt64Vector, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder,
        Vector, VectorRef,
    },
};
use paste::paste;
use snafu::OptionExt;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to convert datafusion type: {}", from))]
    Conversion { from: String, backtrace: Backtrace },

    #[snafu(display("Empty column values read"))]
    EmptyColumnValues { backtrace: Backtrace },

    #[snafu(display("Invalid data type: {}", data_type))]
    InvalidDataType {
        data_type: i32,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert schema, source: {}", source))]
    ConvertSchema {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl TimestampIndex {
    pub fn new(value: u64) -> Self {
        Self { value }
    }
}

impl From<&schema::SchemaRef> for Schema {
    fn from(schema: &schema::SchemaRef) -> Self {
        let column_schemas = schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.into())
            .collect();

        Schema {
            column_schemas,
            timestamp_index: schema
                .timestamp_index()
                .map(|index| TimestampIndex::new(index as u64)),
        }
    }
}

impl TryFrom<Schema> for schema::SchemaRef {
    type Error = Error;

    fn try_from(schema: Schema) -> Result<Self> {
        let column_schemas = schema
            .column_schemas
            .iter()
            .map(schema::ColumnSchema::try_from)
            .collect::<Result<Vec<_>>>()?;

        let schema: schema::SchemaRef = match schema.timestamp_index {
            Some(index) => Arc::new(
                schema::SchemaBuilder::try_from(column_schemas)
                    .context(ConvertSchemaSnafu)?
                    .timestamp_index(index.value as usize)
                    .build()
                    .context(ConvertSchemaSnafu)?,
            ),
            None => Arc::new(schema::Schema::try_new(column_schemas).context(ConvertSchemaSnafu)?),
        };

        Ok(schema)
    }
}

impl From<&schema::ColumnSchema> for ColumnSchema {
    fn from(cs: &schema::ColumnSchema) -> Self {
        Self {
            name: cs.name.clone(),
            data_type: DataType::from(&cs.data_type).into(),
            is_nullable: cs.is_nullable(),
        }
    }
}

impl TryFrom<&ColumnSchema> for schema::ColumnSchema {
    type Error = Error;

    fn try_from(column_schema: &ColumnSchema) -> Result<Self> {
        if let Some(data_type) = DataType::from_i32(column_schema.data_type) {
            Ok(schema::ColumnSchema::new(
                column_schema.name.clone(),
                data_type.into(),
                column_schema.is_nullable,
            ))
        } else {
            InvalidDataTypeSnafu {
                data_type: column_schema.data_type,
            }
            .fail()
        }
    }
}

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
            ConcreteDataType::Timestamp(_) => DataType::Timestamp,
            _ => unimplemented!(), // TODO(jiachun): Maybe support some composite types in the future , such as list, struct, etc.
        }
    }
}

impl From<DataType> for ConcreteDataType {
    fn from(data_type: DataType) -> Self {
        match data_type {
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
            DataType::Timestamp => ConcreteDataType::timestamp_millis_datatype(),
        }
    }
}

#[macro_export]
macro_rules! gen_columns {
    ($key: tt, $vec_ty: ty, $vari: ident, $cast: expr) => {
        paste! {
            pub fn [<gen_columns_ $key>](vector: &VectorRef) -> Result<Column> {
                let mut column = Column::default();
                let mut values = Values::default();
                let vector_ref =
                    vector
                        .as_any()
                        .downcast_ref::<$vec_ty>()
                        .with_context(|| ConversionSnafu {
                            from: std::format!("{:?}", vector.as_ref().data_type()),
                        })?;
                let mut bits: Option<BitVec> = None;

                vector_ref
                    .iter_data()
                    .enumerate()
                    .for_each(|(i, value)| match value {
                        Some($vari) => values.[<$key _values>].push($cast),
                        None => {
                            if (bits.is_none()) {
                                bits = Some(BitVec::repeat(false, vector_ref.len()));
                            }
                            bits.as_mut().map(|x| x.set(i, true));
                        }
                    });

                let null_mask = if let Some(bits) = bits {
                    bits.into_vec()
                } else {
                    Default::default()
                };

                column.values = Some(values);
                column.value_null_mask = null_mask;
                column.num_rows = vector_ref.len() as u64;

                Ok(column)
            }
        }
    };
}

gen_columns!(i8, Int8Vector, v, v as i32);
gen_columns!(i16, Int16Vector, v, v as i32);
gen_columns!(i32, Int32Vector, v, v as i32);
gen_columns!(i64, Int64Vector, v, v as i64);
gen_columns!(u8, UInt8Vector, v, v as u32);
gen_columns!(u16, UInt16Vector, v, v as u32);
gen_columns!(u32, UInt32Vector, v, v as u32);
gen_columns!(u64, UInt64Vector, v, v as u64);
gen_columns!(f32, Float32Vector, v, v);
gen_columns!(f64, Float64Vector, v, v);
gen_columns!(bool, BooleanVector, v, v);
gen_columns!(binary, BinaryVector, v, v.to_vec());
gen_columns!(string, StringVector, v, v.to_string());
gen_columns!(timestamp, TimestampVector, v, v.value());

#[macro_export]
macro_rules! gen_put_data {
    ($key: tt, $builder_type: ty, $vari: ident, $cast: expr) => {
        paste! {
            pub fn [<gen_put_data_ $key>](column: Column) -> Result<VectorRef> {
                let values = column.values.context(EmptyColumnValuesSnafu {})?;
                let mut vector_iter = values.[<$key _values>].iter();
                let num_rows = column.num_rows as usize;
                let mut builder = <$builder_type>::with_capacity(num_rows);

                if column.value_null_mask.is_empty() {
                    (0..num_rows)
                        .for_each(|_| builder.push(vector_iter.next().map(|$vari| $cast)));
                } else {
                    BitVec::from_vec(column.value_null_mask)
                        .into_iter()
                        .take(num_rows)
                        .for_each(|is_null| {
                            if is_null {
                                builder.push(None);
                            } else {
                                builder.push(vector_iter.next().map(|$vari| $cast));
                            }
                        });
                }


                Ok(Arc::new(builder.finish()))
            }
        }
    };
}

gen_put_data!(i8, Int8VectorBuilder, v, *v as i8);
gen_put_data!(i16, Int16VectorBuilder, v, *v as i16);
gen_put_data!(i32, Int32VectorBuilder, v, *v);
gen_put_data!(i64, Int64VectorBuilder, v, *v);
gen_put_data!(u8, UInt8VectorBuilder, v, *v as u8);
gen_put_data!(u16, UInt16VectorBuilder, v, *v as u16);
gen_put_data!(u32, UInt32VectorBuilder, v, *v as u32);
gen_put_data!(u64, UInt64VectorBuilder, v, *v as u64);
gen_put_data!(f32, Float32VectorBuilder, v, *v as f32);
gen_put_data!(f64, Float64VectorBuilder, v, *v as f64);
gen_put_data!(bool, BooleanVectorBuilder, v, *v);
gen_put_data!(binary, BinaryVectorBuilder, v, v.as_slice());
gen_put_data!(string, StringVectorBuilder, v, v.as_str());
gen_put_data!(timestamp, TimestampVectorBuilder, v, (*v).into());

pub fn gen_columns(vector: &VectorRef) -> Result<Column> {
    match vector.data_type() {
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
        ConcreteDataType::Binary(_) => gen_columns_binary(vector),
        ConcreteDataType::String(_) => gen_columns_string(vector),
        ConcreteDataType::Timestamp(_) => gen_columns_timestamp(vector),
        _ => {
            unimplemented!() // TODO(jiachun): Maybe support some composite types in the future, such as list, struct, etc.
        }
    }
}

pub fn gen_put_data_vector(data_type: ConcreteDataType, column: Column) -> Result<VectorRef> {
    match data_type {
        ConcreteDataType::Boolean(_) => gen_put_data_bool(column),
        ConcreteDataType::Int8(_) => gen_put_data_i8(column),
        ConcreteDataType::Int16(_) => gen_put_data_i16(column),
        ConcreteDataType::Int32(_) => gen_put_data_i32(column),
        ConcreteDataType::Int64(_) => gen_put_data_i64(column),
        ConcreteDataType::UInt8(_) => gen_put_data_u8(column),
        ConcreteDataType::UInt16(_) => gen_put_data_u16(column),
        ConcreteDataType::UInt32(_) => gen_put_data_u32(column),
        ConcreteDataType::UInt64(_) => gen_put_data_u64(column),
        ConcreteDataType::Float32(_) => gen_put_data_f32(column),
        ConcreteDataType::Float64(_) => gen_put_data_f64(column),
        ConcreteDataType::Binary(_) => gen_put_data_binary(column),
        ConcreteDataType::String(_) => gen_put_data_string(column),
        ConcreteDataType::Timestamp(_) => gen_put_data_timestamp(column),
        _ => unimplemented!(), // TODO(jiachun): Maybe support some composite types in the future, such as list, struct, etc.
    }
}
