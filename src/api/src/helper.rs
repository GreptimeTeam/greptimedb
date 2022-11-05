use common_base::BitVec;
use common_time::timestamp::TimeUnit;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::v1::column::Values;
use crate::v1::Column;
use crate::v1::ColumnDataType;

#[derive(Debug, PartialEq, Eq)]
pub struct ColumnDataTypeWrapper(ColumnDataType);

impl ColumnDataTypeWrapper {
    pub fn try_new(datatype: i32) -> Result<Self> {
        let datatype = ColumnDataType::from_i32(datatype)
            .context(error::UnknownColumnDataTypeSnafu { datatype })?;
        Ok(Self(datatype))
    }

    pub fn datatype(&self) -> ColumnDataType {
        self.0
    }
}

impl From<ColumnDataTypeWrapper> for ConcreteDataType {
    fn from(datatype: ColumnDataTypeWrapper) -> Self {
        match datatype.0 {
            ColumnDataType::Boolean => ConcreteDataType::boolean_datatype(),
            ColumnDataType::Int8 => ConcreteDataType::int8_datatype(),
            ColumnDataType::Int16 => ConcreteDataType::int16_datatype(),
            ColumnDataType::Int32 => ConcreteDataType::int32_datatype(),
            ColumnDataType::Int64 => ConcreteDataType::int64_datatype(),
            ColumnDataType::Uint8 => ConcreteDataType::uint8_datatype(),
            ColumnDataType::Uint16 => ConcreteDataType::uint16_datatype(),
            ColumnDataType::Uint32 => ConcreteDataType::uint32_datatype(),
            ColumnDataType::Uint64 => ConcreteDataType::uint64_datatype(),
            ColumnDataType::Float32 => ConcreteDataType::float32_datatype(),
            ColumnDataType::Float64 => ConcreteDataType::float64_datatype(),
            ColumnDataType::Binary => ConcreteDataType::binary_datatype(),
            ColumnDataType::String => ConcreteDataType::string_datatype(),
            ColumnDataType::Date => ConcreteDataType::date_datatype(),
            ColumnDataType::Datetime => ConcreteDataType::datetime_datatype(),
            ColumnDataType::Timestamp => ConcreteDataType::timestamp_millis_datatype(),
        }
    }
}

impl TryFrom<ConcreteDataType> for ColumnDataTypeWrapper {
    type Error = error::Error;

    fn try_from(datatype: ConcreteDataType) -> Result<Self> {
        let datatype = ColumnDataTypeWrapper(match datatype {
            ConcreteDataType::Boolean(_) => ColumnDataType::Boolean,
            ConcreteDataType::Int8(_) => ColumnDataType::Int8,
            ConcreteDataType::Int16(_) => ColumnDataType::Int16,
            ConcreteDataType::Int32(_) => ColumnDataType::Int32,
            ConcreteDataType::Int64(_) => ColumnDataType::Int64,
            ConcreteDataType::UInt8(_) => ColumnDataType::Uint8,
            ConcreteDataType::UInt16(_) => ColumnDataType::Uint16,
            ConcreteDataType::UInt32(_) => ColumnDataType::Uint32,
            ConcreteDataType::UInt64(_) => ColumnDataType::Uint64,
            ConcreteDataType::Float32(_) => ColumnDataType::Float32,
            ConcreteDataType::Float64(_) => ColumnDataType::Float64,
            ConcreteDataType::Binary(_) => ColumnDataType::Binary,
            ConcreteDataType::String(_) => ColumnDataType::String,
            ConcreteDataType::Date(_) => ColumnDataType::Date,
            ConcreteDataType::DateTime(_) => ColumnDataType::Datetime,
            ConcreteDataType::Timestamp(_) => ColumnDataType::Timestamp,
            ConcreteDataType::Null(_) | ConcreteDataType::List(_) => {
                return error::IntoColumnDataTypeSnafu { from: datatype }.fail()
            }
        });
        Ok(datatype)
    }
}

impl Values {
    pub fn with_capacity(datatype: ColumnDataType, capacity: usize) -> Self {
        match datatype {
            ColumnDataType::Boolean => Values {
                bool_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Int8 => Values {
                i8_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Int16 => Values {
                i16_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Int32 => Values {
                i32_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Int64 => Values {
                i64_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Uint8 => Values {
                u8_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Uint16 => Values {
                u16_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Uint32 => Values {
                u32_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Uint64 => Values {
                u64_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Float32 => Values {
                f32_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Float64 => Values {
                f64_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Binary => Values {
                binary_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::String => Values {
                string_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Date => Values {
                date_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Datetime => Values {
                datetime_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
            ColumnDataType::Timestamp => Values {
                ts_millis_values: Vec::with_capacity(capacity),
                ..Default::default()
            },
        }
    }
}

impl Column {
    // The type of vals must be same.
    pub fn push_vals(&mut self, origin_count: usize, vals: Vec<Value>) {
        if self.values.is_none() {
            self.values = Some(Values::default());
        }
        let mut null_mask = BitVec::from_slice(&self.null_mask);
        null_mask.reserve_exact(origin_count + vals.len());
        null_mask.extend(BitVec::repeat(false, vals.len()));

        // It is safe to use unwrap here, because it has been verified before.
        let values = self.values.as_mut().unwrap();
        for (idx, val) in vals.into_iter().enumerate() {
            match val {
                Value::Null => null_mask.set(idx + origin_count, true),
                Value::Boolean(val) => values.bool_values.push(val),
                Value::UInt8(val) => values.u8_values.push(val.into()),
                Value::UInt16(val) => values.u16_values.push(val.into()),
                Value::UInt32(val) => values.u32_values.push(val),
                Value::UInt64(val) => values.u64_values.push(val),
                Value::Int8(val) => values.i8_values.push(val.into()),
                Value::Int16(val) => values.i16_values.push(val.into()),
                Value::Int32(val) => values.i32_values.push(val),
                Value::Int64(val) => values.i64_values.push(val),
                Value::Float32(val) => values.f32_values.push(*val),
                Value::Float64(val) => values.f64_values.push(*val),
                Value::String(val) => values.string_values.push(val.as_utf8().to_string()),
                Value::Binary(val) => values.binary_values.push(val.to_vec()),
                Value::Date(val) => values.date_values.push(val.val()),
                Value::DateTime(val) => values.datetime_values.push(val.val()),
                Value::Timestamp(val) => values
                    .ts_millis_values
                    .push(val.convert_to(TimeUnit::Millisecond)),
                Value::List(_) => unreachable!(),
            }
        }
        self.null_mask = null_mask.into_vec();
    }
}

impl From<ConcreteDataType> for ColumnDataType {
    fn from(t: ConcreteDataType) -> Self {
        match t {
            ConcreteDataType::Boolean(_) => ColumnDataType::Boolean,
            ConcreteDataType::Int8(_) => ColumnDataType::Int8,
            ConcreteDataType::Int16(_) => ColumnDataType::Int16,
            ConcreteDataType::Int32(_) => ColumnDataType::Int32,
            ConcreteDataType::Int64(_) => ColumnDataType::Int64,
            ConcreteDataType::UInt8(_) => ColumnDataType::Uint8,
            ConcreteDataType::UInt16(_) => ColumnDataType::Uint16,
            ConcreteDataType::UInt32(_) => ColumnDataType::Uint32,
            ConcreteDataType::UInt64(_) => ColumnDataType::Uint64,
            ConcreteDataType::Float32(_) => ColumnDataType::Float32,
            ConcreteDataType::Float64(_) => ColumnDataType::Float64,
            ConcreteDataType::Binary(_) => ColumnDataType::Binary,
            ConcreteDataType::String(_) => ColumnDataType::String,
            ConcreteDataType::Date(_) => ColumnDataType::Date,
            ConcreteDataType::DateTime(_) => ColumnDataType::Datetime,
            ConcreteDataType::Timestamp(_) => ColumnDataType::Timestamp,
            ConcreteDataType::List(_) => unreachable!(),
            ConcreteDataType::Null(_) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_with_capacity() {
        let values = Values::with_capacity(ColumnDataType::Int8, 2);
        let values = values.i8_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Int32, 2);
        let values = values.i32_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Int64, 2);
        let values = values.i64_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Uint8, 2);
        let values = values.u8_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Uint32, 2);
        let values = values.u32_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Uint64, 2);
        let values = values.u64_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Float32, 2);
        let values = values.f32_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Float64, 2);
        let values = values.f64_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Binary, 2);
        let values = values.binary_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Boolean, 2);
        let values = values.bool_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::String, 2);
        let values = values.string_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Date, 2);
        let values = values.date_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Datetime, 2);
        let values = values.datetime_values;
        assert_eq!(2, values.capacity());

        let values = Values::with_capacity(ColumnDataType::Timestamp, 2);
        let values = values.ts_millis_values;
        assert_eq!(2, values.capacity());
    }

    #[test]
    fn test_concrete_datatype_from_column_datatype() {
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Boolean).into()
        );
        assert_eq!(
            ConcreteDataType::int8_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int8).into()
        );
        assert_eq!(
            ConcreteDataType::int16_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int16).into()
        );
        assert_eq!(
            ConcreteDataType::int32_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int32).into()
        );
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int64).into()
        );
        assert_eq!(
            ConcreteDataType::uint8_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint8).into()
        );
        assert_eq!(
            ConcreteDataType::uint16_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint16).into()
        );
        assert_eq!(
            ConcreteDataType::uint32_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint32).into()
        );
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint64).into()
        );
        assert_eq!(
            ConcreteDataType::float32_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Float32).into()
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Float64).into()
        );
        assert_eq!(
            ConcreteDataType::binary_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Binary).into()
        );
        assert_eq!(
            ConcreteDataType::string_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::String).into()
        );
        assert_eq!(
            ConcreteDataType::date_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Date).into()
        );
        assert_eq!(
            ConcreteDataType::datetime_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Datetime).into()
        );
        assert_eq!(
            ConcreteDataType::timestamp_millis_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Timestamp).into()
        );
    }

    #[test]
    fn test_column_datatype_from_concrete_datatype() {
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Boolean),
            ConcreteDataType::boolean_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int8),
            ConcreteDataType::int8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int16),
            ConcreteDataType::int16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int32),
            ConcreteDataType::int32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int64),
            ConcreteDataType::int64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint8),
            ConcreteDataType::uint8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint16),
            ConcreteDataType::uint16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint32),
            ConcreteDataType::uint32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint64),
            ConcreteDataType::uint64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Float32),
            ConcreteDataType::float32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Float64),
            ConcreteDataType::float64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Binary),
            ConcreteDataType::binary_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::String),
            ConcreteDataType::string_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Date),
            ConcreteDataType::date_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Datetime),
            ConcreteDataType::datetime_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Timestamp),
            ConcreteDataType::timestamp_millis_datatype()
                .try_into()
                .unwrap()
        );

        let result: Result<ColumnDataTypeWrapper> = ConcreteDataType::null_datatype().try_into();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Failed to create column datatype from Null(NullType)"
        );

        let result: Result<ColumnDataTypeWrapper> =
            ConcreteDataType::list_datatype(ConcreteDataType::boolean_datatype()).try_into();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Failed to create column datatype from List(ListType { inner: Boolean(BooleanType) })"
        );
    }

    #[test]
    fn test_column_put_vals() {
        use crate::v1::column::SemanticType;
        // Some(false), None, Some(true), Some(true)
        let mut column = Column {
            column_name: "test".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(Values {
                bool_values: vec![false, true, true],
                ..Default::default()
            }),
            null_mask: vec![2],
            datatype: ColumnDataType::Boolean as i32,
        };
        let row_count = 4;
        column.push_vals(row_count, vec![true.into(), Value::Null, false.into()]);
        // Some(false), None, Some(true), Some(true), Some(true), None, Some(false)
        let bool_values = column.values.unwrap().bool_values;
        assert_eq!(vec![false, true, true, true, false], bool_values);
        let null_mask = column.null_mask;
        assert_eq!(34, null_mask[0]);
    }

    #[test]
    fn test_convert_concrete_to_grpc_datatype() {
        let concrete_type = ConcreteDataType::int8_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Int8);

        let concrete_type = ConcreteDataType::int16_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Int16);

        let concrete_type = ConcreteDataType::int32_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Int32);

        let concrete_type = ConcreteDataType::int64_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Int64);

        let concrete_type = ConcreteDataType::uint8_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Uint8);

        let concrete_type = ConcreteDataType::uint16_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Uint16);

        let concrete_type = ConcreteDataType::uint32_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Uint32);

        let concrete_type = ConcreteDataType::uint64_datatype();
        let grpc_type: ColumnDataType = concrete_type.into();
        assert_eq!(grpc_type, ColumnDataType::Uint64);
    }
}
