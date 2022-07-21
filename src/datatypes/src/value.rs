use std::cmp::Ordering;

use common_base::bytes::{Bytes, StringBytes};
use datafusion_common::ScalarValue;
pub use ordered_float::OrderedFloat;
use serde::{Serialize, Serializer};

use crate::prelude::*;

pub type OrderedF32 = OrderedFloat<f32>;
pub type OrderedF64 = OrderedFloat<f64>;

/// Value holds a single arbitrary value of any [DataType](crate::data_type::DataType).
///
/// Although compare Value with different data type is allowed, it is recommended to only
/// compare Value with same data type. Comparing Value with different data type may not
/// behaves as what you expect.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Null,

    // Numeric types:
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedF32),
    Float64(OrderedF64),

    // String types:
    String(StringBytes),
    Binary(Bytes),

    // Date & Time types:
    Date(i32),
    DateTime(i64),

    List(ListValue),
}

impl Value {
    /// Returns data type of the value.
    ///
    /// # Panics
    /// Panics if the data type is not supported.
    pub fn data_type(&self) -> ConcreteDataType {
        match self {
            Value::Null => ConcreteDataType::null_datatype(),
            Value::Boolean(_) => ConcreteDataType::boolean_datatype(),
            Value::UInt8(_) => ConcreteDataType::uint8_datatype(),
            Value::UInt16(_) => ConcreteDataType::uint16_datatype(),
            Value::UInt32(_) => ConcreteDataType::uint32_datatype(),
            Value::UInt64(_) => ConcreteDataType::uint64_datatype(),
            Value::Int8(_) => ConcreteDataType::int8_datatype(),
            Value::Int16(_) => ConcreteDataType::int16_datatype(),
            Value::Int32(_) => ConcreteDataType::int32_datatype(),
            Value::Int64(_) => ConcreteDataType::int64_datatype(),
            Value::Float32(_) => ConcreteDataType::float32_datatype(),
            Value::Float64(_) => ConcreteDataType::float64_datatype(),
            Value::String(_) => ConcreteDataType::string_datatype(),
            Value::Binary(_) => ConcreteDataType::binary_datatype(),
            Value::Date(_) | Value::DateTime(_) | Value::List(_) => {
                unimplemented!("Unsupported data type of value {:?}", self)
            }
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

macro_rules! impl_from {
    ($Variant:ident, $Type:ident) => {
        impl From<$Type> for Value {
            fn from(value: $Type) -> Self {
                Value::$Variant(value.into())
            }
        }

        impl From<Option<$Type>> for Value {
            fn from(value: Option<$Type>) -> Self {
                match value {
                    Some(v) => Value::$Variant(v.into()),
                    None => Value::Null,
                }
            }
        }
    };
}

impl_from!(Boolean, bool);
impl_from!(UInt8, u8);
impl_from!(UInt16, u16);
impl_from!(UInt32, u32);
impl_from!(UInt64, u64);
impl_from!(Int8, i8);
impl_from!(Int16, i16);
impl_from!(Int32, i32);
impl_from!(Int64, i64);
impl_from!(Float32, f32);
impl_from!(Float64, f64);
impl_from!(String, StringBytes);
impl_from!(Binary, Bytes);

impl From<String> for Value {
    fn from(string: String) -> Value {
        Value::String(string.into())
    }
}

impl From<&str> for Value {
    fn from(string: &str) -> Value {
        Value::String(string.into())
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Value {
        Value::Binary(bytes.into())
    }
}

impl From<&[u8]> for Value {
    fn from(bytes: &[u8]) -> Value {
        Value::Binary(bytes.into())
    }
}

impl From<Vec<Value>> for Value {
    fn from(vs: Vec<Value>) -> Self {
        match vs.len() {
            0 => Value::Null,
            _ => {
                assert!(
                    vs.windows(2).all(|w| w[0].data_type() == w[1].data_type()),
                    "All values' datatypes are not the same!"
                );
                let datatype = vs[0].data_type();
                Value::List(ListValue::new(Some(Box::new(vs)), datatype))
            }
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Null => serde_json::Value::Null.serialize(serializer),
            Value::Boolean(v) => v.serialize(serializer),
            Value::UInt8(v) => v.serialize(serializer),
            Value::UInt16(v) => v.serialize(serializer),
            Value::UInt32(v) => v.serialize(serializer),
            Value::UInt64(v) => v.serialize(serializer),
            Value::Int8(v) => v.serialize(serializer),
            Value::Int16(v) => v.serialize(serializer),
            Value::Int32(v) => v.serialize(serializer),
            Value::Int64(v) => v.serialize(serializer),
            Value::Float32(v) => v.serialize(serializer),
            Value::Float64(v) => v.serialize(serializer),
            Value::String(bytes) => bytes.serialize(serializer),
            Value::Binary(bytes) => bytes.serialize(serializer),
            Value::Date(v) => v.serialize(serializer),
            Value::DateTime(v) => v.serialize(serializer),
            Value::List(_) => unimplemented!(),
        }
    }
}

impl From<Value> for ScalarValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Boolean(v) => ScalarValue::Boolean(Some(v)),
            Value::UInt8(v) => ScalarValue::UInt8(Some(v)),
            Value::UInt16(v) => ScalarValue::UInt16(Some(v)),
            Value::UInt32(v) => ScalarValue::UInt32(Some(v)),
            Value::UInt64(v) => ScalarValue::UInt64(Some(v)),
            Value::Int8(v) => ScalarValue::Int8(Some(v)),
            Value::Int16(v) => ScalarValue::Int16(Some(v)),
            Value::Int32(v) => ScalarValue::Int32(Some(v)),
            Value::Int64(v) => ScalarValue::Int64(Some(v)),
            Value::Float32(v) => ScalarValue::Float32(Some(v.0)),
            Value::Float64(v) => ScalarValue::Float64(Some(v.0)),
            Value::String(v) => ScalarValue::LargeUtf8(Some(v.as_utf8().to_string())),
            Value::Binary(v) => ScalarValue::LargeBinary(Some(v.to_vec())),
            Value::Date(v) => ScalarValue::Date32(Some(v)),
            Value::DateTime(v) => ScalarValue::Date64(Some(v)),
            Value::Null => ScalarValue::Boolean(None),
            Value::List(v) => ScalarValue::List(
                v.items
                    .map(|vs| Box::new(vs.into_iter().map(|v| ScalarValue::from(v)).collect())),
                Box::new(v.datatype.as_arrow_type()),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListValue {
    items: Option<Box<Vec<Value>>>,
    /// Inner values datatype, to distinguish empty lists of different datatypes.
    /// Restricted by DataFusion, cannot use null datatype for empty list.
    datatype: ConcreteDataType,
}

impl Eq for ListValue {}

impl ListValue {
    pub fn new(items: Option<Box<Vec<Value>>>, datatype: ConcreteDataType) -> Self {
        Self { items, datatype }
    }
}

impl PartialOrd for ListValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ListValue {
    fn cmp(&self, other: &Self) -> Ordering {
        assert_eq!(
            self.datatype, other.datatype,
            "Cannot compare different datatypes!"
        );
        self.items.cmp(&other.items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_from_inner() {
        assert_eq!(Value::Boolean(true), Value::from(true));
        assert_eq!(Value::Boolean(false), Value::from(false));

        assert_eq!(Value::UInt8(u8::MIN), Value::from(u8::MIN));
        assert_eq!(Value::UInt8(u8::MAX), Value::from(u8::MAX));

        assert_eq!(Value::UInt16(u16::MIN), Value::from(u16::MIN));
        assert_eq!(Value::UInt16(u16::MAX), Value::from(u16::MAX));

        assert_eq!(Value::UInt32(u32::MIN), Value::from(u32::MIN));
        assert_eq!(Value::UInt32(u32::MAX), Value::from(u32::MAX));

        assert_eq!(Value::UInt64(u64::MIN), Value::from(u64::MIN));
        assert_eq!(Value::UInt64(u64::MAX), Value::from(u64::MAX));

        assert_eq!(Value::Int8(i8::MIN), Value::from(i8::MIN));
        assert_eq!(Value::Int8(i8::MAX), Value::from(i8::MAX));

        assert_eq!(Value::Int16(i16::MIN), Value::from(i16::MIN));
        assert_eq!(Value::Int16(i16::MAX), Value::from(i16::MAX));

        assert_eq!(Value::Int32(i32::MIN), Value::from(i32::MIN));
        assert_eq!(Value::Int32(i32::MAX), Value::from(i32::MAX));

        assert_eq!(Value::Int64(i64::MIN), Value::from(i64::MIN));
        assert_eq!(Value::Int64(i64::MAX), Value::from(i64::MAX));

        assert_eq!(
            Value::Float32(OrderedFloat(f32::MIN)),
            Value::from(f32::MIN)
        );
        assert_eq!(
            Value::Float32(OrderedFloat(f32::MAX)),
            Value::from(f32::MAX)
        );

        assert_eq!(
            Value::Float64(OrderedFloat(f64::MIN)),
            Value::from(f64::MIN)
        );
        assert_eq!(
            Value::Float64(OrderedFloat(f64::MAX)),
            Value::from(f64::MAX)
        );

        let string_bytes = StringBytes::from("hello");
        assert_eq!(
            Value::String(string_bytes.clone()),
            Value::from(string_bytes)
        );

        let bytes = Bytes::from(b"world".as_slice());
        assert_eq!(Value::Binary(bytes.clone()), Value::from(bytes));
    }

    #[test]
    fn test_from_values_vec() {
        let vs: Vec<Value> = vec![];
        let v = Value::from(vs);
        assert_eq!(Value::Null, v);

        let vs = vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)];
        let v = Value::from(vs.clone());
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(vs)),
                ConcreteDataType::int32_datatype()
            )),
            v
        );
    }

    #[test]
    fn test_value_datatype() {
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            Value::Boolean(true).data_type()
        );
        assert_eq!(
            ConcreteDataType::uint8_datatype(),
            Value::UInt8(u8::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::uint16_datatype(),
            Value::UInt16(u16::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::uint16_datatype(),
            Value::UInt16(u16::MAX).data_type()
        );
        assert_eq!(
            ConcreteDataType::uint32_datatype(),
            Value::UInt32(u32::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            Value::UInt64(u64::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::int8_datatype(),
            Value::Int8(i8::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::int16_datatype(),
            Value::Int16(i16::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::int32_datatype(),
            Value::Int32(i32::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            Value::Int64(i64::MIN).data_type()
        );
        assert_eq!(
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(f32::MIN)).data_type(),
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(f64::MIN)).data_type(),
        );
        assert_eq!(
            ConcreteDataType::string_datatype(),
            Value::String(StringBytes::from("hello")).data_type(),
        );
        assert_eq!(
            ConcreteDataType::binary_datatype(),
            Value::Binary(Bytes::from(b"world".as_slice())).data_type()
        );
    }

    #[test]
    fn test_value_from_string() {
        let hello = "hello".to_string();
        assert_eq!(
            Value::String(StringBytes::from(hello.clone())),
            Value::from(hello)
        );

        let world = "world";
        assert_eq!(Value::String(StringBytes::from(world)), Value::from(world));
    }

    #[test]
    fn test_value_from_bytes() {
        let hello = b"hello".to_vec();
        assert_eq!(
            Value::Binary(Bytes::from(hello.clone())),
            Value::from(hello)
        );

        let world: &[u8] = b"world";
        assert_eq!(Value::Binary(Bytes::from(world)), Value::from(world));
    }

    #[test]
    fn test_value_into_scalar_value() {
        assert_eq!(
            ScalarValue::Boolean(Some(true)),
            Value::Boolean(true).into()
        );
        assert_eq!(
            ScalarValue::Boolean(Some(true)),
            Value::Boolean(true).into()
        );

        assert_eq!(
            ScalarValue::UInt8(Some(u8::MIN + 1)),
            Value::UInt8(u8::MIN + 1).into()
        );
        assert_eq!(
            ScalarValue::UInt16(Some(u16::MIN + 2)),
            Value::UInt16(u16::MIN + 2).into()
        );
        assert_eq!(
            ScalarValue::UInt32(Some(u32::MIN + 3)),
            Value::UInt32(u32::MIN + 3).into()
        );
        assert_eq!(
            ScalarValue::UInt64(Some(u64::MIN + 4)),
            Value::UInt64(u64::MIN + 4).into()
        );

        assert_eq!(
            ScalarValue::Int8(Some(i8::MIN + 4)),
            Value::Int8(i8::MIN + 4).into()
        );
        assert_eq!(
            ScalarValue::Int16(Some(i16::MIN + 5)),
            Value::Int16(i16::MIN + 5).into()
        );
        assert_eq!(
            ScalarValue::Int32(Some(i32::MIN + 6)),
            Value::Int32(i32::MIN + 6).into()
        );
        assert_eq!(
            ScalarValue::Int64(Some(i64::MIN + 7)),
            Value::Int64(i64::MIN + 7).into()
        );

        assert_eq!(
            ScalarValue::Float32(Some(8.0f32)),
            Value::Float32(OrderedFloat(8.0f32)).into()
        );
        assert_eq!(
            ScalarValue::Float64(Some(9.0f64)),
            Value::Float64(OrderedFloat(9.0f64)).into()
        );

        assert_eq!(
            ScalarValue::LargeUtf8(Some("hello".to_string())),
            Value::String(StringBytes::from("hello")).into()
        );
        assert_eq!(
            ScalarValue::LargeBinary(Some("world".as_bytes().to_vec())),
            Value::Binary(Bytes::from("world".as_bytes())).into()
        );

        assert_eq!(ScalarValue::Date32(Some(10i32)), Value::Date(10i32).into());
        assert_eq!(
            ScalarValue::Date64(Some(20i64)),
            Value::DateTime(20i64).into()
        );

        assert_eq!(ScalarValue::Boolean(None), Value::Null.into());
    }
}
