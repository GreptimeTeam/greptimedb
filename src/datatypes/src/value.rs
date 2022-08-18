use std::cmp::Ordering;

use common_base::bytes::{Bytes, StringBytes};
pub use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::prelude::*;

pub type OrderedF32 = OrderedFloat<f32>;
pub type OrderedF64 = OrderedFloat<f64>;

/// Value holds a single arbitrary value of any [DataType](crate::data_type::DataType).
///
/// Although compare Value with different data type is allowed, it is recommended to only
/// compare Value with same data type. Comparing Value with different data type may not
/// behaves as what you expect.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
            Value::List(list) => ConcreteDataType::list_datatype(list.datatype().clone()),
            Value::Date(_) | Value::DateTime(_) => {
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

impl TryFrom<Value> for serde_json::Value {
    type Error = serde_json::Error;

    fn try_from(value: Value) -> serde_json::Result<serde_json::Value> {
        let json_value = match value {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(v) => serde_json::Value::Bool(v),
            Value::UInt8(v) => serde_json::Value::from(v),
            Value::UInt16(v) => serde_json::Value::from(v),
            Value::UInt32(v) => serde_json::Value::from(v),
            Value::UInt64(v) => serde_json::Value::from(v),
            Value::Int8(v) => serde_json::Value::from(v),
            Value::Int16(v) => serde_json::Value::from(v),
            Value::Int32(v) => serde_json::Value::from(v),
            Value::Int64(v) => serde_json::Value::from(v),
            Value::Float32(v) => serde_json::Value::from(v.0),
            Value::Float64(v) => serde_json::Value::from(v.0),
            Value::String(bytes) => serde_json::Value::String(bytes.as_utf8().to_string()),
            Value::Binary(bytes) => serde_json::to_value(bytes)?,
            Value::Date(v) => serde_json::Value::Number(v.into()),
            Value::DateTime(v) => serde_json::Value::Number(v.into()),
            Value::List(v) => serde_json::to_value(v)?,
        };

        Ok(json_value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListValue {
    /// List of nested Values (boxed to reduce size_of(Value))
    #[allow(clippy::box_collection)]
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

    pub fn items(&self) -> &Option<Box<Vec<Value>>> {
        &self.items
    }

    pub fn datatype(&self) -> &ConcreteDataType {
        &self.datatype
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

    fn to_json(value: Value) -> serde_json::Value {
        value.try_into().unwrap()
    }

    #[test]
    fn test_to_json_value() {
        assert_eq!(serde_json::Value::Null, to_json(Value::Null));
        assert_eq!(serde_json::Value::Bool(true), to_json(Value::Boolean(true)));
        assert_eq!(
            serde_json::Value::Number(20u8.into()),
            to_json(Value::UInt8(20))
        );
        assert_eq!(
            serde_json::Value::Number(20i8.into()),
            to_json(Value::Int8(20))
        );
        assert_eq!(
            serde_json::Value::Number(2000u16.into()),
            to_json(Value::UInt16(2000))
        );
        assert_eq!(
            serde_json::Value::Number(2000i16.into()),
            to_json(Value::Int16(2000))
        );
        assert_eq!(
            serde_json::Value::Number(3000u32.into()),
            to_json(Value::UInt32(3000))
        );
        assert_eq!(
            serde_json::Value::Number(3000i32.into()),
            to_json(Value::Int32(3000))
        );
        assert_eq!(
            serde_json::Value::Number(4000u64.into()),
            to_json(Value::UInt64(4000))
        );
        assert_eq!(
            serde_json::Value::Number(4000i64.into()),
            to_json(Value::Int64(4000))
        );
        assert_eq!(
            serde_json::Value::from(125.0f32),
            to_json(Value::Float32(125.0.into()))
        );
        assert_eq!(
            serde_json::Value::from(125.0f64),
            to_json(Value::Float64(125.0.into()))
        );
        assert_eq!(
            serde_json::Value::String(String::from("hello")),
            to_json(Value::String(StringBytes::from("hello")))
        );
        assert_eq!(
            serde_json::Value::from(b"world".as_slice()),
            to_json(Value::Binary(Bytes::from(b"world".as_slice())))
        );
        assert_eq!(
            serde_json::Value::Number(5000i32.into()),
            to_json(Value::Date(5000))
        );
        assert_eq!(
            serde_json::Value::Number(5000i64.into()),
            to_json(Value::DateTime(5000))
        );

        let json_value: serde_json::Value =
            serde_json::from_str(r#"{"items":[{"Int32":123}],"datatype":{"Int32":{}}}"#).unwrap();
        assert_eq!(
            json_value,
            to_json(Value::List(ListValue {
                items: Some(Box::new(vec![Value::Int32(123)])),
                datatype: ConcreteDataType::int32_datatype(),
            }))
        );
    }
}
