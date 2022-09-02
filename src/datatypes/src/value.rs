use std::cmp::Ordering;

use common_base::bytes::{Bytes, StringBytes};
use common_time::date::Date;
use common_time::datetime::DateTime;
pub use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::error::{self, Result};
use crate::prelude::*;
use crate::vectors::ListVector;

pub type OrderedF32 = OrderedFloat<f32>;
pub type OrderedF64 = OrderedFloat<f64>;

/// Value holds a single arbitrary value of any [DataType](crate::data_type::DataType).
///
/// Although compare Value with different data type is allowed, it is recommended to only
/// compare Value with same data type. Comparing Value with different data type may not
/// behaves as what you expect.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Value {
    // Null is a little special, we'd like to keep it as the first variant, so a `Null`
    // should less than all other variants.
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
    Date(Date),
    DateTime(DateTime),

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
            Value::Date(_) => ConcreteDataType::date_datatype(),
            Value::DateTime(_) => ConcreteDataType::date_datatype(),
        }
    }

    /// Returns true if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Cast itself to [ListValue].
    pub fn as_list(&self) -> Result<Option<&ListValue>> {
        match self {
            Value::Null => Ok(None),
            Value::List(v) => Ok(Some(v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast {:?} to list value", other),
            }
            .fail(),
        }
    }

    /// Cast itself to [ValueRef].
    pub fn as_value_ref(&self) -> ValueRef {
        match self {
            Value::Null => ValueRef::Null,
            Value::Boolean(v) => ValueRef::Boolean(*v),
            Value::UInt8(v) => ValueRef::UInt8(*v),
            Value::UInt16(v) => ValueRef::UInt16(*v),
            Value::UInt32(v) => ValueRef::UInt32(*v),
            Value::UInt64(v) => ValueRef::UInt64(*v),
            Value::Int8(v) => ValueRef::Int8(*v),
            Value::Int16(v) => ValueRef::Int16(*v),
            Value::Int32(v) => ValueRef::Int32(*v),
            Value::Int64(v) => ValueRef::Int64(*v),
            Value::Float32(v) => ValueRef::Float32(*v),
            Value::Float64(v) => ValueRef::Float64(*v),
            Value::String(v) => ValueRef::String(v.as_utf8()),
            Value::Binary(v) => ValueRef::Binary(v),
            Value::Date(v) => ValueRef::Date(*v),
            Value::DateTime(v) => ValueRef::DateTime(*v),
            Value::List(v) => ValueRef::List(ListValueRef::Ref(v)),
        }
    }
}

macro_rules! impl_value_from {
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

impl_value_from!(Boolean, bool);
impl_value_from!(UInt8, u8);
impl_value_from!(UInt16, u16);
impl_value_from!(UInt32, u32);
impl_value_from!(UInt64, u64);
impl_value_from!(Int8, i8);
impl_value_from!(Int16, i16);
impl_value_from!(Int32, i32);
impl_value_from!(Int64, i64);
impl_value_from!(Float32, f32);
impl_value_from!(Float64, f64);
impl_value_from!(String, StringBytes);
impl_value_from!(Binary, Bytes);

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
            Value::Date(v) => serde_json::Value::Number(v.val().into()),
            Value::DateTime(v) => serde_json::Value::Number(v.val().into()),
            Value::List(v) => serde_json::to_value(v)?,
        };

        Ok(json_value)
    }
}

/// List value.
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

/// Reference to [Value].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueRef<'a> {
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
    String(&'a str),
    Binary(&'a [u8]),

    // Date & Time types:
    Date(Date),
    DateTime(DateTime),

    List(ListValueRef<'a>),
}

impl<'a> ValueRef<'a> {
    /// Returns true if this is null.
    pub fn is_null(&self) -> bool {
        matches!(self, ValueRef::Null)
    }

    /// Cast itself to binary slice.
    pub fn as_binary(&self) -> Result<Option<&[u8]>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::Binary(v) => Ok(Some(v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to binary", other),
            }
            .fail(),
        }
    }

    /// Cast itself to string slice.
    pub fn as_string(&self) -> Result<Option<&str>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::String(v) => Ok(Some(v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to string", other),
            }
            .fail(),
        }
    }

    /// Cast itself to boolean.
    pub fn as_boolean(&self) -> Result<Option<bool>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::Boolean(v) => Ok(Some(*v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to boolean", other),
            }
            .fail(),
        }
    }

    /// Cast itself to [Date].
    pub fn as_date(&self) -> Result<Option<Date>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::Date(v) => Ok(Some(*v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to date", other),
            }
            .fail(),
        }
    }

    /// Cast itself to [DateTime].
    pub fn as_datetime(&self) -> Result<Option<DateTime>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::DateTime(v) => Ok(Some(*v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to datetime", other),
            }
            .fail(),
        }
    }

    /// Cast itself to [ListValueRef].
    pub fn as_list(&self) -> Result<Option<ListValueRef>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::List(v) => Ok(Some(*v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to list", other),
            }
            .fail(),
        }
    }
}

/// A helper trait to convert copyable types to `ValueRef`.
///
/// It removes the lifetime parameter from the trait and also avoid confusion
/// with `Into<ValueRef>` in generic codes.
pub trait IntoValueRef {
    /// Convert itself to [ValueRef].
    fn into_value_ref(self) -> ValueRef<'static>;
}

macro_rules! impl_value_ref_from {
    ($Variant:ident, $Type:ident) => {
        impl From<$Type> for ValueRef<'_> {
            fn from(value: $Type) -> Self {
                ValueRef::$Variant(value.into())
            }
        }

        impl IntoValueRef for $Type {
            fn into_value_ref(self) -> ValueRef<'static> {
                ValueRef::$Variant(self.into())
            }
        }

        impl From<Option<$Type>> for ValueRef<'_> {
            fn from(value: Option<$Type>) -> Self {
                match value {
                    Some(v) => ValueRef::$Variant(v.into()),
                    None => ValueRef::Null,
                }
            }
        }

        impl IntoValueRef for Option<$Type> {
            fn into_value_ref(self) -> ValueRef<'static> {
                match self {
                    Some(v) => ValueRef::$Variant(v.into()),
                    None => ValueRef::Null,
                }
            }
        }
    };
}

impl_value_ref_from!(Boolean, bool);
impl_value_ref_from!(UInt8, u8);
impl_value_ref_from!(UInt16, u16);
impl_value_ref_from!(UInt32, u32);
impl_value_ref_from!(UInt64, u64);
impl_value_ref_from!(Int8, i8);
impl_value_ref_from!(Int16, i16);
impl_value_ref_from!(Int32, i32);
impl_value_ref_from!(Int64, i64);
impl_value_ref_from!(Float32, f32);
impl_value_ref_from!(Float64, f64);

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(string: &'a str) -> ValueRef<'a> {
        ValueRef::String(string)
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(bytes: &'a [u8]) -> ValueRef<'a> {
        ValueRef::Binary(bytes)
    }
}

/// Reference to a [ListValue].
// Comparision still requires some allocation (call of `to_value()`) and might be avoidable.
#[derive(Debug, Clone, Copy)]
pub enum ListValueRef<'a> {
    Indexed { vector: &'a ListVector, idx: usize },
    Ref(&'a ListValue),
}

impl<'a> ListValueRef<'a> {
    fn to_value(self) -> Value {
        match self {
            ListValueRef::Indexed { vector, idx } => vector.get(idx),
            ListValueRef::Ref(v) => Value::List((*v).clone()),
        }
    }
}

impl<'a> PartialEq for ListValueRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.to_value().eq(&other.to_value())
    }
}

impl<'a> Eq for ListValueRef<'a> {}

impl<'a> Ord for ListValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Respect the order of `Value` by converting into value before comparision.
        self.to_value().cmp(&other.to_value())
    }
}

impl<'a> PartialOrd for ListValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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
            to_json(Value::Date(common_time::date::Date::new(5000)))
        );
        assert_eq!(
            serde_json::Value::Number(5000i64.into()),
            to_json(Value::DateTime(DateTime::new(5000)))
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

    #[test]
    fn test_null_value() {
        assert!(Value::Null.is_null());
        assert!(!Value::Boolean(true).is_null());
        assert!(Value::Null < Value::Boolean(false));
        assert!(Value::Null < Value::Int32(10));
    }

    #[test]
    fn test_null_value_ref() {
        assert!(ValueRef::Null.is_null());
        assert!(!ValueRef::Boolean(true).is_null());
        assert!(ValueRef::Null < ValueRef::Boolean(false));
        assert!(ValueRef::Null < ValueRef::Int32(10));
    }

    #[test]
    fn test_as_value_ref() {
        macro_rules! check_as_value_ref {
            ($Variant: ident, $data: expr) => {
                let value = Value::$Variant($data);
                let value_ref = value.as_value_ref();
                let expect_ref = ValueRef::$Variant($data);

                assert_eq!(expect_ref, value_ref);
            };
        }

        assert_eq!(ValueRef::Null, Value::Null.as_value_ref());
        check_as_value_ref!(Boolean, true);
        check_as_value_ref!(UInt8, 123);
        check_as_value_ref!(UInt16, 123);
        check_as_value_ref!(UInt32, 123);
        check_as_value_ref!(UInt64, 123);
        check_as_value_ref!(Int8, -12);
        check_as_value_ref!(Int16, -12);
        check_as_value_ref!(Int32, -12);
        check_as_value_ref!(Int64, -12);
        check_as_value_ref!(Float32, OrderedF32::from(16.0));
        check_as_value_ref!(Float64, OrderedF64::from(16.0));

        assert_eq!(
            ValueRef::String("hello"),
            Value::String("hello".into()).as_value_ref()
        );
        assert_eq!(
            ValueRef::Binary(b"hello"),
            Value::Binary("hello".as_bytes().into()).as_value_ref()
        );

        check_as_value_ref!(Date, Date::new(103));
        check_as_value_ref!(DateTime, DateTime::new(1034));

        let list = ListValue {
            items: None,
            datatype: ConcreteDataType::int32_datatype(),
        };
        assert_eq!(
            ValueRef::List(ListValueRef::Ref(&list)),
            Value::List(list.clone()).as_value_ref()
        );
    }

    #[test]
    fn test_value_ref_as() {
        macro_rules! check_as_null {
            ($method: ident) => {
                assert_eq!(None, ValueRef::Null.$method().unwrap());
            };
        }

        check_as_null!(as_binary);
        check_as_null!(as_string);
        check_as_null!(as_boolean);
        check_as_null!(as_date);
        check_as_null!(as_datetime);
        check_as_null!(as_list);

        macro_rules! check_as_correct {
            ($data: expr, $Variant: ident, $method: ident) => {
                assert_eq!(Some($data), ValueRef::$Variant($data).$method().unwrap());
            };
        }

        check_as_correct!("hello", String, as_string);
        check_as_correct!("hello".as_bytes(), Binary, as_binary);
        check_as_correct!(true, Boolean, as_boolean);
        check_as_correct!(Date::new(123), Date, as_date);
        check_as_correct!(DateTime::new(12), DateTime, as_datetime);
        let list = ListValue {
            items: None,
            datatype: ConcreteDataType::int32_datatype(),
        };
        check_as_correct!(ListValueRef::Ref(&list), List, as_list);

        let wrong_value = ValueRef::Int32(12345);
        assert!(wrong_value.as_binary().is_err());
        assert!(wrong_value.as_string().is_err());
        assert!(wrong_value.as_boolean().is_err());
        assert!(wrong_value.as_date().is_err());
        assert!(wrong_value.as_datetime().is_err());
        assert!(wrong_value.as_list().is_err());
    }

    #[test]
    fn test_into_value_ref() {
        macro_rules! check_into_value_ref {
            ($Variant: ident, $data: expr, $PrimitiveType: ident, $Wrapper: ident) => {
                let data: $PrimitiveType = $data;
                assert_eq!(
                    ValueRef::$Variant($Wrapper::from(data)),
                    data.into_value_ref()
                );
                assert_eq!(
                    ValueRef::$Variant($Wrapper::from(data)),
                    ValueRef::from(data)
                );
                assert_eq!(
                    ValueRef::$Variant($Wrapper::from(data)),
                    Some(data).into_value_ref()
                );
                assert_eq!(
                    ValueRef::$Variant($Wrapper::from(data)),
                    ValueRef::from(Some(data))
                );
                let x: Option<$PrimitiveType> = None;
                assert_eq!(ValueRef::Null, x.into_value_ref());
                assert_eq!(ValueRef::Null, x.into());
            };
        }

        macro_rules! check_primitive_into_value_ref {
            ($Variant: ident, $data: expr, $PrimitiveType: ident) => {
                check_into_value_ref!($Variant, $data, $PrimitiveType, $PrimitiveType)
            };
        }

        check_primitive_into_value_ref!(Boolean, true, bool);
        check_primitive_into_value_ref!(UInt8, 10, u8);
        check_primitive_into_value_ref!(UInt16, 20, u16);
        check_primitive_into_value_ref!(UInt32, 30, u32);
        check_primitive_into_value_ref!(UInt64, 40, u64);
        check_primitive_into_value_ref!(Int8, -10, i8);
        check_primitive_into_value_ref!(Int16, -20, i16);
        check_primitive_into_value_ref!(Int32, -30, i32);
        check_primitive_into_value_ref!(Int64, -40, i64);
        check_into_value_ref!(Float32, 10.0, f32, OrderedF32);
        check_into_value_ref!(Float64, 10.0, f64, OrderedF64);

        let hello = "hello";
        assert_eq!(
            ValueRef::Binary(hello.as_bytes()),
            ValueRef::from(hello.as_bytes())
        );
        assert_eq!(ValueRef::String(hello), ValueRef::from(hello));
    }
}
