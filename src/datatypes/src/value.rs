use common_base::bytes::{Bytes, StringBytes};
use serde::Serialize;

/// Value holds a single arbitrary value of any [DataType](crate::data_type::DataType).
#[derive(Debug, PartialEq, Serialize, Clone)]
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
    Float32(f32),
    Float64(f64),

    // String types:
    String(StringBytes),
    Binary(Bytes),

    // Date & Time types:
    Date(i32),
    DateTime(i64),
}

macro_rules! impl_from {
    ($Variant:ident, $Type:ident) => {
        impl From<$Type> for Value {
            fn from(value: $Type) -> Self {
                Value::$Variant(value)
            }
        }

        impl From<Option<$Type>> for Value {
            fn from(value: Option<$Type>) -> Self {
                match value {
                    Some(v) => Value::$Variant(v),
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

impl From<&[u8]> for Value {
    fn from(s: &[u8]) -> Self {
        Value::Binary(Bytes(s.to_vec()))
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(StringBytes(s.to_string().into_bytes()))
    }
}
