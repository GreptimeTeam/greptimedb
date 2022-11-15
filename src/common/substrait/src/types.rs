//! Methods that perform convertion between Substrait's type ([Type](SType)) and GreptimeDB's type ([ConcreteDataType]).
//!
//! Substrait use [type variation](https://substrait.io/types/type_variations/) to express different "logical types".
//! Current we only have variations on integer types. Variation 0 (system prefered) are the same with base types, which
//! are signed integer (i.e. I8 -> [i8]), and Variation 1 stands for unsigned integer (i.e. I8 -> [u8]).

use datatypes::prelude::ConcreteDataType;
use substrait_proto::protobuf::r#type::{self as s_type, Kind, Nullability};
use substrait_proto::protobuf::Type as SType;

use crate::error::{Result, UnsupportedConcreteTypeSnafu, UnsupportedSubstraitTypeSnafu};

macro_rules! substrait_kind {
    ($desc:ident, $concrete_ty:ident) => {{
        let nullable = $desc.nullability() == Nullability::Nullable;
        let ty = ConcreteDataType::$concrete_ty();
        Ok((ty, nullable))
    }};

    ($desc:ident, $concrete_ty:expr) => {{
        let nullable = $desc.nullability() == Nullability::Nullable;
        Ok(($concrete_ty, nullable))
    }};

    ($desc:ident, $concrete_ty_0:ident, $concrete_ty_1:ident) => {{
        let nullable = $desc.nullability() == Nullability::Nullable;
        let ty = match $desc.type_variation_reference {
            0 => ConcreteDataType::$concrete_ty_0(),
            1 => ConcreteDataType::$concrete_ty_1(),
            _ => UnsupportedSubstraitTypeSnafu {
                ty: format!("{:?}", $desc),
            }
            .fail()?,
        };
        Ok((ty, nullable))
    }};
}

/// Convert Substrait [Type](SType) to GreptimeDB's [ConcreteDataType]. The bool in return
/// tuple is the nullability identifier.
pub fn to_concrete_type(ty: &SType) -> Result<(ConcreteDataType, bool)> {
    if ty.kind.is_none() {
        return Ok((ConcreteDataType::null_datatype(), true));
    }
    let kind = ty.kind.as_ref().unwrap();
    match kind {
        Kind::Bool(desc) => substrait_kind!(desc, boolean_datatype),
        Kind::I8(desc) => substrait_kind!(desc, int8_datatype, uint8_datatype),
        Kind::I16(desc) => substrait_kind!(desc, int16_datatype, uint16_datatype),
        Kind::I32(desc) => substrait_kind!(desc, int32_datatype, uint32_datatype),
        Kind::I64(desc) => substrait_kind!(desc, int64_datatype, uint64_datatype),
        Kind::Fp32(desc) => substrait_kind!(desc, float32_datatype),
        Kind::Fp64(desc) => substrait_kind!(desc, float64_datatype),
        Kind::String(desc) => substrait_kind!(desc, string_datatype),
        Kind::Binary(desc) => substrait_kind!(desc, binary_datatype),
        Kind::Timestamp(desc) => substrait_kind!(
            desc,
            ConcreteDataType::timestamp_datatype(Default::default())
        ),
        Kind::Date(desc) => substrait_kind!(desc, date_datatype),
        Kind::Time(_)
        | Kind::IntervalYear(_)
        | Kind::IntervalDay(_)
        | Kind::TimestampTz(_)
        | Kind::Uuid(_)
        | Kind::FixedChar(_)
        | Kind::Varchar(_)
        | Kind::FixedBinary(_)
        | Kind::Decimal(_)
        | Kind::Struct(_)
        | Kind::List(_)
        | Kind::Map(_)
        | Kind::UserDefinedTypeReference(_) => UnsupportedSubstraitTypeSnafu {
            ty: format!("{:?}", kind),
        }
        .fail(),
    }
}

macro_rules! build_substrait_kind {
    ($kind:ident,$s_type:ident,$nullable:ident,$variation:literal) => {{
        let nullability = match $nullable {
            Some(true) => Nullability::Nullable,
            Some(false) => Nullability::Required,
            None => Nullability::Unspecified,
        } as _;
        Some(Kind::$kind(s_type::$s_type {
            type_variation_reference: $variation,
            nullability,
        }))
    }};
}

/// Convert GreptimeDB's [ConcreteDataType] to Substrait [Type](SType).
///
/// Refer to [mod level documentation](super::types) for more information about type variation.
pub fn from_concrete_type(ty: ConcreteDataType, nullability: Option<bool>) -> Result<SType> {
    let kind = match ty {
        ConcreteDataType::Null(_) => None,
        ConcreteDataType::Boolean(_) => build_substrait_kind!(Bool, Boolean, nullability, 0),
        ConcreteDataType::Int8(_) => build_substrait_kind!(I8, I8, nullability, 0),
        ConcreteDataType::Int16(_) => build_substrait_kind!(I16, I16, nullability, 0),
        ConcreteDataType::Int32(_) => build_substrait_kind!(I32, I32, nullability, 0),
        ConcreteDataType::Int64(_) => build_substrait_kind!(I64, I64, nullability, 0),
        ConcreteDataType::UInt8(_) => build_substrait_kind!(I8, I8, nullability, 1),
        ConcreteDataType::UInt16(_) => build_substrait_kind!(I16, I16, nullability, 1),
        ConcreteDataType::UInt32(_) => build_substrait_kind!(I32, I32, nullability, 1),
        ConcreteDataType::UInt64(_) => build_substrait_kind!(I64, I64, nullability, 1),
        ConcreteDataType::Float32(_) => build_substrait_kind!(Fp32, Fp32, nullability, 0),
        ConcreteDataType::Float64(_) => build_substrait_kind!(Fp64, Fp64, nullability, 0),
        ConcreteDataType::Binary(_) => build_substrait_kind!(Binary, Binary, nullability, 0),
        ConcreteDataType::String(_) => build_substrait_kind!(String, String, nullability, 0),
        ConcreteDataType::Date(_) => build_substrait_kind!(Date, Date, nullability, 0),
        ConcreteDataType::DateTime(_) => UnsupportedConcreteTypeSnafu { ty }.fail()?,
        ConcreteDataType::Timestamp(_) => {
            build_substrait_kind!(Timestamp, Timestamp, nullability, 0)
        }
        ConcreteDataType::List(_) => UnsupportedConcreteTypeSnafu { ty }.fail()?,
    };

    Ok(SType { kind })
}
