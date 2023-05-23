// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Methods that perform conversion between Substrait's type ([Type](SType)) and GreptimeDB's type ([ConcreteDataType]).
//!
//! Substrait use [type variation](https://substrait.io/types/type_variations/) to express different "logical types".
//! Current we only have variations on integer types. Variation 0 (system preferred) are the same with base types, which
//! are signed integer (i.e. I8 -> [i8]), and Variation 1 stands for unsigned integer (i.e. I8 -> [u8]).

use datafusion::scalar::ScalarValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::TimestampType;
use substrait_proto::proto::expression::literal::LiteralType;
use substrait_proto::proto::r#type::{self as s_type, Kind, Nullability};
use substrait_proto::proto::{Type as SType, Type};

use crate::error::{self, Result, UnsupportedConcreteTypeSnafu, UnsupportedSubstraitTypeSnafu};

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
            ConcreteDataType::timestamp_datatype(
                TimestampType::try_from(desc.type_variation_reference as u64)
                    .map_err(|_| UnsupportedSubstraitTypeSnafu {
                        ty: format!("{kind:?}")
                    }
                    .build())?
                    .unit()
            )
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
        | Kind::UserDefined(_)
        | Kind::UserDefinedTypeReference(_) => UnsupportedSubstraitTypeSnafu {
            ty: format!("{kind:?}"),
        }
        .fail(),
    }
}

macro_rules! build_substrait_kind {
    ($kind:ident,$s_type:ident,$nullable:ident,$variation:expr) => {{
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
        ConcreteDataType::Timestamp(ty) => {
            build_substrait_kind!(Timestamp, Timestamp, nullability, ty.precision() as u32)
        }
        ConcreteDataType::List(_) | ConcreteDataType::Dictionary(_) => {
            UnsupportedConcreteTypeSnafu { ty }.fail()?
        }
    };

    Ok(SType { kind })
}

pub(crate) fn scalar_value_as_literal_type(v: &ScalarValue) -> Result<LiteralType> {
    Ok(if v.is_null() {
        LiteralType::Null(Type { kind: None })
    } else {
        match v {
            ScalarValue::Boolean(Some(v)) => LiteralType::Boolean(*v),
            ScalarValue::Float32(Some(v)) => LiteralType::Fp32(*v),
            ScalarValue::Float64(Some(v)) => LiteralType::Fp64(*v),
            ScalarValue::Int8(Some(v)) => LiteralType::I8(*v as i32),
            ScalarValue::Int16(Some(v)) => LiteralType::I16(*v as i32),
            ScalarValue::Int32(Some(v)) => LiteralType::I32(*v),
            ScalarValue::Int64(Some(v)) => LiteralType::I64(*v),
            ScalarValue::LargeUtf8(Some(v)) => LiteralType::String(v.clone()),
            ScalarValue::LargeBinary(Some(v)) => LiteralType::Binary(v.clone()),
            ScalarValue::TimestampSecond(Some(seconds), _) => {
                LiteralType::Timestamp(*seconds * 1_000_000)
            }
            ScalarValue::TimestampMillisecond(Some(millis), _) => {
                LiteralType::Timestamp(*millis * 1000)
            }
            ScalarValue::TimestampMicrosecond(Some(micros), _) => LiteralType::Timestamp(*micros),
            ScalarValue::TimestampNanosecond(Some(nanos), _) => {
                LiteralType::Timestamp(*nanos / 1000)
            }
            ScalarValue::Utf8(Some(s)) => LiteralType::String(s.clone()),
            // TODO(LFC): Implement other conversions: ScalarValue => LiteralType
            _ => {
                return error::UnsupportedExprSnafu {
                    name: format!("ScalarValue: {v:?}"),
                }
                .fail()
            }
        }
    })
}

pub(crate) fn literal_type_to_scalar_value(t: LiteralType) -> Result<ScalarValue> {
    Ok(match t {
        LiteralType::Null(Type { kind: Some(kind) }) => match kind {
            Kind::Bool(_) => ScalarValue::Boolean(None),
            Kind::I8(_) => ScalarValue::Int8(None),
            Kind::I16(_) => ScalarValue::Int16(None),
            Kind::I32(_) => ScalarValue::Int32(None),
            Kind::I64(_) => ScalarValue::Int64(None),
            Kind::Fp32(_) => ScalarValue::Float32(None),
            Kind::Fp64(_) => ScalarValue::Float64(None),
            Kind::String(_) => ScalarValue::LargeUtf8(None),
            Kind::Binary(_) => ScalarValue::LargeBinary(None),
            // TODO(LFC): Implement other conversions: Kind => ScalarValue
            _ => {
                return error::UnsupportedSubstraitTypeSnafu {
                    ty: format!("{kind:?}"),
                }
                .fail()
            }
        },
        LiteralType::Boolean(v) => ScalarValue::Boolean(Some(v)),
        LiteralType::I8(v) => ScalarValue::Int8(Some(v as i8)),
        LiteralType::I16(v) => ScalarValue::Int16(Some(v as i16)),
        LiteralType::I32(v) => ScalarValue::Int32(Some(v)),
        LiteralType::I64(v) => ScalarValue::Int64(Some(v)),
        LiteralType::Fp32(v) => ScalarValue::Float32(Some(v)),
        LiteralType::Fp64(v) => ScalarValue::Float64(Some(v)),
        LiteralType::String(v) => ScalarValue::LargeUtf8(Some(v)),
        LiteralType::Binary(v) => ScalarValue::LargeBinary(Some(v)),
        LiteralType::Timestamp(v) => ScalarValue::TimestampMicrosecond(Some(v), None),
        // TODO(LFC): Implement other conversions: LiteralType => ScalarValue
        _ => {
            return error::UnsupportedSubstraitTypeSnafu {
                ty: format!("{t:?}"),
            }
            .fail()
        }
    })
}
