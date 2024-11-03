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

use std::array::TryFromSliceError;

use bytes::Bytes;
use common_decimal::Decimal128;
use common_time::{Date, Timestamp};
use datafusion_common::ScalarValue;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::value::Value;
use num_traits::FromBytes;
use snafu::ensure;
use substrait::substrait_proto_df::proto::expression::literal::user_defined::Val;
use substrait::substrait_proto_df::proto::expression::literal::UserDefined;
use substrait::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    INTERVAL_DAY_TIME_TYPE_REF, INTERVAL_DAY_TIME_TYPE_URL, INTERVAL_MONTH_DAY_NANO_TYPE_REF,
    INTERVAL_MONTH_DAY_NANO_TYPE_URL, INTERVAL_YEAR_MONTH_TYPE_REF, INTERVAL_YEAR_MONTH_TYPE_URL,
    TIMESTAMP_MICRO_TYPE_VARIATION_REF, TIMESTAMP_MILLI_TYPE_VARIATION_REF,
    TIMESTAMP_NANO_TYPE_VARIATION_REF, TIMESTAMP_SECOND_TYPE_VARIATION_REF,
    UNSIGNED_INTEGER_TYPE_VARIATION_REF,
};
use substrait_proto::proto::expression::literal::LiteralType;
use substrait_proto::proto::expression::Literal;
use substrait_proto::proto::r#type::Kind;

use crate::error::{Error, NotImplementedSnafu, PlanSnafu, UnexpectedSnafu};
use crate::transform::substrait_proto;

/// TODO(discord9): this is copy from datafusion-substrait since the original function is not public, will be replace once is exported
pub(crate) fn to_substrait_literal(value: &ScalarValue) -> Result<Literal, Error> {
    if value.is_null() {
        return not_impl_err!("Unsupported literal: {value:?}");
    }
    let (literal_type, type_variation_reference) = match value {
        ScalarValue::Boolean(Some(b)) => (LiteralType::Boolean(*b), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::Int8(Some(n)) => (LiteralType::I8(*n as i32), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt8(Some(n)) => (
            LiteralType::I8(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int16(Some(n)) => (LiteralType::I16(*n as i32), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt16(Some(n)) => (
            LiteralType::I16(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int32(Some(n)) => (LiteralType::I32(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt32(Some(n)) => (
            LiteralType::I32(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int64(Some(n)) => (LiteralType::I64(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt64(Some(n)) => (
            LiteralType::I64(*n as i64),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Float32(Some(f)) => (LiteralType::Fp32(*f), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::Float64(Some(f)) => (LiteralType::Fp64(*f), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::TimestampSecond(Some(t), _) => (
            LiteralType::Timestamp(*t),
            TIMESTAMP_SECOND_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), _) => (
            LiteralType::Timestamp(*t),
            TIMESTAMP_MILLI_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), _) => (
            LiteralType::Timestamp(*t),
            TIMESTAMP_MICRO_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), _) => (
            LiteralType::Timestamp(*t),
            TIMESTAMP_NANO_TYPE_VARIATION_REF,
        ),
        ScalarValue::Date32(Some(d)) => (LiteralType::Date(*d), DATE_32_TYPE_VARIATION_REF),
        _ => (
            not_impl_err!("Unsupported literal: {value:?}")?,
            DEFAULT_TYPE_VARIATION_REF,
        ),
    };

    Ok(Literal {
        nullable: false,
        type_variation_reference,
        literal_type: Some(literal_type),
    })
}

/// Convert a Substrait literal into a Value and its ConcreteDataType (So that we can know type even if the value is null)
pub(crate) fn from_substrait_literal(lit: &Literal) -> Result<(Value, CDT), Error> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => (Value::from(*b), CDT::boolean_datatype()),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => (Value::from(*n as i8), CDT::int8_datatype()),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => (Value::from(*n as u8), CDT::uint8_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => (Value::from(*n as i16), CDT::int16_datatype()),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => (Value::from(*n as u16), CDT::uint16_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => (Value::from(*n), CDT::int32_datatype()),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => (Value::from(*n as u32), CDT::uint32_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => (Value::from(*n), CDT::int64_datatype()),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => (Value::from(*n as u64), CDT::uint64_datatype()),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::Fp32(f)) => (Value::from(*f), CDT::float32_datatype()),
        Some(LiteralType::Fp64(f)) => (Value::from(*f), CDT::float64_datatype()),
        Some(LiteralType::Timestamp(t)) => match lit.type_variation_reference {
            TIMESTAMP_SECOND_TYPE_VARIATION_REF => (
                Value::from(Timestamp::new_second(*t)),
                CDT::timestamp_second_datatype(),
            ),
            TIMESTAMP_MILLI_TYPE_VARIATION_REF => (
                Value::from(Timestamp::new_millisecond(*t)),
                CDT::timestamp_millisecond_datatype(),
            ),
            TIMESTAMP_MICRO_TYPE_VARIATION_REF => (
                Value::from(Timestamp::new_microsecond(*t)),
                CDT::timestamp_microsecond_datatype(),
            ),
            TIMESTAMP_NANO_TYPE_VARIATION_REF => (
                Value::from(Timestamp::new_nanosecond(*t)),
                CDT::timestamp_nanosecond_datatype(),
            ),
            others => not_impl_err!("Unknown type variation reference {others}",)?,
        },
        Some(LiteralType::Date(d)) => (Value::from(Date::new(*d)), CDT::date_datatype()),
        Some(LiteralType::String(s)) => (Value::from(s.clone()), CDT::string_datatype()),
        Some(LiteralType::Binary(b)) | Some(LiteralType::FixedBinary(b)) => {
            (Value::from(b.clone()), CDT::binary_datatype())
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d.value.clone().try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal value from {e:?}"),
                }
                .build()
            })?;
            let p: u8 = d.precision.try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal precision: {e}"),
                }
                .build()
            })?;
            let s: i8 = d.scale.try_into().map_err(|e| {
                PlanSnafu {
                    reason: format!("Failed to parse decimal scale: {e}"),
                }
                .build()
            })?;
            let value = i128::from_le_bytes(value);
            (
                Value::from(Decimal128::new(value, p, s)),
                CDT::decimal128_datatype(p, s),
            )
        }
        Some(LiteralType::Null(ntype)) => (Value::Null, from_substrait_type(ntype)?),
        Some(LiteralType::IntervalDayToSecond(interval)) => {
            let (days, seconds, microseconds) =
                (interval.days, interval.seconds, interval.microseconds);
            let millis = microseconds / 1000 + seconds * 1000;
            let value_interval = common_time::IntervalDayTime::new(days, millis);
            (
                Value::IntervalDayTime(value_interval),
                CDT::interval_day_time_datatype(),
            )
        }
        Some(LiteralType::IntervalYearToMonth(interval)) => (
            Value::IntervalYearMonth(common_time::IntervalYearMonth::new(
                interval.years * 12 + interval.months,
            )),
            CDT::interval_year_month_datatype(),
        ),
        Some(LiteralType::UserDefined(user_defined)) => {
            from_substrait_user_defined_type(user_defined)?
        }
        _ => not_impl_err!("unsupported literal_type: {:?}", &lit.literal_type)?,
    };
    Ok(scalar_value)
}

fn from_bytes<T: FromBytes>(i: &Bytes) -> Result<T, Error>
where
    for<'a> &'a <T as num_traits::FromBytes>::Bytes:
        std::convert::TryFrom<&'a [u8], Error = TryFromSliceError>,
{
    let (int_bytes, _rest) = i.split_at(std::mem::size_of::<T>());
    let i = T::from_le_bytes(int_bytes.try_into().map_err(|e| {
        UnexpectedSnafu {
            reason: format!(
                "Expect slice to be {} bytes, found {} bytes, error={:?}",
                std::mem::size_of::<T>(),
                int_bytes.len(),
                e
            ),
        }
        .build()
    })?);
    Ok(i)
}

fn from_substrait_user_defined_type(user_defined: &UserDefined) -> Result<(Value, CDT), Error> {
    if let UserDefined {
        type_reference,
        type_parameters: _,
        val: Some(Val::Value(val)),
    } = user_defined
    {
        // see https://github.com/apache/datafusion/blob/146b679aa19c7749cc73d0c27440419d6498142b/datafusion/substrait/src/logical_plan/producer.rs#L1957
        // for interval type's transform to substrait
        let ret = match *type_reference {
            INTERVAL_YEAR_MONTH_TYPE_REF => {
                ensure!(
                    val.type_url == INTERVAL_YEAR_MONTH_TYPE_URL,
                    UnexpectedSnafu {
                        reason: format!(
                            "Expect {}, found {} in type_url",
                            INTERVAL_YEAR_MONTH_TYPE_URL, val.type_url
                        )
                    }
                );
                let i: i32 = from_bytes(&val.value)?;
                let value_interval = common_time::IntervalYearMonth::new(i);
                (
                    Value::IntervalYearMonth(value_interval),
                    CDT::interval_year_month_datatype(),
                )
            }
            INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                ensure!(
                    val.type_url == INTERVAL_MONTH_DAY_NANO_TYPE_URL,
                    UnexpectedSnafu {
                        reason: format!(
                            "Expect {}, found {} in type_url",
                            INTERVAL_MONTH_DAY_NANO_TYPE_URL, val.type_url
                        )
                    }
                );
                // TODO(yingwen): Datafusion may change the representation of the interval type.
                let i: i128 = from_bytes(&val.value)?;
                let (months, days, nsecs) = ((i >> 96) as i32, (i >> 64) as i32, i as i64);
                let value_interval = common_time::IntervalMonthDayNano::new(months, days, nsecs);
                (
                    Value::IntervalMonthDayNano(value_interval),
                    CDT::interval_month_day_nano_datatype(),
                )
            }
            INTERVAL_DAY_TIME_TYPE_REF => {
                ensure!(
                    val.type_url == INTERVAL_DAY_TIME_TYPE_URL,
                    UnexpectedSnafu {
                        reason: format!(
                            "Expect {}, found {} in type_url",
                            INTERVAL_DAY_TIME_TYPE_URL, val.type_url
                        )
                    }
                );
                // TODO(yingwen): Datafusion may change the representation of the interval type.
                let i: i64 = from_bytes(&val.value)?;
                let (days, millis) = ((i >> 32) as i32, i as i32);
                let value_interval = common_time::IntervalDayTime::new(days, millis);
                (
                    Value::IntervalDayTime(value_interval),
                    CDT::interval_day_time_datatype(),
                )
            }
            _ => return not_impl_err!("unsupported user defined type: {:?}", user_defined)?,
        };
        Ok(ret)
    } else {
        not_impl_err!("Expect val to be Some(...)")
    }
}

/// convert a Substrait type into a ConcreteDataType
pub fn from_substrait_type(null_type: &substrait_proto::proto::Type) -> Result<CDT, Error> {
    if let Some(kind) = &null_type.kind {
        match kind {
            Kind::Bool(_) => Ok(CDT::boolean_datatype()),
            Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(CDT::int8_datatype()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(CDT::uint8_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(CDT::int16_datatype()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(CDT::uint16_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(CDT::int32_datatype()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(CDT::uint32_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(CDT::int64_datatype()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(CDT::uint64_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Fp32(_) => Ok(CDT::float32_datatype()),
            Kind::Fp64(_) => Ok(CDT::float64_datatype()),
            Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_VARIATION_REF => Ok(CDT::timestamp_second_datatype()),
                TIMESTAMP_MILLI_TYPE_VARIATION_REF => Ok(CDT::timestamp_millisecond_datatype()),
                TIMESTAMP_MICRO_TYPE_VARIATION_REF => Ok(CDT::timestamp_microsecond_datatype()),
                TIMESTAMP_NANO_TYPE_VARIATION_REF => Ok(CDT::timestamp_nanosecond_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF | DATE_64_TYPE_VARIATION_REF => Ok(CDT::date_datatype()),
                v => not_impl_err!("Unsupported Substrait type variation {v} of type {kind:?}"),
            },
            Kind::Binary(_) => Ok(CDT::binary_datatype()),
            Kind::String(_) => Ok(CDT::string_datatype()),
            Kind::Decimal(d) => Ok(CDT::decimal128_datatype(d.precision as u8, d.scale as i8)),
            _ => not_impl_err!("Unsupported Substrait type: {kind:?}"),
        }
    } else {
        not_impl_err!("Null type without kind is not supported")
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::plan::{Plan, TypedPlan};
    use crate::repr::{self, ColumnType, RelationType};
    use crate::transform::test::{create_test_ctx, create_test_query_engine, sql_to_substrait};
    /// test if literal in substrait plan can be correctly converted to flow plan
    #[tokio::test]
    async fn test_literal() {
        let engine = create_test_query_engine();
        let sql = "SELECT 1 FROM numbers";
        let plan = sql_to_substrait(engine.clone(), sql).await;

        let mut ctx = create_test_ctx();
        let flow_plan = TypedPlan::from_substrait_plan(&mut ctx, &plan).await;

        let expected = TypedPlan {
            schema: RelationType::new(vec![ColumnType::new(CDT::int64_datatype(), true)])
                .into_named(vec![Some("Int64(1)".to_string())]),
            plan: Plan::Constant {
                rows: vec![(
                    repr::Row::new(vec![Value::Int64(1)]),
                    repr::Timestamp::MIN,
                    1,
                )],
            },
        };

        assert_eq!(flow_plan.unwrap(), expected);
    }
}
