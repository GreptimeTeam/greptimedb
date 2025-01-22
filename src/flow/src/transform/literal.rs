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
use common_time::timestamp::TimeUnit;
use common_time::{Date, IntervalMonthDayNano, Timestamp};
use datafusion_common::ScalarValue;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::value::Value;
use num_traits::FromBytes;
use snafu::OptionExt;
use substrait::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    UNSIGNED_INTEGER_TYPE_VARIATION_REF,
};
use substrait_proto::proto;
use substrait_proto::proto::expression::literal::{LiteralType, PrecisionTimestamp};
use substrait_proto::proto::expression::Literal;
use substrait_proto::proto::r#type::Kind;

use crate::error::{Error, NotImplementedSnafu, PlanSnafu, UnexpectedSnafu};
use crate::transform::substrait_proto;

#[derive(Debug)]
enum TimestampPrecision {
    Second = 0,
    Millisecond = 3,
    Microsecond = 6,
    Nanosecond = 9,
}

impl TryFrom<i32> for TimestampPrecision {
    type Error = Error;

    fn try_from(prec: i32) -> Result<Self, Self::Error> {
        match prec {
            0 => Ok(Self::Second),
            3 => Ok(Self::Millisecond),
            6 => Ok(Self::Microsecond),
            9 => Ok(Self::Nanosecond),
            _ => not_impl_err!("Unsupported precision: {prec}"),
        }
    }
}

impl TimestampPrecision {
    fn to_time_unit(&self) -> TimeUnit {
        match self {
            Self::Second => TimeUnit::Second,
            Self::Millisecond => TimeUnit::Millisecond,
            Self::Microsecond => TimeUnit::Microsecond,
            Self::Nanosecond => TimeUnit::Nanosecond,
        }
    }

    fn to_cdt(&self) -> CDT {
        match self {
            Self::Second => CDT::timestamp_second_datatype(),
            Self::Millisecond => CDT::timestamp_millisecond_datatype(),
            Self::Microsecond => CDT::timestamp_microsecond_datatype(),
            Self::Nanosecond => CDT::timestamp_nanosecond_datatype(),
        }
    }
}

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
        // TODO(discord9): deal with timezone
        ScalarValue::TimestampSecond(Some(t), _) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                value: *t,
                precision: TimestampPrecision::Second as i32,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), _) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                value: *t,
                precision: TimestampPrecision::Millisecond as i32,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), _) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                value: *t,
                precision: TimestampPrecision::Microsecond as i32,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), _) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                value: *t,
                precision: TimestampPrecision::Nanosecond as i32,
            }),
            DEFAULT_TYPE_VARIATION_REF,
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
        Some(LiteralType::Timestamp(t)) => (
            Value::from(Timestamp::new_microsecond(*t)),
            CDT::timestamp_microsecond_datatype(),
        ),
        Some(LiteralType::PrecisionTimestamp(prec_ts)) => {
            let (prec, val) = (prec_ts.precision, prec_ts.value);
            let prec = TimestampPrecision::try_from(prec)?;
            let unit = prec.to_time_unit();
            let typ = prec.to_cdt();
            (Value::from(Timestamp::new(val, unit)), typ)
        }
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
        Some(LiteralType::IntervalDayToSecond(interval)) => from_interval_day_sec(interval)?,
        Some(LiteralType::IntervalYearToMonth(interval)) => from_interval_year_month(interval)?,
        Some(LiteralType::IntervalCompound(interval_compound)) => {
            let interval_day_time = &interval_compound
                .interval_day_to_second
                .map(|i| from_interval_day_sec(&i))
                .transpose()?;
            let interval_year_month = &interval_compound
                .interval_year_to_month
                .map(|i| from_interval_year_month(&i))
                .transpose()?;
            let mut compound = IntervalMonthDayNano::new(0, 0, 0);
            if let Some(day_sec) = interval_day_time {
                let Value::IntervalDayTime(day_time) = day_sec.0 else {
                    UnexpectedSnafu {
                        reason: format!("Expect IntervalDayTime, found {:?}", day_sec),
                    }
                    .fail()?
                };
                compound.nanoseconds += day_time.milliseconds as i64 * 1_000_000;
                compound.days += day_time.days;
            }

            if let Some(year_month) = interval_year_month {
                let Value::IntervalYearMonth(year_month) = year_month.0 else {
                    UnexpectedSnafu {
                        reason: format!("Expect IntervalYearMonth, found {:?}", year_month),
                    }
                    .fail()?
                };
                compound.months += year_month.months;
            }

            (
                Value::IntervalMonthDayNano(compound),
                CDT::interval_month_day_nano_datatype(),
            )
        }
        _ => not_impl_err!("unsupported literal_type: {:?}", &lit.literal_type)?,
    };
    Ok(scalar_value)
}

fn from_interval_day_sec(
    interval: &proto::expression::literal::IntervalDayToSecond,
) -> Result<(Value, CDT), Error> {
    let (days, seconds, subseconds) = (interval.days, interval.seconds, interval.subseconds);
    let millis = if let Some(prec) = interval.precision_mode {
        use substrait_proto::proto::expression::literal::interval_day_to_second::PrecisionMode;
        match prec {
            PrecisionMode::Precision(e) => {
                if e >= 3 {
                    subseconds
                        / 10_i64
                            .checked_pow((e - 3) as _)
                            .with_context(|| UnexpectedSnafu {
                                reason: format!(
                                    "Overflow when converting interval: {:?}",
                                    interval
                                ),
                            })?
                } else {
                    subseconds
                        * 10_i64
                            .checked_pow((3 - e) as _)
                            .with_context(|| UnexpectedSnafu {
                                reason: format!(
                                    "Overflow when converting interval: {:?}",
                                    interval
                                ),
                            })?
                }
            }
            PrecisionMode::Microseconds(_) => subseconds / 1000,
        }
    } else if subseconds == 0 {
        0
    } else {
        not_impl_err!("unsupported subseconds without precision_mode: {subseconds}")?
    };

    let value_interval = common_time::IntervalDayTime::new(days, seconds * 1000 + millis as i32);

    Ok((
        Value::IntervalDayTime(value_interval),
        CDT::interval_day_time_datatype(),
    ))
}

fn from_interval_year_month(
    interval: &proto::expression::literal::IntervalYearToMonth,
) -> Result<(Value, CDT), Error> {
    let value_interval = common_time::IntervalYearMonth::new(interval.years * 12 + interval.months);

    Ok((
        Value::IntervalYearMonth(value_interval),
        CDT::interval_year_month_datatype(),
    ))
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
            Kind::PrecisionTimestamp(ts) => {
                Ok(TimestampPrecision::try_from(ts.precision)?.to_cdt())
            }
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
