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

use api::helper::{convert_i128_to_interval, convert_to_pb_decimal128};
use api::v1::column::Values;
use common_base::BitVec;
use datatypes::types::{DurationType, IntervalType, TimeType, TimestampType, WrapperType};
use datatypes::vectors::{
    BinaryVector, BooleanVector, DateTimeVector, DateVector, Decimal128Vector,
    DurationMicrosecondVector, DurationMillisecondVector, DurationNanosecondVector,
    DurationSecondVector, Float32Vector, Float64Vector, Int16Vector, Int32Vector, Int64Vector,
    Int8Vector, IntervalDayTimeVector, IntervalMonthDayNanoVector, IntervalYearMonthVector,
    StringVector, TimeMicrosecondVector, TimeMillisecondVector, TimeNanosecondVector,
    TimeSecondVector, TimestampMicrosecondVector, TimestampMillisecondVector,
    TimestampNanosecondVector, TimestampSecondVector, UInt16Vector, UInt32Vector, UInt64Vector,
    UInt8Vector, VectorRef,
};
use snafu::OptionExt;

use crate::error::{ConversionSnafu, Result};

pub fn null_mask(arrays: &[VectorRef], row_count: usize) -> Vec<u8> {
    let null_count: usize = arrays.iter().map(|a| a.null_count()).sum();

    if null_count == 0 {
        return Vec::default();
    }

    let mut null_mask = BitVec::with_capacity(row_count);
    for array in arrays {
        let validity = array.validity();
        if validity.is_all_valid() {
            null_mask.extend_from_bitslice(&BitVec::repeat(false, array.len()));
        } else {
            for i in 0..array.len() {
                null_mask.push(!validity.is_set(i));
            }
        }
    }
    null_mask.into_vec()
}

macro_rules! convert_arrow_array_to_grpc_vals {
    ($data_type: expr, $arrays: ident,  $(($Type: pat, $CastType: ty, $field: ident, $MapFunction: expr)), +) => {{
        use datatypes::data_type::{ConcreteDataType};
        use datatypes::prelude::ScalarVector;

        match $data_type {
            $(
                $Type => {
                    let mut vals = Values::default();
                    for array in $arrays {
                        let array = array.as_any().downcast_ref::<$CastType>().with_context(|| ConversionSnafu {
                            from: format!("{:?}", $data_type),
                        })?;
                        vals.$field.extend(array
                            .iter_data()
                            .filter_map(|i| i.map($MapFunction))
                            .collect::<Vec<_>>());
                    }
                    return Ok(vals);
                },
            )+
            ConcreteDataType::Null(_) | ConcreteDataType::List(_) | ConcreteDataType::Dictionary(_) => unreachable!("Should not send {:?} in gRPC", $data_type),
        }
    }};
}

pub fn values(arrays: &[VectorRef]) -> Result<Values> {
    if arrays.is_empty() {
        return Ok(Values::default());
    }
    let data_type = arrays[0].data_type();

    convert_arrow_array_to_grpc_vals!(
        data_type,
        arrays,
        (
            ConcreteDataType::Boolean(_),
            BooleanVector,
            bool_values,
            |x| { x }
        ),
        (ConcreteDataType::Int8(_), Int8Vector, i8_values, |x| {
            i32::from(x)
        }),
        (ConcreteDataType::Int16(_), Int16Vector, i16_values, |x| {
            i32::from(x)
        }),
        (ConcreteDataType::Int32(_), Int32Vector, i32_values, |x| {
            x
        }),
        (ConcreteDataType::Int64(_), Int64Vector, i64_values, |x| {
            x
        }),
        (ConcreteDataType::UInt8(_), UInt8Vector, u8_values, |x| {
            u32::from(x)
        }),
        (ConcreteDataType::UInt16(_), UInt16Vector, u16_values, |x| {
            u32::from(x)
        }),
        (ConcreteDataType::UInt32(_), UInt32Vector, u32_values, |x| {
            x
        }),
        (ConcreteDataType::UInt64(_), UInt64Vector, u64_values, |x| {
            x
        }),
        (
            ConcreteDataType::Float32(_),
            Float32Vector,
            f32_values,
            |x| { x }
        ),
        (
            ConcreteDataType::Float64(_),
            Float64Vector,
            f64_values,
            |x| { x }
        ),
        (
            ConcreteDataType::Binary(_),
            BinaryVector,
            binary_values,
            |x| { x.into() }
        ),
        (
            ConcreteDataType::String(_),
            StringVector,
            string_values,
            |x| { x.into() }
        ),
        (ConcreteDataType::Date(_), DateVector, date_values, |x| {
            x.val()
        }),
        (
            ConcreteDataType::DateTime(_),
            DateTimeVector,
            datetime_values,
            |x| { x.val() }
        ),
        (
            ConcreteDataType::Timestamp(TimestampType::Second(_)),
            TimestampSecondVector,
            timestamp_second_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Timestamp(TimestampType::Millisecond(_)),
            TimestampMillisecondVector,
            timestamp_millisecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Timestamp(TimestampType::Microsecond(_)),
            TimestampMicrosecondVector,
            timestamp_microsecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)),
            TimestampNanosecondVector,
            timestamp_nanosecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Time(TimeType::Second(_)),
            TimeSecondVector,
            time_second_values,
            |x| { x.into_native() as i64 }
        ),
        (
            ConcreteDataType::Time(TimeType::Millisecond(_)),
            TimeMillisecondVector,
            time_millisecond_values,
            |x| { x.into_native() as i64 }
        ),
        (
            ConcreteDataType::Time(TimeType::Microsecond(_)),
            TimeMicrosecondVector,
            time_microsecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Time(TimeType::Nanosecond(_)),
            TimeNanosecondVector,
            time_nanosecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Interval(IntervalType::YearMonth(_)),
            IntervalYearMonthVector,
            interval_year_month_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Interval(IntervalType::DayTime(_)),
            IntervalDayTimeVector,
            interval_day_time_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Interval(IntervalType::MonthDayNano(_)),
            IntervalMonthDayNanoVector,
            interval_month_day_nano_values,
            |x| { convert_i128_to_interval(x.into_native()) }
        ),
        (
            ConcreteDataType::Duration(DurationType::Second(_)),
            DurationSecondVector,
            duration_second_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Duration(DurationType::Millisecond(_)),
            DurationMillisecondVector,
            duration_millisecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Duration(DurationType::Microsecond(_)),
            DurationMicrosecondVector,
            duration_microsecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Duration(DurationType::Nanosecond(_)),
            DurationNanosecondVector,
            duration_nanosecond_values,
            |x| { x.into_native() }
        ),
        (
            ConcreteDataType::Decimal128(_),
            Decimal128Vector,
            decimal128_values,
            |x| { convert_to_pb_decimal128(x) }
        )
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_convert_arrow_arrays_i32() {
        let array = Int32Vector::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![1, 2, 3], values.i32_values);
    }

    #[test]
    fn test_convert_arrow_array_time_second() {
        let array = TimeSecondVector::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![1, 2, 3], values.time_second_values);
    }

    #[test]
    fn test_convert_arrow_array_interval_year_month() {
        let array = IntervalYearMonthVector::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![1, 2, 3], values.interval_year_month_values);
    }

    #[test]
    fn test_convert_arrow_array_interval_day_time() {
        let array = IntervalDayTimeVector::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![1, 2, 3], values.interval_day_time_values);
    }

    #[test]
    fn test_convert_arrow_array_interval_month_day_nano() {
        let array = IntervalMonthDayNanoVector::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        (0..3).for_each(|i| {
            assert_eq!(values.interval_month_day_nano_values[i].months, 0);
            assert_eq!(values.interval_month_day_nano_values[i].days, 0);
            assert_eq!(
                values.interval_month_day_nano_values[i].nanoseconds,
                i as i64 + 1
            );
        })
    }

    #[test]
    fn test_convert_arrow_array_duration_second() {
        let array = DurationSecondVector::from(vec![Some(1), Some(2), None, Some(3)]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![1, 2, 3], values.duration_second_values);
    }

    #[test]
    fn test_convert_arrow_array_decimal128() {
        let array = Decimal128Vector::from(vec![Some(1), Some(2), None, Some(3)]);

        let vals = values(&[Arc::new(array)]).unwrap();
        (0..3).for_each(|i| {
            assert_eq!(vals.decimal128_values[i].hi, 0);
            assert_eq!(vals.decimal128_values[i].lo, i as i64 + 1);
        });
    }

    #[test]
    fn test_convert_arrow_arrays_string() {
        let array = StringVector::from(vec![
            Some("1".to_string()),
            Some("2".to_string()),
            None,
            Some("3".to_string()),
            None,
        ]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec!["1", "2", "3"], values.string_values);
    }

    #[test]
    fn test_convert_arrow_arrays_bool() {
        let array = BooleanVector::from(vec![Some(true), Some(false), None, Some(false), None]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert_eq!(vec![true, false, false], values.bool_values);
    }

    #[test]
    fn test_convert_arrow_arrays_empty() {
        let array = BooleanVector::from(vec![None, None, None, None, None]);
        let array: VectorRef = Arc::new(array);

        let values = values(&[array]).unwrap();

        assert!(values.bool_values.is_empty());
    }

    #[test]
    fn test_null_mask() {
        let a1: VectorRef = Arc::new(Int32Vector::from(vec![None, Some(2), None]));
        let a2: VectorRef = Arc::new(Int32Vector::from(vec![Some(1), Some(2), None, Some(4)]));
        let mask = null_mask(&[a1, a2], 3 + 4);
        assert_eq!(vec![0b0010_0101], mask);

        let empty: VectorRef = Arc::new(Int32Vector::from(vec![None, None, None]));
        let mask = null_mask(&[empty.clone(), empty.clone(), empty], 9);
        assert_eq!(vec![0b1111_1111, 0b0000_0001], mask);

        let a1: VectorRef = Arc::new(Int32Vector::from(vec![Some(1), Some(2), Some(3)]));
        let a2: VectorRef = Arc::new(Int32Vector::from(vec![Some(4), Some(5), Some(6)]));
        let mask = null_mask(&[a1, a2], 3 + 3);
        assert_eq!(Vec::<u8>::default(), mask);

        let a1: VectorRef = Arc::new(Int32Vector::from(vec![Some(1), Some(2), Some(3)]));
        let a2: VectorRef = Arc::new(Int32Vector::from(vec![Some(4), Some(5), None]));
        let mask = null_mask(&[a1, a2], 3 + 3);
        assert_eq!(vec![0b0010_0000], mask);
    }
}
