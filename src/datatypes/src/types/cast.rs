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

use crate::data_type::ConcreteDataType;
use crate::types::{IntervalType, TimeType};
use crate::value::Value;

// Return true if the src_value can be casted to dest_type,
// Otherwise, return false.
pub fn can_cast_type(src_value: &Value, dest_type: ConcreteDataType) -> bool {
    use ConcreteDataType::*;
    use IntervalType::*;
    use TimeType::*;
    let src_type = src_value.data_type();

    if src_type == dest_type {
        return true;
    }

    match (src_type, dest_type) {
        (_, Null(_)) => true,
        // numeric types
        (
            Boolean(_),
            UInt8(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int8(_) | Int16(_) | Int32(_) | Int64(_)
            | Float32(_) | Float64(_) | String(_),
        ) => true,
        (
            UInt8(_),
            Boolean(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int16(_) | Int32(_) | Int64(_)
            | Float32(_) | Float64(_) | String(_),
        ) => true,
        (
            UInt16(_),
            Boolean(_) | UInt8(_) | UInt32(_) | UInt64(_) | Int32(_) | Int64(_) | Float32(_)
            | Float64(_) | String(_),
        ) => true,
        (UInt32(_), Boolean(_) | UInt64(_) | Int64(_) | Float64(_) | String(_)) => true,
        (UInt64(_), Boolean(_) | String(_)) => true,
        (
            Int8(_),
            Boolean(_) | Int16(_) | Int32(_) | Int64(_) | Float32(_) | Float64(_) | String(_),
        ) => true,
        (Int16(_), Boolean(_) | Int32(_) | Int64(_) | Float32(_) | Float64(_) | String(_)) => true,
        (Int32(_), Boolean(_) | Int64(_) | Float32(_) | Float64(_) | String(_) | Date(_)) => true,
        (Int64(_), Boolean(_) | Float64(_) | String(_) | DateTime(_) | Timestamp(_) | Time(_)) => {
            true
        }
        (
            Float32(_),
            Boolean(_) | UInt8(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int8(_) | Int16(_)
            | Int32(_) | Int64(_) | Float64(_) | String(_),
        ) => true,
        (
            Float64(_),
            Boolean(_) | UInt8(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int8(_) | Int16(_)
            | Int32(_) | Int64(_) | String(_),
        ) => true,
        (
            String(_),
            Boolean(_) | UInt8(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int8(_) | Int16(_)
            | Int32(_) | Int64(_) | Float32(_) | Float64(_) | Date(_) | DateTime(_) | Timestamp(_)
            | Time(_) | Interval(_),
        ) => true,
        // temporal types
        (Date(_), Int32(_) | Timestamp(_) | String(_)) => true,
        (DateTime(_), Int64(_) | Timestamp(_) | String(_)) => true,
        (Timestamp(_), Int64(_) | Date(_) | DateTime(_) | String(_)) => true,
        (Time(_), String(_)) => true,
        (Time(Second(_)), Int32(_)) => true,
        (Time(Millisecond(_)), Int32(_)) => true,
        (Time(Microsecond(_)), Int64(_)) => true,
        (Time(Nanosecond(_)), Int64(_)) => true,
        (Interval(_), String(_)) => true,
        (Interval(YearMonth(_)), Int32(_)) => true,
        (Interval(DayTime(_)), Int64(_)) => true,
        (Interval(MonthDayNano(_)), _) => false,
        // other situations return false
        (_, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use common_base::bytes::StringBytes;
    use common_time::time::Time;
    use common_time::{Date, DateTime, Interval, Timestamp};
    use ordered_float::OrderedFloat;

    use super::*;

    macro_rules! test_can_cast {
        ($src_value: expr, $($dest_type: ident),*) => {
            $(
                let val = $src_value;
                let t = ConcreteDataType::$dest_type();
                assert_eq!(can_cast_type(&val, t), true);
            )*
        };
    }

    #[test]
    fn test_can_cast_type() {
        // uint8 -> other types
        test_can_cast!(
            Value::UInt8(0),
            null_datatype,
            uint8_datatype,
            uint16_datatype,
            uint32_datatype,
            uint64_datatype,
            int16_datatype,
            int32_datatype,
            int64_datatype,
            float32_datatype,
            float64_datatype,
            string_datatype
        );

        // uint16 -> other types
        test_can_cast!(
            Value::UInt16(0),
            null_datatype,
            uint8_datatype,
            uint16_datatype,
            uint32_datatype,
            uint64_datatype,
            int32_datatype,
            int64_datatype,
            float32_datatype,
            float64_datatype,
            string_datatype
        );

        // uint32 -> other types
        test_can_cast!(
            Value::UInt32(0),
            null_datatype,
            uint32_datatype,
            uint64_datatype,
            int64_datatype,
            float64_datatype,
            string_datatype
        );

        // uint64 -> other types
        test_can_cast!(
            Value::UInt64(0),
            null_datatype,
            uint64_datatype,
            string_datatype
        );

        // int8 -> other types
        test_can_cast!(
            Value::Int8(0),
            null_datatype,
            int16_datatype,
            int32_datatype,
            int64_datatype,
            float32_datatype,
            float64_datatype,
            string_datatype
        );

        // int16 -> other types
        test_can_cast!(
            Value::Int16(0),
            null_datatype,
            int16_datatype,
            int32_datatype,
            int64_datatype,
            float32_datatype,
            float64_datatype,
            string_datatype
        );

        // int32 -> other types
        test_can_cast!(
            Value::Int32(0),
            null_datatype,
            int32_datatype,
            int64_datatype,
            float32_datatype,
            float64_datatype,
            string_datatype,
            date_datatype
        );

        // int64 -> other types
        test_can_cast!(
            Value::Int64(0),
            null_datatype,
            int64_datatype,
            float64_datatype,
            string_datatype,
            datetime_datatype,
            timestamp_second_datatype,
            time_second_datatype
        );

        // float32 -> other types
        test_can_cast!(
            Value::Float32(OrderedFloat(0.0)),
            null_datatype,
            uint8_datatype,
            uint16_datatype,
            uint32_datatype,
            uint64_datatype,
            int8_datatype,
            int16_datatype,
            int32_datatype,
            int64_datatype,
            float32_datatype,
            float64_datatype,
            string_datatype
        );

        // float64 -> other types
        test_can_cast!(
            Value::Float64(OrderedFloat(0.0)),
            null_datatype,
            uint8_datatype,
            uint16_datatype,
            uint32_datatype,
            uint64_datatype,
            int8_datatype,
            int16_datatype,
            int32_datatype,
            int64_datatype,
            float64_datatype,
            string_datatype
        );

        // string -> other types
        test_can_cast!(
            Value::String(StringBytes::from("0")),
            null_datatype,
            uint8_datatype,
            uint16_datatype,
            uint32_datatype,
            uint64_datatype,
            int8_datatype,
            int16_datatype,
            int32_datatype,
            int64_datatype,
            float32_datatype,
            float64_datatype,
            string_datatype,
            date_datatype,
            datetime_datatype,
            timestamp_second_datatype,
            time_second_datatype,
            interval_year_month_datatype,
            interval_day_time_datatype,
            interval_month_day_nano_datatype
        );

        // date -> other types
        test_can_cast!(
            Value::Date(Date::from_str("2021-01-01").unwrap()),
            null_datatype,
            int32_datatype,
            timestamp_second_datatype,
            string_datatype
        );

        // datetime -> other types
        test_can_cast!(
            Value::DateTime(DateTime::from_str("2021-01-01 00:00:00").unwrap()),
            null_datatype,
            int64_datatype,
            timestamp_second_datatype,
            string_datatype
        );

        // timestamp -> other types
        test_can_cast!(
            Value::Timestamp(Timestamp::from_str("2021-01-01 00:00:00").unwrap()),
            null_datatype,
            int64_datatype,
            date_datatype,
            datetime_datatype,
            string_datatype
        );

        // time -> other types
        test_can_cast!(
            Value::Time(Time::new_second(0)),
            null_datatype,
            string_datatype
        );

        // interval -> other types
        test_can_cast!(
            Value::Interval(Interval::from_year_month(0)),
            null_datatype,
            string_datatype
        );
    }
}
