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

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Error, Result};
use crate::types::TimeType;
use crate::value::Value;
use crate::vectors::Helper;

/// Used to cast the value to dest ConcreteDataType temporarily.
/// To keep the same behavior as arrow-rs.
pub fn cast(src_value: Value, dest_type: &ConcreteDataType) -> Result<Value> {
    if src_value == Value::Null {
        return Ok(Value::Null);
    }
    let src_type = src_value.data_type();
    let scalar_value = src_value.try_to_scalar_value(&src_type)?;
    let new_value = Helper::try_from_scalar_value(scalar_value, 1)?
        .cast(dest_type)?
        .get(0);
    Ok(new_value)
}

/// Cast options for cast functions.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct CastOption {
    /// decide how to handle cast failures,
    /// either return NULL (strict=false) or return ERR (strict=true)
    pub strict: bool,
}

impl CastOption {
    pub fn is_strict(&self) -> bool {
        self.strict
    }
}

/// Cast the value to dest_type with CastOption.
///
/// # Arguments
/// * `src_value` - The value to be casted.
/// * `dest_type` - The destination type.
/// * `cast_option` - The CastOption.
///
/// # Returns
/// If success, return the casted value.
/// If CastOption's strict is true, return an error if the cast fails.
/// If CastOption's strict is false, return NULL if the cast fails.
pub fn cast_with_opt(
    src_value: Value,
    dest_type: &ConcreteDataType,
    cast_option: &CastOption,
) -> Result<Value> {
    if !can_cast_type(&src_value, dest_type) {
        if cast_option.strict {
            return Err(invalid_type_cast(&src_value, dest_type));
        } else {
            return Ok(Value::Null);
        }
    }
    let new_value = dest_type.try_cast(src_value.clone());
    match new_value {
        Some(v) => Ok(v),
        None => {
            if cast_option.strict && !src_value.is_null() {
                Err(invalid_type_cast(&src_value, dest_type))
            } else {
                Ok(Value::Null)
            }
        }
    }
}

/// Return true if the src_value can be casted to dest_type,
/// Otherwise, return false.
/// Notice: this function does not promise that the `cast_with_opt` will succeed,
/// it only checks whether the src_value can be casted to dest_type.
pub fn can_cast_type(src_value: &Value, dest_type: &ConcreteDataType) -> bool {
    use ConcreteDataType::*;
    use TimeType::*;
    let src_type = &src_value.data_type();

    if src_type == dest_type {
        return true;
    }

    match (src_type, dest_type) {
        // null type cast
        (_, Null(_)) => true,
        (Null(_), _) => true,

        // boolean type cast
        (_, Boolean(_)) => src_type.is_numeric() || src_type.is_string(),
        (Boolean(_), _) => dest_type.is_numeric() || dest_type.is_string(),

        // numeric types cast
        (
            UInt8(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int8(_) | Int16(_) | Int32(_) | Int64(_)
            | Float32(_) | Float64(_) | String(_),
            UInt8(_) | UInt16(_) | UInt32(_) | UInt64(_) | Int8(_) | Int16(_) | Int32(_) | Int64(_)
            | Float32(_) | Float64(_) | String(_),
        ) => true,

        (String(_), Binary(_)) => true,

        // temporal types cast
        // Date type
        (Date(_), Int32(_) | Timestamp(_) | String(_)) => true,
        (Int32(_) | String(_) | Timestamp(_), Date(_)) => true,
        (Date(_), Date(_)) => true,
        // DateTime type
        (DateTime(_), Int64(_) | Timestamp(_) | String(_)) => true,
        (Int64(_) | Timestamp(_) | String(_), DateTime(_)) => true,
        (DateTime(_), DateTime(_)) => true,
        // Timestamp type
        (Timestamp(_), Int64(_) | String(_)) => true,
        (Int64(_) | String(_), Timestamp(_)) => true,
        (Timestamp(_), Timestamp(_)) => true,
        // Time type
        (Time(_), String(_)) => true,
        (Time(Second(_)), Int32(_)) => true,
        (Time(Millisecond(_)), Int32(_)) => true,
        (Time(Microsecond(_)), Int64(_)) => true,
        (Time(Nanosecond(_)), Int64(_)) => true,
        (Time(_), Time(_)) => true,
        // TODO(QuenKar): interval type cast
        (Interval(_), String(_)) => true,
        (Duration(_), String(_)) => true,
        // other situations return false
        (_, _) => false,
    }
}

fn invalid_type_cast(src_value: &Value, dest_type: &ConcreteDataType) -> Error {
    let src_type = src_value.data_type();
    if src_type.is_string() {
        error::CastTypeSnafu {
            msg: format!("Could not parse string '{}' to {}", src_value, dest_type),
        }
        .build()
    } else if src_type.is_numeric() && dest_type.is_numeric() {
        error::CastTypeSnafu {
            msg: format!(
                "Type {} with value {} can't be cast because the value is out of range for the destination type {}",
                src_type,
                src_value,
                dest_type
            ),
        }
        .build()
    } else {
        error::CastTypeSnafu {
            msg: format!(
                "Type {} with value {} can't be cast to the destination type {}",
                src_type, src_value, dest_type
            ),
        }
        .build()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use common_base::bytes::StringBytes;
    use common_time::time::Time;
    use common_time::timezone::set_default_timezone;
    use common_time::{Date, DateTime, Timestamp};
    use ordered_float::OrderedFloat;

    use super::*;

    macro_rules! test_can_cast {
        ($src_value: expr, $($dest_type: ident),+) => {
            $(
                let val = $src_value;
                let t = ConcreteDataType::$dest_type();
                assert_eq!(can_cast_type(&val, &t), true);
            )*
        };
    }

    macro_rules! test_primitive_cast {
        ($($value: expr),*) => {
            $(
                test_can_cast!(
                    $value,
                    uint8_datatype,
                    uint16_datatype,
                    uint32_datatype,
                    uint64_datatype,
                    int8_datatype,
                    int16_datatype,
                    int32_datatype,
                    int64_datatype,
                    float32_datatype,
                    float64_datatype
                );
            )*
        };
    }

    #[test]
    fn test_cast_with_opt() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        // non-strict mode
        let cast_option = CastOption { strict: false };
        let src_value = Value::Int8(-1);
        let dest_type = ConcreteDataType::uint8_datatype();
        let res = cast_with_opt(src_value, &dest_type, &cast_option);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), Value::Null);

        // strict mode
        let cast_option = CastOption { strict: true };
        let src_value = Value::Int8(-1);
        let dest_type = ConcreteDataType::uint8_datatype();
        let res = cast_with_opt(src_value, &dest_type, &cast_option);
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Type Int8 with value -1 can't be cast because the value is out of range for the destination type UInt8"
        );

        let src_value = Value::String(StringBytes::from("abc"));
        let dest_type = ConcreteDataType::uint8_datatype();
        let res = cast_with_opt(src_value, &dest_type, &cast_option);
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Could not parse string 'abc' to UInt8"
        );

        let src_value = Value::Timestamp(Timestamp::new_second(10));
        let dest_type = ConcreteDataType::int8_datatype();
        let res = cast_with_opt(src_value, &dest_type, &cast_option);
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Type TimestampSecond with value 1970-01-01 08:00:10+0800 can't be cast to the destination type Int8"
        );
    }

    #[test]
    fn test_can_cast_type() {
        // numeric cast
        test_primitive_cast!(
            Value::UInt8(0),
            Value::UInt16(1),
            Value::UInt32(2),
            Value::UInt64(3),
            Value::Int8(4),
            Value::Int16(5),
            Value::Int32(6),
            Value::Int64(7),
            Value::Float32(OrderedFloat(8.0)),
            Value::Float64(OrderedFloat(9.0)),
            Value::String(StringBytes::from("10"))
        );

        // string -> other types
        test_can_cast!(
            Value::String(StringBytes::from("0")),
            null_datatype,
            boolean_datatype,
            date_datatype,
            datetime_datatype,
            timestamp_second_datatype,
            binary_datatype
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
    }
}
