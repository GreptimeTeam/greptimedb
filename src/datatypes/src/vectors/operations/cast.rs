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

macro_rules! cast_non_constant {
    ($vector: expr, $to_type: expr) => {{
        use arrow::compute;
        use snafu::ResultExt;

        use crate::data_type::DataType;
        use crate::vectors::helper::Helper;

        let arrow_array = $vector.to_arrow_array();
        let casted = compute::cast(&arrow_array, &$to_type.as_arrow_type())
            .context(crate::error::ArrowComputeSnafu)?;
        Helper::try_into_vector(casted)
    }};
}

pub(crate) use cast_non_constant;

/// There are already many test cases in arrow:
/// https://github.com/apache/arrow-rs/blob/59016e53e5cfa1d368009ed640d1f3dce326e7bb/arrow-cast/src/cast.rs#L3349-L7584
/// So we don't(can't) want to copy these cases, just test some cases which are important for us.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::date::Date;
    use common_time::timestamp::{TimeUnit, Timestamp};

    use crate::types::{LogicalPrimitiveType, *};
    use crate::vectors::{ConcreteDataType, *};

    fn get_cast_values<T>(vector: &VectorRef, dt: &ConcreteDataType) -> Vec<String>
    where
        T: LogicalPrimitiveType,
    {
        let c = vector.cast(dt).unwrap();
        let a = c.as_any().downcast_ref::<PrimitiveVector<T>>().unwrap();
        let mut v: Vec<String> = vec![];
        for i in 0..vector.len() {
            if a.is_null(i) {
                v.push("null".to_string())
            } else {
                v.push(format!("{}", a.get(i)));
            }
        }
        v
    }

    #[test]
    fn test_cast_from_f64() {
        let f64_values: Vec<f64> = vec![
            i64::MIN as f64,
            i32::MIN as f64,
            i16::MIN as f64,
            i8::MIN as f64,
            0_f64,
            u8::MAX as f64,
            u16::MAX as f64,
            u32::MAX as f64,
            u64::MAX as f64,
        ];
        let f64_vector: VectorRef = Arc::new(Float64Vector::from_slice(f64_values));

        let f64_expected = vec![
            -9223372036854776000.0,
            -2147483648.0,
            -32768.0,
            -128.0,
            0.0,
            255.0,
            65535.0,
            4294967295.0,
            18446744073709552000.0,
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&f64_vector, &ConcreteDataType::float64_datatype())
                .iter()
                .map(|i| i.parse::<f64>().unwrap())
                .collect::<Vec<f64>>()
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&f64_vector, &ConcreteDataType::int64_datatype())
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&f64_vector, &ConcreteDataType::uint64_datatype())
        );
    }

    #[test]
    fn test_cast_from_date() {
        let i32_values: Vec<i32> = vec![
            i32::MIN,
            i16::MIN as i32,
            i8::MIN as i32,
            0,
            i8::MAX as i32,
            i16::MAX as i32,
            i32::MAX,
        ];
        let date32_vector: VectorRef = Arc::new(DateVector::from_slice(i32_values));

        let i32_expected = vec![
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&date32_vector, &ConcreteDataType::int32_datatype()),
        );

        let i64_expected = vec![
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&date32_vector, &ConcreteDataType::int64_datatype()),
        );
    }

    #[test]
    fn test_cast_timestamp_to_date32() {
        let vector =
            TimestampMillisecondVector::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = vector.cast(&ConcreteDataType::date_datatype()).unwrap();
        let c = b.as_any().downcast_ref::<DateVector>().unwrap();
        assert_eq!(Value::Date(Date::from(10000)), c.get(0));
        assert_eq!(Value::Date(Date::from(17890)), c.get(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_string_to_timestamp() {
        let a1 = Arc::new(StringVector::from(vec![
            Some("2020-09-08T12:00:00+00:00"),
            Some("Not a valid date"),
            None,
        ])) as VectorRef;
        let a2 = Arc::new(StringVector::from(vec![
            Some("2020-09-08T12:00:00+00:00"),
            Some("Not a valid date"),
            None,
        ])) as VectorRef;

        for array in &[a1, a2] {
            let to_type = ConcreteDataType::timestamp_nanosecond_datatype();
            let b = array.cast(&to_type).unwrap();
            let c = b
                .as_any()
                .downcast_ref::<TimestampNanosecondVector>()
                .unwrap();
            assert_eq!(
                Value::Timestamp(Timestamp::new(1599566400000000000, TimeUnit::Nanosecond)),
                c.get(0)
            );
            assert!(c.is_null(1));
            assert!(c.is_null(2));
        }
    }
}
