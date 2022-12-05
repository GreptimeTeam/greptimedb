// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::DataType;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::{ConversionSnafu, Result};
use crate::value::{ListValue, Value};

pub type BinaryArray = arrow::array::LargeBinaryArray;
pub type MutableBinaryArray = arrow::array::LargeBinaryBuilder;
pub type StringArray = arrow::array::StringArray;
pub type MutableStringArray = arrow::array::StringBuilder;

macro_rules! cast_array {
    ($arr: ident, $CastType: ty) => {
        $arr.as_any()
            .downcast_ref::<$CastType>()
            .with_context(|| ConversionSnafu {
                from: format!("{:?}", $arr.data_type()),
            })?
    };
}

// TODO(yingwen): Remove this function.
pub fn arrow_array_get(array: &dyn Array, idx: usize) -> Result<Value> {
    if array.is_null(idx) {
        return Ok(Value::Null);
    }

    let result = match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Boolean(cast_array!(array, BooleanArray).value(idx)),
        DataType::Binary => Value::Binary(cast_array!(array, BinaryArray).value(idx).into()),
        DataType::Int8 => Value::Int8(cast_array!(array, Int8Array).value(idx)),
        DataType::Int16 => Value::Int16(cast_array!(array, Int16Array).value(idx)),
        DataType::Int32 => Value::Int32(cast_array!(array, Int32Array).value(idx)),
        DataType::Int64 => Value::Int64(cast_array!(array, Int64Array).value(idx)),
        DataType::UInt8 => Value::UInt8(cast_array!(array, UInt8Array).value(idx)),
        DataType::UInt16 => Value::UInt16(cast_array!(array, UInt16Array).value(idx)),
        DataType::UInt32 => Value::UInt32(cast_array!(array, UInt32Array).value(idx)),
        DataType::UInt64 => Value::UInt64(cast_array!(array, UInt64Array).value(idx)),
        DataType::Float32 => Value::Float32(cast_array!(array, Float32Array).value(idx).into()),
        DataType::Float64 => Value::Float64(cast_array!(array, Float64Array).value(idx).into()),
        DataType::Utf8 => Value::String(cast_array!(array, StringArray).value(idx).into()),
        DataType::Date32 => Value::Date(cast_array!(array, Date32Array).value(idx).into()),
        DataType::Date64 => Value::DateTime(cast_array!(array, Date64Array).value(idx).into()),
        DataType::Timestamp(t, _) => match t {
            arrow::datatypes::TimeUnit::Second => Value::Timestamp(Timestamp::new(
                cast_array!(array, arrow::array::TimestampSecondArray).value(idx),
                TimeUnit::Second,
            )),
            arrow::datatypes::TimeUnit::Millisecond => Value::Timestamp(Timestamp::new(
                cast_array!(array, arrow::array::TimestampMillisecondArray).value(idx),
                TimeUnit::Millisecond,
            )),
            arrow::datatypes::TimeUnit::Microsecond => Value::Timestamp(Timestamp::new(
                cast_array!(array, arrow::array::TimestampMicrosecondArray).value(idx),
                TimeUnit::Microsecond,
            )),
            arrow::datatypes::TimeUnit::Nanosecond => Value::Timestamp(Timestamp::new(
                cast_array!(array, arrow::array::TimestampNanosecondArray).value(idx),
                TimeUnit::Nanosecond,
            )),
        },
        DataType::List(_) => {
            let array = cast_array!(array, ListArray).value(idx);
            let item_type = ConcreteDataType::try_from(array.data_type())?;
            let values = (0..array.len())
                .map(|i| arrow_array_get(&*array, i))
                .collect::<Result<Vec<Value>>>()?;
            Value::List(ListValue::new(Some(Box::new(values)), item_type))
        }
        _ => unimplemented!("Arrow array datatype: {:?}", array.data_type()),
    };

    Ok(result)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::datatypes::Int32Type;
    use common_time::timestamp::{TimeUnit, Timestamp};
    use paste::paste;

    use super::*;
    use crate::data_type::ConcreteDataType;
    use crate::types::TimestampType;

    macro_rules! test_arrow_array_get_for_timestamps {
        ( $($unit: ident), *) => {
            $(
                paste! {
                    let mut builder = arrow::array::[<Timestamp $unit Array>]::builder(3);
                    builder.append_value(1);
                    builder.append_value(0);
                    builder.append_value(-1);
                    let ts_array = Arc::new(builder.finish()) as Arc<dyn Array>;
                    let v = arrow_array_get(&ts_array, 1).unwrap();
                    assert_eq!(
                        ConcreteDataType::Timestamp(TimestampType::$unit(
                            $crate::types::[<Timestamp $unit Type>]::default(),
                        )),
                        v.data_type()
                    );
                }
            )*
        };
    }

    #[test]
    fn test_timestamp_array() {
        test_arrow_array_get_for_timestamps![Second, Millisecond, Microsecond, Nanosecond];
    }

    #[test]
    fn test_arrow_array_access() {
        let array1 = BooleanArray::from(vec![true, true, false, false]);
        assert_eq!(Value::Boolean(true), arrow_array_get(&array1, 1).unwrap());
        let array1 = Int8Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int8(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = UInt8Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt8(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = Int16Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int16(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = UInt16Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt16(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = Int32Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int32(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = UInt32Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt32(2), arrow_array_get(&array1, 1).unwrap());
        let array = Int64Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int64(2), arrow_array_get(&array, 1).unwrap());
        let array1 = UInt64Array::from(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt64(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = Float32Array::from(vec![1f32, 2f32, 3f32, 4f32]);
        assert_eq!(
            Value::Float32(2f32.into()),
            arrow_array_get(&array1, 1).unwrap()
        );
        let array1 = Float64Array::from(vec![1f64, 2f64, 3f64, 4f64]);
        assert_eq!(
            Value::Float64(2f64.into()),
            arrow_array_get(&array1, 1).unwrap()
        );

        let array2 = StringArray::from(vec![Some("hello"), None, Some("world")]);
        assert_eq!(
            Value::String("hello".into()),
            arrow_array_get(&array2, 0).unwrap()
        );
        assert_eq!(Value::Null, arrow_array_get(&array2, 1).unwrap());

        let array3 = LargeBinaryArray::from(vec![
            Some("hello".as_bytes()),
            None,
            Some("world".as_bytes()),
        ]);
        assert_eq!(Value::Null, arrow_array_get(&array3, 1).unwrap());

        let array = TimestampSecondArray::from(vec![1, 2, 3]);
        let value = arrow_array_get(&array, 1).unwrap();
        assert_eq!(value, Value::Timestamp(Timestamp::new(2, TimeUnit::Second)));
        let array = TimestampMillisecondArray::from(vec![1, 2, 3]);
        let value = arrow_array_get(&array, 1).unwrap();
        assert_eq!(
            value,
            Value::Timestamp(Timestamp::new(2, TimeUnit::Millisecond))
        );
        let array = TimestampMicrosecondArray::from(vec![1, 2, 3]);
        let value = arrow_array_get(&array, 1).unwrap();
        assert_eq!(
            value,
            Value::Timestamp(Timestamp::new(2, TimeUnit::Microsecond))
        );
        let array = TimestampNanosecondArray::from(vec![1, 2, 3]);
        let value = arrow_array_get(&array, 1).unwrap();
        assert_eq!(
            value,
            Value::Timestamp(Timestamp::new(2, TimeUnit::Nanosecond))
        );

        // test list array
        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];
        let arrow_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        let v0 = arrow_array_get(&arrow_array, 0).unwrap();
        match v0 {
            Value::List(list) => {
                assert!(matches!(list.datatype(), ConcreteDataType::Int32(_)));
                let items = list.items().as_ref().unwrap();
                assert_eq!(
                    **items,
                    vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
                );
            }
            _ => unreachable!(),
        }

        assert_eq!(Value::Null, arrow_array_get(&arrow_array, 1).unwrap());
        let v2 = arrow_array_get(&arrow_array, 2).unwrap();
        match v2 {
            Value::List(list) => {
                assert!(matches!(list.datatype(), ConcreteDataType::Int32(_)));
                let items = list.items().as_ref().unwrap();
                assert_eq!(**items, vec![Value::Int32(4), Value::Null, Value::Int32(6)]);
            }
            _ => unreachable!(),
        }
    }
}
