use arrow::array::{
    self, Array, BinaryArray as ArrowBinaryArray, ListArray,
    MutableBinaryArray as ArrowMutableBinaryArray, MutableUtf8Array, PrimitiveArray, Utf8Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use common_time::timestamp::Timestamp;
use snafu::OptionExt;

use crate::error::{ConversionSnafu, Result};
use crate::prelude::ConcreteDataType;
use crate::value::{ListValue, Value};

pub type BinaryArray = ArrowBinaryArray<i64>;
pub type MutableBinaryArray = ArrowMutableBinaryArray<i64>;
pub type MutableStringArray = MutableUtf8Array<i32>;
pub type StringArray = Utf8Array<i32>;

macro_rules! cast_array {
    ($arr: ident, $CastType: ty) => {
        $arr.as_any()
            .downcast_ref::<$CastType>()
            .with_context(|| ConversionSnafu {
                from: format!("{:?}", $arr.data_type()),
            })?
    };
}

pub fn arrow_array_get(array: &dyn Array, idx: usize) -> Result<Value> {
    if array.is_null(idx) {
        return Ok(Value::Null);
    }

    let result = match array.data_type() {
        ArrowDataType::Null => Value::Null,
        ArrowDataType::Boolean => {
            Value::Boolean(cast_array!(array, array::BooleanArray).value(idx))
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => {
            Value::Binary(cast_array!(array, BinaryArray).value(idx).into())
        }
        ArrowDataType::Int8 => Value::Int8(cast_array!(array, PrimitiveArray::<i8>).value(idx)),
        ArrowDataType::Int16 => Value::Int16(cast_array!(array, PrimitiveArray::<i16>).value(idx)),
        ArrowDataType::Int32 => Value::Int32(cast_array!(array, PrimitiveArray::<i32>).value(idx)),
        ArrowDataType::Int64 => Value::Int64(cast_array!(array, PrimitiveArray::<i64>).value(idx)),
        ArrowDataType::UInt8 => Value::UInt8(cast_array!(array, PrimitiveArray::<u8>).value(idx)),
        ArrowDataType::UInt16 => {
            Value::UInt16(cast_array!(array, PrimitiveArray::<u16>).value(idx))
        }
        ArrowDataType::UInt32 => {
            Value::UInt32(cast_array!(array, PrimitiveArray::<u32>).value(idx))
        }
        ArrowDataType::UInt64 => {
            Value::UInt64(cast_array!(array, PrimitiveArray::<u64>).value(idx))
        }
        ArrowDataType::Float32 => {
            Value::Float32(cast_array!(array, PrimitiveArray::<f32>).value(idx).into())
        }
        ArrowDataType::Float64 => {
            Value::Float64(cast_array!(array, PrimitiveArray::<f64>).value(idx).into())
        }
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
            Value::String(cast_array!(array, StringArray).value(idx).into())
        }
        ArrowDataType::Timestamp(t, _) => {
            let value = cast_array!(array, PrimitiveArray::<i64>).value(idx);
            let unit = match ConcreteDataType::from_arrow_time_unit(t) {
                ConcreteDataType::Timestamp(t) => t.unit,
                _ => unreachable!(),
            };
            Value::Timestamp(Timestamp::new(value, unit))
        }
        ArrowDataType::List(_) => {
            let array = cast_array!(array, ListArray::<i32>).value(idx);
            let inner_datatype = ConcreteDataType::try_from(array.data_type())?;
            let values = (0..array.len())
                .map(|i| arrow_array_get(&*array, i))
                .collect::<Result<Vec<Value>>>()?;
            Value::List(ListValue::new(Some(Box::new(values)), inner_datatype))
        }
        _ => unimplemented!("Arrow array datatype: {:?}", array.data_type()),
    };

    Ok(result)
}

#[cfg(test)]
mod test {
    use arrow::array::Int64Array as ArrowI64Array;
    use arrow::array::*;
    use arrow::array::{MutableListArray, MutablePrimitiveArray, TryExtend};
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
    use common_time::timestamp::{TimeUnit, Timestamp};

    use super::*;
    use crate::prelude::Vector;
    use crate::vectors::TimestampVector;

    #[test]
    fn test_arrow_array_access() {
        let array1 = BooleanArray::from_slice(vec![true, true, false, false]);
        assert_eq!(Value::Boolean(true), arrow_array_get(&array1, 1).unwrap());
        let array1 = Int8Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int8(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = UInt8Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt8(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = Int16Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int16(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = UInt16Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt16(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = Int32Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int32(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = UInt32Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt32(2), arrow_array_get(&array1, 1).unwrap());
        let array = ArrowI64Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int64(2), arrow_array_get(&array, 1).unwrap());
        let array1 = UInt64Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::UInt64(2), arrow_array_get(&array1, 1).unwrap());
        let array1 = Float32Array::from_vec(vec![1f32, 2f32, 3f32, 4f32]);
        assert_eq!(
            Value::Float32(2f32.into()),
            arrow_array_get(&array1, 1).unwrap()
        );
        let array1 = Float64Array::from_vec(vec![1f64, 2f64, 3f64, 4f64]);
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

        let array3 = super::BinaryArray::from(vec![
            Some("hello".as_bytes()),
            None,
            Some("world".as_bytes()),
        ]);
        assert_eq!(
            Value::Binary("hello".as_bytes().into()),
            arrow_array_get(&array3, 0).unwrap()
        );
        assert_eq!(Value::Null, arrow_array_get(&array3, 1).unwrap());

        let vector = TimestampVector::new(ArrowI64Array::from_vec(vec![1, 2, 3, 4]));
        let array = vector.to_boxed_arrow_array();
        let value = arrow_array_get(&*array, 1).unwrap();
        assert_eq!(
            value,
            Value::Timestamp(Timestamp::new(2, TimeUnit::Millisecond))
        );

        let array4 = PrimitiveArray::<i64>::from_data(
            DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            Buffer::from_slice(&vec![1, 2, 3, 4]),
            None,
        );
        assert_eq!(
            Value::Timestamp(Timestamp::new(1, TimeUnit::Millisecond)),
            arrow_array_get(&array4, 0).unwrap()
        );

        let array4 = PrimitiveArray::<i64>::from_data(
            DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
            Buffer::from_slice(&vec![1, 2, 3, 4]),
            None,
        );
        assert_eq!(
            Value::Timestamp(Timestamp::new(1, TimeUnit::Nanosecond)),
            arrow_array_get(&array4, 0).unwrap()
        );

        // test list array
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut arrow_array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        arrow_array.try_extend(data).unwrap();
        let arrow_array: ListArray<i32> = arrow_array.into();

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
