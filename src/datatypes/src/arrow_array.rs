use arrow::array::{
    self, Array, BinaryArray as ArrowBinaryArray, MutableBinaryArray as ArrowMutableBinaryArray,
    MutableUtf8Array, PrimitiveArray, Utf8Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use snafu::OptionExt;

use crate::error::{ConversionSnafu, Result};
use crate::value::Value;

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
        // TODO(sunng87): List
        _ => unimplemented!("Arrow array datatype: {:?}", array.data_type()),
    };

    Ok(result)
}

#[cfg(test)]
mod test {
    use arrow::array::*;

    use super::*;

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
        let array1 = Int64Array::from_vec(vec![1, 2, 3, 4]);
        assert_eq!(Value::Int64(2), arrow_array_get(&array1, 1).unwrap());
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
    }
}
