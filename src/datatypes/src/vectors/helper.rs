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

//! Vector helper functions, inspired by databend Series mod

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::compute;
use arrow::compute::kernels::comparison;
use arrow::datatypes::{
    DataType as ArrowDataType, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{DictionaryArray, StructArray};
use arrow_schema::IntervalUnit;
use datafusion_common::ScalarValue;
use snafu::{OptionExt, ResultExt};

use crate::data_type::ConcreteDataType;
use crate::error::{self, ConvertScalarToArrowArraySnafu, Result};
use crate::prelude::DataType;
use crate::scalars::Scalar;
use crate::types::StructType;
use crate::value::Value;
use crate::vectors::struct_vector::StructVector;
use crate::vectors::{
    BinaryVector, BooleanVector, DateVector, Decimal128Vector, DictionaryVector,
    DurationMicrosecondVector, DurationMillisecondVector, DurationNanosecondVector,
    DurationSecondVector, Float32Vector, Float64Vector, Int8Vector, Int16Vector, Int32Vector,
    Int64Vector, IntervalDayTimeVector, IntervalMonthDayNanoVector, IntervalYearMonthVector,
    ListVector, MutableVector, NullVector, StringVector, TimeMicrosecondVector,
    TimeMillisecondVector, TimeNanosecondVector, TimeSecondVector, TimestampMicrosecondVector,
    TimestampMillisecondVector, TimestampNanosecondVector, TimestampSecondVector, UInt8Vector,
    UInt16Vector, UInt32Vector, UInt64Vector, Vector, VectorRef,
};

/// Helper functions for `Vector`.
pub struct Helper;

impl Helper {
    /// Get a pointer to the underlying data of this vectors.
    /// Can be useful for fast comparisons.
    /// # Safety
    /// Assumes that the `vector` is  T.
    pub unsafe fn static_cast<T: Any>(vector: &VectorRef) -> &T {
        let object = vector.as_ref();
        debug_assert!(object.as_any().is::<T>());
        unsafe { &*(object as *const dyn Vector as *const T) }
    }

    pub fn check_get_scalar<T: Scalar>(vector: &VectorRef) -> Result<&<T as Scalar>::VectorType> {
        vector
            .as_any()
            .downcast_ref::<<T as Scalar>::VectorType>()
            .with_context(|| error::UnknownVectorSnafu {
                msg: format!(
                    "downcast vector error, vector type: {:?}, expected vector: {:?}",
                    vector.vector_type_name(),
                    std::any::type_name::<T>(),
                ),
            })
    }

    pub fn check_get<T: 'static + Vector>(vector: &VectorRef) -> Result<&T> {
        vector
            .as_any()
            .downcast_ref::<T>()
            .with_context(|| error::UnknownVectorSnafu {
                msg: format!(
                    "downcast vector error, vector type: {:?}, expected vector: {:?}",
                    vector.vector_type_name(),
                    std::any::type_name::<T>(),
                ),
            })
    }

    pub fn check_get_scalar_vector<T: Scalar>(
        vector: &VectorRef,
    ) -> Result<&<T as Scalar>::VectorType> {
        vector
            .as_any()
            .downcast_ref::<<T as Scalar>::VectorType>()
            .with_context(|| error::UnknownVectorSnafu {
                msg: format!(
                    "downcast vector error, vector type: {:?}, expected vector: {:?}",
                    vector.vector_type_name(),
                    std::any::type_name::<T>(),
                ),
            })
    }

    /// Try to cast an arrow scalar value into vector
    pub fn try_from_scalar_value(value: ScalarValue, length: usize) -> Result<VectorRef> {
        let value = match value {
            // GreptimeDB doesn't support Float16 vectors.
            ScalarValue::Float16(v) => ScalarValue::Float32(v.map(f32::from)),
            ScalarValue::LargeUtf8(v) => ScalarValue::Utf8(v),
            ScalarValue::LargeBinary(v) | ScalarValue::FixedSizeBinary(_, v) => {
                ScalarValue::Binary(v)
            }
            // Timezones are not supported by GreptimeDB vectors.
            ScalarValue::TimestampSecond(v, _) => ScalarValue::TimestampSecond(v, None),
            ScalarValue::TimestampMillisecond(v, _) => ScalarValue::TimestampMillisecond(v, None),
            ScalarValue::TimestampMicrosecond(v, _) => ScalarValue::TimestampMicrosecond(v, None),
            ScalarValue::TimestampNanosecond(v, _) => ScalarValue::TimestampNanosecond(v, None),
            value @ (ScalarValue::Decimal32(_, _, _)
            | ScalarValue::Decimal64(_, _, _)
            | ScalarValue::Decimal256(_, _, _)
            | ScalarValue::FixedSizeList(_)
            | ScalarValue::LargeList(_)
            | ScalarValue::Dictionary(_, _)
            | ScalarValue::Union(_, _, _)
            | ScalarValue::Utf8View(_)
            | ScalarValue::BinaryView(_)
            | ScalarValue::Map(_)
            | ScalarValue::Date64(_)
            | ScalarValue::RunEndEncoded(_, _, _)) => {
                return error::ConversionSnafu {
                    from: format!("Unsupported scalar value: {value}"),
                }
                .fail();
            }
            value => value,
        };

        let array = value
            .to_array_of_size(length)
            .context(ConvertScalarToArrowArraySnafu)?;
        Self::try_into_vector(array)
    }

    /// Try to cast an arrow array into vector
    ///
    /// # Panics
    /// Panic if given arrow data type is not supported.
    pub fn try_into_vector(array: impl AsRef<dyn Array>) -> Result<VectorRef> {
        Ok(match array.as_ref().data_type() {
            ArrowDataType::Null => Arc::new(NullVector::try_from_arrow_array(array)?),
            ArrowDataType::Boolean => Arc::new(BooleanVector::try_from_arrow_array(array)?),
            ArrowDataType::Binary | ArrowDataType::BinaryView => {
                Arc::new(BinaryVector::try_from_arrow_array(array)?)
            }
            ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => {
                let array = arrow::compute::cast(array.as_ref(), &ArrowDataType::Binary)
                    .context(crate::error::ArrowComputeSnafu)?;
                Arc::new(BinaryVector::try_from_arrow_array(array)?)
            }
            ArrowDataType::Int8 => Arc::new(Int8Vector::try_from_arrow_array(array)?),
            ArrowDataType::Int16 => Arc::new(Int16Vector::try_from_arrow_array(array)?),
            ArrowDataType::Int32 => Arc::new(Int32Vector::try_from_arrow_array(array)?),
            ArrowDataType::Int64 => Arc::new(Int64Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt8 => Arc::new(UInt8Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt16 => Arc::new(UInt16Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt32 => Arc::new(UInt32Vector::try_from_arrow_array(array)?),
            ArrowDataType::UInt64 => Arc::new(UInt64Vector::try_from_arrow_array(array)?),
            ArrowDataType::Float32 => Arc::new(Float32Vector::try_from_arrow_array(array)?),
            ArrowDataType::Float64 => Arc::new(Float64Vector::try_from_arrow_array(array)?),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                Arc::new(StringVector::try_from_arrow_array(array)?)
            }
            ArrowDataType::Date32 => Arc::new(DateVector::try_from_arrow_array(array)?),
            ArrowDataType::List(_) => Arc::new(ListVector::try_from_arrow_array(array)?),
            ArrowDataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => Arc::new(TimestampSecondVector::try_from_arrow_array(array)?),
                TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondVector::try_from_arrow_array(array)?)
                }
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondVector::try_from_arrow_array(array)?)
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondVector::try_from_arrow_array(array)?)
                }
            },
            ArrowDataType::Time32(unit) => match unit {
                TimeUnit::Second => Arc::new(TimeSecondVector::try_from_arrow_array(array)?),
                TimeUnit::Millisecond => {
                    Arc::new(TimeMillisecondVector::try_from_arrow_array(array)?)
                }
                // Arrow use time32 for second/millisecond.
                _ => unreachable!(
                    "unexpected arrow array datatype: {:?}",
                    array.as_ref().data_type()
                ),
            },
            ArrowDataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    Arc::new(TimeMicrosecondVector::try_from_arrow_array(array)?)
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimeNanosecondVector::try_from_arrow_array(array)?)
                }
                // Arrow use time64 for microsecond/nanosecond.
                _ => unreachable!(
                    "unexpected arrow array datatype: {:?}",
                    array.as_ref().data_type()
                ),
            },
            ArrowDataType::Interval(unit) => match unit {
                IntervalUnit::YearMonth => {
                    Arc::new(IntervalYearMonthVector::try_from_arrow_array(array)?)
                }
                IntervalUnit::DayTime => {
                    Arc::new(IntervalDayTimeVector::try_from_arrow_array(array)?)
                }
                IntervalUnit::MonthDayNano => {
                    Arc::new(IntervalMonthDayNanoVector::try_from_arrow_array(array)?)
                }
            },
            ArrowDataType::Duration(unit) => match unit {
                TimeUnit::Second => Arc::new(DurationSecondVector::try_from_arrow_array(array)?),
                TimeUnit::Millisecond => {
                    Arc::new(DurationMillisecondVector::try_from_arrow_array(array)?)
                }
                TimeUnit::Microsecond => {
                    Arc::new(DurationMicrosecondVector::try_from_arrow_array(array)?)
                }
                TimeUnit::Nanosecond => {
                    Arc::new(DurationNanosecondVector::try_from_arrow_array(array)?)
                }
            },
            ArrowDataType::Decimal128(_, _) => {
                Arc::new(Decimal128Vector::try_from_arrow_array(array)?)
            }
            ArrowDataType::Dictionary(key, value) => {
                macro_rules! handle_dictionary_key_type {
                    ($key_type:ident) => {{
                        let array = array
                            .as_ref()
                            .as_any()
                            .downcast_ref::<DictionaryArray<$key_type>>()
                            .unwrap(); // Safety: the type is guarded by match arm condition
                        Arc::new(DictionaryVector::new(
                            array.clone(),
                            ConcreteDataType::try_from(value.as_ref())?,
                        )?)
                    }};
                }

                match key.as_ref() {
                    ArrowDataType::Int8 => handle_dictionary_key_type!(Int8Type),
                    ArrowDataType::Int16 => handle_dictionary_key_type!(Int16Type),
                    ArrowDataType::Int32 => handle_dictionary_key_type!(Int32Type),
                    ArrowDataType::Int64 => handle_dictionary_key_type!(Int64Type),
                    ArrowDataType::UInt8 => handle_dictionary_key_type!(UInt8Type),
                    ArrowDataType::UInt16 => handle_dictionary_key_type!(UInt16Type),
                    ArrowDataType::UInt32 => handle_dictionary_key_type!(UInt32Type),
                    ArrowDataType::UInt64 => handle_dictionary_key_type!(UInt64Type),
                    _ => {
                        return error::UnsupportedArrowTypeSnafu {
                            arrow_type: array.as_ref().data_type().clone(),
                        }
                        .fail();
                    }
                }
            }

            ArrowDataType::Struct(fields) => {
                let array = array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                Arc::new(StructVector::try_new(
                    StructType::from(fields),
                    array.clone(),
                )?)
            }
            ArrowDataType::Float16
            | ArrowDataType::LargeList(_)
            | ArrowDataType::FixedSizeList(_, _)
            | ArrowDataType::Union(_, _)
            | ArrowDataType::Decimal256(_, _)
            | ArrowDataType::Map(_, _)
            | ArrowDataType::RunEndEncoded(_, _)
            | ArrowDataType::ListView(_)
            | ArrowDataType::LargeListView(_)
            | ArrowDataType::Date64
            | ArrowDataType::Decimal32(_, _)
            | ArrowDataType::Decimal64(_, _) => {
                return error::UnsupportedArrowTypeSnafu {
                    arrow_type: array.as_ref().data_type().clone(),
                }
                .fail();
            }
        })
    }

    /// Try to cast an vec of values into vector, fail if type is not the same across all values.
    pub fn try_from_row_into_vector(row: &[Value], dt: &ConcreteDataType) -> Result<VectorRef> {
        let mut builder = dt.create_mutable_vector(row.len());
        for val in row {
            builder.try_push_value_ref(&val.as_value_ref())?;
        }
        let vector = builder.to_vector();
        Ok(vector)
    }

    /// Try to cast slice of `arrays` to vectors.
    pub fn try_into_vectors(arrays: &[ArrayRef]) -> Result<Vec<VectorRef>> {
        arrays.iter().map(Self::try_into_vector).collect()
    }

    /// Perform SQL like operation on `names` and a scalar `s`.
    pub fn like_utf8(names: Vec<String>, s: &str) -> Result<VectorRef> {
        let array = StringArray::from(names);

        let s = StringArray::new_scalar(s);
        let filter = comparison::like(&array, &s).context(error::ArrowComputeSnafu)?;

        let result = compute::filter(&array, &filter).context(error::ArrowComputeSnafu)?;
        Helper::try_into_vector(result)
    }

    pub fn like_utf8_filter(names: Vec<String>, s: &str) -> Result<(VectorRef, BooleanVector)> {
        let array = StringArray::from(names);
        let s = StringArray::new_scalar(s);
        let filter = comparison::like(&array, &s).context(error::ArrowComputeSnafu)?;
        let result = compute::filter(&array, &filter).context(error::ArrowComputeSnafu)?;
        let vector = Helper::try_into_vector(result)?;

        Ok((vector, BooleanVector::from(filter)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeBinaryArray, ListArray, NullArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::{Int32Type, IntervalMonthDayNano};
    use arrow_array::{
        BinaryArray, DictionaryArray, FixedSizeBinaryArray, LargeStringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields};
    use common_decimal::Decimal128;
    use common_time::time::Time;
    use common_time::timestamp::TimeUnit;
    use common_time::{Date, Duration};
    use datafusion_common::scalar::ScalarStructBuilder;

    use super::*;
    use crate::value::Value;
    use crate::vectors::ConcreteDataType;

    #[test]
    fn test_try_into_vectors() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![3])),
        ];
        let vectors = Helper::try_into_vectors(&arrays).unwrap();
        vectors.iter().for_each(|v| assert_eq!(1, v.len()));
        assert_eq!(Value::Int32(1), vectors[0].get(0));
        assert_eq!(Value::Int32(2), vectors[1].get(0));
        assert_eq!(Value::Int32(3), vectors[2].get(0));
    }

    #[test]
    fn test_try_into_date_vector() {
        let vector = DateVector::from(vec![Some(1), Some(2), None]);
        let arrow_array = vector.to_arrow_array();
        assert_eq!(&ArrowDataType::Date32, arrow_array.data_type());
        let vector_converted = Helper::try_into_vector(arrow_array).unwrap();
        assert_eq!(vector.len(), vector_converted.len());
        for i in 0..vector_converted.len() {
            assert_eq!(vector.get(i), vector_converted.get(i));
        }
    }

    #[test]
    fn test_try_from_scalar_date_value() {
        let vector = Helper::try_from_scalar_value(ScalarValue::Date32(Some(42)), 3).unwrap();
        assert_eq!(ConcreteDataType::date_datatype(), vector.data_type());
        assert_eq!(3, vector.len());
        for i in 0..vector.len() {
            assert_eq!(Value::Date(Date::new(42)), vector.get(i));
        }
    }

    #[test]
    fn test_try_from_scalar_duration_value() {
        let vector =
            Helper::try_from_scalar_value(ScalarValue::DurationSecond(Some(42)), 3).unwrap();
        assert_eq!(
            ConcreteDataType::duration_second_datatype(),
            vector.data_type()
        );
        assert_eq!(3, vector.len());
        for i in 0..vector.len() {
            assert_eq!(
                Value::Duration(Duration::new(42, TimeUnit::Second)),
                vector.get(i)
            );
        }
    }

    #[test]
    fn test_try_from_scalar_decimal128_value() {
        let vector =
            Helper::try_from_scalar_value(ScalarValue::Decimal128(Some(42), 3, 1), 3).unwrap();
        assert_eq!(
            ConcreteDataType::decimal128_datatype(3, 1),
            vector.data_type()
        );
        assert_eq!(3, vector.len());
        for i in 0..vector.len() {
            assert_eq!(Value::Decimal128(Decimal128::new(42, 3, 1)), vector.get(i));
        }
    }

    #[test]
    fn test_try_from_list_value() {
        let value = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))],
            &ArrowDataType::Int32,
            true,
        ));
        let vector = Helper::try_from_scalar_value(value, 3).unwrap();
        assert_eq!(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int32_datatype())),
            vector.data_type()
        );
        assert_eq!(3, vector.len());
        for i in 0..vector.len() {
            let v = vector.get(i);
            let items = v.as_list().unwrap().unwrap().items();
            assert_eq!(vec![Value::Int32(1), Value::Int32(2)], items);
        }
    }

    #[test]
    fn test_try_from_scalar_value_materializes_values() {
        let vector = Helper::try_from_scalar_value(ScalarValue::Int32(Some(42)), 4).unwrap();
        assert_eq!(ConcreteDataType::int32_datatype(), vector.data_type());
        assert_eq!(4, vector.len());
        assert_eq!(0, vector.null_count());
        for i in 0..vector.len() {
            assert_eq!(Value::Int32(42), vector.get(i));
        }

        let empty = Helper::try_from_scalar_value(ScalarValue::Int32(Some(42)), 0).unwrap();
        assert_eq!(ConcreteDataType::int32_datatype(), empty.data_type());
        assert!(empty.is_empty());

        let nulls = Helper::try_from_scalar_value(ScalarValue::Int32(None), 3).unwrap();
        assert_eq!(3, nulls.len());
        assert_eq!(3, nulls.null_count());
        for i in 0..nulls.len() {
            assert_eq!(Value::Null, nulls.get(i));
        }
    }

    #[test]
    fn test_try_from_scalar_struct_value() {
        let fields = Fields::from(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, true),
        ]);
        let value = ScalarValue::Struct(Arc::new(StructArray::new(
            fields.clone(),
            vec![
                ScalarValue::Int32(Some(7)).to_array().unwrap(),
                ScalarValue::Utf8(Some("greptime".to_string()))
                    .to_array()
                    .unwrap(),
            ],
            None,
        )));

        let vector = Helper::try_from_scalar_value(value, 3).unwrap();
        assert_eq!(
            ConcreteDataType::struct_datatype(StructType::from(&fields)),
            vector.data_type()
        );
        assert_eq!(3, vector.len());
        for i in 0..vector.len() {
            let Value::Struct(value) = vector.get(i) else {
                panic!("expected struct value");
            };
            assert_eq!(
                &[Value::Int32(7), Value::String("greptime".into())],
                value.items()
            );
        }

        let null = ScalarStructBuilder::new_null(fields);
        let vector = Helper::try_from_scalar_value(null, 2).unwrap();
        assert_eq!(2, vector.len());
        assert_eq!(2, vector.null_count());
        assert_eq!(Value::Null, vector.get(0));
        assert_eq!(Value::Null, vector.get(1));
    }

    #[test]
    fn test_try_from_scalar_value_normalizes_arrow_types() {
        let string =
            Helper::try_from_scalar_value(ScalarValue::LargeUtf8(Some("greptime".to_string())), 2)
                .unwrap();
        assert_eq!(ConcreteDataType::string_datatype(), string.data_type());
        assert_eq!(&ArrowDataType::Utf8, string.to_arrow_array().data_type());

        let binary =
            Helper::try_from_scalar_value(ScalarValue::FixedSizeBinary(2, Some(vec![1, 2])), 2)
                .unwrap();
        assert_eq!(ConcreteDataType::binary_datatype(), binary.data_type());
        assert_eq!(&ArrowDataType::Binary, binary.to_arrow_array().data_type());

        let timestamp = Helper::try_from_scalar_value(
            ScalarValue::TimestampMillisecond(Some(42), Some("UTC".into())),
            2,
        )
        .unwrap();
        assert_eq!(
            &ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            timestamp.to_arrow_array().data_type()
        );
    }

    #[test]
    fn test_like_utf8() {
        fn assert_vector(expected: Vec<&str>, actual: &VectorRef) {
            let actual = actual.as_any().downcast_ref::<StringVector>().unwrap();
            assert_eq!(*actual, StringVector::from(expected));
        }

        let names: Vec<String> = vec!["greptime", "hello", "public", "world"]
            .into_iter()
            .map(|x| x.to_string())
            .collect();

        let ret = Helper::like_utf8(names.clone(), "%ll%").unwrap();
        assert_vector(vec!["hello"], &ret);

        let ret = Helper::like_utf8(names.clone(), "%time").unwrap();
        assert_vector(vec!["greptime"], &ret);

        let ret = Helper::like_utf8(names.clone(), "%ld").unwrap();
        assert_vector(vec!["world"], &ret);

        let ret = Helper::like_utf8(names, "%").unwrap();
        assert_vector(vec!["greptime", "hello", "public", "world"], &ret);
    }

    #[test]
    fn test_like_utf8_filter() {
        fn assert_vector(expected: Vec<&str>, actual: &VectorRef) {
            let actual = actual.as_any().downcast_ref::<StringVector>().unwrap();
            assert_eq!(*actual, StringVector::from(expected));
        }

        fn assert_filter(array: Vec<String>, s: &str, expected_filter: &BooleanVector) {
            let array = StringArray::from(array);
            let s = StringArray::new_scalar(s);
            let actual_filter = comparison::like(&array, &s).unwrap();
            assert_eq!(BooleanVector::from(actual_filter), *expected_filter);
        }

        let names: Vec<String> = vec!["greptime", "timeseries", "cloud", "database"]
            .into_iter()
            .map(|x| x.to_string())
            .collect();

        let (table, filter) = Helper::like_utf8_filter(names.clone(), "%ti%").unwrap();
        assert_vector(vec!["greptime", "timeseries"], &table);
        assert_filter(names.clone(), "%ti%", &filter);

        let (tables, filter) = Helper::like_utf8_filter(names.clone(), "%lou").unwrap();
        assert_vector(vec![], &tables);
        assert_filter(names.clone(), "%lou", &filter);

        let (tables, filter) = Helper::like_utf8_filter(names.clone(), "%d%").unwrap();
        assert_vector(vec!["cloud", "database"], &tables);
        assert_filter(names.clone(), "%d%", &filter);
    }

    fn check_try_into_vector(array: impl Array + 'static) {
        let array: ArrayRef = Arc::new(array);
        let vector = Helper::try_into_vector(array.clone()).unwrap();
        assert_eq!(&array, &vector.to_arrow_array());
    }

    #[test]
    fn test_try_into_vector() {
        check_try_into_vector(NullArray::new(2));
        check_try_into_vector(BooleanArray::from(vec![true, false]));
        check_try_into_vector(Int8Array::from(vec![1, 2, 3]));
        check_try_into_vector(Int16Array::from(vec![1, 2, 3]));
        check_try_into_vector(Int32Array::from(vec![1, 2, 3]));
        check_try_into_vector(Int64Array::from(vec![1, 2, 3]));
        check_try_into_vector(UInt8Array::from(vec![1, 2, 3]));
        check_try_into_vector(UInt16Array::from(vec![1, 2, 3]));
        check_try_into_vector(UInt32Array::from(vec![1, 2, 3]));
        check_try_into_vector(UInt64Array::from(vec![1, 2, 3]));
        check_try_into_vector(Float32Array::from(vec![1.0, 2.0, 3.0]));
        check_try_into_vector(Float64Array::from(vec![1.0, 2.0, 3.0]));
        check_try_into_vector(StringArray::from(vec!["hello", "world"]));
        check_try_into_vector(Date32Array::from(vec![1, 2, 3]));
        let data = vec![None, Some(vec![Some(6), Some(7)])];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
        check_try_into_vector(list_array);
        check_try_into_vector(TimestampSecondArray::from(vec![1, 2, 3]));
        check_try_into_vector(TimestampMillisecondArray::from(vec![1, 2, 3]));
        check_try_into_vector(TimestampMicrosecondArray::from(vec![1, 2, 3]));
        check_try_into_vector(TimestampNanosecondArray::from(vec![1, 2, 3]));
        check_try_into_vector(Time32SecondArray::from(vec![1, 2, 3]));
        check_try_into_vector(Time32MillisecondArray::from(vec![1, 2, 3]));
        check_try_into_vector(Time64MicrosecondArray::from(vec![1, 2, 3]));
        check_try_into_vector(Time64NanosecondArray::from(vec![1, 2, 3]));

        // Test dictionary arrays with different key types
        let values = StringArray::from_iter_values(["a", "b", "c"]);

        // Test Int8 keys
        let keys = Int8Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap());
        Helper::try_into_vector(array).unwrap();

        // Test Int16 keys
        let keys = Int16Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap());
        Helper::try_into_vector(array).unwrap();

        // Test Int32 keys
        let keys = Int32Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap());
        Helper::try_into_vector(array).unwrap();

        // Test Int64 keys
        let keys = Int64Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap());
        Helper::try_into_vector(array).unwrap();

        // Test UInt8 keys
        let keys = UInt8Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap());
        Helper::try_into_vector(array).unwrap();

        // Test UInt16 keys
        let keys = UInt16Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap());
        Helper::try_into_vector(array).unwrap();

        // Test UInt32 keys
        let keys = UInt32Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap());
        Helper::try_into_vector(array).unwrap();

        // Test UInt64 keys
        let keys = UInt64Array::from_iter_values([0, 0, 1, 2]);
        let array: ArrayRef = Arc::new(DictionaryArray::try_new(keys, Arc::new(values)).unwrap());
        Helper::try_into_vector(array).unwrap();
    }

    #[test]
    fn test_try_binary_array_into_vector() {
        let input_vec: Vec<&[u8]> = vec!["hello".as_bytes(), "world".as_bytes()];
        let assertion_vector = BinaryVector::from(input_vec.clone());

        let input_arrays: Vec<ArrayRef> = vec![
            Arc::new(LargeBinaryArray::from(input_vec.clone())) as ArrayRef,
            Arc::new(BinaryArray::from(input_vec.clone())) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::new(
                5,
                Buffer::from_vec("helloworld".as_bytes().to_vec()),
                None,
            )) as ArrayRef,
        ];

        for input_array in input_arrays {
            let vector = Helper::try_into_vector(input_array).unwrap();

            assert_eq!(2, vector.len());
            assert_eq!(0, vector.null_count());

            let output_arrow_array: ArrayRef = vector.to_arrow_array();
            assert_eq!(&DataType::Binary, output_arrow_array.data_type());
            assert_eq!(&assertion_vector.to_arrow_array(), &output_arrow_array);
        }
    }

    #[test]
    fn test_large_string_array_into_vector() {
        let input_vec = vec!["a", "b"];
        let assertion_array = LargeStringArray::from(input_vec.clone());

        let large_string_array: ArrayRef = Arc::new(LargeStringArray::from(input_vec));
        let vector = Helper::try_into_vector(large_string_array).unwrap();
        assert_eq!(2, vector.len());
        assert_eq!(0, vector.null_count());

        let output_arrow_array: LargeStringArray = vector
            .to_arrow_array()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .clone();
        assert_eq!(&assertion_array, &output_arrow_array);
    }

    #[test]
    fn test_try_from_scalar_time_value() {
        let vector = Helper::try_from_scalar_value(ScalarValue::Time32Second(Some(42)), 3).unwrap();
        assert_eq!(ConcreteDataType::time_second_datatype(), vector.data_type());
        assert_eq!(3, vector.len());
        for i in 0..vector.len() {
            assert_eq!(Value::Time(Time::new_second(42)), vector.get(i));
        }
    }

    #[test]
    fn test_try_from_scalar_interval_value() {
        let vector = Helper::try_from_scalar_value(
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(1, 1, 2000))),
            3,
        )
        .unwrap();

        assert_eq!(
            ConcreteDataType::interval_month_day_nano_datatype(),
            vector.data_type()
        );
        assert_eq!(3, vector.len());
        for i in 0..vector.len() {
            assert_eq!(
                Value::IntervalMonthDayNano(IntervalMonthDayNano::new(1, 1, 2000).into()),
                vector.get(i)
            );
        }
    }

    fn check_try_from_row_to_vector(row: Vec<Value>, dt: &ConcreteDataType) {
        let vector = Helper::try_from_row_into_vector(&row, dt).unwrap();
        for (i, item) in row.iter().enumerate().take(vector.len()) {
            assert_eq!(*item, vector.get(i));
        }
    }

    fn check_into_and_from(array: impl Array + 'static) {
        let array: ArrayRef = Arc::new(array);
        let vector = Helper::try_into_vector(array.clone()).unwrap();
        assert_eq!(&array, &vector.to_arrow_array());
        let row: Vec<Value> = (0..array.len()).map(|i| vector.get(i)).collect();
        let dt = vector.data_type();
        check_try_from_row_to_vector(row, &dt);
    }

    #[test]
    fn test_try_from_row_to_vector() {
        check_into_and_from(NullArray::new(2));
        check_into_and_from(BooleanArray::from(vec![true, false]));
        check_into_and_from(Int8Array::from(vec![1, 2, 3]));
        check_into_and_from(Int16Array::from(vec![1, 2, 3]));
        check_into_and_from(Int32Array::from(vec![1, 2, 3]));
        check_into_and_from(Int64Array::from(vec![1, 2, 3]));
        check_into_and_from(UInt8Array::from(vec![1, 2, 3]));
        check_into_and_from(UInt16Array::from(vec![1, 2, 3]));
        check_into_and_from(UInt32Array::from(vec![1, 2, 3]));
        check_into_and_from(UInt64Array::from(vec![1, 2, 3]));
        check_into_and_from(Float32Array::from(vec![1.0, 2.0, 3.0]));
        check_into_and_from(Float64Array::from(vec![1.0, 2.0, 3.0]));
        check_into_and_from(StringArray::from(vec!["hello", "world"]));
        check_into_and_from(Date32Array::from(vec![1, 2, 3]));

        check_into_and_from(TimestampSecondArray::from(vec![1, 2, 3]));
        check_into_and_from(TimestampMillisecondArray::from(vec![1, 2, 3]));
        check_into_and_from(TimestampMicrosecondArray::from(vec![1, 2, 3]));
        check_into_and_from(TimestampNanosecondArray::from(vec![1, 2, 3]));
        check_into_and_from(Time32SecondArray::from(vec![1, 2, 3]));
        check_into_and_from(Time32MillisecondArray::from(vec![1, 2, 3]));
        check_into_and_from(Time64MicrosecondArray::from(vec![1, 2, 3]));
        check_into_and_from(Time64NanosecondArray::from(vec![1, 2, 3]));
    }
}
