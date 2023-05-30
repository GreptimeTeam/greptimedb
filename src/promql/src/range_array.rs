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

//！An extended "array" based on [DictionaryArray].

use std::sync::Arc;

use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::Field;
use datatypes::arrow::array::{Array, ArrayData, ArrayRef, DictionaryArray, Int64Array};
use datatypes::arrow::datatypes::{DataType, Int64Type};
use snafu::{ensure, OptionExt};

use crate::error::{EmptyRangeSnafu, IllegalRangeSnafu, Result};

pub type RangeTuple = (u32, u32);

/// An compound logical "array" type. Represent serval ranges (slices) of one array.
/// It's useful to use case like compute sliding window, or range selector from promql.
///
/// It's build on top of Arrow's [DictionaryArray]. [DictionaryArray] contains two
/// sub-arrays, one for dictionary key and another for dictionary value. Both of them
/// can be arbitrary types, but here the key array is fixed to u32 type.
///
/// ```text
///             │ ┌─────┬─────┬─────┬─────┐
///      Two    │ │ i64 │ i64 │ i64 │ i64 │      Keys
///    Arrays   │ └─────┴─────┴─────┴─────┘     (Fixed to i64)
///      In     │
///   Arrow's   │ ┌────────────────────────────┐
/// Dictionary  │ │                            │ Values
///    Array    │ └────────────────────────────┘(Any Type)
/// ```
///
/// Because the i64 key array is reinterpreted into two u32 for offset and length
/// in [RangeArray] to represent a "range":
///
/// ```text
/// 63            32│31             0
/// ┌───────────────┼───────────────┐
/// │  offset (u32) │  length (u32) │
/// └───────────────┼───────────────┘
/// ```
///
/// Then the [DictionaryArray] can be expanded to several ranges like this:
///
/// ```text
/// Keys
/// ┌───────┬───────┬───────┐      ┌───────┐
/// │ (0,2) │ (1,2) │ (2,2) │ ─┐   │[A,B,C]│ values.slice(0,2)
/// └───────┴───────┴───────┘  │   ├───────┤
///  Values                    ├─► │[B,C,D]│ values.slice(1,2)
/// ┌───┬───┬───┬───┬───┐      │   ├───────┤
/// │ A │ B │ C │ D │ E │     ─┘   │[C,D,E]│ values.slice(2,2)
/// └───┴───┴───┴───┴───┘          └───────┘
/// ```
pub struct RangeArray {
    array: DictionaryArray<Int64Type>,
}

impl RangeArray {
    pub const fn key_type() -> DataType {
        DataType::Int64
    }

    pub fn value_type(&self) -> DataType {
        self.array.value_type()
    }

    pub fn try_new(dict: DictionaryArray<Int64Type>) -> Result<Self> {
        let ranges_iter = dict
            .keys()
            .iter()
            .map(|compound_key| compound_key.map(unpack))
            .collect::<Option<Vec<_>>>()
            .context(EmptyRangeSnafu)?;
        Self::check_ranges(dict.values().len(), ranges_iter)?;

        Ok(Self { array: dict })
    }

    pub fn from_ranges<R>(values: ArrayRef, ranges: R) -> Result<Self>
    where
        R: IntoIterator<Item = RangeTuple> + Clone,
    {
        Self::check_ranges(values.len(), ranges.clone())?;

        unsafe { Ok(Self::from_ranges_unchecked(values, ranges)) }
    }

    /// Construct [RangeArray] from given range without checking its validity.
    ///
    /// # Safety
    ///
    /// Caller should ensure the given range are valid. Otherwise use [`from_ranges`]
    /// instead.
    ///
    /// [`from_ranges`]: crate::range_array::RangeArray#method.from_ranges
    pub unsafe fn from_ranges_unchecked<R>(values: ArrayRef, ranges: R) -> Self
    where
        R: IntoIterator<Item = RangeTuple>,
    {
        let key_array = Int64Array::from_iter(
            ranges
                .into_iter()
                .map(|(offset, length)| pack(offset, length)),
        );

        // Build from ArrayData to bypass the "offset" checker. Because
        // we are not using "keys" as-is.
        // This paragraph is copied from arrow-rs dictionary_array.rs `try_new()`.
        let mut data = ArrayData::builder(DataType::Dictionary(
            Box::new(Self::key_type()),
            Box::new(values.data_type().clone()),
        ))
        .len(key_array.len())
        .add_buffer(key_array.to_data().buffers()[0].clone())
        .add_child_data(values.to_data());
        match key_array.to_data().nulls() {
            Some(buffer) if key_array.to_data().null_count() > 0 => {
                data = data
                    .nulls(Some(buffer.clone()))
                    .null_count(key_array.to_data().null_count());
            }
            _ => data = data.null_count(0),
        }
        let array_data = unsafe { data.build_unchecked() };

        Self {
            array: array_data.into(),
        }
    }

    pub fn len(&self) -> usize {
        self.array.keys().len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.keys().is_empty()
    }

    pub fn get(&self, index: usize) -> Option<ArrayRef> {
        if index >= self.len() {
            return None;
        }

        let compound_key = self.array.keys().value(index);
        let (offset, length) = unpack(compound_key);
        let array = self.array.values().slice(offset as usize, length as usize);

        Some(array)
    }

    /// Return the underlying Arrow's [DictionaryArray]. Notes the dictionary array might be
    /// invalid from Arrow's definition. Be care if try to access this dictionary through
    /// Arrow's API.
    pub fn into_dict(self) -> DictionaryArray<Int64Type> {
        self.array
    }

    fn check_ranges<R>(value_len: usize, ranges: R) -> Result<()>
    where
        R: IntoIterator<Item = RangeTuple>,
    {
        for (offset, length) in ranges.into_iter() {
            ensure!(
                offset as usize + length as usize <= value_len,
                IllegalRangeSnafu {
                    offset,
                    length,
                    len: value_len
                }
            );
        }
        Ok(())
    }

    /// Change the field's datatype to the type after processed by [RangeArray].
    /// Like `Utf8` will become `Dictionary<Int64, Utf8>`.
    pub fn convert_field(field: &Field) -> Field {
        let value_type = Box::new(field.data_type().clone());
        Field::new(
            field.name(),
            Self::convert_data_type(*value_type),
            field.is_nullable(),
        )
    }

    /// Build datatype of wrapped [RangeArray] on given value type.
    pub fn convert_data_type(value_type: DataType) -> DataType {
        DataType::Dictionary(Box::new(Self::key_type()), Box::new(value_type))
    }

    pub fn values(&self) -> &ArrayRef {
        self.array.values()
    }

    pub fn ranges(&self) -> impl Iterator<Item = Option<RangeTuple>> + '_ {
        self.array
            .keys()
            .into_iter()
            .map(|compound| compound.map(unpack))
    }
}

impl Array for RangeArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_data(self) -> ArrayData {
        self.array.into_data()
    }

    fn to_data(&self) -> ArrayData {
        self.array.to_data()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.array.slice(offset, length))
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.array.nulls()
    }

    fn data_type(&self) -> &DataType {
        self.array.data_type()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn offset(&self) -> usize {
        self.array.offset()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.array.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.array.get_array_memory_size()
    }
}

impl std::fmt::Debug for RangeArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ranges = self
            .array
            .keys()
            .iter()
            .map(|compound_key| {
                compound_key.map(|key| {
                    let (offset, length) = unpack(key);
                    offset..(offset + length)
                })
            })
            .collect::<Vec<_>>();
        f.debug_struct("RangeArray")
            .field("base array", self.array.values())
            .field("ranges", &ranges)
            .finish()
    }
}

// util functions

fn pack(offset: u32, length: u32) -> i64 {
    bytemuck::cast::<[u32; 2], i64>([offset, length])
}

fn unpack(compound: i64) -> (u32, u32) {
    let [offset, length] = bytemuck::cast::<i64, [u32; 2]>(compound);
    (offset, length)
}

#[cfg(test)]
mod test {
    use std::fmt::Write;
    use std::sync::Arc;

    use datatypes::arrow::array::UInt64Array;

    use super::*;

    fn expand_format(range_array: &RangeArray) -> String {
        let mut result = String::new();
        for i in 0..range_array.len() {
            writeln!(result, "{:?}", range_array.get(i)).unwrap();
        }
        result
    }

    #[test]
    fn construct_from_ranges() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges = [(0, 2), (0, 5), (1, 1), (3, 3), (8, 1), (9, 0)];

        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        assert_eq!(range_array.len(), 6);

        let expected = String::from(
            "Some(PrimitiveArray<UInt64>\
            \n[\
            \n  1,\
            \n  2,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  1,\
            \n  2,\
            \n  3,\
            \n  4,\
            \n  5,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  2,\
            \n])\
            \
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  4,\
            \n  5,\
            \n  6,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  9,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n])\
            \n",
        );

        let formatted = expand_format(&range_array);
        assert_eq!(formatted, expected);
    }

    #[test]
    fn illegal_range() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges = [(9, 1)];
        assert!(RangeArray::from_ranges(values_array, ranges).is_err());
    }

    #[test]
    fn dict_array_round_trip() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges = [(0, 4), (1, 4), (2, 4), (3, 4), (4, 4), (5, 4)];
        let expected = String::from(
            "Some(PrimitiveArray<UInt64>\
            \n[\
            \n  1,\
            \n  2,\
            \n  3,\
            \n  4,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  2,\
            \n  3,\
            \n  4,\
            \n  5,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  3,\
            \n  4,\
            \n  5,\
            \n  6,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  4,\
            \n  5,\
            \n  6,\
            \n  7,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  5,\
            \n  6,\
            \n  7,\
            \n  8,\
            \n])\
            \nSome(PrimitiveArray<UInt64>\
            \n[\
            \n  6,\
            \n  7,\
            \n  8,\
            \n  9,\
            \n])\
            \n",
        );

        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        assert_eq!(range_array.len(), 6);
        let formatted = expand_format(&range_array);
        assert_eq!(formatted, expected);

        // this dict array is invalid from Arrow's definition
        let dict_array = range_array.into_dict();
        let rounded_range_array = RangeArray::try_new(dict_array).unwrap();
        let formatted = expand_format(&rounded_range_array);
        assert_eq!(formatted, expected);
    }

    #[test]
    fn empty_range_array() {
        let values_array = Arc::new(UInt64Array::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let ranges = [];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        assert!(range_array.is_empty());
    }
}
