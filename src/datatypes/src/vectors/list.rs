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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanBufferBuilder, Int32BufferBuilder, ListArray,
};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::DataType as ArrowDataType;
use serde_json::Value as JsonValue;

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::Result;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::ListType;
use crate::value::{ListValue, ListValueRef, Value, ValueRef};
use crate::vectors::{self, Helper, MutableVector, Validity, Vector, VectorRef};

/// Vector of Lists, basically backed by Arrow's `ListArray`.
#[derive(Debug, PartialEq)]
pub struct ListVector {
    array: ListArray,
    /// The datatype of the items in the list.
    item_type: ConcreteDataType,
}

impl ListVector {
    /// Iterate elements as [VectorRef].
    pub fn values_iter(&self) -> impl Iterator<Item = Result<Option<VectorRef>>> + '_ {
        self.array
            .iter()
            .map(|value_opt| value_opt.map(Helper::try_into_vector).transpose())
    }

    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl Vector for ListVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::List(ListType::new(self.item_type.clone()))
    }

    fn vector_type_name(&self) -> String {
        "ListVector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        Arc::new(self.array.clone())
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        Box::new(self.array.clone())
    }

    fn validity(&self) -> Validity {
        vectors::impl_validity_for_vector!(self.array)
    }

    fn memory_size(&self) -> usize {
        self.array.get_buffer_memory_size()
    }

    fn null_count(&self) -> usize {
        self.array.null_count()
    }

    fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        Arc::new(Self {
            array: self.array.slice(offset, length),
            item_type: self.item_type.clone(),
        })
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        let array = &self.array.value(index);
        let vector = Helper::try_into_vector(array).unwrap_or_else(|_| {
            panic!(
                "arrow array with datatype {:?} cannot converted to our vector",
                array.data_type()
            )
        });
        let values = (0..vector.len())
            .map(|i| vector.get(i))
            .collect::<Vec<Value>>();
        Value::List(ListValue::new(
            Some(Box::new(values)),
            self.item_type.clone(),
        ))
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        ValueRef::List(ListValueRef::Indexed {
            vector: self,
            idx: index,
        })
    }
}

impl Serializable for ListVector {
    fn serialize_to_json(&self) -> Result<Vec<JsonValue>> {
        self.array
            .iter()
            .map(|v| match v {
                None => Ok(JsonValue::Null),
                Some(v) => Helper::try_into_vector(v)
                    .and_then(|v| v.serialize_to_json())
                    .map(JsonValue::Array),
            })
            .collect()
    }
}

impl From<ListArray> for ListVector {
    fn from(array: ListArray) -> Self {
        let item_type = ConcreteDataType::from_arrow_type(match array.data_type() {
            ArrowDataType::List(field) => field.data_type(),
            other => panic!("Try to create ListVector from an arrow array with type {other:?}"),
        });
        Self { array, item_type }
    }
}

vectors::impl_try_from_arrow_array_for_vector!(ListArray, ListVector);

pub struct ListIter<'a> {
    vector: &'a ListVector,
    idx: usize,
}

impl<'a> ListIter<'a> {
    fn new(vector: &'a ListVector) -> ListIter {
        ListIter { vector, idx: 0 }
    }
}

impl<'a> Iterator for ListIter<'a> {
    type Item = Option<ListValueRef<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.vector.len() {
            return None;
        }

        let idx = self.idx;
        self.idx += 1;

        if self.vector.is_null(idx) {
            return Some(None);
        }

        Some(Some(ListValueRef::Indexed {
            vector: self.vector,
            idx,
        }))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.vector.len(), Some(self.vector.len()))
    }
}

impl ScalarVector for ListVector {
    type OwnedItem = ListValue;
    type RefItem<'a> = ListValueRef<'a>;
    type Iter<'a> = ListIter<'a>;
    type Builder = ListVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(ListValueRef::Indexed { vector: self, idx })
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        ListIter::new(self)
    }
}

// Ports from arrow's GenericListBuilder.
// See https://github.com/apache/arrow-rs/blob/94565bca99b5d9932a3e9a8e094aaf4e4384b1e5/arrow-array/src/builder/generic_list_builder.rs
/// [ListVector] builder.
pub struct ListVectorBuilder {
    item_type: ConcreteDataType,
    offsets_builder: Int32BufferBuilder,
    null_buffer_builder: NullBufferBuilder,
    values_builder: Box<dyn MutableVector>,
}

impl ListVectorBuilder {
    /// Creates a new [`ListVectorBuilder`]. `item_type` is the data type of the list item, `capacity`
    /// is the number of items to pre-allocate space for in this builder.
    pub fn with_type_capacity(item_type: ConcreteDataType, capacity: usize) -> ListVectorBuilder {
        let mut offsets_builder = Int32BufferBuilder::new(capacity + 1);
        offsets_builder.append(0);
        // The actual required capacity might be greater than the capacity of the `ListVector`
        // if the child vector has more than one element.
        let values_builder = item_type.create_mutable_vector(capacity);

        ListVectorBuilder {
            item_type,
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            values_builder,
        }
    }

    /// Finish the current variable-length list vector slot.
    fn finish_list(&mut self, is_valid: bool) {
        self.offsets_builder
            .append(i32::try_from(self.values_builder.len()).unwrap());
        self.null_buffer_builder.append(is_valid);
    }

    fn push_list_value(&mut self, list_value: &ListValue) -> Result<()> {
        if let Some(items) = list_value.items() {
            for item in &**items {
                self.values_builder
                    .try_push_value_ref(item.as_value_ref())?;
            }
        }

        self.finish_list(true);
        Ok(())
    }
}

impl MutableVector for ListVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::list_datatype(self.item_type.clone())
    }

    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_vector(&mut self) -> VectorRef {
        Arc::new(self.finish())
    }

    fn to_vector_cloned(&self) -> VectorRef {
        Arc::new(self.finish_cloned())
    }

    fn try_push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        if let Some(list_ref) = value.as_list()? {
            match list_ref {
                ListValueRef::Indexed { vector, idx } => match vector.get(idx).as_list()? {
                    Some(list_value) => self.push_list_value(list_value)?,
                    None => self.push_null(),
                },
                ListValueRef::Ref { val } => self.push_list_value(val)?,
            }
        } else {
            self.push_null();
        }

        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        for idx in offset..offset + length {
            let value = vector.get_ref(idx);
            self.try_push_value_ref(value)?;
        }

        Ok(())
    }

    fn push_null(&mut self) {
        self.finish_list(false);
    }
}

impl ScalarVectorBuilder for ListVectorBuilder {
    type VectorType = ListVector;

    fn with_capacity(_capacity: usize) -> Self {
        panic!("Must use ListVectorBuilder::with_type_capacity()");
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        // We expect the input ListValue has the same inner type as the builder when using
        // push(), so just panic if `push_value_ref()` returns error, which indicate an
        // invalid input value type.
        self.try_push_value_ref(value.into()).unwrap_or_else(|e| {
            panic!(
                "Failed to push value, expect value type {:?}, err:{}",
                self.item_type, e
            );
        });
    }

    fn finish(&mut self) -> Self::VectorType {
        let len = self.len();
        let values_vector = self.values_builder.to_vector();
        let values_arr = values_vector.to_arrow_array();
        let values_data = values_arr.to_data();

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.null_buffer_builder.finish();
        // Re-initialize the offsets_builder.
        self.offsets_builder.append(0);
        let data_type = ConcreteDataType::list_datatype(self.item_type.clone()).as_arrow_type();
        let array_data_builder = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(values_data)
            .null_bit_buffer(null_bit_buffer);

        let array_data = unsafe { array_data_builder.build_unchecked() };
        let array = ListArray::from(array_data);

        ListVector {
            array,
            item_type: self.item_type.clone(),
        }
    }

    // Port from https://github.com/apache/arrow-rs/blob/ef6932f31e243d8545e097569653c8d3f1365b4d/arrow-array/src/builder/generic_list_builder.rs#L302-L325
    fn finish_cloned(&self) -> Self::VectorType {
        let len = self.len();
        let values_vector = self.values_builder.to_vector_cloned();
        let values_arr = values_vector.to_arrow_array();
        let values_data = values_arr.to_data();

        let offset_buffer = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        let nulls = self.null_buffer_builder.finish_cloned();

        let data_type = ConcreteDataType::list_datatype(self.item_type.clone()).as_arrow_type();
        let array_data_builder = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(values_data)
            .nulls(nulls);

        let array_data = unsafe { array_data_builder.build_unchecked() };
        let array = ListArray::from(array_data);

        ListVector {
            array,
            item_type: self.item_type.clone(),
        }
    }
}

// Ports from https://github.com/apache/arrow-rs/blob/94565bca99b5d9932a3e9a8e094aaf4e4384b1e5/arrow-array/src/builder/null_buffer_builder.rs
/// Builder for creating the null bit buffer.
/// This builder only materializes the buffer when we append `false`.
/// If you only append `true`s to the builder, what you get will be
/// `None` when calling [`finish`](#method.finish).
/// This optimization is **very** important for the performance.
#[derive(Debug)]
struct NullBufferBuilder {
    bitmap_builder: Option<BooleanBufferBuilder>,
    /// Store the length of the buffer before materializing.
    len: usize,
    capacity: usize,
}

impl NullBufferBuilder {
    /// Creates a new empty builder.
    /// `capacity` is the number of bits in the null buffer.
    fn new(capacity: usize) -> Self {
        Self {
            bitmap_builder: None,
            len: 0,
            capacity,
        }
    }

    fn len(&self) -> usize {
        if let Some(b) = &self.bitmap_builder {
            b.len()
        } else {
            self.len
        }
    }

    /// Appends a `true` into the builder
    /// to indicate that this item is not null.
    #[inline]
    fn append_non_null(&mut self) {
        if let Some(buf) = self.bitmap_builder.as_mut() {
            buf.append(true)
        } else {
            self.len += 1;
        }
    }

    /// Appends a `false` into the builder
    /// to indicate that this item is null.
    #[inline]
    fn append_null(&mut self) {
        self.materialize_if_needed();
        self.bitmap_builder.as_mut().unwrap().append(false);
    }

    /// Appends a boolean value into the builder.
    #[inline]
    fn append(&mut self, not_null: bool) {
        if not_null {
            self.append_non_null()
        } else {
            self.append_null()
        }
    }

    /// Builds the null buffer and resets the builder.
    /// Returns `None` if the builder only contains `true`s.
    fn finish(&mut self) -> Option<Buffer> {
        let buf = self.bitmap_builder.take().map(Into::into);
        self.len = 0;
        buf
    }

    /// Builds the [NullBuffer] without resetting the builder.
    fn finish_cloned(&self) -> Option<NullBuffer> {
        let buffer = self.bitmap_builder.as_ref()?.finish_cloned();
        Some(NullBuffer::new(buffer))
    }

    #[inline]
    fn materialize_if_needed(&mut self) {
        if self.bitmap_builder.is_none() {
            self.materialize()
        }
    }

    #[cold]
    fn materialize(&mut self) {
        if self.bitmap_builder.is_none() {
            let mut b = BooleanBufferBuilder::new(self.len.max(self.capacity));
            b.append_n(self.len, true);
            self.bitmap_builder = Some(b);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use arrow::array::{Int32Array, Int32Builder, ListBuilder};
    use serde_json::json;

    use super::*;
    use crate::scalars::ScalarRef;
    use crate::types::ListType;
    use crate::vectors::Int32Vector;

    pub fn new_list_vector(data: &[Option<Vec<Option<i32>>>]) -> ListVector {
        let mut builder =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::int32_datatype(), 8);
        for vec_opt in data {
            if let Some(vec) = vec_opt {
                let values = vec.iter().map(|v| Value::from(*v)).collect();
                let values = Some(Box::new(values));
                let list_value = ListValue::new(values, ConcreteDataType::int32_datatype());

                builder.push(Some(ListValueRef::Ref { val: &list_value }));
            } else {
                builder.push(None);
            }
        }

        builder.finish()
    }

    fn new_list_array(data: &[Option<Vec<Option<i32>>>]) -> ListArray {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for vec_opt in data {
            if let Some(vec) = vec_opt {
                for value_opt in vec {
                    builder.values().append_option(*value_opt);
                }

                builder.append(true);
            } else {
                builder.append(false);
            }
        }

        builder.finish()
    }

    #[test]
    fn test_list_vector() {
        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let list_vector = new_list_vector(&data);

        assert_eq!(
            ConcreteDataType::List(ListType::new(ConcreteDataType::int32_datatype())),
            list_vector.data_type()
        );
        assert_eq!("ListVector", list_vector.vector_type_name());
        assert_eq!(3, list_vector.len());
        assert!(!list_vector.is_null(0));
        assert!(list_vector.is_null(1));
        assert!(!list_vector.is_null(2));

        let arrow_array = new_list_array(&data);
        assert_eq!(
            arrow_array,
            *list_vector
                .to_arrow_array()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
        );
        let validity = list_vector.validity();
        assert!(!validity.is_all_null());
        assert!(!validity.is_all_valid());
        assert!(validity.is_set(0));
        assert!(!validity.is_set(1));
        assert!(validity.is_set(2));
        assert_eq!(256, list_vector.memory_size());

        let slice = list_vector.slice(0, 2).to_arrow_array();
        let sliced_array = slice.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            Int32Array::from_iter_values([1, 2, 3]),
            *sliced_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
        );
        assert!(sliced_array.is_null(1));

        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(1),
                    Value::Int32(2),
                    Value::Int32(3)
                ])),
                ConcreteDataType::int32_datatype()
            )),
            list_vector.get(0)
        );
        let value_ref = list_vector.get_ref(0);
        assert!(matches!(
            value_ref,
            ValueRef::List(ListValueRef::Indexed { .. })
        ));
        let value_ref = list_vector.get_ref(1);
        if let ValueRef::List(ListValueRef::Indexed { idx, .. }) = value_ref {
            assert_eq!(1, idx);
        } else {
            unreachable!()
        }
        assert_eq!(Value::Null, list_vector.get(1));
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(4),
                    Value::Null,
                    Value::Int32(6)
                ])),
                ConcreteDataType::int32_datatype()
            )),
            list_vector.get(2)
        );
    }

    #[test]
    fn test_from_arrow_array() {
        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let arrow_array = new_list_array(&data);
        let array_ref: ArrayRef = Arc::new(arrow_array);
        let expect = new_list_vector(&data);

        // Test try from ArrayRef
        let list_vector = ListVector::try_from_arrow_array(array_ref).unwrap();
        assert_eq!(expect, list_vector);

        // Test from
        let arrow_array = new_list_array(&data);
        let list_vector = ListVector::from(arrow_array);
        assert_eq!(expect, list_vector);
    }

    #[test]
    fn test_iter_list_vector_values() {
        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let list_vector = new_list_vector(&data);

        assert_eq!(
            ConcreteDataType::List(ListType::new(ConcreteDataType::int32_datatype())),
            list_vector.data_type()
        );
        let mut iter = list_vector.values_iter();
        assert_eq!(
            Arc::new(Int32Vector::from_slice([1, 2, 3])) as VectorRef,
            *iter.next().unwrap().unwrap().unwrap()
        );
        assert!(iter.next().unwrap().unwrap().is_none());
        assert_eq!(
            Arc::new(Int32Vector::from(vec![Some(4), None, Some(6)])) as VectorRef,
            *iter.next().unwrap().unwrap().unwrap(),
        );
        assert!(iter.next().is_none())
    }

    #[test]
    fn test_serialize_to_json() {
        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let list_vector = new_list_vector(&data);
        assert_eq!(
            vec![json!([1, 2, 3]), json!(null), json!([4, null, 6]),],
            list_vector.serialize_to_json().unwrap()
        );
    }

    #[test]
    fn test_list_vector_builder() {
        let mut builder =
            ListType::new(ConcreteDataType::int32_datatype()).create_mutable_vector(3);
        builder.push_value_ref(ValueRef::List(ListValueRef::Ref {
            val: &ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(4),
                    Value::Null,
                    Value::Int32(6),
                ])),
                ConcreteDataType::int32_datatype(),
            ),
        }));
        assert!(builder.try_push_value_ref(ValueRef::Int32(123)).is_err());

        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(7), Some(8), None]),
        ];
        let input = new_list_vector(&data);
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice([13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(new_list_vector(&[
            Some(vec![Some(4), None, Some(6)]),
            None,
            Some(vec![Some(7), Some(8), None]),
        ]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_list_vector_for_scalar() {
        let mut builder =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::int32_datatype(), 2);
        builder.push(None);
        builder.push(Some(ListValueRef::Ref {
            val: &ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(4),
                    Value::Null,
                    Value::Int32(6),
                ])),
                ConcreteDataType::int32_datatype(),
            ),
        }));
        let vector = builder.finish();

        let expect = new_list_vector(&[None, Some(vec![Some(4), None, Some(6)])]);
        assert_eq!(expect, vector);

        assert!(vector.get_data(0).is_none());
        assert_eq!(
            ListValueRef::Indexed {
                vector: &vector,
                idx: 1
            },
            vector.get_data(1).unwrap()
        );
        assert_eq!(
            *vector.get(1).as_list().unwrap().unwrap(),
            vector.get_data(1).unwrap().to_owned_scalar()
        );

        let mut iter = vector.iter_data();
        assert!(iter.next().unwrap().is_none());
        assert_eq!(
            ListValueRef::Indexed {
                vector: &vector,
                idx: 1
            },
            iter.next().unwrap().unwrap()
        );
        assert!(iter.next().is_none());

        let mut iter = vector.iter_data();
        assert_eq!(2, iter.size_hint().0);
        assert_eq!(
            ListValueRef::Indexed {
                vector: &vector,
                idx: 1
            },
            iter.nth(1).unwrap().unwrap()
        );
    }

    #[test]
    fn test_list_vector_builder_finish_cloned() {
        let mut builder =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::int32_datatype(), 2);
        builder.push(None);
        builder.push(Some(ListValueRef::Ref {
            val: &ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(4),
                    Value::Null,
                    Value::Int32(6),
                ])),
                ConcreteDataType::int32_datatype(),
            ),
        }));
        let vector = builder.finish_cloned();
        assert_eq!(vector.len(), 2);
        assert_eq!(builder.len(), 2);
    }
}
