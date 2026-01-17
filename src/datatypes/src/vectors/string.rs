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

use arrow::array::{Array, ArrayBuilder, ArrayIter, ArrayRef};
use snafu::ResultExt;

use crate::arrow_array::{
    LargeStringArray, MutableLargeStringArray, MutableStringArray, MutableStringViewArray,
    StringArray, StringViewArray,
};
use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// Internal representation for string arrays
#[derive(Debug, PartialEq)]
enum StringArrayData {
    String(StringArray),
    LargeString(LargeStringArray),
    StringView(StringViewArray),
}

/// Vector of strings.
#[derive(Debug, PartialEq)]
pub struct StringVector {
    array: StringArrayData,
}

impl StringVector {
    pub(crate) fn as_arrow(&self) -> &dyn Array {
        match &self.array {
            StringArrayData::String(array) => array,
            StringArrayData::LargeString(array) => array,
            StringArrayData::StringView(array) => array,
        }
    }

    /// Create a StringVector from a regular StringArray
    pub fn from_string_array(array: StringArray) -> Self {
        Self {
            array: StringArrayData::String(array),
        }
    }

    /// Create a StringVector from a LargeStringArray
    pub fn from_large_string_array(array: LargeStringArray) -> Self {
        Self {
            array: StringArrayData::LargeString(array),
        }
    }

    /// Create a StringVector from a StringViewArray
    pub fn from_string_view_array(array: StringViewArray) -> Self {
        Self {
            array: StringArrayData::StringView(array),
        }
    }

    pub fn from_slice<T: AsRef<str>>(slice: &[T]) -> Self {
        Self::from_string_array(StringArray::from_iter(
            slice.iter().map(|s| Some(s.as_ref())),
        ))
    }
}

impl From<StringArray> for StringVector {
    fn from(array: StringArray) -> Self {
        Self::from_string_array(array)
    }
}

impl From<LargeStringArray> for StringVector {
    fn from(array: LargeStringArray) -> Self {
        Self::from_large_string_array(array)
    }
}

impl From<StringViewArray> for StringVector {
    fn from(array: StringViewArray) -> Self {
        Self::from_string_view_array(array)
    }
}

impl From<Vec<Option<String>>> for StringVector {
    fn from(data: Vec<Option<String>>) -> Self {
        Self::from_string_array(StringArray::from_iter(data))
    }
}

impl From<Vec<Option<&str>>> for StringVector {
    fn from(data: Vec<Option<&str>>) -> Self {
        Self::from_string_array(StringArray::from_iter(data))
    }
}

impl From<&[Option<String>]> for StringVector {
    fn from(data: &[Option<String>]) -> Self {
        Self::from_string_array(StringArray::from_iter(data))
    }
}

impl From<&[Option<&str>]> for StringVector {
    fn from(data: &[Option<&str>]) -> Self {
        Self::from_string_array(StringArray::from_iter(data))
    }
}

impl From<Vec<String>> for StringVector {
    fn from(data: Vec<String>) -> Self {
        Self::from_string_array(StringArray::from_iter(data.into_iter().map(Some)))
    }
}

impl From<Vec<&str>> for StringVector {
    fn from(data: Vec<&str>) -> Self {
        Self::from_string_array(StringArray::from_iter(data.into_iter().map(Some)))
    }
}

impl Vector for StringVector {
    fn data_type(&self) -> ConcreteDataType {
        match &self.array {
            StringArrayData::String(_) => ConcreteDataType::string_datatype(),
            StringArrayData::LargeString(_) => ConcreteDataType::large_string_datatype(),
            StringArrayData::StringView(_) => ConcreteDataType::utf8_view_datatype(),
        }
    }

    fn vector_type_name(&self) -> String {
        "StringVector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        match &self.array {
            StringArrayData::String(array) => array.len(),
            StringArrayData::LargeString(array) => array.len(),
            StringArrayData::StringView(array) => array.len(),
        }
    }

    fn to_arrow_array(&self) -> ArrayRef {
        match &self.array {
            StringArrayData::String(array) => Arc::new(array.clone()),
            StringArrayData::LargeString(array) => Arc::new(array.clone()),
            StringArrayData::StringView(array) => Arc::new(array.clone()),
        }
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        match &self.array {
            StringArrayData::String(array) => Box::new(array.clone()),
            StringArrayData::LargeString(array) => Box::new(array.clone()),
            StringArrayData::StringView(array) => Box::new(array.clone()),
        }
    }

    fn validity(&self) -> Validity {
        match &self.array {
            StringArrayData::String(array) => vectors::impl_validity_for_vector!(array),
            StringArrayData::LargeString(array) => vectors::impl_validity_for_vector!(array),
            StringArrayData::StringView(array) => vectors::impl_validity_for_vector!(array),
        }
    }

    fn memory_size(&self) -> usize {
        match &self.array {
            StringArrayData::String(array) => array.get_buffer_memory_size(),
            StringArrayData::LargeString(array) => array.get_buffer_memory_size(),
            StringArrayData::StringView(array) => array.get_buffer_memory_size(),
        }
    }

    fn null_count(&self) -> usize {
        match &self.array {
            StringArrayData::String(array) => array.null_count(),
            StringArrayData::LargeString(array) => array.null_count(),
            StringArrayData::StringView(array) => array.null_count(),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match &self.array {
            StringArrayData::String(array) => array.is_null(row),
            StringArrayData::LargeString(array) => array.is_null(row),
            StringArrayData::StringView(array) => array.is_null(row),
        }
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        match &self.array {
            StringArrayData::String(array) => {
                Arc::new(Self::from_string_array(array.slice(offset, length)))
            }
            StringArrayData::LargeString(array) => {
                Arc::new(Self::from_large_string_array(array.slice(offset, length)))
            }
            StringArrayData::StringView(array) => {
                Arc::new(Self::from_string_view_array(array.slice(offset, length)))
            }
        }
    }

    fn get(&self, index: usize) -> Value {
        match &self.array {
            StringArrayData::String(array) => vectors::impl_get_for_vector!(array, index),
            StringArrayData::LargeString(array) => vectors::impl_get_for_vector!(array, index),
            StringArrayData::StringView(array) => vectors::impl_get_for_vector!(array, index),
        }
    }

    fn get_ref(&self, index: usize) -> ValueRef<'_> {
        match &self.array {
            StringArrayData::String(array) => vectors::impl_get_ref_for_vector!(array, index),
            StringArrayData::LargeString(array) => vectors::impl_get_ref_for_vector!(array, index),
            StringArrayData::StringView(array) => vectors::impl_get_ref_for_vector!(array, index),
        }
    }
}

pub enum StringIter<'a> {
    String(ArrayIter<&'a StringArray>),
    LargeString(ArrayIter<&'a LargeStringArray>),
    StringView(ArrayIter<&'a StringViewArray>),
}

impl<'a> Iterator for StringIter<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StringIter::String(iter) => iter.next(),
            StringIter::LargeString(iter) => iter.next(),
            StringIter::StringView(iter) => iter.next(),
        }
    }
}

impl ScalarVector for StringVector {
    type OwnedItem = String;
    type RefItem<'a> = &'a str;
    type Iter<'a> = StringIter<'a>;
    type Builder = StringVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        match &self.array {
            StringArrayData::String(array) => {
                if array.is_valid(idx) {
                    Some(array.value(idx))
                } else {
                    None
                }
            }
            StringArrayData::LargeString(array) => {
                if array.is_valid(idx) {
                    Some(array.value(idx))
                } else {
                    None
                }
            }
            StringArrayData::StringView(array) => {
                if array.is_valid(idx) {
                    Some(array.value(idx))
                } else {
                    None
                }
            }
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        match &self.array {
            StringArrayData::String(array) => StringIter::String(array.iter()),
            StringArrayData::LargeString(array) => StringIter::LargeString(array.iter()),
            StringArrayData::StringView(array) => StringIter::StringView(array.iter()),
        }
    }
}

/// Internal representation for mutable string arrays
enum MutableStringArrayData {
    String(MutableStringArray),
    LargeString(MutableLargeStringArray),
    StringView(MutableStringViewArray),
}

pub struct StringVectorBuilder {
    mutable_array: MutableStringArrayData,
}

impl Default for StringVectorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StringVectorBuilder {
    /// Create a builder for regular strings
    pub fn new() -> Self {
        Self {
            mutable_array: MutableStringArrayData::String(MutableStringArray::new()),
        }
    }

    /// Create a builder for large strings
    pub fn new_large() -> Self {
        Self {
            mutable_array: MutableStringArrayData::LargeString(MutableLargeStringArray::new()),
        }
    }

    /// Create a builder for view strings
    pub fn new_view() -> Self {
        Self {
            mutable_array: MutableStringArrayData::StringView(MutableStringViewArray::new()),
        }
    }

    /// Create a builder for regular strings with capacity
    pub fn with_string_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableStringArrayData::String(MutableStringArray::with_capacity(
                capacity, 0,
            )),
        }
    }

    /// Create a builder for large strings with capacity
    pub fn with_large_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableStringArrayData::LargeString(
                MutableLargeStringArray::with_capacity(capacity, 0),
            ),
        }
    }

    /// Create a builder for view strings with capacity
    pub fn with_view_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableStringArrayData::StringView(
                MutableStringViewArray::with_capacity(capacity),
            ),
        }
    }
}

impl MutableVector for StringVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        match &self.mutable_array {
            MutableStringArrayData::String(_) => ConcreteDataType::string_datatype(),
            MutableStringArrayData::LargeString(_) => ConcreteDataType::large_string_datatype(),
            MutableStringArrayData::StringView(_) => ConcreteDataType::utf8_view_datatype(),
        }
    }

    fn len(&self) -> usize {
        match &self.mutable_array {
            MutableStringArrayData::String(array) => array.len(),
            MutableStringArrayData::LargeString(array) => array.len(),
            MutableStringArrayData::StringView(array) => array.len(),
        }
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
    fn try_push_value_ref(&mut self, value: &ValueRef) -> Result<()> {
        match value.try_into_string()? {
            Some(v) => match &mut self.mutable_array {
                MutableStringArrayData::String(array) => array.append_value(v),
                MutableStringArrayData::LargeString(array) => array.append_value(v),
                MutableStringArrayData::StringView(array) => array.append_value(v),
            },
            None => match &mut self.mutable_array {
                MutableStringArrayData::String(array) => array.append_null(),
                MutableStringArrayData::LargeString(array) => array.append_null(),
                MutableStringArrayData::StringView(array) => array.append_null(),
            },
        }
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        vectors::impl_extend_for_builder!(self, vector, StringVector, offset, length)
    }

    fn push_null(&mut self) {
        match &mut self.mutable_array {
            MutableStringArrayData::String(array) => array.append_null(),
            MutableStringArrayData::LargeString(array) => array.append_null(),
            MutableStringArrayData::StringView(array) => array.append_null(),
        }
    }
}

impl ScalarVectorBuilder for StringVectorBuilder {
    type VectorType = StringVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableStringArrayData::String(MutableStringArray::with_capacity(
                capacity, 0,
            )),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        match value {
            Some(v) => match &mut self.mutable_array {
                MutableStringArrayData::String(array) => array.append_value(v),
                MutableStringArrayData::LargeString(array) => array.append_value(v),
                MutableStringArrayData::StringView(array) => array.append_value(v),
            },
            None => match &mut self.mutable_array {
                MutableStringArrayData::String(array) => array.append_null(),
                MutableStringArrayData::LargeString(array) => array.append_null(),
                MutableStringArrayData::StringView(array) => array.append_null(),
            },
        }
    }

    fn finish(&mut self) -> Self::VectorType {
        match &mut self.mutable_array {
            MutableStringArrayData::String(array) => {
                StringVector::from_string_array(array.finish())
            }
            MutableStringArrayData::LargeString(array) => {
                StringVector::from_large_string_array(array.finish())
            }
            MutableStringArrayData::StringView(array) => {
                StringVector::from_string_view_array(array.finish())
            }
        }
    }

    fn finish_cloned(&self) -> Self::VectorType {
        match &self.mutable_array {
            MutableStringArrayData::String(array) => {
                StringVector::from_string_array(array.finish_cloned())
            }
            MutableStringArrayData::LargeString(array) => {
                StringVector::from_large_string_array(array.finish_cloned())
            }
            MutableStringArrayData::StringView(array) => {
                StringVector::from_string_view_array(array.finish_cloned())
            }
        }
    }
}

impl Serializable for StringVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        self.iter_data()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(error::SerializeSnafu)
    }
}

impl StringVector {
    pub fn try_from_arrow_array(
        array: impl AsRef<dyn Array>,
    ) -> crate::error::Result<StringVector> {
        let array = array.as_ref();

        if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
            Ok(StringVector::from_string_array(string_array.clone()))
        } else if let Some(large_string_array) = array.as_any().downcast_ref::<LargeStringArray>() {
            Ok(StringVector::from_large_string_array(
                large_string_array.clone(),
            ))
        } else if let Some(string_view_array) = array.as_any().downcast_ref::<StringViewArray>() {
            Ok(StringVector::from_string_view_array(
                string_view_array.clone(),
            ))
        } else {
            Err(crate::error::UnsupportedArrowTypeSnafu {
                arrow_type: array.data_type().clone(),
            }
            .build())
        }
    }
}

#[cfg(test)]
mod tests {

    use std::vec;

    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn test_string_vector_build_get() {
        let mut builder = StringVectorBuilder::with_capacity(4);
        builder.push(Some("hello"));
        builder.push(None);
        builder.push(Some("world"));
        let vector = builder.finish();

        assert_eq!(Some("hello"), vector.get_data(0));
        assert_eq!(None, vector.get_data(1));
        assert_eq!(Some("world"), vector.get_data(2));

        // Get out of bound
        assert!(vector.try_get(3).is_err());

        assert_eq!(Value::String("hello".into()), vector.get(0));
        assert_eq!(Value::Null, vector.get(1));
        assert_eq!(Value::String("world".into()), vector.get(2));

        let mut iter = vector.iter_data();
        assert_eq!("hello", iter.next().unwrap().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!("world", iter.next().unwrap().unwrap());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_string_view_vector_build_get() {
        let mut builder = StringVectorBuilder::with_view_capacity(4);
        builder.push(Some("hello"));
        builder.push(None);
        builder.push(Some("world"));
        let vector = builder.finish();

        assert_eq!(ConcreteDataType::utf8_view_datatype(), vector.data_type());

        let arrow_arr = vector.to_arrow_array();
        assert_eq!(&DataType::Utf8View, arrow_arr.data_type());
    }

    #[test]
    fn test_string_vector_builder() {
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push_value_ref(&ValueRef::String("hello"));
        assert!(builder.try_push_value_ref(&ValueRef::Int32(123)).is_err());

        let input = StringVector::from_slice(&["world", "one", "two"]);
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(
            builder
                .extend_slice_of(&crate::vectors::Int32Vector::from_slice([13]), 0, 1)
                .is_err()
        );
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(StringVector::from_slice(&["hello", "one", "two"]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_string_vector_misc() {
        let strs = vec!["hello", "greptime", "rust"];
        let v = StringVector::from(strs.clone());
        assert_eq!(3, v.len());
        assert_eq!("StringVector", v.vector_type_name());
        assert!(!v.is_const());
        assert!(v.validity().is_all_valid());
        assert!(!v.only_null());
        assert_eq!(1040, v.memory_size());

        for (i, s) in strs.iter().enumerate() {
            assert_eq!(Value::from(*s), v.get(i));
            assert_eq!(ValueRef::from(*s), v.get_ref(i));
            assert_eq!(Value::from(*s), v.try_get(i).unwrap());
        }

        let arrow_arr = v.to_arrow_array();
        assert_eq!(3, arrow_arr.len());
        assert_eq!(&DataType::Utf8, arrow_arr.data_type());
    }

    #[test]
    fn test_serialize_string_vector() {
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("hello"));
        builder.push(None);
        builder.push(Some("world"));
        let string_vector = builder.finish();
        let serialized =
            serde_json::to_string(&string_vector.serialize_to_json().unwrap()).unwrap();
        assert_eq!(r#"["hello",null,"world"]"#, serialized);
    }

    #[test]
    fn test_from_arrow_array() {
        let mut builder = MutableStringArray::new();
        builder.append_option(Some("A"));
        builder.append_option(Some("B"));
        builder.append_null();
        builder.append_option(Some("D"));
        let string_array: StringArray = builder.finish();
        let vector = StringVector::from(string_array);
        assert_eq!(
            r#"["A","B",null,"D"]"#,
            serde_json::to_string(&vector.serialize_to_json().unwrap()).unwrap(),
        );
    }

    #[test]
    fn test_from_non_option_string() {
        let nul = String::from_utf8(vec![0]).unwrap();
        let corpus = vec!["😅😅😅", "😍😍😍😍", "🥵🥵", nul.as_str()];
        let vector = StringVector::from(corpus);
        let serialized = serde_json::to_string(&vector.serialize_to_json().unwrap()).unwrap();
        assert_eq!(r#"["😅😅😅","😍😍😍😍","🥵🥵","\u0000"]"#, serialized);

        let corpus = vec![
            "🀀🀀🀀".to_string(),
            "🀁🀁🀁".to_string(),
            "🀂🀂🀂".to_string(),
            "🀃🀃🀃".to_string(),
            "🀆🀆".to_string(),
        ];
        let vector = StringVector::from(corpus);
        let serialized = serde_json::to_string(&vector.serialize_to_json().unwrap()).unwrap();
        assert_eq!(r#"["🀀🀀🀀","🀁🀁🀁","🀂🀂🀂","🀃🀃🀃","🀆🀆"]"#, serialized);
    }

    #[test]
    fn test_string_vector_builder_finish_cloned() {
        let mut builder = StringVectorBuilder::with_capacity(1024);
        builder.push(Some("1"));
        builder.push(Some("2"));
        builder.push(Some("3"));
        let vector = builder.finish_cloned();
        assert_eq!(vector.len(), 3);
        assert_eq!(
            r#"["1","2","3"]"#,
            serde_json::to_string(&vector.serialize_to_json().unwrap()).unwrap(),
        );
        assert_eq!(builder.len(), 3);
    }
}
