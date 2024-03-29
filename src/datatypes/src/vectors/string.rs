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

use crate::arrow_array::{MutableStringArray, StringArray};
use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// Vector of strings.
#[derive(Debug, PartialEq)]
pub struct StringVector {
    array: StringArray,
}

impl StringVector {
    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl From<StringArray> for StringVector {
    fn from(array: StringArray) -> Self {
        Self { array }
    }
}

impl From<Vec<Option<String>>> for StringVector {
    fn from(data: Vec<Option<String>>) -> Self {
        Self {
            array: StringArray::from_iter(data),
        }
    }
}

impl From<Vec<Option<&str>>> for StringVector {
    fn from(data: Vec<Option<&str>>) -> Self {
        Self {
            array: StringArray::from_iter(data),
        }
    }
}

impl From<&[Option<String>]> for StringVector {
    fn from(data: &[Option<String>]) -> Self {
        Self {
            array: StringArray::from_iter(data),
        }
    }
}

impl From<&[Option<&str>]> for StringVector {
    fn from(data: &[Option<&str>]) -> Self {
        Self {
            array: StringArray::from_iter(data),
        }
    }
}

impl From<Vec<String>> for StringVector {
    fn from(data: Vec<String>) -> Self {
        Self {
            array: StringArray::from_iter(data.into_iter().map(Some)),
        }
    }
}

impl From<Vec<&str>> for StringVector {
    fn from(data: Vec<&str>) -> Self {
        Self {
            array: StringArray::from_iter(data.into_iter().map(Some)),
        }
    }
}

impl Vector for StringVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::string_datatype()
    }

    fn vector_type_name(&self) -> String {
        "StringVector".to_string()
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
        Arc::new(Self::from(self.array.slice(offset, length)))
    }

    fn get(&self, index: usize) -> Value {
        vectors::impl_get_for_vector!(self.array, index)
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        vectors::impl_get_ref_for_vector!(self.array, index)
    }
}

impl ScalarVector for StringVector {
    type OwnedItem = String;
    type RefItem<'a> = &'a str;
    type Iter<'a> = ArrayIter<&'a StringArray>;
    type Builder = StringVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(self.array.value(idx))
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        self.array.iter()
    }
}

pub struct StringVectorBuilder {
    mutable_array: MutableStringArray,
}

impl MutableVector for StringVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::string_datatype()
    }

    fn len(&self) -> usize {
        self.mutable_array.len()
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
        match value.as_string()? {
            Some(v) => self.mutable_array.append_value(v),
            None => self.mutable_array.append_null(),
        }
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        vectors::impl_extend_for_builder!(self, vector, StringVector, offset, length)
    }

    fn push_null(&mut self) {
        self.mutable_array.append_null()
    }
}

impl ScalarVectorBuilder for StringVectorBuilder {
    type VectorType = StringVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableStringArray::with_capacity(capacity, 0),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        match value {
            Some(v) => self.mutable_array.append_value(v),
            None => self.mutable_array.append_null(),
        }
    }

    fn finish(&mut self) -> Self::VectorType {
        StringVector {
            array: self.mutable_array.finish(),
        }
    }

    fn finish_cloned(&self) -> Self::VectorType {
        StringVector {
            array: self.mutable_array.finish_cloned(),
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

vectors::impl_try_from_arrow_array_for_vector!(StringArray, StringVector);

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
    fn test_string_vector_builder() {
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push_value_ref(ValueRef::String("hello"));
        assert!(builder.try_push_value_ref(ValueRef::Int32(123)).is_err());

        let input = StringVector::from_slice(&["world", "one", "two"]);
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice([13]), 0, 1)
            .is_err());
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
        assert_eq!(1088, v.memory_size());

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
