use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MutableArray, Utf8ValuesIter};
use arrow::bitmap::utils::ZipValidity;
use serde_json::Value as JsonValue;
use snafu::{OptionExt, ResultExt};

use crate::arrow_array::{MutableStringArray, StringArray};
use crate::data_type::ConcreteDataType;
use crate::error::{Result, SerializeSnafu};
use crate::scalars::{common, ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::StringType;
use crate::value::{Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

/// String array wrapper
#[derive(Debug, Clone, PartialEq)]
pub struct StringVector {
    array: StringArray,
}

impl From<StringArray> for StringVector {
    fn from(array: StringArray) -> Self {
        Self { array }
    }
}

impl From<Vec<Option<String>>> for StringVector {
    fn from(data: Vec<Option<String>>) -> Self {
        Self {
            array: StringArray::from(data),
        }
    }
}

impl From<Vec<String>> for StringVector {
    fn from(data: Vec<String>) -> Self {
        Self {
            array: StringArray::from(
                data.into_iter()
                    .map(Option::Some)
                    .collect::<Vec<Option<String>>>(),
            ),
        }
    }
}

impl From<Vec<Option<&str>>> for StringVector {
    fn from(data: Vec<Option<&str>>) -> Self {
        Self {
            array: StringArray::from(data),
        }
    }
}

impl From<Vec<&str>> for StringVector {
    fn from(data: Vec<&str>) -> Self {
        Self {
            array: StringArray::from(
                data.into_iter()
                    .map(Option::Some)
                    .collect::<Vec<Option<&str>>>(),
            ),
        }
    }
}

impl Vector for StringVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::String(StringType::default())
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

    fn to_box_arrow_array(&self) -> Box<dyn Array> {
        Box::new(self.array.clone())
    }

    fn validity(&self) -> Validity {
        vectors::impl_validity_for_vector!(self.array)
    }

    fn memory_size(&self) -> usize {
        self.len() * std::mem::size_of::<i64>() + self.array.values().len()
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

    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        common::replicate_scalar_vector(self, offsets)
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        vectors::impl_get_ref_for_vector!(self.array, index)
    }
}

impl ScalarVector for StringVector {
    type OwnedItem = String;
    type RefItem<'a> = &'a str;
    type Iter<'a> = ZipValidity<'a, &'a str, Utf8ValuesIter<'a, i32>>;
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
    buffer: MutableStringArray,
}

impl MutableVector for StringVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::string_datatype()
    }

    fn len(&self) -> usize {
        self.buffer.len()
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

    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        self.buffer.push(value.as_string()?);
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        vectors::impl_extend_for_builder!(self.buffer, vector, StringVector, offset, length)
    }
}

impl ScalarVectorBuilder for StringVectorBuilder {
    type VectorType = StringVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: MutableStringArray::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.buffer.push(value)
    }

    fn finish(&mut self) -> Self::VectorType {
        Self::VectorType {
            array: std::mem::take(&mut self.buffer).into(),
        }
    }
}

impl Serializable for StringVector {
    fn serialize_to_json(&self) -> crate::error::Result<Vec<JsonValue>> {
        self.iter_data()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null),
                Some(s) => serde_json::to_value(s),
            })
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

vectors::impl_try_from_arrow_array_for_vector!(StringArray, StringVector);

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;
    use serde_json;

    use super::*;
    use crate::data_type::DataType;

    #[test]
    fn test_string_vector_misc() {
        let strs = vec!["hello", "greptime", "rust"];
        let v = StringVector::from(strs.clone());
        assert_eq!(3, v.len());
        assert_eq!("StringVector", v.vector_type_name());
        assert!(!v.is_const());
        assert_eq!(Validity::AllValid, v.validity());
        assert!(!v.only_null());
        assert_eq!(41, v.memory_size());

        for (i, s) in strs.iter().enumerate() {
            assert_eq!(Value::from(*s), v.get(i));
            assert_eq!(ValueRef::from(*s), v.get_ref(i));
            assert_eq!(Value::from(*s), v.try_get(i).unwrap());
        }

        let arrow_arr = v.to_arrow_array();
        assert_eq!(3, arrow_arr.len());
        assert_eq!(&ArrowDataType::Utf8, arrow_arr.data_type());
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
        builder.push(Some("A"));
        builder.push(Some("B"));
        builder.push::<&str>(None);
        builder.push(Some("D"));
        let string_array: StringArray = builder.into();
        let vector = StringVector::from(string_array);
        assert_eq!(
            r#"["A","B",null,"D"]"#,
            serde_json::to_string(&vector.serialize_to_json().unwrap()).unwrap(),
        );
    }

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
        let mut builder = StringType::default().create_mutable(3);
        builder.push_value_ref(ValueRef::String("hello")).unwrap();
        assert!(builder.push_value_ref(ValueRef::Int32(123)).is_err());

        let input = StringVector::from_slice(&["world", "one", "two"]);
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice(&[13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(StringVector::from_slice(&["hello", "one", "two"]));
        assert_eq!(expect, vector);
    }
}
