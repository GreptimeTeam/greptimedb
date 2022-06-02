use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MutableArray, Utf8ValuesIter};
use arrow::bitmap::utils::ZipValidity;
use serde_json::Value;
use snafu::OptionExt;
use snafu::ResultExt;

use crate::arrow_array::{MutableStringArray, StringArray};
use crate::data_type::ConcreteDataType;
use crate::error::SerializeSnafu;
use crate::prelude::{MutableVector, ScalarVectorBuilder, Validity, Vector, VectorRef};
use crate::scalars::{common::replicate_scalar_vector, ScalarVector};
use crate::serialize::Serializable;
use crate::types::StringType;
use crate::value::Value as DataValue;
use crate::vectors::impl_try_from_arrow_array_for_vector;

/// String array wrapper
#[derive(Debug, Clone)]
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

    fn validity(&self) -> Validity {
        match self.array.validity() {
            Some(bitmap) => Validity::Slots(bitmap),
            None => Validity::AllValid,
        }
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        Arc::new(Self::from(self.array.slice(offset, length)))
    }

    fn get(&self, index: usize) -> DataValue {
        self.array.value(index).into()
    }

    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        replicate_scalar_vector(self, offsets)
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
    fn serialize_to_json(&self) -> crate::error::Result<Vec<Value>> {
        self.iter_data()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null),
                Some(s) => serde_json::to_value(s),
            })
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

impl_try_from_arrow_array_for_vector!(StringArray, StringVector);

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

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

        let mut iter = vector.iter_data();
        assert_eq!("hello", iter.next().unwrap().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!("world", iter.next().unwrap().unwrap());
        assert_eq!(None, iter.next());
    }
}
