use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Utf8ValuesIter};
use arrow::bitmap::utils::ZipValidity;
use serde_json::Value;
use snafu::OptionExt;
use snafu::ResultExt;

use crate::arrow_array::{MutableStringArray, StringArray};
use crate::data_type::ConcreteDataType;
use crate::error::SerializeSnafu;
use crate::impl_try_from_arrow_array_for_vector;
use crate::prelude::{ScalarVectorBuilder, Vector};
use crate::scalars::ScalarVector;
use crate::serialize::Serializable;
use crate::types::StringType;

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

impl Vector for StringVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::String(StringType::default())
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
}

impl ScalarVector for StringVector {
    type RefItem<'a> = &'a str;
    type Iter<'a> = ZipValidity<'a, &'a str, Utf8ValuesIter<'a, i32>>;
    type Builder = StringVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        match idx < self.array.len() {
            true => Some(self.array.value(idx)),
            false => None,
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        self.array.iter()
    }
}

pub struct StringVectorBuilder {
    buffer: MutableStringArray,
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

    fn finish(self) -> Self::VectorType {
        Self::VectorType {
            array: self.buffer.into(),
        }
    }
}

impl Serializable for StringVector {
    fn serialize_to_json(&self) -> crate::error::Result<Vec<Value>> {
        self.array
            .iter()
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
    use super::*;

    #[test]
    pub fn test_serialize_string_vector() {
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("hello"));
        builder.push(None);
        builder.push(Some("world"));
        let string_vector = builder.finish();
        let serialized = serialize_to_json_string(string_vector.serialize_to_json().unwrap());
        assert_eq!(r#"["hello",null,"world"]"#, serialized);
    }

    pub fn serialize_to_json_string<T>(val: T) -> String
    where
        T: serde::Serialize,
    {
        let mut output = vec![];
        let mut serializer = serde_json::Serializer::new(&mut output);
        val.serialize(&mut serializer).unwrap();
        String::from_utf8_lossy(&output).into()
    }

    #[test]
    pub fn test_from_arrow_array() {
        let mut builder = MutableStringArray::new();
        builder.push(Some("A"));
        builder.push(Some("B"));
        builder.push::<&str>(None);
        builder.push(Some("D"));
        let string_array: StringArray = builder.into();
        let vector = StringVector::from(string_array);
        assert_eq!(
            r#"["A","B",null,"D"]"#,
            serialize_to_json_string(vector.serialize_to_json().unwrap())
        );
    }
}
