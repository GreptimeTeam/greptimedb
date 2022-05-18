use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::BinaryValueIter;
use arrow::bitmap::utils::ZipValidity;
use snafu::ResultExt;

use crate::arrow_array::{LargeBinaryArray, MutableLargeBinaryArray};
use crate::data_type::DataTypeRef;
use crate::error::Result;
use crate::error::SerializeSnafu;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::BinaryType;
use crate::vectors::Vector;

/// Vector of binary strings.
#[derive(Debug)]
pub struct BinaryVector {
    array: LargeBinaryArray,
}

impl Vector for BinaryVector {
    fn data_type(&self) -> DataTypeRef {
        BinaryType::arc()
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

impl ScalarVector for BinaryVector {
    type RefItem<'a> = &'a [u8];
    type Iter<'a> = ZipValidity<'a, &'a [u8], BinaryValueIter<'a, i64>>;
    type Builder = BinaryVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if idx < self.len() {
            Some(self.array.value(idx))
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        self.array.iter()
    }
}

pub struct BinaryVectorBuilder {
    mutable_array: MutableLargeBinaryArray,
}

impl ScalarVectorBuilder for BinaryVectorBuilder {
    type VectorType = BinaryVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableLargeBinaryArray::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.mutable_array.push(value);
    }

    fn finish(self) -> Self::VectorType {
        BinaryVector {
            array: self.mutable_array.into(),
        }
    }
}

impl Serializable for BinaryVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        self.array
            .iter()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null), // if binary vector not present, map to NULL
                Some(vec) => serde_json::to_value(vec),
            })
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

#[cfg(test)]
mod tests {
    use serde::*;

    use super::BinaryVector;
    use crate::arrow_array::LargeBinaryArray;
    use crate::serialize::Serializable;

    #[test]
    pub fn test_serialize_binary_vector_to_json() {
        let vector = BinaryVector {
            array: LargeBinaryArray::from_slice(&vec![vec![1, 2, 3], vec![1, 2, 3]]),
        };

        let json_value = vector.serialize_to_json().unwrap();
        let mut output = vec![];
        let mut serializer = serde_json::ser::Serializer::new(&mut output);
        json_value.serialize(&mut serializer).unwrap();
        assert_eq!("[[1,2,3],[1,2,3]]", String::from_utf8_lossy(&output));
    }
}
