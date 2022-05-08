use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::BinaryValueIter;
use arrow::bitmap::utils::ZipValidity;
use snafu::ResultExt;

use crate::data_type::DataTypeRef;
use crate::errors::Result;
use crate::errors::SerializeSnafu;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::BinaryType;
use crate::vectors::Vector;
use crate::{LargeBinaryArray, MutableLargeBinaryArray};

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
        let x: serde_json::Result<Vec<serde_json::Value>> = self
            .array
            .iter()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null), // if binary vector not present, map to NULL
                Some(vec) => serde_json::to_value(vec),
            })
            .collect();
        x.context(SerializeSnafu)
    }
}

#[cfg(test)]
mod tests {
    use serde::ser::SerializeStruct;
    use serde::*;

    use super::BinaryVector;
    use crate::serialize::Serializable;
    use crate::LargeBinaryArray;

    struct TestStruct {
        name: String,
        data: BinaryVector,
    }

    impl serde::Serialize for TestStruct {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut s = serializer.serialize_struct("testStruct", 2).unwrap();
            s.serialize_field("name", &self.name).unwrap();
            s.serialize_field("data", &self.data.serialize_to_json().unwrap())
                .unwrap();
            s.end()
        }
    }

    /// test serialize a struct to JOSN
    #[test]
    pub fn test_serialize_struct() {
        let test_struct = TestStruct {
            name: "some test".to_string(),
            data: BinaryVector {
                array: LargeBinaryArray::from_slice(&vec![vec![1, 2, 3], vec![1, 2, 3]]),
            },
        };

        let mut output = vec![];
        let mut serializer = serde_json::ser::Serializer::new(&mut output);
        test_struct.serialize(&mut serializer).unwrap();
        let result_string = String::from_utf8_lossy(&output);
        assert_eq!(
            "{\"name\":\"some test\",\"data\":[[1,2,3],[1,2,3]]}".to_string(),
            result_string
        );
    }
}
