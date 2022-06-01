use std::any::Any;
use std::sync::Arc;

use arrow::array::BinaryValueIter;
use arrow::array::{Array, ArrayRef, BinaryArray};
use arrow::bitmap::utils::ZipValidity;
use snafu::OptionExt;
use snafu::ResultExt;

use crate::arrow_array::{LargeBinaryArray, MutableLargeBinaryArray};
use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::error::SerializeSnafu;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::BinaryType;
use crate::vectors::impl_try_from_arrow_array_for_vector;
use crate::vectors::{Validity, Vector};

/// Vector of binary strings.
#[derive(Debug)]
pub struct BinaryVector {
    array: LargeBinaryArray,
}

impl From<BinaryArray<i64>> for BinaryVector {
    fn from(array: BinaryArray<i64>) -> Self {
        Self { array }
    }
}

impl Vector for BinaryVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Binary(BinaryType::default())
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
}

impl ScalarVector for BinaryVector {
    type RefItem<'a> = &'a [u8];
    type Iter<'a> = ZipValidity<'a, &'a [u8], BinaryValueIter<'a, i64>>;
    type Builder = BinaryVectorBuilder;

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
        self.iter_data()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null), // if binary vector not present, map to NULL
                Some(vec) => serde_json::to_value(vec),
            })
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

impl_try_from_arrow_array_for_vector!(LargeBinaryArray, BinaryVector);

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;
    use crate::arrow_array::LargeBinaryArray;
    use crate::serialize::Serializable;

    #[test]
    fn test_serialize_binary_vector_to_json() {
        let vector = BinaryVector::from(LargeBinaryArray::from_slice(&vec![
            vec![1, 2, 3],
            vec![1, 2, 3],
        ]));

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[[1,2,3],[1,2,3]]",
            serde_json::to_string(&json_value).unwrap()
        );
    }

    #[test]
    fn test_serialize_binary_vector_with_null_to_json() {
        let mut builder = BinaryVectorBuilder::with_capacity(4);
        builder.push(Some(&[1, 2, 3]));
        builder.push(None);
        builder.push(Some(&[4, 5, 6]));
        let vector = builder.finish();

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[[1,2,3],null,[4,5,6]]",
            serde_json::to_string(&json_value).unwrap()
        );
    }

    #[test]
    fn test_from_arrow_array() {
        let arrow_array = LargeBinaryArray::from_slice(&vec![vec![1, 2, 3], vec![1, 2, 3]]);
        let original = arrow_array.clone();
        let vector = BinaryVector::from(arrow_array);
        assert_eq!(original, vector.array);
    }

    #[test]
    fn test_binary_vector_build_get() {
        let mut builder = BinaryVectorBuilder::with_capacity(4);
        builder.push(Some(b"hello"));
        builder.push(Some(b"happy"));
        builder.push(Some(b"world"));
        builder.push(None);

        let vector = builder.finish();
        assert_eq!(b"hello", vector.get_data(0).unwrap());
        assert_eq!(None, vector.get_data(3));

        let mut iter = vector.iter_data();
        assert_eq!(b"hello", iter.next().unwrap().unwrap());
        assert_eq!(b"happy", iter.next().unwrap().unwrap());
        assert_eq!(b"world", iter.next().unwrap().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_binary_vector_validity() {
        let mut builder = BinaryVectorBuilder::with_capacity(4);
        builder.push(Some(b"hello"));
        builder.push(Some(b"world"));
        let vector = builder.finish();
        assert_eq!(0, vector.null_count());
        assert_eq!(Validity::AllValid, vector.validity());

        let mut builder = BinaryVectorBuilder::with_capacity(3);
        builder.push(Some(b"hello"));
        builder.push(None);
        builder.push(Some(b"world"));
        let vector = builder.finish();
        assert_eq!(1, vector.null_count());
        let validity = vector.validity();
        let slots = validity.slots().unwrap();
        assert_eq!(1, slots.null_count());
        assert!(!slots.get_bit(1));
    }
}
