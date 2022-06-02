use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray};
use arrow::array::{BinaryValueIter, MutableArray};
use arrow::bitmap::utils::ZipValidity;
use snafu::OptionExt;
use snafu::ResultExt;

use crate::arrow_array::{LargeBinaryArray, MutableLargeBinaryArray};
use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::error::SerializeSnafu;
use crate::scalars::{common, ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::value::Value;
use crate::vectors::impl_try_from_arrow_array_for_vector;
use crate::vectors::{MutableVector, Validity, Vector, VectorRef};

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

impl From<Vec<Option<Vec<u8>>>> for BinaryVector {
    fn from(data: Vec<Option<Vec<u8>>>) -> Self {
        Self {
            array: LargeBinaryArray::from(data),
        }
    }
}

impl Vector for BinaryVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::binary_datatype()
    }

    fn vector_type_name(&self) -> String {
        "BinaryVector".to_string()
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

    fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        Arc::new(Self::from(self.array.slice(offset, length)))
    }

    fn get(&self, index: usize) -> Value {
        if self.array.is_valid(index) {
            // Safety: The index have been checked by `is_valid()`.
            unsafe { Value::Binary(self.array.value_unchecked(index).into()) }
        } else {
            Value::Null
        }
    }

    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        common::replicate_scalar_vector(self, offsets)
    }
}

impl ScalarVector for BinaryVector {
    type OwnedItem = Vec<u8>;
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

impl MutableVector for BinaryVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::binary_datatype()
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

    fn finish(&mut self) -> Self::VectorType {
        BinaryVector {
            array: std::mem::take(&mut self.mutable_array).into(),
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
    use arrow::datatypes::DataType as ArrowDataType;
    use common_base::bytes::Bytes;
    use serde_json;

    use super::*;
    use crate::arrow_array::LargeBinaryArray;
    use crate::serialize::Serializable;

    #[test]
    fn test_binary_vector_misc() {
        let v = BinaryVector::from(LargeBinaryArray::from_slice(&vec![
            vec![1, 2, 3],
            vec![1, 2, 3],
        ]));

        assert_eq!(2, v.len());
        assert_eq!("BinaryVector", v.vector_type_name());
        assert!(!v.is_const());
        assert_eq!(Validity::AllValid, v.validity());
        assert!(!v.only_null());

        for i in 0..2 {
            assert!(!v.is_null(i));
            assert_eq!(Value::Binary(Bytes::from(vec![1, 2, 3])), v.get(i));
        }

        let arrow_arr = v.to_arrow_array();
        assert_eq!(2, arrow_arr.len());
        assert_eq!(&ArrowDataType::LargeBinary, arrow_arr.data_type());
    }

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

        assert_eq!(Value::Binary(b"hello".as_slice().into()), vector.get(0));
        assert_eq!(Value::Null, vector.get(3));

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
