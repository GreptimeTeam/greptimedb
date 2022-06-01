use std::any::Any;
use std::borrow::Borrow;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, MutableBooleanArray};
use arrow::bitmap::utils::{BitmapIter, ZipValidity};
use snafu::OptionExt;
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::BooleanType;
use crate::vectors::impl_try_from_arrow_array_for_vector;
use crate::vectors::{Validity, Vector};

/// Vector of boolean.
#[derive(Debug)]
pub struct BooleanVector {
    array: BooleanArray,
}

impl From<Vec<bool>> for BooleanVector {
    fn from(data: Vec<bool>) -> Self {
        BooleanVector {
            array: BooleanArray::from_slice(&data),
        }
    }
}

impl From<BooleanArray> for BooleanVector {
    fn from(array: BooleanArray) -> Self {
        Self { array }
    }
}

impl From<Vec<Option<bool>>> for BooleanVector {
    fn from(data: Vec<Option<bool>>) -> Self {
        BooleanVector {
            array: BooleanArray::from(data),
        }
    }
}

impl<Ptr: Borrow<Option<bool>>> FromIterator<Ptr> for BooleanVector {
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        BooleanVector {
            array: BooleanArray::from_iter(iter),
        }
    }
}

impl Vector for BooleanVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Boolean(BooleanType::default())
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

impl ScalarVector for BooleanVector {
    type RefItem<'a> = bool;
    type Iter<'a> = ZipValidity<'a, bool, BitmapIter<'a>>;
    type Builder = BooleanVectorBuilder;

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

pub struct BooleanVectorBuilder {
    mutable_array: MutableBooleanArray,
}

impl ScalarVectorBuilder for BooleanVectorBuilder {
    type VectorType = BooleanVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            mutable_array: MutableBooleanArray::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.mutable_array.push(value);
    }

    fn finish(self) -> Self::VectorType {
        BooleanVector {
            array: self.mutable_array.into(),
        }
    }
}

impl Serializable for BooleanVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        self.iter_data()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(crate::error::SerializeSnafu)
    }
}

impl_try_from_arrow_array_for_vector!(BooleanArray, BooleanVector);

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;
    use crate::serialize::Serializable;

    #[test]
    fn test_serialize_boolean_vector_to_json() {
        let vector = BooleanVector::from(vec![true, false, true, true, false, false]);

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[true,false,true,true,false,false]",
            serde_json::to_string(&json_value).unwrap(),
        );
    }

    #[test]
    fn test_serialize_boolean_vector_with_null_to_json() {
        let vector = BooleanVector::from(vec![Some(true), None, Some(false)]);

        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[true,null,false]",
            serde_json::to_string(&json_value).unwrap(),
        );
    }

    #[test]
    fn test_boolean_vector_from_vec() {
        let input = vec![false, true, false, true];
        let vec = BooleanVector::from(input.clone());
        assert_eq!(4, vec.len());
        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(Some(v), vec.get_data(i), "failed at {}", i)
        }
    }

    #[test]
    fn test_boolean_vector_from_iter() {
        let input = vec![Some(false), Some(true), Some(false), Some(true)];
        let vec = input.iter().collect::<BooleanVector>();
        assert_eq!(4, vec.len());
        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(v, vec.get_data(i), "failed at {}", i)
        }
    }

    #[test]
    fn test_boolean_vector_from_vec_option() {
        let input = vec![Some(false), Some(true), None, Some(true)];
        let vec = BooleanVector::from(input.clone());
        assert_eq!(4, vec.len());
        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(v, vec.get_data(i), "failed at {}", i)
        }
    }

    #[test]
    fn test_boolean_vector_build_get() {
        let input = [Some(true), None, Some(false)];
        let mut builder = BooleanVectorBuilder::with_capacity(3);
        for v in input {
            builder.push(v);
        }
        let vector = builder.finish();
        assert_eq!(input.len(), vector.len());

        let res: Vec<_> = vector.iter_data().collect();
        assert_eq!(input, &res[..]);

        for (i, v) in input.into_iter().enumerate() {
            assert_eq!(v, vector.get_data(i));
        }
    }

    #[test]
    fn test_boolean_vector_validity() {
        let vector = BooleanVector::from(vec![Some(true), None, Some(false)]);
        assert_eq!(1, vector.null_count());
        let validity = vector.validity();
        let slots = validity.slots().unwrap();
        assert_eq!(1, slots.null_count());
        assert!(!slots.get_bit(1));

        let vector = BooleanVector::from(vec![true, false, false]);
        assert_eq!(0, vector.null_count());
        assert_eq!(Validity::AllValid, vector.validity());
    }
}
