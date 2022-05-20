use std::any::Any;
use std::borrow::Borrow;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, MutableBooleanArray};
use arrow::bitmap::utils::{BitmapIter, ZipValidity};
use snafu::OptionExt;
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::impl_try_from_arrow_array_for_vector;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::BooleanType;
use crate::vectors::Vector;

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
}

impl ScalarVector for BooleanVector {
    type RefItem<'a> = bool;
    type Iter<'a> = ZipValidity<'a, bool, BitmapIter<'a>>;
    type Builder = BooleanVectorBuilder;

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
    use serde::*;

    use super::*;
    use crate::serialize::Serializable;

    #[test]
    pub fn test_serialize_boolean_vector_to_json() {
        let vector = BooleanVector {
            array: BooleanArray::from_slice(&vec![true, false, true, true, false, false]),
        };

        let json_value = vector.serialize_to_json().unwrap();
        let mut output = vec![];
        let mut serializer = serde_json::ser::Serializer::new(&mut output);
        json_value.serialize(&mut serializer).unwrap();
        assert_eq!(
            "[true,false,true,true,false,false]",
            String::from_utf8_lossy(&output)
        );
    }

    #[test]
    fn test_boolean_vector_from_vec() {
        let vec = BooleanVector::from(vec![false, true, false, true]);
        assert_eq!(4, vec.len());
        for i in 0..4 {
            assert_eq!(
                i == 1 || i == 3,
                vec.get_data(i).unwrap(),
                "failed at {}",
                i
            )
        }
    }

    #[test]
    fn test_boolean_vector_from_iter() {
        let v = vec![Some(false), Some(true), Some(false), Some(true)];
        let vec = v.into_iter().collect::<BooleanVector>();
        assert_eq!(4, vec.len());
        for i in 0..3 {
            assert_eq!(
                i == 1 || i == 3,
                vec.get_data(i).unwrap(),
                "failed at {}",
                i
            )
        }
    }

    #[test]
    fn test_boolean_vector_from_vec_option() {
        let vec = BooleanVector::from(vec![Some(false), Some(true), None, Some(true)]);
        assert_eq!(4, vec.len());
        for i in 0..4 {
            assert_eq!(
                i == 1 || i == 3,
                vec.get_data(i).unwrap(),
                "failed at {}",
                i
            )
        }
    }

    #[test]
    fn test_boolean_vector_builder() {
        let mut builder = BooleanVectorBuilder::with_capacity(4);
        builder.push(Some(false));
        builder.push(Some(true));
        builder.push(Some(false));
        builder.push(Some(true));

        let vec = builder.finish();

        assert_eq!(4, vec.len());
        for i in 0..4 {
            assert_eq!(
                i == 1 || i == 3,
                vec.get_data(i).unwrap(),
                "failed at {}",
                i
            )
        }
    }
}
