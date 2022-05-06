use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::BinaryValueIter;
use arrow::bitmap::utils::ZipValidity;

use crate::data_type::DataTypeRef;
use crate::scalars::{ScalarVector, ScalarVectorBuilder};
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
