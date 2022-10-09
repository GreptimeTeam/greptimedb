use snafu::ensure;

use super::{MutableVector, Vector};
use crate::{
    error,
    prelude::{ScalarVector, ScalarVectorBuilder},
    serialize::Serializable,
    value::GeometryValue,
};

mod point;
#[derive(Debug, Clone, PartialEq)]
pub enum GeometryVector {}

impl Vector for GeometryVector {
    fn data_type(&self) -> crate::data_type::ConcreteDataType {
        todo!()
    }

    fn vector_type_name(&self) -> String {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn to_arrow_array(&self) -> arrow::array::ArrayRef {
        todo!()
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn arrow::array::Array> {
        todo!()
    }

    fn validity(&self) -> super::Validity {
        todo!()
    }

    fn memory_size(&self) -> usize {
        todo!()
    }

    fn is_null(&self, row: usize) -> bool {
        todo!()
    }

    fn slice(&self, offset: usize, length: usize) -> super::VectorRef {
        todo!()
    }

    fn get(&self, index: usize) -> crate::value::Value {
        todo!()
    }

    fn get_ref(&self, index: usize) -> crate::value::ValueRef {
        todo!()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn null_count(&self) -> usize {
        match self.validity() {
            super::Validity::Slots(bitmap) => bitmap.null_count(),
            super::Validity::AllValid => 0,
            super::Validity::AllNull => self.len(),
        }
    }

    fn is_const(&self) -> bool {
        false
    }

    fn only_null(&self) -> bool {
        self.null_count() == self.len()
    }

    fn try_get(&self, index: usize) -> crate::Result<crate::value::Value> {
        ensure!(
            index < self.len(),
            error::BadArrayAccessSnafu {
                index,
                size: self.len()
            }
        );
        Ok(self.get(index))
    }
}

impl ScalarVector for GeometryVector {
    type OwnedItem = GeometryValue;

    type RefItem<'a> = &'a GeometryValue;

    type Iter<'a> = GeometryVectorIter<'a>;

    type Builder = GeometryVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        todo!()
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        todo!()
    }
}

pub struct GeometryVectorIter<'a> {
    vector: &'a GeometryVector,
}

impl<'a> Iterator for GeometryVectorIter<'a> {
    type Item = Option<&'a GeometryValue>;
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub enum GeometryVectorBuilder {}

impl MutableVector for GeometryVectorBuilder {
    fn data_type(&self) -> crate::data_type::ConcreteDataType {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        todo!()
    }

    fn to_vector(&mut self) -> super::VectorRef {
        todo!()
    }

    fn push_value_ref(&mut self, value: crate::value::ValueRef) -> crate::Result<()> {
        todo!()
    }

    fn extend_slice_of(
        &mut self,
        vector: &dyn Vector,
        offset: usize,
        length: usize,
    ) -> crate::Result<()> {
        todo!()
    }
}

impl ScalarVectorBuilder for GeometryVectorBuilder {
    type VectorType = GeometryVector;

    fn with_capacity(capacity: usize) -> Self {
        todo!()
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        todo!()
    }

    fn finish(&mut self) -> Self::VectorType {
        todo!()
    }
}

impl Serializable for GeometryVector {
    fn serialize_to_json(&self) -> crate::Result<Vec<serde_json::Value>> {
        todo!()
    }
}
