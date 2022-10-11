use std::sync::Arc;

use arrow::array::{Array, MutableArray};
use snafu::{ensure, OptionExt};

use self::point::{PointVector, PointVectorBuilder};
use super::{MutableVector, Validity, Value, Vector};
use crate::value::ValueRef;
use crate::vectors::{impl_try_from_arrow_array_for_vector, impl_validity_for_vector};
use crate::{
    data_type::ConcreteDataType,
    error,
    prelude::{ScalarVector, ScalarVectorBuilder},
    serialize::Serializable,
    value::GeometryValue,
};

mod point;
#[derive(Debug, Clone, PartialEq)]
pub enum GeometryVector {
    PointVector(PointVector),
}

impl Vector for GeometryVector {
    fn data_type(&self) -> crate::data_type::ConcreteDataType {
        ConcreteDataType::geometry_datatype()
    }

    fn vector_type_name(&self) -> String {
        "GeometryVector".to_string()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        match self {
            GeometryVector::PointVector(vec) => vec.array.len(),
        }
    }

    fn to_arrow_array(&self) -> arrow::array::ArrayRef {
        match self {
            GeometryVector::PointVector(vec) => Arc::new(vec.array.clone()),
        }
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn arrow::array::Array> {
        match self {
            GeometryVector::PointVector(vec) => Box::new(vec.array.clone()),
        }
    }

    fn validity(&self) -> super::Validity {
        match self {
            GeometryVector::PointVector(vec) => vec.validity(),
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            GeometryVector::PointVector(point_vector) => point_vector.memory_size(),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            GeometryVector::PointVector(point_vector) => point_vector.array.is_null(row),
        }
    }

    fn slice(&self, offset: usize, length: usize) -> super::VectorRef {
        match self {
            GeometryVector::PointVector(vec) => {
                Arc::new(GeometryVector::PointVector(vec.slice(offset, length)))
            }
        }
    }

    fn get(&self, index: usize) -> crate::value::Value {
        match self {
            GeometryVector::PointVector(vec) => vec.get(index),
        }
    }

    fn get_ref(&self, index: usize) -> crate::value::ValueRef {
        match self {
            GeometryVector::PointVector(vec) => vec.get_ref(index),
        }
    }
}

impl ScalarVector for GeometryVector {
    type OwnedItem = GeometryValue;

    type RefItem<'a> = GeometryValue;

    type Iter<'a> = GeometryVectorIter<'a>;

    type Builder = GeometryVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        match self {
            GeometryVector::PointVector(vec) => match self.get(idx) {
                Value::Null => None,
                Value::Geometry(geo_value) => Some(geo_value),
                _ => unreachable!(),
            },
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        GeometryVectorIter {
            vector: self,
            pos: 0,
        }
    }
}

pub struct GeometryVectorIter<'a> {
    vector: &'a GeometryVector,
    pos: usize,
}

impl<'a> Iterator for GeometryVectorIter<'a> {
    type Item = Option<GeometryValue>;
    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        self.pos = self.pos + 1;

        if self.vector.len() <= pos {
            return None;
        }

        match self.vector {
            GeometryVector::PointVector(vec) => match vec.get(pos) {
                Value::Null => Some(None),
                Value::Geometry(geo_value) => Some(Some(geo_value)),
                _ => unreachable!(),
            },
        }
    }
}

pub enum GeometryVectorBuilder {
    PointVectorBuilder(PointVectorBuilder),
}

impl MutableVector for GeometryVectorBuilder {
    fn data_type(&self) -> crate::data_type::ConcreteDataType {
        ConcreteDataType::geometry_datatype()
    }

    fn len(&self) -> usize {
        match self {
            GeometryVectorBuilder::PointVectorBuilder(builder) => builder.array_x.len(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn to_vector(&mut self) -> super::VectorRef {
        Arc::new(self.finish())
    }

    fn push_value_ref(&mut self, value: crate::value::ValueRef) -> crate::Result<()> {
        match value {
            ValueRef::Geometry(value) => {
                self.push(Some(value));
                Ok(())
            }
            ValueRef::Null => {
                self.push(None);
                Ok(())
            }
            _ => unimplemented!(),
        }
    }

    fn extend_slice_of(
        &mut self,
        vector: &dyn Vector,
        offset: usize,
        length: usize,
    ) -> crate::Result<()> {
        let concrete_vector = vector
            .as_any()
            .downcast_ref::<GeometryVector>()
            .with_context(|| crate::error::CastTypeSnafu {
                msg: format!(
                    "Failed to cast vector from {} to {}",
                    vector.vector_type_name(),
                    stringify!(GeometryVector)
                ),
            })?;

        for idx in offset..offset + length {
            let value = concrete_vector.get_ref(idx);
            self.push_value_ref(value)?;
        }
        Ok(())
    }
}

impl ScalarVectorBuilder for GeometryVectorBuilder {
    type VectorType = GeometryVector;

    fn with_capacity(capacity: usize) -> Self {
        unimplemented!()
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        match self {
            GeometryVectorBuilder::PointVectorBuilder(builder) => builder.push(value),
        }
    }

    fn finish(&mut self) -> Self::VectorType {
        match self {
            GeometryVectorBuilder::PointVectorBuilder(builder) => {
                GeometryVector::PointVector(builder.finish())
            }
        }
    }
}

impl Serializable for GeometryVector {
    fn serialize_to_json(&self) -> crate::Result<Vec<serde_json::Value>> {
        todo!()
    }
}
