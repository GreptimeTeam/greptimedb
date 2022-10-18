use std::sync::Arc;

use arrow::array::{Array, MutableArray};
use snafu::{OptionExt, ResultExt};

use self::point::{PointVector, PointVectorBuilder};
use super::{MutableVector, Vector};
use crate::error::SerializeSnafu;
use crate::prelude::ScalarRef;
use crate::types::GeometryType;
use crate::value::{GeometryValueRef, ValueRef};
use crate::{
    data_type::ConcreteDataType,
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
        let subtype = match self {
            Self::PointVector(_) => GeometryType::Point,
        };

        ConcreteDataType::geometry_datatype(subtype)
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
        if self.is_null(index) {
            return ValueRef::Null;
        }
        ValueRef::Geometry(GeometryValueRef::Indexed {
            vector: self,
            idx: index,
        })
    }
}

impl ScalarVector for GeometryVector {
    type OwnedItem = GeometryValue;

    type RefItem<'a> = GeometryValueRef<'a>;

    type Iter<'a> = GeometryVectorIter<'a>;

    type Builder = GeometryVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        match self.get_ref(idx) {
            ValueRef::Null => None,
            ValueRef::Geometry(geo_ref) => Some(geo_ref),
            _ => unreachable!(),
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
    type Item = Option<GeometryValueRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        self.pos += 1;

        if self.vector.len() <= pos {
            return None;
        }
        if self.vector.is_null(pos) {
            return Some(None);
        }
        Some(Some(GeometryValueRef::Indexed {
            vector: self.vector,
            idx: pos,
        }))
    }
}

pub enum GeometryVectorBuilder {
    PointVectorBuilder(PointVectorBuilder),
}

impl GeometryVectorBuilder {
    pub fn new_point_vector_builder() -> Self {
        Self::PointVectorBuilder(PointVectorBuilder::new())
    }
    pub fn with_capacity_point_vector_builder(capacity: usize) -> Self {
        Self::PointVectorBuilder(PointVectorBuilder::with_capacity(capacity))
    }
}

impl MutableVector for GeometryVectorBuilder {
    fn data_type(&self) -> crate::data_type::ConcreteDataType {
        let subtype = match self {
            Self::PointVectorBuilder(_) => GeometryType::Point,
        };

        ConcreteDataType::geometry_datatype(subtype)
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
        match self {
            GeometryVectorBuilder::PointVectorBuilder(builder) => match value {
                ValueRef::Null => builder.push(None),
                ValueRef::Geometry(geo_ref) => builder.push(Some(geo_ref.to_owned_scalar())),
                _ => unreachable!(),
            },
        }
        Ok(())
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

    fn with_capacity(_capacity: usize) -> Self {
        unimplemented!()
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        match value {
            Some(geo_ref) => self.push_value_ref(ValueRef::Geometry(geo_ref)).unwrap(),
            None => self.push_value_ref(ValueRef::Null).unwrap(),
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
        self.iter_data()
            .map(|v| match v {
                None => Ok(serde_json::Value::Null),
                Some(s) => serde_json::to_value(s.to_owned_scalar()),
            })
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_point_vector() {
        let mut builder = GeometryVectorBuilder::with_capacity_point_vector_builder(0);

        let value = GeometryValue::new_point(2.0, 1.0);

        builder.push(Some(GeometryValueRef::Ref { val: &value }));
        builder.push(None);
        assert_eq!(builder.len(), 2);

        let vector = builder.finish();
        assert_eq!(vector.len(), 2);

        assert!(vector.get(1).is_null());
        assert!(vector.get_data(1).is_none());

        assert_eq!(vector.get(0), value.to_value());
        assert_eq!(vector.get_data(0).unwrap().to_owned_scalar(), value);

        assert_eq!(
            vector.data_type(),
            ConcreteDataType::geometry_datatype(GeometryType::Point)
        );

        let iter = vector.iter_data();
        let mut cnt: usize = 0;

        for i in iter {
            assert_eq!(i, vector.get_data(cnt));
            cnt += 1;
        }
        assert_eq!(cnt, vector.len());

        let slice = vector.slice(0, 2);
        let mut builder = GeometryVectorBuilder::new_point_vector_builder();

        builder.extend_slice_of(slice.as_ref(), 0, 2).unwrap();

        let another = builder.finish();

        assert_eq!(vector.get(0), another.get(0));
        assert_eq!(vector.get(1), another.get(1));

        assert_eq!(vector.get_data(0), another.get_data(0));

        assert_eq!(vector.memory_size(), 32); //2 elements (f64,f64)=2*2*8

        assert_eq!(
            format!("{:?}",vector.serialize_to_json().unwrap()),
            "[Object {\"Point\": Object {\"type\": String(\"Point\"), \"coordinates\": Array [Number(2.0), Number(1.0)]}}, Null]".to_string()
        )
    }
}
