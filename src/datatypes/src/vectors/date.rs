use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MutableArray, MutablePrimitiveArray, PrimitiveArray};
use common_time::date::Date;
use snafu::{OptionExt, ResultExt};

use crate::data_type::ConcreteDataType;
use crate::error::{ConversionSnafu, SerializeSnafu};
use crate::prelude::{ScalarVectorBuilder, Validity, Value, Vector, VectorRef};
use crate::scalars::ScalarVector;
use crate::serialize::Serializable;
use crate::vectors::{MutableVector, PrimitiveIter, PrimitiveVector};

#[derive(Debug, Clone)]
pub struct DateVector {
    array: PrimitiveVector<i32>,
}

impl DateVector {
    pub fn new(array: PrimitiveArray<i32>) -> Self {
        Self {
            array: PrimitiveVector { array },
        }
    }

    pub fn try_from_arrow_array(array: impl AsRef<dyn Array>) -> crate::error::Result<Self> {
        Ok(Self::new(
            array
                .as_ref()
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .with_context(|| ConversionSnafu {
                    from: format!("{:?}", array.as_ref().data_type()),
                })?
                .clone(),
        ))
    }
}

impl Vector for DateVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::date_datatype()
    }

    fn vector_type_name(&self) -> String {
        "DateVector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        let validity = self.array.array.validity().cloned();
        let buffer = self.array.array.values().clone();
        Arc::new(PrimitiveArray::new(
            arrow::datatypes::DataType::Date32,
            buffer,
            validity,
        ))
    }

    fn validity(&self) -> Validity {
        self.array.validity()
    }

    fn memory_size(&self) -> usize {
        self.array.len()
    }

    fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        self.array.slice(offset, length)
    }

    fn get(&self, index: usize) -> Value {
        self.array.get(index)
    }

    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        self.array.replicate(offsets)
    }
}

pub struct DateIter<'a> {
    iter: PrimitiveIter<'a, i32>,
}

impl<'a> Iterator for DateIter<'a> {
    type Item = Option<Date>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|v| v.map(|v| Date::try_new(v).unwrap()))
    }
}

impl ScalarVector for DateVector {
    type OwnedItem = Date;
    type RefItem<'a> = Date;
    type Iter<'a> = DateIter<'a>;

    type Builder = DateVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        self.array.get_data(idx).map(|v| Date::try_new(v).unwrap())
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        DateIter {
            iter: self.array.iter_data(),
        }
    }
}

impl Serializable for DateVector {
    fn serialize_to_json(&self) -> crate::error::Result<Vec<serde_json::Value>> {
        self.array
            .iter_data()
            .map(|v| v.map(|d| Date::try_new(d).unwrap()))
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

pub struct DateVectorBuilder {
    buffer: MutablePrimitiveArray<i32>,
}

impl MutableVector for DateVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::date_datatype()
    }

    fn len(&self) -> usize {
        self.buffer.len()
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

impl ScalarVectorBuilder for DateVectorBuilder {
    type VectorType = DateVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: MutablePrimitiveArray::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.buffer.push(value.map(|d| d.val()))
    }

    fn finish(&mut self) -> Self::VectorType {
        Self::VectorType {
            array: PrimitiveVector::new(std::mem::take(&mut self.buffer).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_build_date_vector() {
        let mut builder = DateVectorBuilder::with_capacity(4);
        builder.push(Some(Date::try_new(1).unwrap()));
        builder.push(None);
        builder.push(Some(Date::try_new(-1).unwrap()));
        let vector = builder.finish();
        assert_eq!(3, vector.len());
        assert_eq!(Some(Date::try_new(1).unwrap()), vector.get_data(0));
        assert_eq!(None, vector.get_data(1));
        assert_eq!(Some(Date::try_new(-1).unwrap()), vector.get_data(2));
        let mut iter = vector.iter_data();
        assert_eq!(Some(Date::try_new(1).unwrap()), iter.next().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!(Some(Date::try_new(-1).unwrap()), iter.next().unwrap());
    }

    #[test]
    pub fn test_date_scalar() {
        let vector =
            DateVector::from_slice(&[Date::try_new(1).unwrap(), Date::try_new(2).unwrap()]);
        assert_eq!(2, vector.len());
        assert_eq!(Some(Date::try_new(1).unwrap()), vector.get_data(0));
        assert_eq!(Some(Date::try_new(2).unwrap()), vector.get_data(1));
    }
}
