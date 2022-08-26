use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use common_time::date::Date;
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::ConversionSnafu;
use crate::prelude::{ScalarVectorBuilder, Validity, Value, Vector, VectorRef};
use crate::scalars::ScalarVector;
use crate::serialize::Serializable;
use crate::vectors::{MutableVector, PrimitiveIter, PrimitiveVector, PrimitiveVectorBuilder};

#[derive(Debug, Clone, PartialEq)]
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
        self.array.memory_size()
    }

    fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        self.array.slice(offset, length)
    }

    fn get(&self, index: usize) -> Value {
        match self.array.get(index) {
            Value::Int32(v) => Value::Date(Date::new(v)),
            Value::Null => Value::Null,
            _ => {
                unreachable!()
            }
        }
    }

    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        self.array.replicate(offsets)
    }
}

impl From<Vec<Option<i32>>> for DateVector {
    fn from(data: Vec<Option<i32>>) -> Self {
        Self {
            array: PrimitiveVector::<i32>::from(data),
        }
    }
}

pub struct DateIter<'a> {
    iter: PrimitiveIter<'a, i32>,
}

impl<'a> Iterator for DateIter<'a> {
    type Item = Option<Date>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.map(Date::new))
    }
}

impl ScalarVector for DateVector {
    type OwnedItem = Date;
    type RefItem<'a> = Date;
    type Iter<'a> = DateIter<'a>;

    type Builder = DateVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        self.array.get_data(idx).map(Date::new)
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        DateIter {
            iter: self.array.iter_data(),
        }
    }
}

impl Serializable for DateVector {
    fn serialize_to_json(&self) -> crate::error::Result<Vec<serde_json::Value>> {
        Ok(self
            .array
            .iter_data()
            .map(|v| v.map(Date::new))
            .map(|v| match v {
                None => serde_json::Value::Null,
                Some(v) => v.into(),
            })
            .collect::<Vec<_>>())
    }
}

pub struct DateVectorBuilder {
    buffer: PrimitiveVectorBuilder<i32>,
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
            buffer: PrimitiveVectorBuilder::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.buffer.push(value.map(|d| d.val()))
    }

    fn finish(&mut self) -> Self::VectorType {
        Self::VectorType {
            array: self.buffer.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_build_date_vector() {
        let mut builder = DateVectorBuilder::with_capacity(4);
        builder.push(Some(Date::new(1)));
        builder.push(None);
        builder.push(Some(Date::new(-1)));
        let vector = builder.finish();
        assert_eq!(3, vector.len());
        assert_eq!(Some(Date::new(1)), vector.get_data(0));
        assert_eq!(None, vector.get_data(1));
        assert_eq!(Some(Date::new(-1)), vector.get_data(2));
        let mut iter = vector.iter_data();
        assert_eq!(Some(Date::new(1)), iter.next().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!(Some(Date::new(-1)), iter.next().unwrap());
    }

    #[test]
    pub fn test_date_scalar() {
        let vector = DateVector::from_slice(&[Date::new(1), Date::new(2)]);
        assert_eq!(2, vector.len());
        assert_eq!(Some(Date::new(1)), vector.get_data(0));
        assert_eq!(Some(Date::new(2)), vector.get_data(1));
    }
}
