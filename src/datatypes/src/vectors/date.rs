use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use common_time::date::Date;
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::prelude::*;
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

    pub fn try_from_arrow_array(array: impl AsRef<dyn Array>) -> Result<Self> {
        Ok(Self::new(
            array
                .as_ref()
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .with_context(|| error::ConversionSnafu {
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

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        let validity = self.array.array.validity().cloned();
        let buffer = self.array.array.values().clone();
        Box::new(PrimitiveArray::new(
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

    fn get_ref(&self, index: usize) -> ValueRef {
        match self.array.get(index) {
            Value::Int32(v) => ValueRef::Date(Date::new(v)),
            Value::Null => ValueRef::Null,
            _ => {
                unreachable!()
            }
        }
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
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
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

    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        self.buffer.push(value.as_date()?.map(|d| d.val()));
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        let concrete_vector = vector
            .as_any()
            .downcast_ref::<DateVector>()
            .with_context(|| error::CastTypeSnafu {
                msg: format!(
                    "Failed to convert vector from {} to DateVector",
                    vector.vector_type_name()
                ),
            })?;
        self.buffer
            .extend_slice_of(&concrete_vector.array, offset, length)?;
        Ok(())
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
    use crate::data_type::DataType;
    use crate::types::DateType;

    #[test]
    fn test_build_date_vector() {
        let mut builder = DateVectorBuilder::with_capacity(4);
        builder.push(Some(Date::new(1)));
        builder.push(None);
        builder.push(Some(Date::new(-1)));
        let vector = builder.finish();
        assert_eq!(3, vector.len());
        assert_eq!(Value::Date(Date::new(1)), vector.get(0));
        assert_eq!(ValueRef::Date(Date::new(1)), vector.get_ref(0));
        assert_eq!(Some(Date::new(1)), vector.get_data(0));
        assert_eq!(None, vector.get_data(1));
        assert_eq!(Value::Null, vector.get(1));
        assert_eq!(ValueRef::Null, vector.get_ref(1));
        assert_eq!(Some(Date::new(-1)), vector.get_data(2));
        let mut iter = vector.iter_data();
        assert_eq!(Some(Date::new(1)), iter.next().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!(Some(Date::new(-1)), iter.next().unwrap());
    }

    #[test]
    fn test_date_scalar() {
        let vector = DateVector::from_slice(&[Date::new(1), Date::new(2)]);
        assert_eq!(2, vector.len());
        assert_eq!(Some(Date::new(1)), vector.get_data(0));
        assert_eq!(Some(Date::new(2)), vector.get_data(1));
    }

    #[test]
    fn test_date_vector_builder() {
        let input = DateVector::from_slice(&[Date::new(1), Date::new(2), Date::new(3)]);

        let mut builder = DateType::default().create_mutable_vector(3);
        builder
            .push_value_ref(ValueRef::Date(Date::new(5)))
            .unwrap();
        assert!(builder.push_value_ref(ValueRef::Int32(123)).is_err());
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice(&[13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(DateVector::from_slice(&[
            Date::new(5),
            Date::new(2),
            Date::new(3),
        ]));
        assert_eq!(expect, vector);
    }
}
