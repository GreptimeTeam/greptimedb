use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use common_time::datetime::DateTime;
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::ConversionSnafu;
use crate::prelude::{
    MutableVector, ScalarVector, ScalarVectorBuilder, Validity, Value, ValueRef, Vector, VectorRef,
};
use crate::serialize::Serializable;
use crate::vectors::{PrimitiveIter, PrimitiveVector, PrimitiveVectorBuilder};

#[derive(Debug, Clone, PartialEq)]
pub struct DateTimeVector {
    array: PrimitiveVector<i64>,
}

impl DateTimeVector {
    pub fn new(array: PrimitiveArray<i64>) -> Self {
        Self {
            array: PrimitiveVector { array },
        }
    }

    pub fn try_from_arrow_array(array: impl AsRef<dyn Array>) -> crate::error::Result<Self> {
        Ok(Self::new(
            array
                .as_ref()
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .with_context(|| ConversionSnafu {
                    from: format!("{:?}", array.as_ref().data_type()),
                })?
                .clone(),
        ))
    }
}

impl Vector for DateTimeVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::datetime_datatype()
    }

    fn vector_type_name(&self) -> String {
        "DateTimeVector".to_string()
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
            arrow::datatypes::DataType::Date64,
            buffer,
            validity,
        ))
    }

    fn to_box_arrow_array(&self) -> Box<dyn Array> {
        let validity = self.array.array.validity().cloned();
        let buffer = self.array.array.values().clone();
        Box::new(PrimitiveArray::new(
            arrow::datatypes::DataType::Date64,
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
            Value::Int64(v) => Value::DateTime(DateTime::new(v)),
            Value::Null => Value::Null,
            _ => {
                unreachable!()
            }
        }
    }

    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        self.array.replicate(offsets)
    }

    fn cmp_element(&self, i: usize, other: &dyn Vector, j: usize) -> Ordering {
        let right = other.as_any().downcast_ref::<DateTimeVector>().unwrap();
        self.get(i).cmp(&right.get(j))
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        match self.array.get(index) {
            Value::Int64(v) => ValueRef::DateTime(DateTime::new(v)),
            Value::Null => ValueRef::Null,
            _ => {
                unreachable!()
            }
        }
    }
}

impl Serializable for DateTimeVector {
    fn serialize_to_json(&self) -> crate::Result<Vec<serde_json::Value>> {
        Ok(self
            .array
            .iter_data()
            .map(|v| v.map(DateTime::new))
            .map(|v| match v {
                None => serde_json::Value::Null,
                Some(v) => v.into(),
            })
            .collect::<Vec<_>>())
    }
}

impl From<Vec<Option<i64>>> for DateTimeVector {
    fn from(data: Vec<Option<i64>>) -> Self {
        Self {
            array: PrimitiveVector::<i64>::from(data),
        }
    }
}

pub struct DateTimeVectorBuilder {
    buffer: PrimitiveVectorBuilder<i64>,
}

impl ScalarVectorBuilder for DateTimeVectorBuilder {
    type VectorType = DateTimeVector;

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

impl MutableVector for DateTimeVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::datetime_datatype()
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

pub struct DateTimeIter<'a> {
    iter: PrimitiveIter<'a, i64>,
}

impl<'a> Iterator for DateTimeIter<'a> {
    type Item = Option<DateTime>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.map(DateTime::new))
    }
}

impl ScalarVector for DateTimeVector {
    type OwnedItem = DateTime;
    type RefItem<'a> = DateTime;
    type Iter<'a> = DateTimeIter<'a>;
    type Builder = DateTimeVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        self.array.get_data(idx).map(DateTime::new)
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        DateTimeIter {
            iter: self.array.iter_data(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    pub fn test_datetime_vector() {
        let v = DateTimeVector::new(PrimitiveArray::from_vec(vec![1, 2, 3]));
        assert_eq!(ConcreteDataType::datetime_datatype(), v.data_type());
        assert_eq!(3, v.len());
        assert_eq!("DateTimeVector", v.vector_type_name());
        assert_eq!(
            &arrow::datatypes::DataType::Date64,
            v.to_arrow_array().data_type()
        );
        let mut iter = v.iter_data();
        assert_eq!(Some(DateTime::new(1)), iter.next().unwrap());
        assert_eq!(Some(DateTime::new(2)), iter.next().unwrap());
        assert_eq!(Some(DateTime::new(3)), iter.next().unwrap());
        assert!(!v.is_null(0));
        assert_eq!(24, v.memory_size()); // size of i64 * 3

        assert_matches!(v.validity(), Validity::AllValid);
        if let Value::DateTime(d) = v.get(0) {
            assert_eq!(1, d.val());
        } else {
            unreachable!()
        }
        assert_eq!(
            "[\"1970-01-01 00:00:01\",\"1970-01-01 00:00:02\",\"1970-01-01 00:00:03\"]",
            serde_json::to_string(&v.serialize_to_json().unwrap()).unwrap()
        );
    }

    #[test]
    pub fn test_datetime_vector_builder() {
        let mut builder = DateTimeVectorBuilder::with_capacity(3);
        builder.push(Some(DateTime::new(1)));
        builder.push(None);
        builder.push(Some(DateTime::new(-1)));

        let v = builder.finish();
        assert_eq!(ConcreteDataType::datetime_datatype(), v.data_type());
        assert_eq!(Value::DateTime(DateTime::new(1)), v.get(0));
        assert_eq!(Value::Null, v.get(1));
        assert_eq!(Value::DateTime(DateTime::new(-1)), v.get(2));
    }
}
