use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, MutableArray, MutablePrimitiveArray};
use common_time::date::Date;

use crate::data_type::ConcreteDataType;
use crate::prelude::{ScalarVectorBuilder, Validity, Value, Vector, VectorRef};
use crate::scalars::ScalarVector;
use crate::serialize::Serializable;
use crate::vectors::{MutableVector, PrimitiveIter, PrimitiveVector};

#[derive(Debug, Clone)]
pub struct DateVector {
    array: PrimitiveVector<i32>,
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
        self.array.to_arrow_array()
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
        self.array.serialize_to_json()
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
        self.buffer.push(value.map(|v| v.0))
    }

    fn finish(&mut self) -> Self::VectorType {
        Self::VectorType {
            array: PrimitiveVector::new(std::mem::take(&mut self.buffer).into()),
        }
    }
}
