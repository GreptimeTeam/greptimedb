use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use common_time::datetime::DateTime;
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::ConversionSnafu;
use crate::prelude::{
    MutableVector, ScalarVector, ScalarVectorBuilder, Validity, Value, Vector, VectorRef,
};
use crate::serialize::Serializable;
use crate::vectors::{PrimitiveIter, PrimitiveVector, PrimitiveVectorBuilder};

#[derive(Debug, Clone)]
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
            Value::Int64(v) => {
                Value::DateTime(DateTime::try_new(v).expect("Not expected to overflow here"))
            }
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

impl Serializable for DateTimeVector {
    fn serialize_to_json(&self) -> crate::Result<Vec<serde_json::Value>> {
        Ok(self
            .array
            .iter_data()
            .map(|v| v.map(|d| DateTime::try_new(d).unwrap()))
            .map(|v| match v {
                None => serde_json::Value::Null,
                Some(v) => v.into(),
            })
            .collect::<Vec<_>>())
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
        self.iter
            .next()
            .map(|v| v.map(|v| DateTime::try_new(v).unwrap()))
    }
}

impl ScalarVector for DateTimeVector {
    type OwnedItem = DateTime;
    type RefItem<'a> = DateTime;
    type Iter<'a> = DateTimeIter<'a>;
    type Builder = DateTimeVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        self.array
            .get_data(idx)
            .map(|v| DateTime::try_new(v).unwrap())
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        DateTimeIter {
            iter: self.array.iter_data(),
        }
    }
}
