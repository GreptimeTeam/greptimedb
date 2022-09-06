use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray};
use common_time::timestamp::{TimeUnit, Timestamp};

use crate::data_type::{ConcreteDataType, DataType};
use crate::prelude::{
    MutableVector, ScalarVector, ScalarVectorBuilder, Validity, Value, Vector, VectorRef,
};
use crate::serialize::Serializable;
use crate::types::TimestampType;
use crate::vectors::{PrimitiveIter, PrimitiveVector, PrimitiveVectorBuilder};

/// `TimestampVector` stores timestamp in microseconds since UNIX Epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct TimestampVector {
    array: PrimitiveVector<i64>,
}

impl Vector for TimestampVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond)
    }

    fn vector_type_name(&self) -> String {
        "TimestampVector".to_string()
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
            TimestampType::new(TimeUnit::Microsecond).as_arrow_type(),
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
            Value::Null => Value::Null,
            Value::Int64(v) => Value::Timestamp(Timestamp::new(v, TimeUnit::Microsecond)),
            _ => {
                unreachable!()
            }
        }
    }

    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        self.array.replicate(offsets)
    }
}

impl TimestampVector {
    pub fn new(array: PrimitiveArray<i64>) -> Self {
        Self {
            array: PrimitiveVector::new(array),
        }
    }
}

impl Serializable for TimestampVector {
    fn serialize_to_json(&self) -> crate::Result<Vec<serde_json::Value>> {
        Ok(self
            .array
            .iter_data()
            .map(|v| match v {
                None => serde_json::Value::Null,
                Some(v) => v.into(),
            })
            .collect::<Vec<_>>())
    }
}

impl ScalarVector for TimestampVector {
    type OwnedItem = Timestamp;
    type RefItem<'a> = Timestamp;
    type Iter<'a> = TimestampDataIter<'a>;
    type Builder = TimestampVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        self.array
            .get_data(idx)
            .map(|v| Timestamp::new(v, TimeUnit::Microsecond))
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        TimestampDataIter {
            iter: self.array.iter_data(),
        }
    }
}

pub struct TimestampDataIter<'a> {
    iter: PrimitiveIter<'a, i64>,
}

impl<'a> Iterator for TimestampDataIter<'a> {
    type Item = Option<Timestamp>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|v| v.map(|v| Timestamp::new(v, TimeUnit::Microsecond)))
    }
}

pub struct TimestampVectorBuilder {
    buffer: PrimitiveVectorBuilder<i64>,
}

impl MutableVector for TimestampVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond)
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

impl ScalarVectorBuilder for TimestampVectorBuilder {
    type VectorType = TimestampVector;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: PrimitiveVectorBuilder::with_capacity(capacity),
        }
    }

    /// Pushes a Timestamp value into vector builder. The timestamp must be with time unit
    /// `Second`/`MilliSecond`/`Microsecond`.
    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        self.buffer
            .push(value.map(|v| v.unify_to(TimeUnit::Microsecond)));
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
    pub fn test_build_timestamp_vector() {
        let mut builder = TimestampVectorBuilder::with_capacity(3);
        builder.push(Some(Timestamp::new(1, TimeUnit::Second)));
        builder.push(None);
        builder.push(Some(Timestamp::new(2, TimeUnit::Microsecond)));

        let vector = builder.finish();
        assert_eq!(
            ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond),
            vector.data_type()
        );
        assert_eq!(3, vector.len());
        assert_eq!(
            Value::Timestamp(Timestamp::new(1000000, TimeUnit::Microsecond)),
            vector.get(0)
        );

        assert_eq!(Value::Null, vector.get(1));
        assert_eq!(
            Value::Timestamp(Timestamp::new(2, TimeUnit::Microsecond)),
            vector.get(2)
        );

        assert_eq!(
            vec![
                Some(Timestamp::new(1000000, TimeUnit::Microsecond)),
                None,
                Some(Timestamp::new(2, TimeUnit::Microsecond)),
            ],
            vector.iter_data().collect::<Vec<_>>()
        );
    }
}
