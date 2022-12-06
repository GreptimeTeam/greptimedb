// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use common_time::timestamp::{TimeUnit, Timestamp};
use snafu::OptionExt;

use crate::data_type::{ConcreteDataType, DataType};
use crate::error;
use crate::error::Result;
use crate::prelude::{
    MutableVector, ScalarVector, ScalarVectorBuilder, Validity, Value, ValueRef, Vector, VectorRef,
};
use crate::serialize::Serializable;
use crate::types::TimestampType;
use crate::vectors::{PrimitiveIter, PrimitiveVector, PrimitiveVectorBuilder};

/// `TimestampVector` stores timestamp in millisecond since UNIX Epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct TimestampVector {
    array: PrimitiveVector<i64>,
}

impl TimestampVector {
    pub fn new(array: PrimitiveArray<i64>) -> Self {
        Self {
            array: PrimitiveVector { array },
        }
    }

    pub fn try_from_arrow_array(array: impl AsRef<dyn Array>) -> Result<Self> {
        Ok(Self::new(
            array
                .as_ref()
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .with_context(|| error::ConversionSnafu {
                    from: format!("{:?}", array.as_ref().data_type()),
                })?
                .clone(),
        ))
    }

    pub fn from_values<I: IntoIterator<Item = i64>>(iter: I) -> Self {
        Self {
            array: PrimitiveVector {
                array: PrimitiveArray::from_values(iter),
            },
        }
    }

    pub(crate) fn as_arrow(&self) -> &dyn Array {
        self.array.as_arrow()
    }
}

impl Vector for TimestampVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::timestamp_millisecond_datatype()
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
            TimestampType::new(TimeUnit::Millisecond).as_arrow_type(),
            buffer,
            validity,
        ))
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        let validity = self.array.array.validity().cloned();
        let values = self.array.array.values().clone();
        Box::new(PrimitiveArray::new(
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            values,
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
        Arc::new(Self {
            array: PrimitiveVector {
                array: self.array.array.slice(offset, length),
            },
        })
    }

    fn get(&self, index: usize) -> Value {
        match self.array.get(index) {
            Value::Null => Value::Null,
            Value::Int64(v) => Value::Timestamp(Timestamp::from_millis(v)),
            _ => {
                unreachable!()
            }
        }
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        match self.array.get(index) {
            Value::Int64(v) => ValueRef::Timestamp(Timestamp::from_millis(v)),
            Value::Null => ValueRef::Null,
            _ => unreachable!(),
        }
    }
}

impl Serializable for TimestampVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
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
        self.array.get_data(idx).map(Timestamp::from_millis)
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
        self.iter.next().map(|v| v.map(Timestamp::from_millis))
    }
}

pub struct TimestampVectorBuilder {
    buffer: PrimitiveVectorBuilder<i64>,
}

impl MutableVector for TimestampVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::timestamp_millisecond_datatype()
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
        // TODO(hl): vector and vector builder should also support customized time unit.
        self.buffer.push(
            value
                .as_timestamp()?
                .map(|t| t.convert_to(TimeUnit::Millisecond)),
        );
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        let concrete_vector = vector
            .as_any()
            .downcast_ref::<TimestampVector>()
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
            .push(value.map(|v| v.convert_to(TimeUnit::Millisecond)));
    }

    fn finish(&mut self) -> Self::VectorType {
        Self::VectorType {
            array: self.buffer.finish(),
        }
    }
}

pub(crate) fn replicate_timestamp(vector: &TimestampVector, offsets: &[usize]) -> VectorRef {
    let array = crate::vectors::primitive::replicate_primitive_with_type(
        &vector.array,
        offsets,
        vector.data_type(),
    );
    Arc::new(TimestampVector { array })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_build_timestamp_vector() {
        let mut builder = TimestampVectorBuilder::with_capacity(3);
        builder.push(Some(Timestamp::new(1, TimeUnit::Second)));
        builder.push(None);
        builder.push(Some(Timestamp::new(2, TimeUnit::Millisecond)));

        let vector = builder.finish();
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            vector.data_type()
        );
        assert_eq!(3, vector.len());
        assert_eq!(
            Value::Timestamp(Timestamp::new(1000, TimeUnit::Millisecond)),
            vector.get(0)
        );

        assert_eq!(Value::Null, vector.get(1));
        assert_eq!(
            Value::Timestamp(Timestamp::new(2, TimeUnit::Millisecond)),
            vector.get(2)
        );

        assert_eq!(
            vec![
                Some(Timestamp::new(1000, TimeUnit::Millisecond)),
                None,
                Some(Timestamp::new(2, TimeUnit::Millisecond)),
            ],
            vector.iter_data().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_timestamp_from_arrow() {
        let vector =
            TimestampVector::from_slice(&[Timestamp::from_millis(1), Timestamp::from_millis(2)]);
        let arrow = vector.as_arrow().slice(0, vector.len());
        let vector2 = TimestampVector::try_from_arrow_array(&arrow).unwrap();
        assert_eq!(vector, vector2);
    }
}
