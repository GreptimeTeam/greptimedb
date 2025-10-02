// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::NullBufferBuilder;
use arrow::compute::TakeOptions;
use arrow::datatypes::DataType as ArrowDataType;
use arrow_array::{Array, ArrayRef, StructArray};
use snafu::ResultExt;

use crate::error::{ArrowComputeSnafu, Result, SerializeSnafu, UnsupportedOperationSnafu};
use crate::prelude::{ConcreteDataType, DataType, ScalarVector, ScalarVectorBuilder};
use crate::serialize::Serializable;
use crate::types::StructType;
use crate::value::{StructValue, StructValueRef, Value, ValueRef};
use crate::vectors::operations::VectorOp;
use crate::vectors::{self, Helper, MutableVector, Validity, Vector, VectorRef};

/// A simple wrapper around `StructArray` to represent a vector of structs in GreptimeDB.
#[derive(Debug, PartialEq)]
pub struct StructVector {
    array: StructArray,
    fields: StructType,
}

impl StructVector {
    pub fn new(fields: StructType, array: StructArray) -> Self {
        StructVector { array, fields }
    }

    pub fn array(&self) -> &StructArray {
        &self.array
    }

    pub fn as_arrow(&self) -> &dyn Array {
        &self.array
    }

    pub fn struct_type(&self) -> &StructType {
        &self.fields
    }
}

impl Vector for StructVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::struct_datatype(self.fields.clone())
    }

    fn vector_type_name(&self) -> String {
        "StructVector".to_string()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        Arc::new(self.array.clone())
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        Box::new(self.array.clone())
    }

    fn validity(&self) -> Validity {
        vectors::impl_validity_for_vector!(self.array)
    }

    fn memory_size(&self) -> usize {
        self.array.get_buffer_memory_size()
    }

    fn null_count(&self) -> usize {
        self.array.null_count()
    }

    fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        Arc::new(StructVector {
            array: self.array.slice(offset, length),
            fields: self.fields.clone(),
        })
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        let mut values = BTreeMap::new();
        for (i, field) in self.fields.fields().iter().enumerate() {
            let field_array = &self.array.column(i);
            let field_vector = Helper::try_into_vector(field_array).unwrap_or_else(|_| {
                panic!(
                    "arrow array with datatype {:?} cannot converted to our vector",
                    field_array.data_type()
                )
            });

            values.insert(field.name().to_string(), field_vector.get(index));
        }

        Value::Struct(StructValue::new(values, self.fields.clone()))
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        ValueRef::Struct(StructValueRef::Indexed {
            vector: self,
            idx: index,
        })
    }
}

impl VectorOp for StructVector {
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        let column_arrays = self
            .array
            .columns()
            .iter()
            .map(|col| {
                let vector = Helper::try_into_vector(col)
                    .expect("Failed to replicate struct vector columns");
                vector.replicate(offsets).to_arrow_array()
            })
            .collect::<Vec<_>>();
        let replicated_array = StructArray::new(
            self.array.fields().clone(),
            column_arrays,
            self.array.nulls().cloned(),
        );
        Arc::new(StructVector::new(self.fields.clone(), replicated_array))
    }

    fn cast(&self, _to_type: &ConcreteDataType) -> Result<VectorRef> {
        UnsupportedOperationSnafu {
            op: "cast",
            vector_type: self.vector_type_name(),
        }
        .fail()
    }

    fn filter(&self, filter: &vectors::BooleanVector) -> Result<VectorRef> {
        let filtered =
            datafusion_common::arrow::compute::filter(&self.array, filter.as_boolean_array())
                .context(ArrowComputeSnafu)
                .and_then(Helper::try_into_vector)?;
        Ok(filtered)
    }

    fn take(&self, indices: &vectors::UInt32Vector) -> Result<VectorRef> {
        let take_result = datafusion_common::arrow::compute::take(
            &self.array,
            indices.as_arrow(),
            Some(TakeOptions { check_bounds: true }),
        )
        .context(ArrowComputeSnafu)
        .and_then(Helper::try_into_vector)?;
        Ok(take_result)
    }
}

impl Serializable for StructVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        let mut vectors = BTreeMap::new();
        for (field, value) in self.array.fields().iter().zip(self.array.columns().iter()) {
            let value_vector = Helper::try_into_vector(value)?;
            vectors.insert(field.name().clone(), value_vector);
        }

        let mut results = Vec::with_capacity(self.array().len());
        for idx in 0..self.array().len() {
            let mut result = serde_json::Map::new();
            for field in vectors.keys() {
                let field_value = vectors.get(field).unwrap().get(idx);
                result.insert(
                    field.to_string(),
                    field_value.try_into().context(SerializeSnafu)?,
                );
            }

            results.push(result.into());
        }

        Ok(results)
    }
}

impl From<StructArray> for StructVector {
    fn from(array: StructArray) -> Self {
        let fields = match array.data_type() {
            ArrowDataType::Struct(fields) => {
                StructType::try_from(fields).expect("Failed to create StructType")
            }
            other => panic!("Try to create StructVector from an arrow array with type {other:?}"),
        };
        Self { array, fields }
    }
}

vectors::impl_try_from_arrow_array_for_vector!(StructArray, StructVector);

impl ScalarVector for StructVector {
    type OwnedItem = StructValue;
    type RefItem<'a> = StructValueRef<'a>;
    type Iter<'a> = StructIter<'a>;
    type Builder = StructVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(StructValueRef::Indexed { vector: self, idx })
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        StructIter::new(self)
    }
}

pub struct StructIter<'a> {
    vector: &'a StructVector,
    index: usize,
}

impl<'a> StructIter<'a> {
    pub fn new(vector: &'a StructVector) -> Self {
        Self { vector, index: 0 }
    }
}

impl<'a> Iterator for StructIter<'a> {
    type Item = Option<StructValueRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.vector.len() {
            let idx = self.index;
            self.index += 1;

            if self.vector.is_null(idx) {
                Some(None)
            } else {
                let value = StructValueRef::Indexed {
                    vector: self.vector,
                    idx,
                };

                Some(Some(value))
            }
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.vector.len(), Some(self.vector.len()))
    }
}

pub struct StructVectorBuilder {
    value_builders: Vec<Box<dyn MutableVector>>,
    null_buffer: NullBufferBuilder,
    fields: StructType,
}

impl StructVectorBuilder {
    pub fn with_type_and_capacity(fields: StructType, capacity: usize) -> Self {
        let value_builders = fields
            .fields()
            .iter()
            .map(|f| f.data_type().create_mutable_vector(capacity))
            .collect();
        Self {
            value_builders,
            null_buffer: NullBufferBuilder::new(capacity),
            fields,
        }
    }

    fn push_struct_value(&mut self, struct_value: &StructValue) -> Result<()> {
        for (index, field) in self.fields.fields().iter().enumerate() {
            let value = struct_value
                .items()
                .get(field.name())
                .unwrap_or(&Value::Null);
            self.value_builders[index].try_push_value_ref(&value.as_value_ref())?;
        }
        self.null_buffer.append_non_null();

        Ok(())
    }

    fn push_null_struct_value(&mut self) -> Result<()> {
        for builder in &mut self.value_builders {
            builder.push_null();
        }
        self.null_buffer.append_null();

        Ok(())
    }
}

impl MutableVector for StructVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::struct_datatype(self.fields.clone())
    }

    fn len(&self) -> usize {
        self.null_buffer.len()
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

    fn to_vector_cloned(&self) -> VectorRef {
        Arc::new(self.finish_cloned())
    }

    fn try_push_value_ref(&mut self, value: &ValueRef) -> Result<()> {
        if let Some(struct_ref) = value.as_struct()? {
            match struct_ref {
                StructValueRef::Indexed { vector, idx } => match vector.get(idx).as_struct()? {
                    Some(struct_value) => self.push_struct_value(struct_value)?,
                    None => self.push_null(),
                },
                StructValueRef::Ref(val) => self.push_struct_value(val)?,
                StructValueRef::RefMap { val, fields } => {
                    let struct_value = StructValue::new(
                        val.iter()
                            .map(|(key, v)| (key.to_string(), Value::from(v.clone())))
                            .collect(),
                        fields.clone(),
                    );
                    self.push_struct_value(&struct_value)?;
                }
            }
        } else {
            self.push_null();
        }

        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        for idx in offset..offset + length {
            let value = vector.get_ref(idx);
            self.try_push_value_ref(&value)?;
        }

        Ok(())
    }

    fn push_null(&mut self) {
        self.push_null_struct_value().expect("failed to push null");
    }
}

impl ScalarVectorBuilder for StructVectorBuilder {
    type VectorType = StructVector;

    fn with_capacity(_capacity: usize) -> Self {
        panic!("Must use StructVectorBuilder::with_type_capacity()");
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        // We expect the input ListValue has the same inner type as the builder when using
        // push(), so just panic if `push_value_ref()` returns error, which indicate an
        // invalid input value type.
        self.try_push_value_ref(&value.map(ValueRef::Struct).unwrap_or(ValueRef::Null))
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to push value, expect value type {:?}, err:{}",
                    self.fields, e
                );
            });
    }

    fn finish(&mut self) -> Self::VectorType {
        let arrays = self
            .value_builders
            .iter_mut()
            .map(|b| b.to_vector().to_arrow_array())
            .collect();
        let struct_array = StructArray::new(
            self.fields.as_arrow_fields(),
            arrays,
            self.null_buffer.finish(),
        );

        StructVector::new(self.fields.clone(), struct_array)
    }

    fn finish_cloned(&self) -> Self::VectorType {
        let arrays = self
            .value_builders
            .iter()
            .map(|b| b.to_vector_cloned().to_arrow_array())
            .collect();

        let struct_array = StructArray::new(
            self.fields.as_arrow_fields(),
            arrays,
            self.null_buffer.finish_cloned(),
        );
        StructVector::new(self.fields.clone(), struct_array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::tests::*;

    #[test]
    fn test_struct_vector_builder() {
        let struct_type = build_struct_type();

        let struct_values = (0..10).map(|_| build_struct_value());
        let mut builder = StructVectorBuilder::with_type_and_capacity(struct_type.clone(), 20);
        for value in struct_values {
            builder.push(Some(StructValueRef::Ref(&value)));
        }

        builder.push_nulls(5);

        let vector = builder.finish();
        assert_eq!(
            vector.data_type(),
            ConcreteDataType::struct_datatype(struct_type.clone())
        );
        assert_eq!(vector.len(), 15);
        assert_eq!(vector.null_count(), 5);

        let mut null_count = 0;
        for item in vector.iter_data() {
            if let Some(value) = item.as_ref() {
                assert_eq!(value.struct_type(), &struct_type);
            } else {
                null_count += 1;
            }
        }
        assert_eq!(5, null_count);
    }
}
