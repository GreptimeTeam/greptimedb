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
use std::sync::Arc;

use arrow::array::NullBufferBuilder;
use arrow::compute::TakeOptions;
use arrow::datatypes::DataType as ArrowDataType;
use arrow_array::{Array, ArrayRef, StructArray};
use datafusion_common::ScalarValue;
use snafu::{ResultExt, ensure};

use crate::error::{
    ArrowComputeSnafu, ConversionSnafu, Error, InconsistentStructFieldsAndItemsSnafu, Result,
    SerializeSnafu, UnsupportedOperationSnafu,
};
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
    pub fn try_new(fields: StructType, array: StructArray) -> Result<Self> {
        ensure!(
            fields.fields().len() == array.fields().len(),
            InconsistentStructFieldsAndItemsSnafu {
                field_len: fields.fields().len(),
                item_len: array.fields().len(),
            }
        );
        Ok(StructVector { array, fields })
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

        let values = (0..self.fields.fields().len())
            .map(|i| {
                let field_array = &self.array.column(i);

                if field_array.is_null(i) {
                    Value::Null
                } else {
                    let scalar_value = ScalarValue::try_from_array(field_array, index).unwrap();
                    Value::try_from(scalar_value).unwrap()
                }
            })
            .collect();

        Value::Struct(StructValue::try_new(values, self.fields.clone()).unwrap())
    }

    fn get_ref(&self, index: usize) -> ValueRef<'_> {
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
        Arc::new(StructVector::try_new(self.fields.clone(), replicated_array).unwrap())
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
        let vectors = self
            .array
            .columns()
            .iter()
            .map(|value_array| Helper::try_into_vector(value_array))
            .collect::<Result<Vec<_>>>()?;

        (0..self.array.len())
            .map(|idx| {
                let mut result = serde_json::Map::with_capacity(vectors.len());
                for (field, vector) in self.fields.fields().iter().zip(vectors.iter()) {
                    let field_value = vector.get(idx);
                    result.insert(
                        field.name().to_string(),
                        field_value.try_into().context(SerializeSnafu)?,
                    );
                }
                Ok(result.into())
            })
            .collect::<Result<Vec<serde_json::Value>>>()
    }
}

impl TryFrom<StructArray> for StructVector {
    type Error = Error;

    fn try_from(array: StructArray) -> Result<Self> {
        let fields = match array.data_type() {
            ArrowDataType::Struct(fields) => StructType::try_from(fields)?,
            other => ConversionSnafu {
                from: other.to_string(),
            }
            .fail()?,
        };
        Ok(Self { array, fields })
    }
}

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
        for (index, value) in struct_value.items().iter().enumerate() {
            self.value_builders[index].try_push_value_ref(&value.as_value_ref())?;
        }
        self.null_buffer.append_non_null();

        Ok(())
    }

    fn push_null_struct_value(&mut self) {
        for builder in &mut self.value_builders {
            builder.push_null();
        }
        self.null_buffer.append_null();
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
                StructValueRef::RefList { val, fields } => {
                    let struct_value = StructValue::try_new(
                        val.iter().map(|v| Value::from(v.clone())).collect(),
                        fields.clone(),
                    )?;
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
        self.push_null_struct_value();
    }
}

impl ScalarVectorBuilder for StructVectorBuilder {
    type VectorType = StructVector;

    fn with_capacity(_capacity: usize) -> Self {
        panic!("Must use StructVectorBuilder::with_type_capacity()");
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
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

        StructVector::try_new(self.fields.clone(), struct_array).unwrap()
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
        StructVector::try_new(self.fields.clone(), struct_array).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StructField;
    use crate::value::ListValue;
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

        let value = vector.get(2);
        if let Value::Struct(struct_value) = value {
            assert_eq!(struct_value.struct_type(), &struct_type);
            let mut items = struct_value.items().iter();
            assert_eq!(items.next(), Some(&Value::Int32(1)));
            assert_eq!(items.next(), Some(&Value::String("tom".into())));
            assert_eq!(items.next(), Some(&Value::UInt8(25)));
            assert_eq!(items.next(), Some(&Value::String("94038".into())));
            assert_eq!(items.next(), Some(&Value::List(build_list_value())));
            assert_eq!(items.next(), None);
        } else {
            panic!("Expected a struct value");
        }
    }

    #[test]
    fn test_deep_nested_struct_list() {
        // level 1: struct
        let struct_type = ConcreteDataType::struct_datatype(build_struct_type());
        let struct_value = build_struct_value();
        // level 2: list
        let list_type = ConcreteDataType::list_datatype(struct_type.clone());
        let list_value = ListValue::new(
            vec![
                Value::Struct(struct_value.clone()),
                Value::Struct(struct_value.clone()),
            ],
            struct_type.clone(),
        );
        // level 3: struct
        let root_type = StructType::new(vec![StructField::new(
            "items".to_string(),
            list_type,
            false,
        )]);
        let root_value = StructValue::new(vec![Value::List(list_value)], root_type.clone());

        let mut builder = StructVectorBuilder::with_type_and_capacity(root_type.clone(), 20);
        builder.push(Some(StructValueRef::Ref(&root_value)));

        let vector = builder.finish();
        assert_eq!(vector.len(), 1);
        assert_eq!(vector.null_count(), 0);
        assert_eq!(
            vector.data_type(),
            ConcreteDataType::struct_datatype(root_type)
        );
        assert_eq!(vector.get(0), Value::Struct(root_value));
    }
}
