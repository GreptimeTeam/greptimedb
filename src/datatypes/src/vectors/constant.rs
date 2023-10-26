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
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array};
use snafu::{ensure, ResultExt};

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result, SerializeSnafu};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::{BooleanVector, Helper, UInt32Vector, Validity, Vector, VectorRef};

#[derive(Clone)]
pub struct ConstantVector {
    length: usize,
    vector: VectorRef,
}

impl ConstantVector {
    /// Create a new [ConstantVector].
    ///
    /// # Panics
    /// Panics if `vector.len() != 1`.
    pub fn new(vector: VectorRef, length: usize) -> Self {
        assert_eq!(1, vector.len());

        // Avoid const recursion.
        if vector.is_const() {
            let vec: &ConstantVector = unsafe { Helper::static_cast(&vector) };
            return Self::new(vec.inner().clone(), length);
        }
        Self { vector, length }
    }

    pub fn inner(&self) -> &VectorRef {
        &self.vector
    }

    /// Returns the constant value.
    pub fn get_constant_ref(&self) -> ValueRef {
        self.vector.get_ref(0)
    }

    pub(crate) fn replicate_vector(&self, offsets: &[usize]) -> VectorRef {
        assert_eq!(offsets.len(), self.len());

        if offsets.is_empty() {
            return self.slice(0, 0);
        }

        Arc::new(ConstantVector::new(
            self.vector.clone(),
            *offsets.last().unwrap(),
        ))
    }

    pub(crate) fn filter_vector(&self, filter: &BooleanVector) -> Result<VectorRef> {
        let length = self.len() - filter.false_count();
        if length == self.len() {
            return Ok(Arc::new(self.clone()));
        }
        Ok(Arc::new(ConstantVector::new(self.inner().clone(), length)))
    }

    pub(crate) fn cast_vector(&self, to_type: &ConcreteDataType) -> Result<VectorRef> {
        Ok(Arc::new(ConstantVector::new(
            self.inner().cast(to_type)?,
            self.length,
        )))
    }

    pub(crate) fn take_vector(&self, indices: &UInt32Vector) -> Result<VectorRef> {
        if indices.is_empty() {
            return Ok(self.slice(0, 0));
        }
        ensure!(
            indices.null_count() == 0,
            error::UnsupportedOperationSnafu {
                op: "taking a null index",
                vector_type: self.vector_type_name(),
            }
        );

        let len = self.len();
        let arr = indices.to_arrow_array();
        let indices_arr = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
        if !arrow::compute::min_boolean(
            &arrow::compute::kernels::cmp::lt(indices_arr, &UInt32Array::new_scalar(len as u32))
                .unwrap(),
        )
        .unwrap()
        {
            panic!("Array index out of bounds, cannot take index out of the length of the array: {len}");
        }

        Ok(Arc::new(ConstantVector::new(
            self.inner().clone(),
            indices.len(),
        )))
    }
}

impl Vector for ConstantVector {
    fn data_type(&self) -> ConcreteDataType {
        self.vector.data_type()
    }

    fn vector_type_name(&self) -> String {
        "ConstantVector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.length
    }

    fn to_arrow_array(&self) -> ArrayRef {
        let v = self.vector.replicate(&[self.length]);
        v.to_arrow_array()
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        let v = self.vector.replicate(&[self.length]);
        v.to_boxed_arrow_array()
    }

    fn is_const(&self) -> bool {
        true
    }

    fn validity(&self) -> Validity {
        if self.vector.is_null(0) {
            Validity::all_null(self.length)
        } else {
            Validity::all_valid(self.length)
        }
    }

    fn memory_size(&self) -> usize {
        self.vector.memory_size()
    }

    fn is_null(&self, _row: usize) -> bool {
        self.vector.is_null(0)
    }

    fn only_null(&self) -> bool {
        self.vector.is_null(0)
    }

    fn slice(&self, _offset: usize, length: usize) -> VectorRef {
        Arc::new(Self {
            vector: self.vector.clone(),
            length,
        })
    }

    fn get(&self, _index: usize) -> Value {
        self.vector.get(0)
    }

    fn get_ref(&self, _index: usize) -> ValueRef {
        self.vector.get_ref(0)
    }

    fn null_count(&self) -> usize {
        if self.only_null() {
            self.len()
        } else {
            0
        }
    }
}

impl fmt::Debug for ConstantVector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConstantVector([{:?}; {}])", self.get(0), self.len())
    }
}

impl Serializable for ConstantVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        std::iter::repeat(self.get(0))
            .take(self.len())
            .map(serde_json::Value::try_from)
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;

    use super::*;
    use crate::vectors::Int32Vector;

    #[test]
    fn test_constant_vector_misc() {
        let a = Int32Vector::from_slice(vec![1]);
        let c = ConstantVector::new(Arc::new(a), 10);

        assert_eq!("ConstantVector", c.vector_type_name());
        assert!(c.is_const());
        assert_eq!(10, c.len());
        assert!(c.validity().is_all_valid());
        assert!(!c.only_null());
        assert_eq!(64, c.memory_size());

        for i in 0..10 {
            assert!(!c.is_null(i));
            assert_eq!(Value::Int32(1), c.get(i));
        }

        let arrow_arr = c.to_arrow_array();
        assert_eq!(10, arrow_arr.len());
        assert_eq!(&ArrowDataType::Int32, arrow_arr.data_type());
    }

    #[test]
    fn test_debug_null_array() {
        let a = Int32Vector::from_slice(vec![1]);
        let c = ConstantVector::new(Arc::new(a), 10);

        let s = format!("{c:?}");
        assert_eq!(s, "ConstantVector([Int32(1); 10])");
    }

    #[test]
    fn test_serialize_json() {
        let a = Int32Vector::from_slice(vec![1]);
        let c = ConstantVector::new(Arc::new(a), 10);

        let s = serde_json::to_string(&c.serialize_to_json().unwrap()).unwrap();
        assert_eq!(s, "[1,1,1,1,1,1,1,1,1,1]");
    }
}
