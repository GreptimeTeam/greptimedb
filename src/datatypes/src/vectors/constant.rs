use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::ArrayRef;
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::{Result, SerializeSnafu};
use crate::serialize::Serializable;
use crate::value::Value;
use crate::vectors::Helper;
use crate::vectors::{Vector, VectorRef};

#[derive(Clone)]
pub struct ConstantVector {
    length: usize,
    vector: VectorRef,
}

impl ConstantVector {
    pub fn new(vector: VectorRef, length: usize) -> Self {
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
}

impl Vector for ConstantVector {
    fn data_type(&self) -> ConcreteDataType {
        self.vector.data_type()
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

    fn is_const(&self) -> bool {
        true
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
    // just for resize
    fn replicate(&self, offsets: &[usize]) -> VectorRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        Arc::new(Self::new(self.vector.clone(), *offsets.last().unwrap()))
    }
}

impl fmt::Debug for ConstantVector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConstantVector({})", self.len())
    }
}

impl Serializable for ConstantVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        vec![self.get(0); self.len()]
            .into_iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<_>>()
            .context(SerializeSnafu)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use super::*;

    #[test]
    fn test_constant_vector() {}

    #[test]
    fn test_debug_null_array() {}

    #[test]
    fn test_serialize_json() {}
}
