use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use snafu::ResultExt;

use crate::data_type::ConcreteDataType;
use crate::error::{Result, SerializeSnafu};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::Helper;
use crate::vectors::{BooleanVector, Validity, Vector, VectorRef};

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
            Validity::AllNull
        } else {
            Validity::AllValid
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

pub(crate) fn replicate_constant(vector: &ConstantVector, offsets: &[usize]) -> VectorRef {
    assert_eq!(offsets.len(), vector.len());

    if offsets.is_empty() {
        return vector.slice(0, 0);
    }

    Arc::new(ConstantVector::new(
        vector.vector.clone(),
        *offsets.last().unwrap(),
    ))
}

pub(crate) fn filter_constant(
    vector: &ConstantVector,
    filter: &BooleanVector,
) -> Result<VectorRef> {
    let length = filter.len() - filter.as_boolean_array().values().null_count();
    if length == vector.len() {
        return Ok(Arc::new(vector.clone()));
    }
    Ok(Arc::new(ConstantVector::new(
        vector.inner().clone(),
        length,
    )))
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
        assert_eq!(Validity::AllValid, c.validity());
        assert!(!c.only_null());
        assert_eq!(4, c.memory_size());

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

        let s = format!("{:?}", c);
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
