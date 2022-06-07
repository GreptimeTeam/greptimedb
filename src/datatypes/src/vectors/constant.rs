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
use crate::vectors::{Validity, Vector, VectorRef};

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

    fn get_unchecked(&self, _index: usize) -> Value {
        self.vector.get_unchecked(0)
    }

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
        write!(
            f,
            "ConstantVector([{:?}; {}])",
            self.get(0).unwrap_or(Value::Null),
            self.len()
        )
    }
}

impl Serializable for ConstantVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        vec![self.get(0)?; self.len()]
            .into_iter()
            .map(serde_json::to_value)
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
        assert_eq!(Validity::AllValid, c.validity());
        assert!(!c.only_null());

        for i in 0..10 {
            assert!(!c.is_null(i));
            assert_eq!(Value::Int32(1), c.get_unchecked(i));
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
