use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, NullArray};
use arrow::datatypes::DataType as ArrowDataType;
use snafu::{ensure, OptionExt};

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::serialize::Serializable;
use crate::types::NullType;
use crate::value::{Value, ValueRef};
use crate::vectors::{self, MutableVector, Validity, Vector, VectorRef};

#[derive(PartialEq)]
pub struct NullVector {
    array: NullArray,
}

impl NullVector {
    pub fn new(n: usize) -> Self {
        Self {
            array: NullArray::new(ArrowDataType::Null, n),
        }
    }

    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl From<NullArray> for NullVector {
    fn from(array: NullArray) -> Self {
        Self { array }
    }
}

impl Vector for NullVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Null(NullType::default())
    }

    fn vector_type_name(&self) -> String {
        "NullVector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
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
        Validity::AllNull
    }

    fn memory_size(&self) -> usize {
        0
    }

    fn is_null(&self, _row: usize) -> bool {
        true
    }

    fn only_null(&self) -> bool {
        true
    }

    fn slice(&self, _offset: usize, length: usize) -> VectorRef {
        Arc::new(Self::new(length))
    }

    fn get(&self, _index: usize) -> Value {
        // Skips bound check for null array.
        Value::Null
    }

    fn get_ref(&self, _index: usize) -> ValueRef {
        // Skips bound check for null array.
        ValueRef::Null
    }
}

impl fmt::Debug for NullVector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NullVector({})", self.len())
    }
}

impl Serializable for NullVector {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        Ok(std::iter::repeat(serde_json::Value::Null)
            .take(self.len())
            .collect())
    }
}

vectors::impl_try_from_arrow_array_for_vector!(NullArray, NullVector);

#[derive(Default)]
pub struct NullVectorBuilder {
    length: usize,
}

impl MutableVector for NullVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::null_datatype()
    }

    fn len(&self) -> usize {
        self.length
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_vector(&mut self) -> VectorRef {
        let vector = Arc::new(NullVector::new(self.length));
        self.length = 0;
        vector
    }

    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        ensure!(
            value.is_null(),
            error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to null", value),
            }
        );

        self.length += 1;
        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        vector
            .as_any()
            .downcast_ref::<NullVector>()
            .with_context(|| error::CastTypeSnafu {
                msg: format!(
                    "Failed to convert vector from {} to NullVector",
                    vector.vector_type_name()
                ),
            })?;
        assert!(
            offset + length <= vector.len(),
            "offset {} + length {} must less than {}",
            offset,
            length,
            vector.len()
        );

        self.length += length;
        Ok(())
    }
}

pub(crate) fn replicate_null(vector: &NullVector, offsets: &[usize]) -> VectorRef {
    assert_eq!(offsets.len(), vector.len());

    if offsets.is_empty() {
        return vector.slice(0, 0);
    }

    Arc::new(NullVector::new(*offsets.last().unwrap()))
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;
    use crate::data_type::DataType;

    #[test]
    fn test_null_vector_misc() {
        let v = NullVector::new(32);

        assert_eq!(v.len(), 32);
        assert_eq!(0, v.memory_size());
        let arrow_arr = v.to_arrow_array();
        assert_eq!(arrow_arr.null_count(), 32);

        let array2 = arrow_arr.slice(8, 16);
        assert_eq!(array2.len(), 16);
        assert_eq!(array2.null_count(), 16);

        assert_eq!("NullVector", v.vector_type_name());
        assert!(!v.is_const());
        assert_eq!(Validity::AllNull, v.validity());
        assert!(v.only_null());

        for i in 0..32 {
            assert!(v.is_null(i));
            assert_eq!(Value::Null, v.get(i));
            assert_eq!(ValueRef::Null, v.get_ref(i));
        }
    }

    #[test]
    fn test_debug_null_vector() {
        let array = NullVector::new(1024 * 1024);
        assert_eq!(format!("{:?}", array), "NullVector(1048576)");
    }

    #[test]
    fn test_serialize_json() {
        let vector = NullVector::new(3);
        let json_value = vector.serialize_to_json().unwrap();
        assert_eq!(
            "[null,null,null]",
            serde_json::to_string(&json_value).unwrap()
        );
    }

    #[test]
    fn test_null_vector_validity() {
        let vector = NullVector::new(5);
        assert_eq!(Validity::AllNull, vector.validity());
        assert_eq!(5, vector.null_count());
    }

    #[test]
    fn test_null_vector_builder() {
        let mut builder = NullType::default().create_mutable_vector(3);
        builder.push_value_ref(ValueRef::Null).unwrap();
        assert!(builder.push_value_ref(ValueRef::Int32(123)).is_err());

        let input = NullVector::new(3);
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice(&[13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(input);
        assert_eq!(expect, vector);
    }
}
