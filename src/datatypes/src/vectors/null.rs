use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::{Array, NullArray};
use arrow::datatypes::DataType as ArrowDataType;
use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::Result;
use crate::serialize::Serializable;
use crate::types::NullType;
use crate::vectors::impl_try_from_arrow_array_for_vector;
use crate::vectors::{Validity, Vector};

pub struct NullVector {
    array: NullArray,
}

impl NullVector {
    pub fn new(n: usize) -> Self {
        Self {
            array: NullArray::new(ArrowDataType::Null, n),
        }
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        Arc::new(self.array.clone())
    }

    fn validity(&self) -> Validity {
        Validity::AllNull
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

impl_try_from_arrow_array_for_vector!(NullArray, NullVector);

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_null_vector() {
        let vector = NullVector::new(32);

        assert_eq!(vector.len(), 32);
        let arrow_arr = vector.to_arrow_array();
        assert_eq!(arrow_arr.null_count(), 32);

        let array2 = arrow_arr.slice(8, 16);
        assert_eq!(array2.len(), 16);
        assert_eq!(array2.null_count(), 16);
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
}
