use std::any::Any;

use crate::data_type::DataTypeRef;
use crate::types::binary_type::BinaryType;
use crate::vectors::Vector;
use crate::LargeBinaryArray;

/// Vector of binary strings.
#[derive(Debug)]
pub struct BinaryVector {
    array: LargeBinaryArray,
}

impl Vector for BinaryVector {
    fn data_type(&self) -> DataTypeRef {
        BinaryType::arc()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }
}
