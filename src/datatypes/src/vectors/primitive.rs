use std::any::Any;

use arrow2::array::PrimitiveArray;

use crate::data_type::DataTypeRef;
use crate::types::primitive_traits::Primitive;
use crate::types::primitive_type::CreateDataType;
use crate::vectors::Vector;

/// Vector for primitive data types.
pub struct PrimitiveVector<T: Primitive> {
    array: PrimitiveArray<T>,
}

impl<T: Primitive> PrimitiveVector<T> {
    pub fn new(array: PrimitiveArray<T>) -> Self {
        Self { array }
    }
}

impl<T: Primitive + CreateDataType> Vector for PrimitiveVector<T> {
    fn data_type(&self) -> DataTypeRef {
        T::create_data_type()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }
}
