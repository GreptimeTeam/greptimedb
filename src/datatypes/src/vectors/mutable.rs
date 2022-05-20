use std::any::Any;

use crate::prelude::*;

pub trait MutableVector: Send + Sync {
    fn data_type(&self) -> ConcreteDataType;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn as_any(&self) -> &dyn Any;

    fn as_mut_any(&mut self) -> &mut dyn Any;

    // /// Push a value into the mutable vector.
    // ///
    // /// # Panics
    // /// Panics if the data type of the value differs from the mutable vector's data type.
    // fn push_value(&mut self, value: &Value);

    fn to_vector(&mut self) -> VectorRef;
}
