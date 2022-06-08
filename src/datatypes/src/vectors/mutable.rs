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

    fn to_vector(&mut self) -> VectorRef;
}
