pub mod binary;
pub mod primitive;

use std::any::Any;
use std::sync::Arc;

use arrow2::array::ArrayRef;
pub use binary::*;
pub use primitive::*;

use crate::data_type::DataTypeRef;

/// Vector of data values.
pub trait Vector: Send + Sync {
    /// Returns the data type of the vector.
    ///
    /// This may require heap allocation.
    fn data_type(&self) -> DataTypeRef;

    /// Returns the vector as [Any](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns number of elements in the vector.
    fn len(&self) -> usize;

    /// Returns whether the vector is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert this vector to a new arrow [ArrayRef].
    fn to_arrow_array(&self) -> ArrayRef;
}

pub type VectorRef = Arc<dyn Vector>;
