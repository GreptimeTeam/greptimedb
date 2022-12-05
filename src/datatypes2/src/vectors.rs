// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use snafu::ensure;

use crate::data_type::ConcreteDataType;
use crate::error::{self, Result};
use crate::serialize::Serializable;
use crate::value::{Value, ValueRef};
use crate::vectors::operations::VectorOp;

mod binary;
mod boolean;
mod constant;
mod date;
mod datetime;
mod eq;
mod helper;
mod list;
mod null;
mod operations;
mod primitive;
mod string;
mod timestamp;
mod validity;

pub use binary::{BinaryVector, BinaryVectorBuilder};
pub use boolean::{BooleanVector, BooleanVectorBuilder};
pub use constant::ConstantVector;
pub use date::{DateVector, DateVectorBuilder};
pub use datetime::{DateTimeVector, DateTimeVectorBuilder};
pub use helper::Helper;
pub use list::{ListIter, ListVector, ListVectorBuilder};
pub use null::{NullVector, NullVectorBuilder};
pub use primitive::{
    Float32Vector, Float32VectorBuilder, Float64Vector, Float64VectorBuilder, Int16Vector,
    Int16VectorBuilder, Int32Vector, Int32VectorBuilder, Int64Vector, Int64VectorBuilder,
    Int8Vector, Int8VectorBuilder, PrimitiveIter, PrimitiveVector, PrimitiveVectorBuilder,
    UInt16Vector, UInt16VectorBuilder, UInt32Vector, UInt32VectorBuilder, UInt64Vector,
    UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder,
};
pub use string::{StringVector, StringVectorBuilder};
pub use timestamp::{
    TimestampMicrosecondVector, TimestampMicrosecondVectorBuilder, TimestampMillisecondVector,
    TimestampMillisecondVectorBuilder, TimestampNanosecondVector, TimestampNanosecondVectorBuilder,
    TimestampSecondVector, TimestampSecondVectorBuilder,
};
pub use validity::Validity;

// TODO(yingwen): arrow 28.0 implements Clone for all arrays, we could upgrade to it and simplify
// some codes in methods such as `to_arrow_array()` and `to_boxed_arrow_array()`.
/// Vector of data values.
pub trait Vector: Send + Sync + Serializable + Debug + VectorOp {
    /// Returns the data type of the vector.
    ///
    /// This may require heap allocation.
    fn data_type(&self) -> ConcreteDataType;

    fn vector_type_name(&self) -> String;

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

    /// Convert this vector to a new boxed arrow [Array].
    fn to_boxed_arrow_array(&self) -> Box<dyn Array>;

    /// Returns the validity of the Array.
    fn validity(&self) -> Validity;

    /// Returns the memory size of vector.
    fn memory_size(&self) -> usize;

    /// The number of null slots on this [`Vector`].
    /// # Implementation
    /// This is `O(1)`.
    fn null_count(&self) -> usize;

    /// Returns true when it's a ConstantColumn
    fn is_const(&self) -> bool {
        false
    }

    /// Returns whether row is null.
    fn is_null(&self, row: usize) -> bool;

    /// If the only value vector can contain is NULL.
    fn only_null(&self) -> bool {
        self.null_count() == self.len()
    }

    /// Slices the `Vector`, returning a new `VectorRef`.
    ///
    /// # Panics
    /// This function panics if `offset + length > self.len()`.
    fn slice(&self, offset: usize, length: usize) -> VectorRef;

    /// Returns the clone of value at `index`.
    ///
    /// # Panics
    /// Panic if `index` is out of bound.
    fn get(&self, index: usize) -> Value;

    /// Returns the clone of value at `index` or error if `index`
    /// is out of bound.
    fn try_get(&self, index: usize) -> Result<Value> {
        ensure!(
            index < self.len(),
            error::BadArrayAccessSnafu {
                index,
                size: self.len()
            }
        );
        Ok(self.get(index))
    }

    /// Returns the reference of value at `index`.
    ///
    /// # Panics
    /// Panic if `index` is out of bound.
    fn get_ref(&self, index: usize) -> ValueRef;
}

pub type VectorRef = Arc<dyn Vector>;

/// Mutable vector that could be used to build an immutable vector.
pub trait MutableVector: Send + Sync {
    /// Returns the data type of the vector.
    fn data_type(&self) -> ConcreteDataType;

    /// Returns the length of the vector.
    fn len(&self) -> usize;

    /// Returns whether the vector is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to Any, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    /// Convert to mutable Any, to enable dynamic casting.
    fn as_mut_any(&mut self) -> &mut dyn Any;

    /// Convert `self` to an (immutable) [VectorRef] and reset `self`.
    fn to_vector(&mut self) -> VectorRef;

    /// Push value ref to this mutable vector.
    ///
    /// Returns error if data type unmatch.
    fn push_value_ref(&mut self, value: ValueRef) -> Result<()>;

    /// Extend this mutable vector by slice of `vector`.
    ///
    /// Returns error if data type unmatch.
    ///
    /// # Panics
    /// Panics if `offset + length > vector.len()`.
    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()>;
}

/// Helper to define `try_from_arrow_array(array: arrow::array::ArrayRef)` function.
macro_rules! impl_try_from_arrow_array_for_vector {
    ($Array: ident, $Vector: ident) => {
        impl $Vector {
            pub fn try_from_arrow_array(
                array: impl AsRef<dyn arrow::array::Array>,
            ) -> crate::error::Result<$Vector> {
                use snafu::OptionExt;

                let data = array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<$Array>()
                    .with_context(|| crate::error::ConversionSnafu {
                        from: std::format!("{:?}", array.as_ref().data_type()),
                    })?
                    .data()
                    .clone();

                let concrete_array = $Array::from(data);
                Ok($Vector::from(concrete_array))
            }
        }
    };
}

macro_rules! impl_validity_for_vector {
    ($array: expr) => {
        Validity::from_array_data($array.data())
    };
}

macro_rules! impl_get_for_vector {
    ($array: expr, $index: ident) => {
        if $array.is_valid($index) {
            // Safety: The index have been checked by `is_valid()`.
            unsafe { $array.value_unchecked($index).into() }
        } else {
            Value::Null
        }
    };
}

macro_rules! impl_get_ref_for_vector {
    ($array: expr, $index: ident) => {
        if $array.is_valid($index) {
            // Safety: The index have been checked by `is_valid()`.
            unsafe { $array.value_unchecked($index).into() }
        } else {
            ValueRef::Null
        }
    };
}

macro_rules! impl_extend_for_builder {
    ($mutable_vector: expr, $vector: ident, $VectorType: ident, $offset: ident, $length: ident) => {{
        use snafu::OptionExt;

        let sliced_vector = $vector.slice($offset, $length);
        let concrete_vector = sliced_vector
            .as_any()
            .downcast_ref::<$VectorType>()
            .with_context(|| crate::error::CastTypeSnafu {
                msg: format!(
                    "Failed to cast vector from {} to {}",
                    $vector.vector_type_name(),
                    stringify!($VectorType)
                ),
            })?;
        for value in concrete_vector.iter_data() {
            $mutable_vector.push(value);
        }
        Ok(())
    }};
}

pub(crate) use {
    impl_extend_for_builder, impl_get_for_vector, impl_get_ref_for_vector,
    impl_try_from_arrow_array_for_vector, impl_validity_for_vector,
};

#[cfg(test)]
pub mod tests {
    use arrow::array::{Array, Int32Array, UInt8Array};
    use serde_json;

    use super::*;
    use crate::data_type::DataType;
    use crate::types::{Int32Type, LogicalPrimitiveType};
    use crate::vectors::helper::Helper;

    #[test]
    fn test_df_columns_to_vector() {
        let df_column: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let vector = Helper::try_into_vector(df_column).unwrap();
        assert_eq!(
            Int32Type::build_data_type().as_arrow_type(),
            vector.data_type().as_arrow_type()
        );
    }

    #[test]
    fn test_serialize_i32_vector() {
        let df_column: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let json_value = Helper::try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
    }

    #[test]
    fn test_serialize_i8_vector() {
        let df_column: Arc<dyn Array> = Arc::new(UInt8Array::from(vec![1, 2, 3]));
        let json_value = Helper::try_into_vector(df_column)
            .unwrap()
            .serialize_to_json()
            .unwrap();
        assert_eq!("[1,2,3]", serde_json::to_string(&json_value).unwrap());
    }
}
