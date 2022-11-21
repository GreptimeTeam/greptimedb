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

use crate::error::Result;
use crate::prelude::*;

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
