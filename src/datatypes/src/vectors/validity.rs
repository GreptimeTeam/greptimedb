// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::array::ArrayData;
use arrow::buffer::NullBuffer;

#[derive(Debug, PartialEq)]
enum ValidityKind {
    /// Whether the array slot is valid or not (null).
    Slots {
        bitmap: NullBuffer,
        len: usize,
        null_count: usize,
    },
    /// All slots are valid.
    AllValid { len: usize },
    /// All slots are null.
    AllNull { len: usize },
}

/// Validity of a vector.
#[derive(Debug, PartialEq)]
pub struct Validity {
    kind: ValidityKind,
}

impl Validity {
    /// Creates a `Validity` from [`ArrayData`].
    pub fn from_array_data(data: ArrayData) -> Validity {
        match data.nulls() {
            Some(null_buf) => Validity {
                kind: ValidityKind::Slots {
                    bitmap: null_buf.clone(),
                    len: data.len(),
                    null_count: data.null_count(),
                },
            },
            None => Validity::all_valid(data.len()),
        }
    }

    /// Returns `Validity` that all elements are valid.
    pub fn all_valid(len: usize) -> Validity {
        Validity {
            kind: ValidityKind::AllValid { len },
        }
    }

    /// Returns `Validity` that all elements are null.
    pub fn all_null(len: usize) -> Validity {
        Validity {
            kind: ValidityKind::AllNull { len },
        }
    }

    /// Returns whether `i-th` bit is set.
    pub fn is_set(&self, i: usize) -> bool {
        match &self.kind {
            ValidityKind::Slots { bitmap, .. } => bitmap.is_valid(i),
            ValidityKind::AllValid { len } => i < *len,
            ValidityKind::AllNull { .. } => false,
        }
    }

    /// Returns true if all bits are null.
    pub fn is_all_null(&self) -> bool {
        match self.kind {
            ValidityKind::Slots {
                len, null_count, ..
            } => len == null_count,
            ValidityKind::AllValid { .. } => false,
            ValidityKind::AllNull { .. } => true,
        }
    }

    /// Returns true if all bits are valid.
    pub fn is_all_valid(&self) -> bool {
        match self.kind {
            ValidityKind::Slots { null_count, .. } => null_count == 0,
            ValidityKind::AllValid { .. } => true,
            ValidityKind::AllNull { .. } => false,
        }
    }

    /// The number of null slots.
    pub fn null_count(&self) -> usize {
        match self.kind {
            ValidityKind::Slots { null_count, .. } => null_count,
            ValidityKind::AllValid { .. } => 0,
            ValidityKind::AllNull { len } => len,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array};

    use super::*;

    #[test]
    fn test_all_valid() {
        let validity = Validity::all_valid(5);
        assert!(validity.is_all_valid());
        assert!(!validity.is_all_null());
        assert_eq!(0, validity.null_count());
        for i in 0..5 {
            assert!(validity.is_set(i));
        }
        assert!(!validity.is_set(5));
    }

    #[test]
    fn test_all_null() {
        let validity = Validity::all_null(5);
        assert!(validity.is_all_null());
        assert!(!validity.is_all_valid());
        assert_eq!(5, validity.null_count());
        for i in 0..5 {
            assert!(!validity.is_set(i));
        }
        assert!(!validity.is_set(5));
    }

    #[test]
    fn test_from_array_data() {
        let array = Int32Array::from_iter([None, Some(1), None]);
        let validity = Validity::from_array_data(array.to_data());
        assert_eq!(2, validity.null_count());
        assert!(!validity.is_set(0));
        assert!(validity.is_set(1));
        assert!(!validity.is_set(2));
        assert!(!validity.is_all_null());
        assert!(!validity.is_all_valid());

        let array = Int32Array::from_iter([None, None]);
        let validity = Validity::from_array_data(array.to_data());
        assert!(validity.is_all_null());
        assert!(!validity.is_all_valid());
        assert_eq!(2, validity.null_count());

        let array = Int32Array::from_iter_values([1, 2]);
        let validity = Validity::from_array_data(array.to_data());
        assert!(!validity.is_all_null());
        assert!(validity.is_all_valid());
        assert_eq!(0, validity.null_count());
    }
}
