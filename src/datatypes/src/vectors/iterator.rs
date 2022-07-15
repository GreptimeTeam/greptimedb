use std::slice::Iter;

use arrow::array::Utf8ValuesIter;
use arrow::bitmap::utils::ZipValidity;

use crate::types::Primitive;
use crate::vectors::{PrimitiveVector, StringVector};

impl<T: Primitive> PrimitiveVector<T> {
    #[inline]
    pub fn iter(&self) -> ZipValidity<&T, Iter<T>> {
        self.array.iter()
    }
}

impl StringVector {
    #[inline]
    pub fn iter(&self) -> ZipValidity<&str, Utf8ValuesIter<i64>> {
        self.array.iter()
    }
}
