// Copyright (c) 2019 Stepan Koltsov
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
// OR OTHER DEALINGS IN THE SOFTWARE.

// The Clear trait is copied from https://github.com/stepancheg/rust-protobuf/blob/v2.28.0/protobuf/src/clear.rs
// The RepeatedField struct is copied from https://github.com/stepancheg/rust-protobuf/blob/v2.28.0/protobuf/src/repeated.rs
// This code is to leverage the pooling mechanism to avoid frequent heap allocation/de-allocation when decoding deeply nested structs.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::default::Default;
use std::hash::{Hash, Hasher};
use std::iter::{FromIterator, IntoIterator};
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::{fmt, slice, vec};

use bytes::Bytes;

const NULL_BYTES: &[u8] = &[];

/// anything that can be cleared
pub trait Clear {
    /// Clear this make, make it equivalent to newly created object.
    fn clear(&mut self);
}

impl Clear for &[u8] {
    fn clear(&mut self) {
        *self = NULL_BYTES;
    }
}

impl<T> Clear for Option<T> {
    fn clear(&mut self) {
        self.take();
    }
}

impl Clear for String {
    fn clear(&mut self) {
        String::clear(self);
    }
}

impl<T> Clear for Vec<T> {
    fn clear(&mut self) {
        Vec::clear(self);
    }
}

impl Clear for Bytes {
    fn clear(&mut self) {
        Bytes::clear(self);
    }
}

/// Wrapper around vector to avoid deallocations on clear.
pub struct RepeatedField<T> {
    vec: Vec<T>,
    len: usize,
}

impl<T> RepeatedField<T> {
    /// Return number of elements in this container.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this container is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clear.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl<T> Default for RepeatedField<T> {
    #[inline]
    fn default() -> RepeatedField<T> {
        RepeatedField {
            vec: Vec::new(),
            len: 0,
        }
    }
}

impl<T> RepeatedField<T> {
    /// Create new empty container.
    #[inline]
    pub fn new() -> RepeatedField<T> {
        Default::default()
    }

    /// Create a contained with data from given vec.
    #[inline]
    pub fn from_vec(vec: Vec<T>) -> RepeatedField<T> {
        let len = vec.len();
        RepeatedField { vec, len }
    }

    /// Convert data into vec.
    #[inline]
    pub fn into_vec(self) -> Vec<T> {
        let mut vec = self.vec;
        vec.truncate(self.len);
        vec
    }

    /// Return current capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.vec.capacity()
    }

    /// View data as slice.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.vec[..self.len]
    }

    /// View data as mutable slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.vec[..self.len]
    }

    /// Get subslice of this container.
    #[inline]
    pub fn slice(&self, start: usize, end: usize) -> &[T] {
        &self.as_ref()[start..end]
    }

    /// Get slice from given index.
    #[inline]
    pub fn slice_from(&self, start: usize) -> &[T] {
        &self.as_ref()[start..]
    }

    /// Get slice to given index.
    #[inline]
    pub fn slice_to(&self, end: usize) -> &[T] {
        &self.as_ref()[..end]
    }

    /// View this container as two slices split at given index.
    #[inline]
    pub fn split_at(&self, mid: usize) -> (&[T], &[T]) {
        self.as_ref().split_at(mid)
    }

    /// View this container as two mutable slices split at given index.
    #[inline]
    pub fn split_at_mut(&mut self, mid: usize) -> (&mut [T], &mut [T]) {
        self.as_mut_slice().split_at_mut(mid)
    }

    /// View all but first elements of this container.
    #[inline]
    pub fn tail(&self) -> &[T] {
        &self.as_ref()[1..]
    }

    /// Last element of this container.
    #[inline]
    pub fn last(&self) -> Option<&T> {
        self.as_ref().last()
    }

    /// Mutable last element of this container.
    #[inline]
    pub fn last_mut(&mut self) -> Option<&mut T> {
        self.as_mut_slice().last_mut()
    }

    /// View all but last elements of this container.
    #[inline]
    pub fn init(&self) -> &[T] {
        let s = self.as_ref();
        &s[0..s.len() - 1]
    }

    /// Push an element to the end.
    #[inline]
    pub fn push(&mut self, value: T) {
        if self.len == self.vec.len() {
            self.vec.push(value);
        } else {
            self.vec[self.len] = value;
        }
        self.len += 1;
    }

    /// Pop last element.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            None
        } else {
            self.vec.truncate(self.len);
            self.len -= 1;
            self.vec.pop()
        }
    }

    /// Insert an element at specified position.
    #[inline]
    pub fn insert(&mut self, index: usize, value: T) {
        assert!(index <= self.len);
        self.vec.insert(index, value);
        self.len += 1;
    }

    /// Remove an element from specified position.
    #[inline]
    pub fn remove(&mut self, index: usize) -> T {
        assert!(index < self.len);
        self.len -= 1;
        self.vec.remove(index)
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all elements `e` such that `f(&e)` returns `false`.
    /// This method operates in place, visiting each element exactly once in the
    /// original order, and preserves the order of the retained elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use servers::repeated_field::RepeatedField;
    ///
    /// let mut vec = RepeatedField::from(vec![1, 2, 3, 4]);
    /// vec.retain(|&x| x % 2 == 0);
    /// assert_eq!(vec, RepeatedField::from(vec![2, 4]));
    /// ```
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&T) -> bool,
    {
        // suboptimal
        self.vec.truncate(self.len);
        self.vec.retain(f);
        self.len = self.vec.len();
    }

    /// Truncate at specified length.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if self.len > len {
            self.len = len;
        }
    }

    /// Reverse in place.
    #[inline]
    pub fn reverse(&mut self) {
        self.as_mut_slice().reverse()
    }

    /// Immutable data iterator.
    #[inline]
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.as_ref().iter()
    }

    /// Mutable data iterator.
    #[inline]
    pub fn iter_mut(&mut self) -> slice::IterMut<'_, T> {
        self.as_mut_slice().iter_mut()
    }

    /// Sort elements with given comparator.
    #[inline]
    pub fn sort_by<F>(&mut self, compare: F)
    where
        F: Fn(&T, &T) -> Ordering,
    {
        self.as_mut_slice().sort_by(compare)
    }

    /// Get data as raw pointer.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.vec.as_ptr()
    }

    /// Get data a mutable raw pointer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.vec.as_mut_ptr()
    }
}

impl<T: Default + Clear> RepeatedField<T> {
    /// Push default value.
    /// This operation could be faster than `rf.push(Default::default())`,
    /// because it may reuse previously allocated and cleared element.
    pub fn push_default(&mut self) -> &mut T {
        if self.len == self.vec.len() {
            self.vec.push(Default::default());
        } else {
            self.vec[self.len].clear();
        }
        self.len += 1;
        self.last_mut().unwrap()
    }
}

impl<T> From<Vec<T>> for RepeatedField<T> {
    #[inline]
    fn from(values: Vec<T>) -> RepeatedField<T> {
        RepeatedField::from_vec(values)
    }
}

impl<'a, T: Clone> From<&'a [T]> for RepeatedField<T> {
    #[inline]
    fn from(values: &'a [T]) -> RepeatedField<T> {
        RepeatedField::from_slice(values)
    }
}

impl<T> From<RepeatedField<T>> for Vec<T> {
    #[inline]
    fn from(val: RepeatedField<T>) -> Self {
        val.into_vec()
    }
}

impl<T: Clone> RepeatedField<T> {
    /// Copy slice data to `RepeatedField`
    #[inline]
    pub fn from_slice(values: &[T]) -> RepeatedField<T> {
        RepeatedField::from_vec(values.to_vec())
    }

    /// Copy slice data to `RepeatedField`
    #[inline]
    pub fn from_ref<X: AsRef<[T]>>(values: X) -> RepeatedField<T> {
        RepeatedField::from_slice(values.as_ref())
    }

    /// Copy this data into new vec.
    #[inline]
    pub fn to_vec(&self) -> Vec<T> {
        self.as_ref().to_vec()
    }
}

impl<T: Clone> Clone for RepeatedField<T> {
    #[inline]
    fn clone(&self) -> RepeatedField<T> {
        RepeatedField {
            vec: self.to_vec(),
            len: self.len(),
        }
    }
}

impl<T> FromIterator<T> for RepeatedField<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> RepeatedField<T> {
        RepeatedField::from_vec(FromIterator::from_iter(iter))
    }
}

impl<'a, T> IntoIterator for &'a RepeatedField<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> slice::Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut RepeatedField<T> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;

    fn into_iter(self) -> slice::IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T> IntoIterator for RepeatedField<T> {
    type Item = T;
    type IntoIter = vec::IntoIter<T>;

    fn into_iter(mut self) -> vec::IntoIter<T> {
        self.vec.truncate(self.len);
        self.vec.into_iter()
    }
}

impl<T: PartialEq> PartialEq for RepeatedField<T> {
    #[inline]
    fn eq(&self, other: &RepeatedField<T>) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl<T: Eq> Eq for RepeatedField<T> {}

impl<T: PartialEq> PartialEq<[T]> for RepeatedField<T> {
    fn eq(&self, other: &[T]) -> bool {
        self.as_slice() == other
    }
}

impl<T: PartialEq> PartialEq<RepeatedField<T>> for [T] {
    fn eq(&self, other: &RepeatedField<T>) -> bool {
        self == other.as_slice()
    }
}

impl<T: PartialEq> RepeatedField<T> {
    /// True iff this container contains given element.
    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        self.as_ref().contains(value)
    }
}

impl<T: Hash> Hash for RepeatedField<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl<T> AsRef<[T]> for RepeatedField<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        &self.vec[..self.len]
    }
}

impl<T> Borrow<[T]> for RepeatedField<T> {
    #[inline]
    fn borrow(&self) -> &[T] {
        &self.vec[..self.len]
    }
}

impl<T> Deref for RepeatedField<T> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        &self.vec[..self.len]
    }
}

impl<T> DerefMut for RepeatedField<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.vec[..self.len]
    }
}

impl<T> Index<usize> for RepeatedField<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &T {
        &self.as_ref()[index]
    }
}

impl<T> IndexMut<usize> for RepeatedField<T> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut T {
        &mut self.as_mut_slice()[index]
    }
}

impl<T> Extend<T> for RepeatedField<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.vec.truncate(self.len);
        self.vec.extend(iter);
        self.len = self.vec.len();
    }
}

impl<'a, T: Copy + 'a> Extend<&'a T> for RepeatedField<T> {
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.vec.truncate(self.len);
        self.vec.extend(iter);
        self.len = self.vec.len();
    }
}

impl<T: fmt::Debug> fmt::Debug for RepeatedField<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use crate::repeated_field::RepeatedField;

    #[test]
    fn test_null_ptr() {
        let mut vec: RepeatedField<&'static [u8]> = RepeatedField::new();
        let borrowed_value = vec.push_default();
        *borrowed_value = b"hello";
        vec.clear();
        let new_value = vec.push_default();
        assert!((*new_value).is_empty());
    }
}
