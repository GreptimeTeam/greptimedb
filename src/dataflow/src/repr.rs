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

//! basically a wrapper around the `datatype` crate
//! for basic Data Representation

mod relation;

use std::borrow::Borrow;
use std::slice::SliceIndex;

use datatypes::value::Value;
use serde::{Deserialize, Serialize};
/// System-wide Record count difference type.
pub type Diff = i64;

/// System-wide default timestamp type
pub type Timestamp = u64;

/// Default type for a repr of changes to a collection.
pub type DiffRow = (Row, Timestamp, Diff);

/// A row is a vector of values.
///
/// TODO(discord9): use a more efficient representation
///i.e. more compact like raw u8 of \[tag0, value0, tag1, value1, ...\]

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Row {
    pub inner: Vec<Value>,
}

impl Row {
    pub fn empty() -> Self {
        Self { inner: vec![] }
    }
    pub fn new(row: Vec<Value>) -> Self {
        Self { inner: row }
    }
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.inner.get(idx)
    }
    pub fn clear(&mut self) {
        self.inner.clear();
    }
    pub fn packer(&mut self) -> &mut Vec<Value> {
        self.inner.clear();
        &mut self.inner
    }
    pub fn pack<I>(iter: I) -> Row
    where
        I: IntoIterator<Item = Value>,
    {
        Self {
            inner: iter.into_iter().collect(),
        }
    }
    pub fn unpack(&self) -> Vec<Value> {
        self.inner.clone()
    }
    pub fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Value>,
    {
        self.inner.extend(iter);
    }
    pub fn into_iter(self) -> impl Iterator<Item = Value> {
        self.inner.into_iter()
    }
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.inner.iter()
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

#[test]
fn test_row() {
    let row = Row::empty();
    let row_1 = Row::new(vec![]);
    assert_eq!(row, row_1);
    let mut row_2 = Row::new(vec![Value::Int32(1), Value::Int32(2)]);
    assert_eq!(row_2.get(0), Some(&Value::Int32(1)));
    row_2.clear();
    assert_eq!(row_2.get(0), None);
    row_2
        .packer()
        .extend(vec![Value::Int32(1), Value::Int32(2)]);
    assert_eq!(row_2.get(0), Some(&Value::Int32(1)));
    row_2.extend(vec![Value::Int32(1), Value::Int32(2)]);
    assert_eq!(row_2.len(), 4);
    let row_3 = Row::pack(row_2.into_iter());
    assert_eq!(row_3.len(), 4);
    let row_4 = Row::pack(row_3.iter().cloned());
    assert_eq!(row_3, row_4);
}
