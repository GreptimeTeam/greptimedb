//! basically a wrapper around the `datatype` crate
//! for basic Data Representation
use std::borrow::Borrow;
use std::slice::SliceIndex;

use datatypes::value::Value;
use serde::{Deserialize, Serialize};

/// System-wide Record count difference type.
pub type Diff = i64;

/// A row is a vector of values.
///
/// TODO(discord9): use a more efficient representation
///i.e. more compact like raw u8 of \[tag0, value0, tag1, value1, ...\]

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Row {
    inner: Vec<Value>,
}

impl Row {
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
}

/// System-wide default timestamp type
pub type Timestamp = u64;
