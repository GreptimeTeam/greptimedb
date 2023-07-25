//! basically a wrapper around the `datatype` crate
//! for basic Data Representation
use std::borrow::Borrow;

use datatypes::value::Value;

/// System-wide Record count difference type.
pub type Diff = i64;

/// A row is a vector of values.
///
/// TODO(discord9): use a more efficient representation
///i.e. more compact like raw u8 of \[tag0, value0, tag1, value1, ...\]

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    inner: Vec<Value>,
}

impl Row {
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
}

/// System-wide default timestamp type
pub type Timestamp = u64;
