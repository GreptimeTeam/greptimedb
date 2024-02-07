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

use api::helper::{pb_value_to_value_ref, value_to_grpc_value};
use api::v1::Row as ProtoRow;
use datatypes::data_type::ConcreteDataType;
use datatypes::types::cast;
use datatypes::types::cast::CastOption;
use datatypes::value::Value;
use itertools::Itertools;
pub(crate) use relation::{RelationDesc, RelationType};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::expr::error::{CastValueSnafu, EvalError};

/// System-wide Record count difference type.
pub type Diff = i64;

/// System-wide default timestamp type
pub type Timestamp = i64;

/// Default type for a repr of changes to a collection.
pub type DiffRow = (Row, Timestamp, Diff);

/// Convert a value that is or can be converted to Datetime to internal timestamp
pub fn value_to_internal_ts(value: Value) -> Result<Timestamp, EvalError> {
    match value {
        Value::DateTime(ts) => Ok(ts.val()),
        arg => {
            let arg_ty = arg.data_type();
            let res = cast(arg, &ConcreteDataType::datetime_datatype()).context({
                CastValueSnafu {
                    from: arg_ty,
                    to: ConcreteDataType::datetime_datatype(),
                }
            })?;
            if let Value::DateTime(ts) = res {
                Ok(ts.val())
            } else {
                unreachable!()
            }
        }
    }
}

/// A row is a vector of values.
///
/// TODO(discord9): use a more efficient representation
/// i.e. more compact like raw u8 of \[tag0, value0, tag1, value1, ...\]
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
    /// clear and return the inner vector
    ///
    /// useful if you want to reuse the vector as a buffer
    pub fn packer(&mut self) -> &mut Vec<Value> {
        self.inner.clear();
        &mut self.inner
    }
    /// pack a iterator of values into a row
    pub fn pack<I>(iter: I) -> Row
    where
        I: IntoIterator<Item = Value>,
    {
        Self {
            inner: iter.into_iter().collect(),
        }
    }
    /// unpack a row into a vector of values
    pub fn unpack(self) -> Vec<Value> {
        self.inner
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

impl From<ProtoRow> for Row {
    fn from(row: ProtoRow) -> Self {
        Row::pack(
            row.values
                .iter()
                .map(|pb_val| -> Value { pb_value_to_value_ref(pb_val, &None).into() }),
        )
    }
}

impl From<Row> for ProtoRow {
    fn from(row: Row) -> Self {
        let values = row
            .unpack()
            .into_iter()
            .map(value_to_grpc_value)
            .collect_vec();
        ProtoRow { values }
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
