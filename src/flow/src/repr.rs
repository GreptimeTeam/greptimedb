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

use api::helper::{pb_value_to_value_ref, value_to_grpc_value};
use api::v1::Row as ProtoRow;
use datatypes::data_type::ConcreteDataType;
use datatypes::types::cast;
use datatypes::value::Value;
use itertools::Itertools;
pub(crate) use relation::{ColumnType, Key, RelationDesc, RelationType};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::expr::error::{CastValueSnafu, EvalError, InvalidArgumentSnafu};

/// System-wide Record count difference type. Useful for capture data change
///
/// i.e. +1 means insert one record, -1 means remove,
/// and +/-n means insert/remove multiple duplicate records.
pub type Diff = i64;

/// System-wide default timestamp type, in milliseconds
pub type Timestamp = i64;

/// System-wide default duration type, in milliseconds
pub type Duration = i64;

/// Default type for a repr of changes to a collection.
pub type DiffRow = (Row, Timestamp, Diff);

/// Row with key-value pair, timestamp and diff
pub type KeyValDiffRow = ((Row, Row), Timestamp, Diff);

/// broadcast channel capacity, can be important to memory consumption, since this influence how many
/// updates can be buffered in memory in the entire dataflow
/// TODO(discord9): add config for this, so cpu&mem usage can be balanced and configured by this
pub const BROADCAST_CAP: usize = 1024;

/// The maximum capacity of the send buffer, to prevent the buffer from growing too large
pub const SEND_BUF_CAP: usize = BROADCAST_CAP * 2;

/// Flow worker will try to at least accumulate this many rows before processing them(if one second havn't passed)
pub const BATCH_SIZE: usize = 32 * 16384;

/// Convert a value that is or can be converted to Datetime to internal timestamp
///
/// support types are: `Date`, `DateTime`, `TimeStamp`, `i64`
pub fn value_to_internal_ts(value: Value) -> Result<Timestamp, EvalError> {
    let is_supported_time_type = |arg: &Value| {
        let ty = arg.data_type();
        matches!(
            ty,
            ConcreteDataType::Date(..)
                | ConcreteDataType::DateTime(..)
                | ConcreteDataType::Timestamp(..)
        )
    };
    match value {
        Value::DateTime(ts) => Ok(ts.val()),
        Value::Int64(ts) => Ok(ts),
        arg if is_supported_time_type(&arg) => {
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
        _ => InvalidArgumentSnafu {
            reason: format!("Expect a time type or i64, got {:?}", value.data_type()),
        }
        .fail(),
    }
}

/// A row is a vector of values.
///
/// TODO(discord9): use a more efficient representation
/// i.e. more compact like raw u8 of \[tag0, value0, tag1, value1, ...\]
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Row {
    /// The inner vector of values
    pub inner: Vec<Value>,
}

impl Row {
    /// Create an empty row
    pub fn empty() -> Self {
        Self { inner: vec![] }
    }

    /// Returns true if the Row contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Create a row from a vector of values
    pub fn new(row: Vec<Value>) -> Self {
        Self { inner: row }
    }

    /// Get the value at the given index
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.inner.get(idx)
    }

    /// Clear the row
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

    /// extend the row with values from an iterator
    pub fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Value>,
    {
        self.inner.extend(iter);
    }

    /// Creates a consuming iterator, that is, one that moves each value out of the `Row` (from start to end). The `Row` cannot be used after calling this
    pub fn into_iter(self) -> impl Iterator<Item = Value> {
        self.inner.into_iter()
    }

    /// Returns an iterator over the slice.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.inner.iter()
    }

    /// Returns the number of elements in the row, also known as its 'length'.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl From<Vec<Value>> for Row {
    fn from(row: Vec<Value>) -> Self {
        Row::new(row)
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
#[cfg(test)]
mod test {
    use common_time::{Date, DateTime};

    use super::*;

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

    #[test]
    fn test_cast_to_internal_ts() {
        {
            let a = Value::from(1i32);
            let b = Value::from(1i64);
            let c = Value::DateTime(DateTime::new(1i64));
            let d = Value::from(1.0);

            assert!(value_to_internal_ts(a).is_err());
            assert_eq!(value_to_internal_ts(b).unwrap(), 1i64);
            assert_eq!(value_to_internal_ts(c).unwrap(), 1i64);
            assert!(value_to_internal_ts(d).is_err());
        }

        {
            // time related type
            let a = Value::Date(Date::new(1));
            assert_eq!(value_to_internal_ts(a).unwrap(), 86400 * 1000i64);
            let b = Value::Timestamp(common_time::Timestamp::new_second(1));
            assert_eq!(value_to_internal_ts(b).unwrap(), 1000i64);
            let c = Value::Time(common_time::time::Time::new_second(1));
            assert!(matches!(
                value_to_internal_ts(c),
                Err(EvalError::InvalidArgument { .. })
            ));
        }
    }
}
