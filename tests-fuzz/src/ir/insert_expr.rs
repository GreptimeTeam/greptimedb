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

use std::fmt::{Debug, Display};

use datatypes::value::Value;

use crate::ir::{Column, Ident};

pub struct InsertIntoExpr {
    pub table_name: Ident,
    pub omit_column_list: bool,
    pub columns: Vec<Column>,
    pub values_list: Vec<RowValues>,
}

impl InsertIntoExpr {
    /// Returns the timestamp column
    pub fn timestamp_column(&self) -> Option<Column> {
        self.columns.iter().find(|c| c.is_time_index()).cloned()
    }

    /// Returns index of the timestamp column
    pub fn timestamp_column_idx(&self) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .find_map(|(idx, c)| if c.is_time_index() { Some(idx) } else { None })
    }

    /// Returns a vector of columns that are primary keys or time indices.
    pub fn primary_key_columns(&self) -> Vec<Column> {
        self.columns
            .iter()
            .filter(|c| c.is_primary_key() || c.is_time_index())
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Returns the indices of columns that are primary keys or time indices.
    pub fn primary_key_column_idx(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter_map(|(i, c)| {
                if c.is_primary_key() || c.is_time_index() {
                    Some(i)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}

pub type RowValues = Vec<RowValue>;

#[derive(PartialEq, PartialOrd, Clone)]
pub enum RowValue {
    Value(Value),
    Default,
}

impl RowValue {
    #[allow(clippy::should_implement_trait)]
    pub fn cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (RowValue::Value(Value::Null), RowValue::Value(v2)) => v2.partial_cmp(&Value::Null),
            (RowValue::Value(v1), RowValue::Value(Value::Null)) => Value::Null.partial_cmp(v1),
            (RowValue::Value(v1), RowValue::Value(v2)) => v1.partial_cmp(v2),
            _ => panic!("Invalid comparison: {:?} and {:?}", self, other),
        }
    }
}

impl Display for RowValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowValue::Value(v) => match v {
                Value::Null => write!(f, "NULL"),
                v @ (Value::String(_) | Value::Timestamp(_) | Value::Date(_)) => {
                    write!(f, "'{}'", v)
                }
                v => write!(f, "{}", v),
            },
            RowValue::Default => write!(f, "DEFAULT"),
        }
    }
}

impl Debug for RowValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowValue::Value(v) => match v {
                Value::Null => write!(f, "NULL"),
                v @ (Value::String(_) | Value::Timestamp(_) | Value::Date(_)) => {
                    write!(f, "'{}'", v)
                }
                v => write!(f, "{}", v),
            },
            RowValue::Default => write!(f, "DEFAULT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;
    use datatypes::value::Value;

    use crate::ir::insert_expr::RowValue;

    #[test]
    fn test_value_cmp() {
        let time_stampe1 =
            Value::Timestamp(Timestamp::from_str_utc("-39988-01-31 01:21:12.848697+0000").unwrap());
        let time_stampe2 =
            Value::Timestamp(Timestamp::from_str_utc("+12970-09-22 08:40:58.392839+0000").unwrap());
        let v1 = RowValue::Value(time_stampe1);
        let v2 = RowValue::Value(time_stampe2);
        assert_eq!(v1.cmp(&v2), Some(std::cmp::Ordering::Less));
    }
}
