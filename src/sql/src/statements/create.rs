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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use crate::ast::{ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value as SqlValue};

const LINE_SEP: &str = ",\n";
const COMMA_SEP: &str = ", ";
const INDENT: usize = 2;

macro_rules! format_indent {
    ($arg: expr) => {
        format!("{}{}", format_args!("{: >1$}", "", INDENT), $arg)
    };
}

macro_rules! format_list_indent {
    ($list: expr) => {
        $list.iter().map(|e| format_indent!(e)).join(LINE_SEP)
    };
}

macro_rules! format_list_comma {
    ($list: expr) => {
        $list.iter().map(|e| format!("{}", e)).join(COMMA_SEP)
    };
}

/// Time index name, used in table constraints.
pub const TIME_INDEX: &str = "__time_index";

#[inline]
pub fn is_time_index(constrait: &TableConstraint) -> bool {
    matches!(constrait, TableConstraint::Unique {
        name: Some(name),
        is_primary: false,
        ..
    }  if name.value == TIME_INDEX)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateTable {
    /// Create if not exists
    pub if_not_exists: bool,
    pub table_id: u32,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: Vec<SqlOption>,
    pub partitions: Option<Partitions>,
}

impl CreateTable {
    #[inline]
    fn format_constraints(&self) -> String {
        self.constraints
            .iter()
            .map(|c| {
                if is_time_index(c) {
                    let TableConstraint::Unique { columns, ..} = c else { unreachable!() };

                    format_indent!(format!("TIME INDEX ({})", format_list_comma!(columns)))
                } else {
                    format_indent!(c)
                }
            })
            .join(LINE_SEP)
    }

    #[inline]
    fn format_partitions(&self) -> String {
        if let Some(partitions) = &self.partitions {
            format!("{}\n", partitions)
        } else {
            "".to_string()
        }
    }

    #[inline]
    fn format_if_not_exits(&self) -> &str {
        if self.if_not_exists {
            "IF NOT EXISTS"
        } else {
            ""
        }
    }
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"CREATE TABLE {} {} (
{},
{}
)
{}ENGINE={}
WITH(
{}
)"#,
            self.format_if_not_exits(),
            self.name,
            format_list_indent!(self.columns),
            self.format_constraints(),
            self.format_partitions(),
            self.engine,
            format_list_indent!(self.options),
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Partitions {
    pub column_list: Vec<Ident>,
    pub entries: Vec<PartitionEntry>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PartitionEntry {
    pub name: Ident,
    pub value_list: Vec<SqlValue>,
}

impl Display for PartitionEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PARTITION {} VALUES LESS THAN ({})",
            self.name,
            format_list_comma!(self.value_list),
        )
    }
}

impl Display for Partitions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"PARTITION BY RANGE COLUMNS ({}) (
{}
)"#,
            format_list_comma!(self.column_list),
            format_list_indent!(self.entries),
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateDatabase {
    pub name: ObjectName,
    /// Create if not exists
    pub if_not_exists: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateExternalTable {
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use crate::parser::ParserContext;
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_create_table() {
        let sql = r"create table if not exists demo(
                             host string,
                             ts bigint,
                             cpu double default 0,
                             memory double,
                             TIME INDEX (ts),
                             PRIMARY KEY(ts, host)
                       )
                       PARTITION BY RANGE COLUMNS (ts) (
                         PARTITION r0 VALUES LESS THAN (5),
                         PARTITION r1 VALUES LESS THAN (9),
                         PARTITION r2 VALUES LESS THAN (MAXVALUE),
                       )
                       engine=mito
                       with(regions=1, ttl='7d');
         ";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        match &result[0] {
            Statement::CreateTable(c) => {
                let new_sql = format!("{}", c);
                assert_eq!(
                    r#"CREATE TABLE IF NOT EXISTS demo (
  host STRING,
  ts BIGINT,
  cpu DOUBLE DEFAULT 0,
  memory DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (ts, host)
)
PARTITION BY RANGE COLUMNS (ts) (
  PARTITION r0 VALUES LESS THAN (5),
  PARTITION r1 VALUES LESS THAN (9),
  PARTITION r2 VALUES LESS THAN (MAXVALUE)
)
ENGINE=mito
WITH(
  regions = 1,
  ttl = '7d'
)"#,
                    &new_sql
                );

                let new_result =
                    ParserContext::create_with_dialect(&new_sql, &GenericDialect {}).unwrap();
                assert_eq!(result, new_result);
            }
            _ => unreachable!(),
        }
    }
}
