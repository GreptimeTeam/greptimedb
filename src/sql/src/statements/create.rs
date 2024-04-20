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

use std::fmt::{Display, Formatter};

use common_catalog::consts::FILE_ENGINE;
use itertools::Itertools;
use sqlparser::ast::Expr;
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value as SqlValue};
use crate::statements::OptionMap;
use crate::util::redact_option_secret;

const LINE_SEP: &str = ",\n";
const COMMA_SEP: &str = ", ";
const INDENT: usize = 2;

macro_rules! format_indent {
    ($fmt: expr, $arg: expr) => {
        format!($fmt, format_args!("{: >1$}", "", INDENT), $arg)
    };
    ($arg: expr) => {
        format_indent!("{}{}", $arg)
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

macro_rules! format_sorted_hashmap {
    ($hashmap:expr) => {{
        let hashmap = $hashmap;
        let mut sorted_keys: Vec<&String> = hashmap.keys().collect();
        sorted_keys.sort();
        let mut result = String::new();
        for key in sorted_keys {
            if let Some(val) = hashmap.get(key) {
                result.push_str(&redact_option_secret(key, val));
            }
        }
        result
    }};
}

/// Time index name, used in table constraints.
pub const TIME_INDEX: &str = "__time_index";

#[inline]
pub fn is_time_index(constraint: &TableConstraint) -> bool {
    matches!(constraint, TableConstraint::Unique {
        name: Some(name),
        is_primary: false,
        ..
    }  if name.value == TIME_INDEX)
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut)]
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
    fn format_constraints(&self) -> String {
        self.constraints
            .iter()
            .map(|c| {
                if is_time_index(c) {
                    let TableConstraint::Unique { columns, .. } = c else {
                        unreachable!()
                    };

                    format_indent!("{}TIME INDEX ({})", format_list_comma!(columns))
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
            String::default()
        }
    }

    #[inline]
    fn format_if_not_exists(&self) -> &str {
        if self.if_not_exists {
            "IF NOT EXISTS"
        } else {
            ""
        }
    }

    #[inline]
    fn format_options(&self) -> String {
        if self.options.is_empty() {
            String::default()
        } else {
            let options: Vec<&SqlOption> = self.options.iter().sorted().collect();
            let options = format_list_indent!(options);
            format!(
                r#"WITH(
{options}
)"#
            )
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut)]
pub struct Partitions {
    pub column_list: Vec<Ident>,
    pub exprs: Vec<Expr>,
}

impl Partitions {
    /// set quotes to all [Ident]s from column list
    pub fn set_quote(&mut self, quote_style: char) {
        self.column_list
            .iter_mut()
            .for_each(|c| c.quote_style = Some(quote_style));
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut)]
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
        if !self.column_list.is_empty() {
            write!(
                f,
                "PARTITION ON COLUMNS ({}) (\n{}\n)",
                format_list_comma!(self.column_list),
                format_list_indent!(self.exprs),
            )
        } else {
            write!(f, "")
        }
    }
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let if_not_exists = self.format_if_not_exists();
        let name = &self.name;
        let columns = format_list_indent!(self.columns);
        let constraints = self.format_constraints();
        let partitions = self.format_partitions();
        let engine = &self.engine;
        let options = self.format_options();
        let maybe_external = if self.engine == FILE_ENGINE {
            "EXTERNAL "
        } else {
            ""
        };
        write!(
            f,
            r#"CREATE {maybe_external}TABLE {if_not_exists} {name} (
{columns},
{constraints}
)
{partitions}ENGINE={engine}
{options}"#
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut)]
pub struct CreateDatabase {
    pub name: ObjectName,
    /// Create if not exists
    pub if_not_exists: bool,
}

impl CreateDatabase {
    /// Creates a statement for `CREATE DATABASE`
    pub fn new(name: ObjectName, if_not_exists: bool) -> Self {
        Self {
            name,
            if_not_exists,
        }
    }

    pub fn name(&self) -> &ObjectName {
        &self.name
    }
}

impl Display for CreateDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("CREATE DATABASE")?;
        if self.if_not_exists {
            f.write_str(" IF NOT EXISTS")?;
        }
        let name = self.name();
        write!(f, r#" {name}"#)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut)]
pub struct CreateExternalTable {
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    /// All keys are lowercase.
    pub options: OptionMap,
    pub if_not_exists: bool,
    pub engine: String,
}

impl CreateExternalTable {
    fn format_constraints(&self) -> String {
        self.constraints
            .iter()
            .map(|c| {
                if is_time_index(c) {
                    let TableConstraint::Unique { columns, .. } = c else {
                        unreachable!()
                    };

                    format_indent!("{}TIME INDEX ({})", format_list_comma!(columns))
                } else {
                    format_indent!(c)
                }
            })
            .join(LINE_SEP)
    }

    #[inline]
    fn format_if_not_exists(&self) -> &str {
        if self.if_not_exists {
            "IF NOT EXISTS"
        } else {
            ""
        }
    }

    #[inline]
    fn format_options(&self) -> String {
        if self.options.map.is_empty() {
            String::default()
        } else {
            let options = format_sorted_hashmap!(&self.options.map);
            format!(
                r#"WITH(
{options}
)"#
            )
        }
    }
}

impl Display for CreateExternalTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = &self.name;
        let columns = format_list_indent!(self.columns);
        let constraints = self.format_constraints();
        let options = self.format_options();
        let if_not_exists = self.format_if_not_exists();
        let engine = &self.engine;
        write!(
            f,
            r#"CREATE EXTERNAL TABLE {if_not_exists} {name} (
{columns},
{constraints}
)
ENGINE={engine}
{options}"#
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut)]
pub struct CreateTableLike {
    /// Table name
    pub table_name: ObjectName,
    /// The table that is designated to be imitated by `Like`
    pub source_name: ObjectName,
}

impl Display for CreateTableLike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let table_name = &self.table_name;
        let source_name = &self.source_name;
        write!(f, r#"CREATE TABLE {table_name} LIKE {source_name}"#)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::error::Error;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_create_table() {
        let sql = r"create table if not exists demo(
                             host string,
                             ts timestamp,
                             cpu double default 0,
                             memory double,
                             TIME INDEX (ts),
                             PRIMARY KEY(host)
                       )
                       PARTITION ON COLUMNS (host) (
                            host = 'a',
                            host > 'a',
                       )
                       engine=mito
                       with(regions=1, ttl='7d', storage='File');
         ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        match &result[0] {
            Statement::CreateTable(c) => {
                let new_sql = format!("\n{}", c);
                assert_eq!(
                    r#"
CREATE TABLE IF NOT EXISTS demo (
  host STRING,
  ts TIMESTAMP,
  cpu DOUBLE DEFAULT 0,
  memory DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (host)
)
PARTITION ON COLUMNS (host) (
  host = 'a',
  host > 'a'
)
ENGINE=mito
WITH(
  regions = 1,
  storage = 'File',
  ttl = '7d'
)"#,
                    &new_sql
                );

                let new_result = ParserContext::create_with_dialect(
                    &new_sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default(),
                )
                .unwrap();
                assert_eq!(result, new_result);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_display_empty_partition_column() {
        let sql = r"create table if not exists demo(
            host string,
            ts timestamp,
            cpu double default 0,
            memory double,
            TIME INDEX (ts),
            PRIMARY KEY(ts, host)
            );
        ";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        match &result[0] {
            Statement::CreateTable(c) => {
                let new_sql = format!("\n{}", c);
                assert_eq!(
                    r#"
CREATE TABLE IF NOT EXISTS demo (
  host STRING,
  ts TIMESTAMP,
  cpu DOUBLE DEFAULT 0,
  memory DOUBLE,
  TIME INDEX (ts),
  PRIMARY KEY (ts, host)
)
ENGINE=mito
"#,
                    &new_sql
                );

                let new_result = ParserContext::create_with_dialect(
                    &new_sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default(),
                )
                .unwrap();
                assert_eq!(result, new_result);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_validate_table_options() {
        let sql = r"create table if not exists demo(
            host string,
            ts timestamp,
            cpu double default 0,
            memory double,
            TIME INDEX (ts),
            PRIMARY KEY(host)
      )
      PARTITION ON COLUMNS (host) ()
      engine=mito
      with(regions=1, ttl='7d', 'compaction.type'='world');
";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        match &result[0] {
            Statement::CreateTable(c) => {
                assert_eq!(3, c.options.len());
            }
            _ => unreachable!(),
        }

        let sql = r"create table if not exists demo(
            host string,
            ts timestamp,
            cpu double default 0,
            memory double,
            TIME INDEX (ts),
            PRIMARY KEY(host)
      )
      PARTITION ON COLUMNS (host) ()
      engine=mito
      with(regions=1, ttl='7d', hello='world');
";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert_matches!(result, Err(Error::InvalidTableOption { .. }))
    }

    #[test]
    fn test_display_create_database() {
        let sql = r"create database test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateDatabase { .. });

        match &stmts[0] {
            Statement::CreateDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
CREATE DATABASE test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"create database if not exists test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateDatabase { .. });

        match &stmts[0] {
            Statement::CreateDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
CREATE DATABASE IF NOT EXISTS test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_create_table_like() {
        let sql = r"create table t2 like t1;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateTableLike { .. });

        match &stmts[0] {
            Statement::CreateTableLike(create) => {
                let new_sql = format!("\n{}", create);
                assert_eq!(
                    r#"
CREATE TABLE t2 LIKE t1"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_create_external_table() {
        let sql = r#"CREATE EXTERNAL TABLE city (
            host string,
            ts timestamp,
            cpu float64 default 0,
            memory float64,
            TIME INDEX (ts),
            PRIMARY KEY(host)
) WITH (location='/var/data/city.csv', format='csv');"#;
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::CreateExternalTable { .. });

        match &stmts[0] {
            Statement::CreateExternalTable(create) => {
                let new_sql = format!("\n{}", create);
                assert_eq!(
                    r#"
CREATE EXTERNAL TABLE  city (
  host STRING,
  ts TIMESTAMP,
  cpu FLOAT64 DEFAULT 0,
  memory FLOAT64,
  TIME INDEX (ts),
  PRIMARY KEY (host)
)
ENGINE=file
WITH(
format = csv,location = /var/data/city.csv,
)"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
