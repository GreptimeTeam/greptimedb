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

use std::fmt::Display;

use itertools::Itertools;
use serde::Serialize;
use sqlparser::ast::{ObjectName, Visit, VisitMut, Visitor, VisitorMut};

/// TRUNCATE TABLE statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TruncateTable {
    table_name: ObjectName,
    time_ranges: Vec<(sqlparser::ast::Value, sqlparser::ast::Value)>,
}

impl Visit for TruncateTable {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ::std::ops::ControlFlow<V::Break> {
        self.table_name.visit(visitor)?;
        for (start, end) in &self.time_ranges {
            start.visit(visitor)?;
            end.visit(visitor)?;
        }
        ::std::ops::ControlFlow::Continue(())
    }
}

impl VisitMut for TruncateTable {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ::std::ops::ControlFlow<V::Break> {
        sqlparser::ast::VisitMut::visit(&mut self.table_name, visitor)?;
        for (start, end) in &mut self.time_ranges {
            start.visit(visitor)?;
            end.visit(visitor)?;
        }
        ::std::ops::ControlFlow::Continue(())
    }
}

impl TruncateTable {
    /// Creates a statement for `TRUNCATE TABLE`
    pub fn new(table_name: ObjectName) -> Self {
        Self {
            table_name,
            time_ranges: Vec::new(),
        }
    }

    /// Creates a statement for `TRUNCATE TABLE RANGE`
    pub fn new_with_ranges(
        table_name: ObjectName,
        time_ranges: Vec<(sqlparser::ast::Value, sqlparser::ast::Value)>,
    ) -> Self {
        Self {
            table_name,
            time_ranges,
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn time_ranges(&self) -> &[(sqlparser::ast::Value, sqlparser::ast::Value)] {
        &self.time_ranges
    }
}

impl Display for TruncateTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table_name = self.table_name();
        write!(f, r#"TRUNCATE TABLE {table_name}"#)?;
        if self.time_ranges.is_empty() {
            return Ok(());
        }

        write!(f, " FILE RANGE ")?;
        write!(
            f,
            "{}",
            self.time_ranges
                .iter()
                .map(|(start, end)| format!("({}, {})", start, end))
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_for_truncate_table() {
        let sql = r"truncate table t1;";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::TruncateTable { .. });
        match &stmts[0] {
            Statement::TruncateTable(trunc) => {
                let new_sql = format!("\n{}", trunc);
                assert_eq!(
                    r#"
TRUNCATE TABLE t1"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"truncate table t1 file range (1,2);";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::TruncateTable { .. });
        match &stmts[0] {
            Statement::TruncateTable(trunc) => {
                let new_sql = format!("\n{}", trunc);
                assert_eq!(
                    r#"
TRUNCATE TABLE t1 FILE RANGE (1, 2)"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"truncate table t1 file range (1,2), (3,4);";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::TruncateTable { .. });
        match &stmts[0] {
            Statement::TruncateTable(trunc) => {
                let new_sql = format!("\n{}", trunc);
                assert_eq!(
                    r#"
TRUNCATE TABLE t1 FILE RANGE (1, 2), (3, 4)"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
