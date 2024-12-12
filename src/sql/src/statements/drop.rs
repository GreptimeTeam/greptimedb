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

use serde::Serialize;
use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

/// DROP TABLE statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct DropTable {
    table_names: Vec<ObjectName>,

    /// drop table if exists
    drop_if_exists: bool,
}

impl DropTable {
    /// Creates a statement for `DROP TABLE`
    pub fn new(table_names: Vec<ObjectName>, if_exists: bool) -> Self {
        Self {
            table_names,
            drop_if_exists: if_exists,
        }
    }

    pub fn table_names(&self) -> &[ObjectName] {
        &self.table_names
    }

    pub fn drop_if_exists(&self) -> bool {
        self.drop_if_exists
    }
}

impl Display for DropTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DROP TABLE")?;
        if self.drop_if_exists() {
            f.write_str(" IF EXISTS")?;
        }
        let table_names = self.table_names();
        for (i, table_name) in table_names.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, " {}", table_name)?;
        }
        Ok(())
    }
}

/// DROP DATABASE statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct DropDatabase {
    name: ObjectName,
    /// drop table if exists
    drop_if_exists: bool,
}

impl DropDatabase {
    /// Creates a statement for `DROP DATABASE`
    pub fn new(name: ObjectName, if_exists: bool) -> Self {
        Self {
            name,
            drop_if_exists: if_exists,
        }
    }

    pub fn name(&self) -> &ObjectName {
        &self.name
    }

    pub fn drop_if_exists(&self) -> bool {
        self.drop_if_exists
    }
}

impl Display for DropDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DROP DATABASE")?;
        if self.drop_if_exists() {
            f.write_str(" IF EXISTS")?;
        }
        let name = self.name();
        write!(f, r#" {name}"#)
    }
}

/// DROP FLOW statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct DropFlow {
    flow_name: ObjectName,
    /// drop flow if exists
    drop_if_exists: bool,
}

impl DropFlow {
    /// Creates a statement for `DROP DATABASE`
    pub fn new(flow_name: ObjectName, if_exists: bool) -> Self {
        Self {
            flow_name,
            drop_if_exists: if_exists,
        }
    }

    /// Returns the flow name.
    pub fn flow_name(&self) -> &ObjectName {
        &self.flow_name
    }

    /// Return the `drop_if_exists`.
    pub fn drop_if_exists(&self) -> bool {
        self.drop_if_exists
    }
}

impl Display for DropFlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DROP FLOW")?;
        if self.drop_if_exists() {
            f.write_str(" IF EXISTS")?;
        }
        let flow_name = self.flow_name();
        write!(f, r#" {flow_name}"#)
    }
}

/// `DROP VIEW` statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct DropView {
    // The view name
    pub view_name: ObjectName,
    // drop view if exists
    pub drop_if_exists: bool,
}

impl Display for DropView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP VIEW{} {}",
            if self.drop_if_exists {
                " IF EXISTS"
            } else {
                ""
            },
            self.view_name
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
    fn test_display_drop_database() {
        let sql = r"drop database test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropDatabase { .. });

        match &stmts[0] {
            Statement::DropDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP DATABASE test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"drop database if exists test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropDatabase { .. });

        match &stmts[0] {
            Statement::DropDatabase(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP DATABASE IF EXISTS test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_drop_table() {
        let sql = r"drop table test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropTable { .. });

        match &stmts[0] {
            Statement::DropTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP TABLE test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"drop table test1, test2;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropTable { .. });

        match &stmts[0] {
            Statement::DropTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP TABLE test1, test2"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"drop table if exists test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropTable { .. });

        match &stmts[0] {
            Statement::DropTable(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP TABLE IF EXISTS test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_drop_flow() {
        let sql = r"drop flow test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropFlow { .. });

        match &stmts[0] {
            Statement::DropFlow(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP FLOW test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }

        let sql = r"drop flow if exists test;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DropFlow { .. });

        match &stmts[0] {
            Statement::DropFlow(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
DROP FLOW IF EXISTS test"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
