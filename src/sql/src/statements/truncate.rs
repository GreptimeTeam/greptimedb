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

use serde::{Deserialize, Serialize};
use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

/// TRUNCATE TABLE statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize, Deserialize)]
pub struct TruncateTable {
    table_name: ObjectName,
}

impl TruncateTable {
    /// Creates a statement for `TRUNCATE TABLE`
    pub fn new(table_name: ObjectName) -> Self {
        Self { table_name }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }
}

impl Display for TruncateTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table_name = self.table_name();
        write!(f, r#"TRUNCATE TABLE {table_name}"#)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_for_tuncate_table() {
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
    }
}
