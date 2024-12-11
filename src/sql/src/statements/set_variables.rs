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
use sqlparser::ast::{Expr, ObjectName};
use sqlparser_derive::{Visit, VisitMut};

/// SET variables statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize, Deserialize)]
pub struct SetVariables {
    pub variable: ObjectName,
    pub value: Vec<Expr>,
}

impl Display for SetVariables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variable = &self.variable;
        let value = &self
            .value
            .iter()
            .map(|expr| format!("{}", expr))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, r#"SET {variable} = {value}"#)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_show_variables() {
        let sql = r"set delayed_insert_timeout=300;";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::SetVariables { .. });

        match &stmts[0] {
            Statement::SetVariables(set) => {
                let new_sql = format!("\n{}", set);
                assert_eq!(
                    r#"
SET delayed_insert_timeout = 300"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
