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

use snafu::prelude::*;
use sqlparser::ast::ObjectName;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::query::Query;
use crate::statements::statement::Statement;

impl ParserContext<'_> {
    /// Parses select and it's variants.
    pub(crate) fn parse_query(&mut self) -> Result<Statement> {
        self.table_aliases.clear();
        let spquery = self.parser.parse_query().context(error::SyntaxSnafu)?;

        self.process_query_table_aliases(&spquery)?;

        Ok(Statement::Query(Box::new(Query::try_from(*spquery)?)))
    }

    fn process_query_table_aliases(&mut self, query: &sqlparser::ast::Query) -> Result<()> {
        self.process_set_expr_table_aliases(&query.body)?;
        Ok(())
    }

    fn process_set_expr_table_aliases(&mut self, set_expr: &sqlparser::ast::SetExpr) -> Result<()> {
        match set_expr {
            sqlparser::ast::SetExpr::Select(select) => {
                for table_with_joins in &select.from {
                    self.process_table_with_joins(&table_with_joins)?;
                }
            }
            sqlparser::ast::SetExpr::Query(query) => {
                self.process_set_expr_table_aliases(&query.body)?;
            }
            sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
                self.process_set_expr_table_aliases(left)?;
                self.process_set_expr_table_aliases(right)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn process_table_with_joins(
        &mut self,
        table_with_joins: &sqlparser::ast::TableWithJoins,
    ) -> Result<()> {
        self.process_table_factor(&table_with_joins.relation)?;
        for join in &table_with_joins.joins {
            self.process_table_factor(&join.relation)?;
        }

        Ok(())
    }

    fn process_table_factor(&mut self, table_factor: &sqlparser::ast::TableFactor) -> Result<()> {
        match table_factor {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                if let Some(alias) = alias {
                    let alias_name = &alias.name;
                    let full_table_name = Self::convert_sql_object_name(name.clone());
                    self.add_table_alias(alias_name.to_string(), full_table_name.clone())?;
                    println!("Found table alias: {} AS {}", full_table_name, alias_name);
                }
            }
            sqlparser::ast::TableFactor::Derived {
                alias, subquery, ..
            } => {
                if let Some(alias) = alias {
                    let alias_name = &alias.name;
                    let derived_name =
                        ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                            sqlparser::ast::Ident::new(format!("derived_{}", alias_name)),
                        )]);
                    self.add_table_alias(alias_name.to_string(), derived_name)?;
                    println!("Registered subquery alias: {}", alias_name);
                }
                self.process_set_expr_table_aliases(&subquery.body)?;
            }
            sqlparser::ast::TableFactor::NestedJoin { .. } => {}
            sqlparser::ast::TableFactor::TableFunction { .. } => {}
            _ => {}
        }
        Ok(())
    }

    fn convert_sql_object_name(name: sqlparser::ast::ObjectName) -> ObjectName {
        ObjectName(name.0.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};

    #[test]
    pub fn test_parse_query() {
        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

        let _ =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
    }

    #[test]
    pub fn test_parse_invalid_query() {
        let sql = "SELECT * FROM table_1 WHERE";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .output_msg()
            .contains("Expected: an expression"));
    }

    #[test]
    pub fn test_parse_table_with_alias_query() {
        let sql = "SELECT u.name, u.age FROM users AS u";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_ok());
    }

    #[test]
    pub fn test_duplicate_alias_error() {
        let sql = "SELECT * FROM users AS u, orders AS u";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err());
        assert!(result.unwrap_err().output_msg().contains("Duplicate alias"));
    }
}
