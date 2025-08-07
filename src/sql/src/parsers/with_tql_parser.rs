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

use std::fmt;

use serde::Serialize;
use snafu::ResultExt;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Cte, Ident, ObjectName, Query as SpQuery, TableAlias, TableAliasColumnDef, With,
};
use sqlparser::keywords::Keyword;
use sqlparser::parser::IsOptional;
use sqlparser::tokenizer::Token;
use sqlparser_derive::{Visit, VisitMut};

use crate::dialect::GreptimeDbDialect;
use crate::error::{self, Result};
use crate::parser::{ParseOptions, ParserContext};
use crate::parsers::tql_parser;
use crate::statements::query::Query;
use crate::statements::statement::Statement;
use crate::statements::tql::Tql;

/// Content of a CTE - either SQL or TQL
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum CteContent {
    Sql(Box<SpQuery>),
    Tql(Tql),
}

/// A hybrid CTE that can contain either SQL or TQL
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct HybridCte {
    pub name: Ident,
    /// Column aliases for the CTE table. Empty if not specified.
    pub columns: Vec<ObjectName>,
    pub content: CteContent,
}

/// Extended WITH clause that supports hybrid SQL/TQL CTEs
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct HybridCteWith {
    pub recursive: bool,
    pub cte_tables: Vec<HybridCte>,
}

impl fmt::Display for HybridCteWith {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WITH ")?;

        if self.recursive {
            write!(f, "RECURSIVE ")?;
        }

        for (i, cte) in self.cte_tables.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", cte.name)?;

            if !cte.columns.is_empty() {
                write!(f, " (")?;
                for (j, col) in cte.columns.iter().enumerate() {
                    if j > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", col)?;
                }
                write!(f, ")")?;
            }

            write!(f, " AS (")?;
            match &cte.content {
                CteContent::Sql(query) => write!(f, "{}", query)?,
                CteContent::Tql(tql) => write!(f, "{}", tql)?,
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// Parser implementation for hybrid WITH clauses containing TQL
impl ParserContext<'_> {
    /// Parse a WITH clause that may contain TQL CTEs or SQL CTEs.
    pub(crate) fn parse_with_tql(&mut self) -> Result<Statement> {
        // Consume the WITH token
        self.parser
            .expect_keyword(Keyword::WITH)
            .context(error::SyntaxSnafu)?;

        // Check for RECURSIVE keyword
        let recursive = self.parser.parse_keyword(Keyword::RECURSIVE);

        // Parse the CTE list
        let mut tql_cte_tables = Vec::new();
        let mut sql_cte_tables = Vec::new();

        loop {
            let cte = self.parse_hybrid_cte()?;
            match cte.content {
                CteContent::Sql(body) => sql_cte_tables.push(Cte {
                    alias: TableAlias {
                        name: cte.name,
                        columns: cte
                            .columns
                            .into_iter()
                            .flat_map(|col| col.0[0].as_ident().cloned())
                            .map(|name| TableAliasColumnDef {
                                name,
                                data_type: None,
                            })
                            .collect(),
                    },
                    query: body,
                    from: None,
                    materialized: None,
                    closing_paren_token: AttachedToken::empty(),
                }),
                CteContent::Tql(_) => tql_cte_tables.push(cte),
            }

            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }

        // Parse the main query
        let main_query = self.parser.parse_query().context(error::SyntaxSnafu)?;

        // Convert the hybrid CTEs to a standard query with hybrid metadata
        let hybrid_cte = HybridCteWith {
            recursive,
            cte_tables: tql_cte_tables,
        };

        // Create a Query statement with hybrid CTE metadata
        let mut query = Query::try_from(*main_query)?;
        query.hybrid_cte = Some(hybrid_cte);
        query.inner.with = Some(With {
            recursive,
            cte_tables: sql_cte_tables,
            with_token: AttachedToken::empty(),
        });

        Ok(Statement::Query(Box::new(query)))
    }

    /// Parse a single CTE that can be either SQL or TQL
    fn parse_hybrid_cte(&mut self) -> Result<HybridCte> {
        // Parse CTE name
        let name = self.parser.parse_identifier().context(error::SyntaxSnafu)?;
        let name = Self::canonicalize_identifier(name);

        // Parse optional column list
        let columns = self
            .parser
            .parse_parenthesized_qualified_column_list(IsOptional::Optional, true)
            .context(error::SyntaxSnafu)?;

        // Expect AS keyword
        self.parser
            .expect_keyword(Keyword::AS)
            .context(error::SyntaxSnafu)?;

        // Parse the CTE content
        self.parser
            .expect_token(&Token::LParen)
            .context(error::SyntaxSnafu)?;

        let content = self.parse_cte_content()?;

        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;

        Ok(HybridCte {
            name,
            columns,
            content,
        })
    }

    /// Determine if CTE contains TQL or SQL and parse accordingly
    fn parse_cte_content(&mut self) -> Result<CteContent> {
        // Check if the next token is TQL
        if let Token::Word(w) = &self.parser.peek_token().token {
            if w.keyword == Keyword::NoKeyword
                && w.quote_style.is_none()
                && w.value.to_uppercase() == tql_parser::TQL
            {
                let tql = self.parse_tql_content_in_cte()?;
                return Ok(CteContent::Tql(tql));
            }
        }

        // Parse as SQL query
        let sql_query = self.parser.parse_query().context(error::SyntaxSnafu)?;
        Ok(CteContent::Sql(sql_query))
    }

    /// Parse TQL content within a CTE by reusing the standard TQL parser.
    ///
    /// This method consumes all tokens that belong to the TQL statement and
    /// stops right **before** the closing `)` of the CTE so that the caller
    /// can handle it normally.
    ///
    /// Only `TQL EVAL` is supported inside CTEs.
    fn parse_tql_content_in_cte(&mut self) -> Result<Tql> {
        let mut collected: Vec<Token> = Vec::new();
        let mut paren_depth = 0usize;

        loop {
            let token_with_span = self.parser.peek_token();

            // Guard against unexpected EOF
            if token_with_span.token == Token::EOF {
                return Err(error::InvalidSqlSnafu {
                    msg: "Unexpected end of input while parsing TQL inside CTE".to_string(),
                }
                .build());
            }

            // Stop **before** the closing parenthesis that ends the CTE
            if token_with_span.token == Token::RParen && paren_depth == 0 {
                break;
            }

            // Consume the token and push it into our buffer
            let consumed = self.parser.next_token();
            match consumed.token {
                Token::LParen => paren_depth += 1,
                Token::RParen => {
                    // This RParen must belong to a nested expression since
                    // `paren_depth > 0` here. Decrease depth accordingly.
                    paren_depth = paren_depth.saturating_sub(1);
                }
                _ => {}
            }

            collected.push(consumed.token);
        }

        // Re-construct the SQL string of the isolated TQL statement.
        let tql_string = collected
            .iter()
            .map(|tok| tok.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        // Use the shared parser to turn it into a `Statement`.
        let mut stmts = ParserContext::create_with_dialect(
            &tql_string,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )?;

        if stmts.len() != 1 {
            return Err(error::InvalidSqlSnafu {
                msg: "Expected a single TQL statement inside CTE".to_string(),
            }
            .build());
        }

        match stmts.remove(0) {
            Statement::Tql(Tql::Eval(eval)) => Ok(Tql::Eval(eval)),
            Statement::Tql(_) => Err(error::InvalidSqlSnafu {
                msg: "Only TQL EVAL is supported in CTEs".to_string(),
            }
            .build()),
            _ => Err(error::InvalidSqlSnafu {
                msg: "Expected a TQL statement inside CTE".to_string(),
            }
            .build()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::parsers::with_tql_parser::CteContent;
    use crate::statements::statement::Statement;
    use crate::statements::tql::Tql;

    #[test]
    fn test_parse_hybrid_cte_with_parentheses_in_query() {
        // Test that parentheses within the TQL query don't interfere with CTE parsing
        let sql = r#"
            WITH tql_cte AS (
                TQL EVAL (0, 100, '5s') 
                sum(rate(http_requests_total[1m])) + (max(cpu_usage) * (1 + 0.5))
            ) 
            SELECT * FROM tql_cte
        "#;

        let statements =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(statements.len(), 1);

        let Statement::Query(query) = &statements[0] else {
            panic!("Expected Query statement");
        };
        let hybrid_cte = query.hybrid_cte.as_ref().unwrap();
        assert_eq!(hybrid_cte.cte_tables.len(), 1);

        // Should be TQL content
        assert!(matches!(
            hybrid_cte.cte_tables[0].content,
            CteContent::Tql(_)
        ));

        // Check that the query includes the parentheses (spaces are added by tokenizer)
        if let CteContent::Tql(Tql::Eval(eval)) = &hybrid_cte.cte_tables[0].content {
            // Verify that complex nested parentheses are preserved correctly
            assert!(eval
                .query
                .contains("sum ( rate ( http_requests_total [ 1 m ] ) )"));
            assert!(eval.query.contains("( max ( cpu_usage ) * ( 1 + 0.5 ) )"));
            // Most importantly, verify the parentheses counting didn't break the parsing
            assert!(eval.query.contains("+ ( max"));
        }
    }

    #[test]
    fn test_parse_hybrid_cte_sql_and_tql() {
        let sql = r#"
            WITH 
                sql_cte(ts, value, label) AS (SELECT timestamp, val, name FROM metrics),
                tql_cte(time, metric_value) AS (TQL EVAL (0, 100, '5s') cpu_usage)
            SELECT s.ts, s.value, t.metric_value 
            FROM sql_cte s JOIN tql_cte t ON s.ts = t.time
        "#;

        let statements =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(statements.len(), 1);

        let Statement::Query(query) = &statements[0] else {
            panic!("Expected Query statement");
        };
        let hybrid_cte = query.hybrid_cte.as_ref().unwrap();
        assert_eq!(hybrid_cte.cte_tables.len(), 1); // only TQL CTE presents here

        // First CTE should be TQL with column aliases
        let second_cte = &hybrid_cte.cte_tables[0];
        assert!(matches!(second_cte.content, CteContent::Tql(_)));
        assert_eq!(second_cte.columns.len(), 2);
        assert_eq!(
            second_cte
                .columns
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(" "),
            "time metric_value"
        );
    }
}
