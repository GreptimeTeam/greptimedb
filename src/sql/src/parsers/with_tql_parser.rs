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

use serde::Serialize;
use snafu::ResultExt;
use sqlparser::ast::{Ident, Query as SpQuery};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::parsers::tql_parser;
use crate::statements::query::Query;
use crate::statements::statement::Statement;
use crate::statements::tql::{Tql, TqlEval};

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
    /// Column aliases for the CTE table
    pub columns: Option<Vec<Ident>>,
    pub content: CteContent,
}

/// Extended WITH clause that supports hybrid SQL/TQL CTEs
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct HybridCteWith {
    pub recursive: bool,
    pub cte_tables: Vec<HybridCte>,
}

/// Parser implementation for hybrid WITH clauses containing TQL
impl ParserContext<'_> {
    /// Parse a WITH clause that may contain TQL CTEs
    pub(crate) fn parse_with_tql(&mut self) -> Result<Statement> {
        // Consume the WITH token
        self.parser
            .expect_keyword(Keyword::WITH)
            .context(error::SyntaxSnafu)?;

        // Check for RECURSIVE keyword
        let recursive = self.parser.parse_keyword(Keyword::RECURSIVE);

        // Parse the CTE list
        let mut cte_tables = Vec::new();

        loop {
            let cte = self.parse_hybrid_cte()?;
            cte_tables.push(cte);

            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }

        // Parse the main query
        let main_query = self.parser.parse_query().context(error::SyntaxSnafu)?;

        // Convert the hybrid CTEs to a standard query with hybrid metadata
        let hybrid_cte = HybridCteWith {
            recursive,
            cte_tables,
        };

        // Create a Query statement with hybrid CTE metadata
        let mut query = Query::try_from(*main_query)?;
        query.hybrid_cte = Some(hybrid_cte);

        Ok(Statement::Query(Box::new(query)))
    }

    /// Parse a single CTE that can be either SQL or TQL
    fn parse_hybrid_cte(&mut self) -> Result<HybridCte> {
        // Parse CTE name
        let name = self.parser.parse_identifier().context(error::SyntaxSnafu)?;
        let name = Self::canonicalize_identifier(name);

        // Parse optional column list
        let columns = if self.parser.consume_token(&Token::LParen) {
            let mut columns = Vec::new();
            loop {
                let column = self.parser.parse_identifier().context(error::SyntaxSnafu)?;
                columns.push(Self::canonicalize_identifier(column));

                if !self.parser.consume_token(&Token::Comma) {
                    break;
                }
            }
            self.parser
                .expect_token(&Token::RParen)
                .context(error::SyntaxSnafu)?;
            Some(columns)
        } else {
            None
        };

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
                // Parse as TQL - consume the TQL keyword first
                let _ = self.parser.next_token();

                // Parse the TQL statement content
                let tql = self.parse_tql_content_in_cte()?;
                return Ok(CteContent::Tql(tql));
            }
        }

        // Parse as SQL query
        let sql_query = self.parser.parse_query().context(error::SyntaxSnafu)?;
        Ok(CteContent::Sql(sql_query))
    }

    /// Parse TQL content within CTE context (after TQL keyword is consumed)
    fn parse_tql_content_in_cte(&mut self) -> Result<Tql> {
        // Check what type of TQL statement this is
        if let Token::Word(w) = &self.parser.peek_token().token {
            let uppercase = w.value.to_uppercase();
            match w.keyword {
                Keyword::NoKeyword
                    if (uppercase == "EVAL" || uppercase == "EVALUATE")
                        && w.quote_style.is_none() =>
                {
                    // Consume EVAL/EVALUATE keyword
                    let _ = self.parser.next_token();

                    // Parse the TQL parameters and query
                    let tql_eval = self.parse_tql_eval_in_cte()?;
                    Ok(Tql::Eval(tql_eval))
                }
                Keyword::EXPLAIN => {
                    // For now, only support EVAL in CTEs
                    Err(error::InvalidSqlSnafu {
                        msg: "TQL EXPLAIN is not supported in CTEs".to_string(),
                    }
                    .build())
                }
                Keyword::ANALYZE => {
                    // For now, only support EVAL in CTEs
                    Err(error::InvalidSqlSnafu {
                        msg: "TQL ANALYZE is not supported in CTEs".to_string(),
                    }
                    .build())
                }
                _ => Err(error::InvalidSqlSnafu {
                    msg: format!("Unknown TQL statement type: {}", w.value),
                }
                .build()),
            }
        } else {
            Err(error::InvalidSqlSnafu {
                msg: "Expected TQL statement type (EVAL, EXPLAIN, ANALYZE)".to_string(),
            }
            .build())
        }
    }

    /// Parse TQL EVAL parameters within CTE context
    fn parse_tql_eval_in_cte(&mut self) -> Result<TqlEval> {
        // Parse parameters
        let (start, end, step, lookback) = if self.parser.consume_token(&Token::LParen) {
            // Parse start parameter
            let start = self.parse_tql_param_value(&[Token::Comma])?;

            // Parse end parameter
            let end = self.parse_tql_param_value(&[Token::Comma])?;

            // Parse step parameter
            let step = self.parse_tql_param_value(&[Token::Comma, Token::RParen])?;

            // Check if there's a lookback parameter
            let lookback = if self.parser.peek_token().token == Token::Comma {
                self.parser.next_token(); // consume comma
                Some(self.parse_tql_param_value(&[Token::RParen])?)
            } else {
                None
            };

            // The closing paren should already be consumed by parse_tql_param_value

            (start, end, step, lookback)
        } else {
            // No parameters, use defaults
            ("0".to_string(), "0".to_string(), "5m".to_string(), None)
        };

        // Parse the query part - everything remaining until we hit the end of the CTE content
        let query = self.parse_tql_query_in_cte()?;

        Ok(TqlEval {
            start,
            end,
            step,
            lookback,
            query,
        })
    }

    /// Parse a single TQL parameter value
    fn parse_tql_param_value(&mut self, delimiters: &[Token]) -> Result<String> {
        let mut tokens = Vec::new();

        // Collect tokens until we hit a delimiter
        while !delimiters.contains(&self.parser.peek_token().token)
            && self.parser.peek_token().token != Token::EOF
        {
            tokens.push(self.parser.next_token().token);
        }

        // Consume the delimiter
        for delimiter in delimiters {
            if self.parser.consume_token(delimiter) {
                break;
            }
        }

        // Convert tokens to string value
        if tokens.is_empty() {
            return Err(error::InvalidSqlSnafu {
                msg: "Expected parameter value".to_string(),
            }
            .build());
        }

        let value = match &tokens[0] {
            Token::Number(n, _) => n.clone(),
            Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => s.clone(),
            Token::Word(w) => w.value.clone(),
            _ => {
                return Err(error::InvalidSqlSnafu {
                    msg: "Invalid parameter value format".to_string(),
                }
                .build());
            }
        };

        Ok(value)
    }

    /// Parse TQL query within CTE context
    fn parse_tql_query_in_cte(&mut self) -> Result<String> {
        // Skip any whitespace or commas
        while matches!(
            self.parser.peek_token().token,
            Token::Comma | Token::Whitespace(_)
        ) {
            self.parser.next_token();
        }

        // Get the starting position for extracting the query text
        let start_token = self.parser.peek_token();
        if start_token.token == Token::EOF {
            return Err(error::InvalidSqlSnafu {
                msg: "Empty TQL query".to_string(),
            }
            .build());
        }

        let _start_location = start_token.span.start;

        // Collect tokens until we find the closing parenthesis of the CTE
        // We need to be careful not to consume the closing paren
        let mut query_tokens = Vec::new();
        let mut paren_depth = 0;

        while self.parser.peek_token().token != Token::EOF {
            let token = &self.parser.peek_token().token;

            match token {
                Token::LParen => {
                    paren_depth += 1;
                    query_tokens.push(self.parser.next_token());
                }
                Token::RParen if paren_depth > 0 => {
                    paren_depth -= 1;
                    query_tokens.push(self.parser.next_token());
                }
                Token::RParen => {
                    // This is the closing paren of the CTE - don't consume it
                    break;
                }
                _ => {
                    query_tokens.push(self.parser.next_token());
                }
            }
        }

        if query_tokens.is_empty() {
            return Err(error::InvalidSqlSnafu {
                msg: "Empty TQL query".to_string(),
            }
            .build());
        }

        // Convert tokens back to string
        let query = query_tokens
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        Ok(query.trim().to_string())
    }

    /// Check if a WITH clause contains TQL by lookahead parsing
    pub(crate) fn contains_tql_in_with(&mut self) -> Result<bool> {
        // Save the current parser position
        let checkpoint = self.parser.index();

        // Perform lookahead parsing to detect TQL
        let has_tql = self.scan_for_tql_in_ctes().unwrap_or(false);

        // Restore parser position by creating a new parser with the original SQL
        // and advancing to the checkpoint position
        let mut new_parser = sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {})
            .try_with_sql(self.sql)
            .context(error::SyntaxSnafu)?;

        // Advance to the checkpoint position
        for _ in 0..checkpoint {
            if new_parser.peek_token() == Token::EOF {
                break;
            }
            new_parser.next_token();
        }

        self.parser = new_parser;

        Ok(has_tql)
    }

    /// Scan through CTE definitions looking for TQL keywords
    fn scan_for_tql_in_ctes(&mut self) -> Result<bool> {
        // Consume WITH
        if !self.parser.parse_keyword(Keyword::WITH) {
            return Ok(false);
        }

        // Skip RECURSIVE if present
        let _ = self.parser.parse_keyword(Keyword::RECURSIVE);

        // Scan through CTEs
        loop {
            // Skip CTE name
            if self.parser.parse_identifier().is_err() {
                break;
            }

            // Skip optional column list
            if self.parser.consume_token(&Token::LParen) {
                let mut paren_depth = 1;
                while paren_depth > 0 && self.parser.peek_token() != Token::EOF {
                    match self.parser.next_token().token {
                        Token::LParen => paren_depth += 1,
                        Token::RParen => paren_depth -= 1,
                        _ => {}
                    }
                }
            }

            // Skip AS keyword
            if !self.parser.parse_keyword(Keyword::AS) {
                break;
            }

            // Check content inside parentheses
            if self.parser.consume_token(&Token::LParen) {
                // Look for TQL keyword
                if let Token::Word(w) = &self.parser.peek_token().token {
                    if w.keyword == Keyword::NoKeyword
                        && w.quote_style.is_none()
                        && w.value.to_uppercase() == tql_parser::TQL
                    {
                        return Ok(true);
                    }
                }

                // Skip to matching closing parenthesis
                let mut paren_depth = 1;
                while paren_depth > 0 && self.parser.peek_token() != Token::EOF {
                    match self.parser.next_token().token {
                        Token::LParen => paren_depth += 1,
                        Token::RParen => paren_depth -= 1,
                        _ => {}
                    }
                }
            }

            // Check for comma to continue to next CTE
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::parsers::with_tql_parser::CteContent;
    use crate::statements::statement::Statement;

    #[test]
    fn test_tql_detection() {
        // this should detect TQL
        let sql = "WITH tql_cte AS (TQL EVAL (0, 100, '5s') up) SELECT * FROM tql_cte";
        let mut parser = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let has_tql = parser.contains_tql_in_with().unwrap();
        assert!(has_tql);

        // normal SQL doesn't trigger TQL detection
        let sql_normal = "WITH normal_cte AS (SELECT * FROM my_table) SELECT * FROM normal_cte";
        let mut parser_normal = ParserContext::new(&GreptimeDbDialect {}, sql_normal).unwrap();
        let has_tql_normal = parser_normal.contains_tql_in_with().unwrap();
        assert!(!has_tql_normal);
    }

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
        if let CteContent::Tql(tql) = &hybrid_cte.cte_tables[0].content {
            if let crate::statements::tql::Tql::Eval(eval) = tql {
                // Verify that complex nested parentheses are preserved correctly
                assert!(eval
                    .query
                    .contains("sum ( rate ( http_requests_total [ 1 m ] ) )"));
                assert!(eval.query.contains("( max ( cpu_usage ) * ( 1 + 0.5 ) )"));
                // Most importantly, verify the parentheses counting didn't break the parsing
                assert!(eval.query.contains("+ ( max"));
            }
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
        assert_eq!(hybrid_cte.cte_tables.len(), 2);

        // First CTE should be SQL with column aliases
        let first_cte = &hybrid_cte.cte_tables[0];
        assert!(matches!(first_cte.content, CteContent::Sql(_)));
        assert!(first_cte.columns.is_some());
        let columns = first_cte.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].value, "ts");
        assert_eq!(columns[1].value, "value");
        assert_eq!(columns[2].value, "label");

        // Second CTE should be TQL with column aliases
        let second_cte = &hybrid_cte.cte_tables[1];
        assert!(matches!(second_cte.content, CteContent::Tql(_)));
        assert!(second_cte.columns.is_some());
        let columns = second_cte.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].value, "time");
        assert_eq!(columns[1].value, "metric_value");
    }
}
