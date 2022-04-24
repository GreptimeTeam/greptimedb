use snafu::ResultExt;
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::errors;
use crate::statements::show_kind::ShowKind;
use crate::statements::statement::Statement;
use crate::statements::statement_show_database::SqlShowDatabase;

/// GrepTime SQL parser context, a simple wrapper for Datafusion SQL parser.
pub struct ParserContext<'a> {
    pub(crate) parser: Parser<'a>,
    pub(crate) sql: &'a str,
}

impl<'a> ParserContext<'a> {
    /// Parses SQL with given dialect
    pub fn create_with_dialect(
        sql: &'a str,
        dialect: &dyn Dialect,
    ) -> Result<Vec<Statement>, errors::ParserError> {
        let mut stmts: Vec<Statement> = Vec::new();
        let mut tokenizer = Tokenizer::new(dialect, sql);

        let tokens: Vec<Token> = tokenizer.tokenize().unwrap();

        let mut parser_ctx = ParserContext {
            sql,
            parser: Parser::new(tokens, dialect),
        };

        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser_ctx.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser_ctx.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser_ctx.unsupported(parser_ctx.parser.peek_token().to_string());
            }

            let statement = parser_ctx.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }

        Ok(stmts)
    }

    /// Parses parser context to a set of statements.
    pub fn parse_statement(&mut self) -> Result<Statement, errors::ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        self.parser.next_token();
                        self.parse_create()
                    }

                    Keyword::EXPLAIN => {
                        self.parser.next_token();
                        self.parse_explain()
                    }

                    Keyword::SHOW => {
                        self.parser.next_token();
                        self.parse_show()
                    }

                    Keyword::INSERT => self.parse_insert(),

                    Keyword::SELECT | Keyword::WITH | Keyword::VALUES => self.parse_query(),

                    // todo(hl) support more statements.
                    _ => self.unsupported(self.parser.peek_token().to_string()),
                }
            }
            Token::LParen => self.parse_query(),
            unsupported => self.unsupported(unsupported.to_string()),
        }
    }

    /// Raises an "unsupported statement" error.
    pub fn unsupported<T>(&self, token: String) -> Result<T, errors::ParserError> {
        Err(errors::ParserError::Unsupported {
            sql: self.sql.to_string(),
            token,
        })
    }

    /// Parses SHOW statements
    /// todo(hl) support `show table`/`show settings`/`show create`/`show users` ect.
    fn parse_show(&mut self) -> Result<Statement, errors::ParserError> {
        if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
            Ok(self.parse_show_databases()?)
        } else {
            self.unsupported(self.parser.peek_token().to_string())
        }
    }

    fn parse_explain(&mut self) -> Result<Statement, errors::ParserError> {
        todo!()
    }

    fn parse_insert(&mut self) -> Result<Statement, errors::ParserError> {
        todo!()
    }

    fn parse_query(&mut self) -> Result<Statement, errors::ParserError> {
        todo!()
    }

    fn parse_create(&mut self) -> Result<Statement, errors::ParserError> {
        todo!()
    }

    pub fn consume_token(&mut self, expected: &str) -> bool {
        if self.parser.peek_token().to_string().to_uppercase() == *expected.to_uppercase() {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    /// Parses `SHOW DATABASES` statement.
    pub fn parse_show_databases(&mut self) -> Result<Statement, errors::ParserError> {
        let tok = self.parser.next_token();
        match &tok {
            Token::EOF | Token::SemiColon => Ok(Statement::ShowDatabases(SqlShowDatabase::create(
                ShowKind::All,
            ))),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(Statement::ShowDatabases(SqlShowDatabase::create(
                    ShowKind::Like(
                        self.parser
                            .parse_identifier()
                            .context(errors::UnexpectedSnafu {
                                sql: self.sql,
                                expected: "LIKE",
                                actual: tok.to_string(),
                            })
                            .unwrap(),
                    ),
                ))),
                Keyword::WHERE => Ok(Statement::ShowDatabases(SqlShowDatabase::create(
                    ShowKind::Where(self.parser.parse_expr().context(errors::UnexpectedSnafu {
                        sql: self.sql.to_string(),
                        expected: "some valid expression".to_string(),
                        actual: self.parser.peek_token().to_string(),
                    })?),
                ))),
                _ => self.unsupported(self.parser.peek_token().to_string()),
            },
            unsupported => self.unsupported(unsupported.to_string()),
        }
    }
}

mod test {
    #[test]
    pub fn test_show_database_all() {
        use std::assert_matches::assert_matches;

        use sqlparser::dialect::GenericDialect;

        use crate::parser::ParserContext;
        use crate::parser::ShowKind;
        use crate::parser::Statement;
        use crate::statements::statement_show_database::SqlShowDatabase;
        let sql = "SHOW DATABASES";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(SqlShowDatabase {
                kind: ShowKind::All
            })
        );
    }

    #[test]
    pub fn test_show_database_like() {
        use std::assert_matches::assert_matches;

        use sqlparser::dialect::GenericDialect;

        use crate::parser::ParserContext;
        use crate::parser::ShowKind;
        use crate::parser::Statement;
        use crate::statements::statement_show_database::SqlShowDatabase;

        let sql = "SHOW DATABASES LIKE test_database";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(SqlShowDatabase {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                })
            })
        );
    }

    #[test]
    pub fn test_show_database_where() {
        use std::assert_matches::assert_matches;

        use sqlparser::dialect::GenericDialect;

        use crate::parser::ParserContext;
        use crate::parser::ShowKind;
        use crate::parser::Statement;
        use crate::statements::statement_show_database::SqlShowDatabase;

        let sql = "SHOW DATABASES WHERE Database LIKE '%whatever1%' OR Database LIKE '%whatever2%'";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(SqlShowDatabase {
                kind: ShowKind::Where(sqlparser::ast::Expr::BinaryOp {
                    left: _,
                    right: _,
                    op: sqlparser::ast::BinaryOperator::Or,
                })
            })
        );
    }
}
