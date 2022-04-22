use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::statement::GtStatement;
use crate::statements::component_show_kind::ShowKind;
use crate::statements::statement_show_database::SqlShowDatabase;

/// GrepTime SQL parser context, a simple wrapper for Datafusion SQL parser.
pub(crate) struct GtParserContext<'a> {
    pub(crate) parser: Parser<'a>,
    pub(crate) sql: &'a str,
}

impl<'a> GtParserContext<'a> {
    /// Parses SQL with given dialect
    pub fn create_with_dialect(
        sql: &'a str,
        dialect: &dyn Dialect,
    ) -> Result<Vec<GtStatement>, ParserError> {
        let mut stmts: Vec<GtStatement> = Vec::new();
        let mut tokenizer = Tokenizer::new(dialect, sql);

        let tokens: Vec<Token> = tokenizer.tokenize()?;

        let mut parser = GtParserContext {
            sql,
            parser: Parser::new(tokens, dialect),
        };

        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.unexpected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }

        Ok(stmts)
    }

    /// Parses parser context to a set of statements.
    pub fn parse_statement(&mut self) -> Result<GtStatement, ParserError> {
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
                    _ => self.unexpected("an SQL statement", Token::Word(w)),
                }
            }
            Token::LParen => self.parse_query(),
            unexpected => self.unexpected("an SQL statement", unexpected),
        }
    }

    /// Raises an "unexpected token" error.
    pub fn unexpected<T>(&self, expected: &str, actual: Token) -> Result<T, ParserError> {
        Err(ParserError::ParserError(format!(
            "Expect: {}, found: {}",
            expected, actual
        )))
    }

    /// Raises an "unsupported statement" error.
    pub fn unsupported<T>(&self, sql: &str) -> Result<T, ParserError> {
        Err(ParserError::ParserError(format!(
            "Unsupported statement: {}",
            sql
        )))
    }

    /// Parses SHOW statements
    /// todo(hl) support `show table`/`show settings`/`show create`/`show users` ect.
    fn parse_show(&mut self) -> Result<GtStatement, ParserError> {
        if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
            Ok(self.parse_show_databases()?)
        } else {
            self.unsupported(self.sql)
        }
    }

    fn parse_explain(&mut self) -> Result<GtStatement, ParserError> {
        todo!()
    }

    fn parse_insert(&mut self) -> Result<GtStatement, ParserError> {
        todo!()
    }

    fn parse_query(&mut self) -> Result<GtStatement, ParserError> {
        todo!()
    }

    fn parse_create(&mut self) -> Result<GtStatement, ParserError> {
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
    pub fn parse_show_databases(&mut self) -> Result<GtStatement, ParserError> {
        let tok = self.parser.next_token();
        match &tok {
            Token::EOF | Token::SemiColon => Ok(GtStatement::ShowDatabases(
                SqlShowDatabase::create(ShowKind::All),
            )),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(GtStatement::ShowDatabases(SqlShowDatabase::create(
                    ShowKind::Like(self.parser.parse_identifier()?),
                ))),
                Keyword::WHERE => Ok(GtStatement::ShowDatabases(SqlShowDatabase::create(
                    ShowKind::Where(self.parser.parse_expr()?),
                ))),
                _ => self.unexpected("like or where", tok),
            },
            _ => self.unexpected("like or where", tok),
        }
    }
}

mod test {
    #[test]
    pub fn test_sql_parser() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let dialect = GenericDialect {}; // or AnsiDialect

        let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

        let ast = Parser::parse_sql(&dialect, sql).unwrap();

        println!("AST: {:?}", ast);
    }

    #[test]
    pub fn test_show_database() {
        use sqlparser::dialect::GenericDialect;

        use crate::parser::GtParserContext;

        let sql = "SHOW DATABASES";
        let result = GtParserContext::create_with_dialect(sql, &GenericDialect {});
        println!("{:?}", result);
    }
}
