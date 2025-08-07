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

use std::collections::HashMap;

use snafu::ResultExt;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Token::Word;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::copy::{
    CopyDatabase, CopyDatabaseArgument, CopyQueryTo, CopyQueryToArgument, CopyTable,
    CopyTableArgument,
};
use crate::statements::statement::Statement;
use crate::util::parse_option_string;

pub type With = HashMap<String, String>;
pub type Connection = HashMap<String, String>;

// COPY tbl TO 'output.parquet';
impl ParserContext<'_> {
    pub(crate) fn parse_copy(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let next = self.parser.peek_token();
        let copy = if next.token == Token::LParen {
            let copy_query = self.parse_copy_query_to()?;

            crate::statements::copy::Copy::CopyQueryTo(copy_query)
        } else if let Word(word) = next.token
            && word.keyword == Keyword::DATABASE
        {
            let _ = self.parser.next_token();
            let copy_database = self.parser_copy_database()?;
            crate::statements::copy::Copy::CopyDatabase(copy_database)
        } else {
            let copy_table = self.parse_copy_table()?;
            crate::statements::copy::Copy::CopyTable(copy_table)
        };

        Ok(Statement::Copy(copy))
    }

    fn parser_copy_database(&mut self) -> Result<CopyDatabase> {
        let database_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a database name",
                actual: self.peek_token_as_string(),
            })?;

        let req = if self.parser.parse_keyword(Keyword::TO) {
            let (with, connection, location, limit) = self.parse_copy_parameters()?;
            if limit.is_some() {
                return error::InvalidSqlSnafu {
                    msg: "limit is not supported",
                }
                .fail();
            }

            let argument = CopyDatabaseArgument {
                database_name,
                with: with.into(),
                connection: connection.into(),
                location,
            };
            CopyDatabase::To(argument)
        } else {
            self.parser
                .expect_keyword(Keyword::FROM)
                .context(error::SyntaxSnafu)?;
            let (with, connection, location, limit) = self.parse_copy_parameters()?;
            if limit.is_some() {
                return error::InvalidSqlSnafu {
                    msg: "limit is not supported",
                }
                .fail();
            }

            let argument = CopyDatabaseArgument {
                database_name,
                with: with.into(),
                connection: connection.into(),
                location,
            };
            CopyDatabase::From(argument)
        };
        Ok(req)
    }

    fn parse_copy_table(&mut self) -> Result<CopyTable> {
        let raw_table_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a table name",
                actual: self.peek_token_as_string(),
            })?;
        let table_name = Self::canonicalize_object_name(raw_table_name);

        if self.parser.parse_keyword(Keyword::TO) {
            let (with, connection, location, limit) = self.parse_copy_parameters()?;
            Ok(CopyTable::To(CopyTableArgument {
                table_name,
                with: with.into(),
                connection: connection.into(),
                location,
                limit,
            }))
        } else {
            self.parser
                .expect_keyword(Keyword::FROM)
                .context(error::SyntaxSnafu)?;
            let (with, connection, location, limit) = self.parse_copy_parameters()?;
            Ok(CopyTable::From(CopyTableArgument {
                table_name,
                with: with.into(),
                connection: connection.into(),
                location,
                limit,
            }))
        }
    }

    fn parse_copy_query_to(&mut self) -> Result<CopyQueryTo> {
        self.parser
            .expect_token(&Token::LParen)
            .with_context(|_| error::UnexpectedSnafu {
                expected: "'('",
                actual: self.peek_token_as_string(),
            })?;
        let query = self.parse_query()?;
        self.parser
            .expect_token(&Token::RParen)
            .with_context(|_| error::UnexpectedSnafu {
                expected: "')'",
                actual: self.peek_token_as_string(),
            })?;
        self.parser
            .expect_keyword(Keyword::TO)
            .context(error::SyntaxSnafu)?;
        let (with, connection, location, limit) = self.parse_copy_parameters()?;
        if limit.is_some() {
            return error::InvalidSqlSnafu {
                msg: "limit is not supported",
            }
            .fail();
        }
        Ok(CopyQueryTo {
            query: Box::new(query),
            arg: CopyQueryToArgument {
                with: with.into(),
                connection: connection.into(),
                location,
            },
        })
    }

    fn parse_copy_parameters(&mut self) -> Result<(With, Connection, String, Option<u64>)> {
        let location =
            self.parser
                .parse_literal_string()
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a file name",
                    actual: self.peek_token_as_string(),
                })?;

        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu)?;

        let with = options
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<With>>()?;

        let connection_options = self
            .parser
            .parse_options(Keyword::CONNECTION)
            .context(error::SyntaxSnafu)?;

        let connection = connection_options
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<Connection>>()?;

        let limit = if self.parser.parse_keyword(Keyword::LIMIT) {
            Some(
                self.parser
                    .parse_literal_uint()
                    .with_context(|_| error::UnexpectedSnafu {
                        expected: "the number of maximum rows",
                        actual: self.peek_token_as_string(),
                    })?,
            )
        } else {
            None
        };

        Ok((with, connection, location, limit))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;
    use crate::statements::statement::Statement::Copy;

    #[test]
    fn test_parse_copy_table() {
        let sql0 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet'";
        let sql1 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' WITH (FORMAT = 'parquet')";
        let result0 = ParserContext::create_with_dialect(
            sql0,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();
        let result1 = ParserContext::create_with_dialect(
            sql1,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();

        for mut result in [result0, result1] {
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Copy(copy) => {
                    let crate::statements::copy::Copy::CopyTable(CopyTable::To(copy_table)) = copy
                    else {
                        unreachable!()
                    };
                    let table = copy_table.table_name.to_string();
                    assert_eq!("catalog0.schema0.tbl", table);

                    let file_name = &copy_table.location;
                    assert_eq!("tbl_file.parquet", file_name);

                    let format = copy_table.format().unwrap();
                    assert_eq!("parquet", format.to_lowercase());
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_copy_table_from_basic() {
        let results = [
            "COPY catalog0.schema0.tbl FROM 'tbl_file.parquet'",
            "COPY catalog0.schema0.tbl FROM 'tbl_file.parquet' WITH (FORMAT = 'parquet')",
        ]
        .iter()
        .map(|sql| {
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
        })
        .collect::<Vec<_>>();

        for mut result in results {
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(crate::statements::copy::Copy::CopyTable(CopyTable::From(
                    copy_table,
                ))) => {
                    let table = copy_table.table_name.to_string();
                    assert_eq!("catalog0.schema0.tbl", table);

                    let file_name = &copy_table.location;
                    assert_eq!("tbl_file.parquet", file_name);

                    let format = copy_table.format().unwrap();
                    assert_eq!("parquet", format.to_lowercase());
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_copy_table_from() {
        struct Test<'a> {
            sql: &'a str,
            expected_pattern: Option<String>,
            expected_connection: HashMap<String, String>,
        }

        let tests = [
            Test {
                sql: "COPY catalog0.schema0.tbl FROM 'tbl_file.parquet' WITH (PATTERN = 'demo.*')",
                expected_pattern: Some("demo.*".into()),
                expected_connection: HashMap::new(),
            },
            Test {
                sql: "COPY catalog0.schema0.tbl FROM 'tbl_file.parquet' WITH (PATTERN = 'demo.*') CONNECTION (FOO='Bar', ONE='two')",
                expected_pattern: Some("demo.*".into()),
                expected_connection: [("foo","Bar"),("one","two")].into_iter().map(|(k,v)|{(k.to_string(),v.to_string())}).collect()
            },
        ];

        for test in tests {
            let mut result = ParserContext::create_with_dialect(
                test.sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(crate::statements::copy::Copy::CopyTable(CopyTable::From(
                    copy_table,
                ))) => {
                    if let Some(expected_pattern) = test.expected_pattern {
                        assert_eq!(copy_table.pattern().unwrap(), expected_pattern);
                    }
                    assert_eq!(
                        copy_table.connection.clone(),
                        test.expected_connection.into()
                    );
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_copy_table_to() {
        struct Test<'a> {
            sql: &'a str,
            expected_connection: HashMap<String, String>,
        }

        let tests = [
            Test {
                sql: "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' ",
                expected_connection: HashMap::new(),
            },
            Test {
                sql: "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' CONNECTION (FOO='Bar', ONE='two')",
                expected_connection: [("foo","Bar"),("one","two")].into_iter().map(|(k,v)|{(k.to_string(),v.to_string())}).collect()
            },
            Test {
                sql:"COPY catalog0.schema0.tbl TO 'tbl_file.parquet' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')",
                expected_connection: [("foo","Bar"),("one","two")].into_iter().map(|(k,v)|{(k.to_string(),v.to_string())}).collect()
            },
        ];

        for test in tests {
            let mut result = ParserContext::create_with_dialect(
                test.sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(crate::statements::copy::Copy::CopyTable(CopyTable::To(
                    copy_table,
                ))) => {
                    assert_eq!(
                        copy_table.connection.clone(),
                        test.expected_connection.into()
                    );
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_copy_database_to() {
        let sql = "COPY DATABASE catalog0.schema0 TO 'tbl_file.parquet' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Copy(crate::statements::copy::Copy::CopyDatabase(stmt)) = stmt else {
            unreachable!()
        };

        let CopyDatabase::To(stmt) = stmt else {
            unreachable!()
        };

        assert_eq!(
            ObjectName::from(vec![Ident::new("catalog0"), Ident::new("schema0")]),
            stmt.database_name
        );
        assert_eq!(
            [("format", "parquet")]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            stmt.with.to_str_map()
        );

        assert_eq!(
            [("foo", "Bar"), ("one", "two")]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            stmt.connection.to_str_map()
        );
    }

    #[test]
    fn test_copy_database_from() {
        let sql = "COPY DATABASE catalog0.schema0 FROM '/a/b/c/' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Copy(crate::statements::copy::Copy::CopyDatabase(stmt)) = stmt else {
            unreachable!()
        };

        let CopyDatabase::From(stmt) = stmt else {
            unreachable!()
        };

        assert_eq!(
            ObjectName::from(vec![Ident::new("catalog0"), Ident::new("schema0")]),
            stmt.database_name
        );
        assert_eq!(
            [("format", "parquet")]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            stmt.with.to_str_map()
        );

        assert_eq!(
            [("foo", "Bar"), ("one", "two")]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            stmt.connection.to_str_map()
        );
    }

    #[test]
    fn test_copy_query_to() {
        let sql = "COPY (SELECT * FROM tbl WHERE ts > 10) TO 'tbl_file.parquet' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Copy(crate::statements::copy::Copy::CopyQueryTo(stmt)) = stmt else {
            unreachable!()
        };

        let query = ParserContext::create_with_dialect(
            "SELECT * FROM tbl WHERE ts > 10",
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap()
        .remove(0);

        assert_eq!(&query, stmt.query.as_ref());
        assert_eq!(
            [("format", "parquet")]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            stmt.arg.with.to_str_map()
        );

        assert_eq!(
            [("foo", "Bar"), ("one", "two")]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            stmt.arg.connection.to_str_map()
        );
    }

    #[test]
    fn test_invalid_copy_query_to() {
        {
            let sql = "COPY SELECT * FROM tbl WHERE ts > 10 TO 'tbl_file.parquet' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')";

            assert!(ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default()
            )
            .is_err())
        }
        {
            let sql = "COPY SELECT * FROM tbl WHERE ts > 10) TO 'tbl_file.parquet' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')";

            assert!(ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default()
            )
            .is_err())
        }
        {
            let sql = "COPY (SELECT * FROM tbl WHERE ts > 10 TO 'tbl_file.parquet' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')";

            assert!(ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default()
            )
            .is_err())
        }
    }
}
