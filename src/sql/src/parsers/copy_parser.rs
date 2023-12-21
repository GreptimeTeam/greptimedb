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
use sqlparser::ast::ObjectName;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token::Word;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::copy::{CopyDatabaseArgument, CopyTable, CopyTableArgument};
use crate::statements::statement::Statement;
use crate::util::parse_option_string;

pub type With = HashMap<String, String>;
pub type Connection = HashMap<String, String>;

// COPY tbl TO 'output.parquet';
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_copy(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let next = self.parser.peek_token();
        let copy = if let Word(word) = next.token
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

    fn parser_copy_database(&mut self) -> Result<CopyDatabaseArgument> {
        let database_name =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a database name",
                    actual: self.peek_token_as_string(),
                })?;

        self.parser
            .expect_keyword(Keyword::TO)
            .context(error::SyntaxSnafu)?;

        let (with, connection, location) = self.parse_copy_to()?;
        Ok(CopyDatabaseArgument {
            database_name,
            with: with.into(),
            connection: connection.into(),
            location,
        })
    }

    fn parse_copy_table(&mut self) -> Result<CopyTable> {
        let raw_table_name =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;
        let table_name = Self::canonicalize_object_name(raw_table_name);

        if self.parser.parse_keyword(Keyword::TO) {
            let (with, connection, location) = self.parse_copy_to()?;
            Ok(CopyTable::To(CopyTableArgument {
                table_name,
                with: with.into(),
                connection: connection.into(),
                location,
            }))
        } else {
            self.parser
                .expect_keyword(Keyword::FROM)
                .context(error::SyntaxSnafu)?;
            Ok(CopyTable::From(self.parse_copy_table_from(table_name)?))
        }
    }

    fn parse_copy_table_from(&mut self, table_name: ObjectName) -> Result<CopyTableArgument> {
        let location =
            self.parser
                .parse_literal_string()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a uri",
                    actual: self.peek_token_as_string(),
                })?;

        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu)?;

        let with = options
            .into_iter()
            .filter_map(|option| {
                parse_option_string(option.value).map(|v| (option.name.value.to_lowercase(), v))
            })
            .collect();

        let connection_options = self
            .parser
            .parse_options(Keyword::CONNECTION)
            .context(error::SyntaxSnafu)?;

        let connection = connection_options
            .into_iter()
            .filter_map(|option| {
                parse_option_string(option.value).map(|v| (option.name.value.to_lowercase(), v))
            })
            .collect();
        Ok(CopyTableArgument {
            table_name,
            with,
            connection,
            location,
        })
    }

    fn parse_copy_to(&mut self) -> Result<(With, Connection, String)> {
        let location =
            self.parser
                .parse_literal_string()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a file name",
                    actual: self.peek_token_as_string(),
                })?;

        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu)?;

        let with = options
            .into_iter()
            .filter_map(|option| {
                parse_option_string(option.value).map(|v| (option.name.value.to_lowercase(), v))
            })
            .collect();

        let connection_options = self
            .parser
            .parse_options(Keyword::CONNECTION)
            .context(error::SyntaxSnafu)?;

        let connection = connection_options
            .into_iter()
            .filter_map(|option| {
                parse_option_string(option.value).map(|v| (option.name.value.to_lowercase(), v))
            })
            .collect();

        Ok((with, connection, location))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use sqlparser::ast::Ident;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::statements::statement::Statement::Copy;

    #[test]
    fn test_parse_copy_table() {
        let sql0 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet'";
        let sql1 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' WITH (FORMAT = 'parquet')";
        let result0 = ParserContext::create_with_dialect(sql0, &GreptimeDbDialect {}).unwrap();
        let result1 = ParserContext::create_with_dialect(sql1, &GreptimeDbDialect {}).unwrap();

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
                    let (catalog, schema, table) =
                        if let [catalog, schema, table] = &copy_table.table_name.0[..] {
                            (
                                catalog.value.clone(),
                                schema.value.clone(),
                                table.value.clone(),
                            )
                        } else {
                            unreachable!()
                        };

                    assert_eq!("catalog0", catalog);
                    assert_eq!("schema0", schema);
                    assert_eq!("tbl", table);

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
        .map(|sql| ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap())
        .collect::<Vec<_>>();

        for mut result in results {
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(crate::statements::copy::Copy::CopyTable(CopyTable::From(
                    copy_table,
                ))) => {
                    let (catalog, schema, table) =
                        if let [catalog, schema, table] = &copy_table.table_name.0[..] {
                            (
                                catalog.value.clone(),
                                schema.value.clone(),
                                table.value.clone(),
                            )
                        } else {
                            unreachable!()
                        };

                    assert_eq!("catalog0", catalog);
                    assert_eq!("schema0", schema);
                    assert_eq!("tbl", table);

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
            let mut result =
                ParserContext::create_with_dialect(test.sql, &GreptimeDbDialect {}).unwrap();
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
            let mut result =
                ParserContext::create_with_dialect(test.sql, &GreptimeDbDialect {}).unwrap();
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
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .pop()
            .unwrap();

        let Copy(crate::statements::copy::Copy::CopyDatabase(stmt)) = stmt else {
            unreachable!()
        };
        assert_eq!(
            ObjectName(vec![Ident::new("catalog0"), Ident::new("schema0")]),
            stmt.database_name
        );
        assert_eq!(
            [("format".to_string(), "parquet".to_string())]
                .into_iter()
                .collect::<HashMap<_, _>>(),
            stmt.with.map
        );

        assert_eq!(
            [
                ("foo".to_string(), "Bar".to_string()),
                ("one".to_string(), "two".to_string())
            ]
            .into_iter()
            .collect::<HashMap<_, _>>(),
            stmt.connection.map
        );
    }
}
