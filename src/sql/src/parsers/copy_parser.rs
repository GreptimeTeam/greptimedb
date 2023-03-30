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

use snafu::ResultExt;
use sqlparser::ast::ObjectName;
use sqlparser::keywords::Keyword;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::copy::{CopyTable, CopyTableArgument, Format};
use crate::statements::statement::Statement;
use crate::util::parse_option_string;

// COPY tbl TO 'output.parquet';
impl<'a> ParserContext<'a> {
    pub(crate) fn parse_copy(&mut self) -> Result<Statement> {
        self.parser.next_token();
        let copy_table = self.parse_copy_table()?;
        Ok(Statement::Copy(copy_table))
    }

    fn parse_copy_table(&mut self) -> Result<CopyTable> {
        let table_name =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;

        if self.parser.parse_keyword(Keyword::TO) {
            Ok(CopyTable::To(self.parse_copy_table_to(table_name)?))
        } else {
            self.parser
                .expect_keyword(Keyword::FROM)
                .context(error::SyntaxSnafu { sql: self.sql })?;
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
            .context(error::SyntaxSnafu { sql: self.sql })?;

        // default format is parquet
        let mut format = Format::Parquet;
        let mut pattern = None;
        for option in options {
            match option.name.value.to_ascii_uppercase().as_str() {
                "FORMAT" => {
                    if let Some(fmt_str) = parse_option_string(option.value) {
                        format = Format::try_from(fmt_str)?;
                    }
                }
                "PATTERN" => {
                    if let Some(v) = parse_option_string(option.value) {
                        pattern = Some(v);
                    }
                }
                //TODO: throws warnings?
                _ => (),
            }
        }

        let connection_options = self
            .parser
            .parse_options(Keyword::CONNECTION)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let connection = connection_options
            .into_iter()
            .filter_map(|option| {
                if let Some(v) = parse_option_string(option.value) {
                    Some((option.name.value.to_uppercase(), v))
                } else {
                    None
                }
            })
            .collect();
        Ok(CopyTableArgument {
            table_name,
            format,
            pattern,
            connection,
            location,
        })
    }

    fn parse_copy_table_to(&mut self, table_name: ObjectName) -> Result<CopyTableArgument> {
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
            .context(error::SyntaxSnafu { sql: self.sql })?;

        // default format is parquet
        let mut format = Format::Parquet;
        for option in options {
            if option.name.value.eq_ignore_ascii_case("FORMAT") {
                if let Some(fmt_str) = parse_option_string(option.value) {
                    format = Format::try_from(fmt_str)?;
                }
            }
        }

        let connection_options = self
            .parser
            .parse_options(Keyword::CONNECTION)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let connection = connection_options
            .into_iter()
            .filter_map(|option| {
                if let Some(v) = parse_option_string(option.value) {
                    Some((option.name.value.to_uppercase(), v))
                } else {
                    None
                }
            })
            .collect();

        Ok(CopyTableArgument {
            table_name,
            format,
            connection,
            pattern: None,
            location,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    fn test_parse_copy_table() {
        let sql0 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet'";
        let sql1 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' WITH (FORMAT = 'parquet')";
        let result0 = ParserContext::create_with_dialect(sql0, &GenericDialect {}).unwrap();
        let result1 = ParserContext::create_with_dialect(sql1, &GenericDialect {}).unwrap();

        for mut result in vec![result0, result1] {
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(CopyTable::To(copy_table)) => {
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

                    let file_name = copy_table.location;
                    assert_eq!("tbl_file.parquet", file_name);

                    let format = copy_table.format;
                    assert_eq!(Format::Parquet, format);
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
        .map(|sql| ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap())
        .collect::<Vec<_>>();

        for mut result in results {
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(CopyTable::From(copy_table)) => {
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

                    let file_name = copy_table.location;
                    assert_eq!("tbl_file.parquet", file_name);

                    let format = copy_table.format;
                    assert_eq!(Format::Parquet, format);
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
                expected_connection: [("FOO","Bar"),("ONE","two")].into_iter().map(|(k,v)|{(k.to_string(),v.to_string())}).collect()
            },
        ];

        for test in tests {
            let mut result =
                ParserContext::create_with_dialect(test.sql, &GenericDialect {}).unwrap();
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(CopyTable::From(copy_table)) => {
                    if let Some(expected_pattern) = test.expected_pattern {
                        assert_eq!(copy_table.pattern.clone().unwrap(), expected_pattern);
                    }
                    assert_eq!(copy_table.connection.clone(), test.expected_connection);
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
                expected_connection: [("FOO","Bar"),("ONE","two")].into_iter().map(|(k,v)|{(k.to_string(),v.to_string())}).collect()
            },
            Test {
                sql:"COPY catalog0.schema0.tbl TO 'tbl_file.parquet' WITH (FORMAT = 'parquet') CONNECTION (FOO='Bar', ONE='two')",
                expected_connection: [("FOO","Bar"),("ONE","two")].into_iter().map(|(k,v)|{(k.to_string(),v.to_string())}).collect()
            },
        ];

        for test in tests {
            let mut result =
                ParserContext::create_with_dialect(test.sql, &GenericDialect {}).unwrap();
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(CopyTable::To(copy_table)) => {
                    assert_eq!(copy_table.connection.clone(), test.expected_connection);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_copy_table_with_unsupopoted_format() {
        let results = [
            "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' WITH (FORMAT = 'unknow_format')",
            "COPY catalog0.schema0.tbl FROM 'tbl_file.parquet' WITH (FORMAT = 'unknow_format')",
        ]
        .iter()
        .map(|sql| ParserContext::create_with_dialect(sql, &GenericDialect {}))
        .collect::<Vec<_>>();

        for result in results {
            assert!(result.is_err());
            assert_matches!(
                result.err().unwrap(),
                error::Error::UnsupportedCopyFormatOption { .. }
            );
        }
    }
}
