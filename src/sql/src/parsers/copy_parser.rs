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
use sqlparser::keywords::Keyword;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::copy::{CopyTable, Format};
use crate::statements::statement::Statement;

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

        self.parser
            .expect_keyword(Keyword::TO)
            .context(error::SyntaxSnafu { sql: self.sql })?;

        let file_name =
            self.parser
                .parse_literal_string()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a file name",
                    actual: self.peek_token_as_string(),
                })?;

        let format = if self.parser.parse_keyword(Keyword::FORMAT) {
            let format =
                self.parser
                    .parse_literal_string()
                    .with_context(|_| error::UnexpectedSnafu {
                        sql: self.sql,
                        expected: "a format name",
                        actual: self.peek_token_as_string(),
                    })?;
            Format::try_from(format)?
        } else {
            // default is Parquet format.
            Format::Parquet
        };

        Ok(CopyTable::new(table_name, file_name, format))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    fn test_parse_copy_table() {
        let sql0 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet'";
        let sql1 = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' FORMAT 'parquet'";
        let result0 = ParserContext::create_with_dialect(sql0, &GenericDialect {}).unwrap();
        let result1 = ParserContext::create_with_dialect(sql1, &GenericDialect {}).unwrap();

        for mut result in vec![result0, result1] {
            assert_eq!(1, result.len());

            let statement = result.remove(0);
            assert_matches!(statement, Statement::Copy { .. });
            match statement {
                Statement::Copy(copy_table) => {
                    let (catalog, schema, table) =
                        if let [catalog, schema, table] = &copy_table.table_name().0[..] {
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

                    let file_name = copy_table.file_name();
                    assert_eq!("tbl_file.parquet", file_name);

                    let format = copy_table.format();
                    assert_eq!(Format::Parquet, *format);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_parse_copy_table_with_unsupopoted_format() {
        let sql = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet' FORMAT 'unknow_format'";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        assert!(result.is_err());
        assert_matches!(
            result.err().unwrap(),
            error::Error::UnsupportedFormat { .. }
        );
    }
}
