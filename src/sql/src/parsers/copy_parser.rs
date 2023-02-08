use snafu::ResultExt;
use sqlparser::keywords::Keyword;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::copy::CopyTable;
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

        Ok(CopyTable::new(table_name, file_name))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    fn test_parse_copy_table() {
        let sql = "COPY catalog0.schema0.tbl TO 'tbl_file.parquet'";
        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
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
            }
            _ => unreachable!(),
        }
    }
}
