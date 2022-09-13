use snafu::ResultExt;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::statements::alter::{AlterTable, AlterTableOperation};
use crate::statements::statement::Statement;

impl<'a> ParserContext<'a> {
    pub(crate) fn parse_alter(&mut self) -> Result<Statement> {
        let alter_table = self.parse().context(error::SyntaxSnafu { sql: self.sql })?;
        Ok(Statement::Alter(alter_table))
    }

    fn parse(&mut self) -> std::result::Result<AlterTable, ParserError> {
        let parser = &mut self.parser;
        parser.expect_keywords(&[Keyword::ALTER, Keyword::TABLE])?;

        let table_name = parser.parse_object_name()?;

        let alter_operation = if parser.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = parser.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else {
                let _ = parser.parse_keyword(Keyword::COLUMN);
                let column_def = parser.parse_column_def()?;
                AlterTableOperation::AddColumn { column_def }
            }
        } else {
            return Err(ParserError::ParserError(format!(
                "expect ADD or DROP after ALTER TABLE, found {}",
                parser.peek_token()
            )));
        };
        Ok(AlterTable::new(table_name, alter_operation))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::ast::{ColumnOption, DataType};
    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    fn test_parse_alter_add_column() {
        let sql = "ALTER TABLE my_metric_1 ADD tagk_i STRING Null;";
        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddColumn { .. });
                match alter_operation {
                    AlterTableOperation::AddColumn { column_def } => {
                        assert_eq!("tagk_i", column_def.name.value);
                        assert_eq!(DataType::String, column_def.data_type);
                        assert!(column_def
                            .options
                            .iter()
                            .any(|o| matches!(o.option, ColumnOption::Null)));
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}
