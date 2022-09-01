use snafu::ResultExt;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;

use crate::error;
use crate::parser::ParserContext;
use crate::parser::Result;
use crate::statements::alter::{AlterTable, AlterTableOperation};
use crate::statements::statement::Statement;

impl<'a> ParserContext<'a> {
    pub(crate) fn parse_alter(&mut self) -> Result<Statement> {
        let alter_table = self.parse().context(error::SyntaxSnafu { sql: self.sql })?;
        Ok(Statement::Alter(alter_table))
    }

    // Sqlparser does not parse MySQL "ALTER ... DROP PRIMARY KEY" correctly,
    // see https://github.com/sqlparser-rs/sqlparser-rs/issues/591.
    // If the issue is resolved, maybe we can switch back to sqlparser's implementation.
    //
    // Up until the current version (0.21.0), sqlparser requires us to use
    // "ALTER ... DROP CONSTRAINT my_constraint" to drop the primary key - the "my_constraint"
    // corresponding to the primary key constraint introduced either in
    // "CREATE TABLE" or "ALTER ... ADD CONSTRAINT". If we were to use this "DROP CONSTRAINT"
    // syntax, we have to store constraint somewhere in table meta and design a whole "constraint"
    // framework (not restrict to only primary key). If we are building a SQL database, I think
    // it's worth the efforts. If not, maybe this easy way is more suitable:
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
        } else if parser.parse_keyword(Keyword::DROP) {
            parser.expect_keywords(&[Keyword::PRIMARY, Keyword::KEY])?;
            AlterTableOperation::DropPrimaryKey
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

    use sqlparser::ast::{ColumnOption, DataType, TableConstraint};
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

    #[test]
    fn test_parse_alter_drop_primary_key() {
        let sql = "ALTER TABLE my_metric_1 DROP PRIMARY KEY;";
        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);
                assert_eq!(
                    &AlterTableOperation::DropPrimaryKey,
                    alter_table.alter_operation()
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parse_alter_add_primary_key() {
        let sql = "ALTER TABLE my_metric_1 ADD PRIMARY KEY (tagk_1, tagk_i);";
        let mut result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, result.len());

        let statement = result.remove(0);
        assert_matches!(statement, Statement::Alter { .. });
        match statement {
            Statement::Alter(alter_table) => {
                assert_eq!("my_metric_1", alter_table.table_name().0[0].value);

                let alter_operation = alter_table.alter_operation();
                assert_matches!(alter_operation, AlterTableOperation::AddConstraint(_));
                match alter_operation {
                    AlterTableOperation::AddConstraint(TableConstraint::Unique {
                        name: _,
                        columns,
                        is_primary,
                    }) => {
                        assert!(is_primary);
                        assert_eq!(2, columns.len());
                        assert_eq!("tagk_1", columns[0].value);
                        assert_eq!("tagk_i", columns[1].value);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}
