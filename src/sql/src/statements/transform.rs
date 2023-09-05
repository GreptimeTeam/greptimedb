//! Some transformers for statements

use std::ops::ControlFlow;

use sqlparser::ast::{visit_expressions_mut, ColumnDef, DataType, Expr};

use crate::error::Result;
use crate::statements::create::{CreateExternalTable, CreateTable};
use crate::statements::statement::Statement;
use crate::statements::TimezoneInfo;

/// Transform statements by rules
pub fn transform_statements(stmts: &mut Vec<Statement>) -> Result<()> {
    for stmt in &mut *stmts {
        transform_stmt(stmt)?;
    }

    visit_expressions_mut(stmts, |expr| {
        match expr {
            Expr::Cast { data_type, .. } => {
                replace_type_alias(data_type);
            }
            Expr::TryCast { data_type, .. } => {
                replace_type_alias(data_type);
            }
            Expr::SafeCast { data_type, .. } => {
                replace_type_alias(data_type);
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });

    Ok(())
}

fn transform_stmt(stmt: &mut Statement) -> Result<()> {
    transform_stmt_type_alias(stmt)?;

    Ok(())
}

fn transform_stmt_type_alias(stmt: &mut Statement) -> Result<()> {
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            columns
                .iter_mut()
                .for_each(|ColumnDef { data_type, .. }| replace_type_alias(data_type));
        }
        Statement::CreateExternalTable(CreateExternalTable { columns, .. }) => {
            columns
                .iter_mut()
                .for_each(|ColumnDef { data_type, .. }| replace_type_alias(data_type));
        }
        _ => {}
    }

    Ok(())
}

fn replace_type_alias(data_type: &mut DataType) {
    match data_type {
        // TODO(dennis): The sqlparser latest version contains the Int8 alias for postres Bigint.
        // Which means 8 bytes in postgres (not 8 bits). If we upgrade the sqlparser, need to process it.
        // See https://docs.rs/sqlparser/latest/sqlparser/ast/enum.DataType.html#variant.Int8
        DataType::Custom(name, tokens) if name.0.len() == 1 && tokens.is_empty() => {
            match name.0[0].value.as_str() {
                // Timestamp type alias
                "TIMESTAMP_S" | "TIMESTAMPSECOND" => {
                    *data_type = DataType::Timestamp(Some(0), TimezoneInfo::None);
                }
                "TIMESTAMP_MS" | "TIMESTAMPMILLISECOND" => {
                    *data_type = DataType::Timestamp(Some(3), TimezoneInfo::None);
                }
                "TIMESTAMP_MICROS" | "TIMESTAMPMICROSECOND" => {
                    *data_type = DataType::Timestamp(Some(6), TimezoneInfo::None);
                }
                "TIMESTAMP_NS" | "TIMESTAMPNANOSECOND" => {
                    *data_type = DataType::Timestamp(Some(9), TimezoneInfo::None);
                }
                // Number type alias
                "INT8" => {
                    *data_type = DataType::TinyInt(None);
                }
                "INT16" => {
                    *data_type = DataType::SmallInt(None);
                }
                "INT32" => {
                    *data_type = DataType::Int(None);
                }
                "INT64" => {
                    *data_type = DataType::BigInt(None);
                }
                "UINT8" => {
                    *data_type = DataType::UnsignedTinyInt(None);
                }
                "UINT16" => {
                    *data_type = DataType::UnsignedSmallInt(None);
                }
                "UINT32" => {
                    *data_type = DataType::UnsignedInt(None);
                }
                "UINT64" => {
                    *data_type = DataType::UnsignedBigInt(None);
                }
                "Float32" => {
                    *data_type = DataType::Float(None);
                }
                "Float64" => {
                    *data_type = DataType::Double;
                }
                _ => {}
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;

    #[test]
    fn test_transform_type_alias() {
        let sql = "SELECT TIMESTAMP '2020-01-01 01:23:45.12345678'::TIMESTAMP(9)";
        let mut stmts = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        transform(&mut stmts).unwrap();

        println!("{:?}", stmts);

        panic!();
    }
}
