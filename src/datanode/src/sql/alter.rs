use common_query::Output;
use snafu::prelude::*;
use sql::statements::alter::{AlterTable, AlterTableOperation};
use sql::statements::{column_def_to_schema, table_idents_to_full_name};
use table::engine::EngineContext;
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest};

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn alter(&self, req: AlterTableRequest) -> Result<Output> {
        let ctx = EngineContext {};
        let table_name = &req.table_name.clone();
        if !self.table_engine.table_exists(&ctx, table_name) {
            return error::TableNotFoundSnafu { table_name }.fail();
        }
        self.table_engine
            .alter_table(&ctx, req)
            .await
            .context(error::AlterTableSnafu { table_name })?;
        // Tried in MySQL, it really prints "Affected Rows: 0".
        Ok(Output::AffectedRows(0))
    }

    pub(crate) fn alter_to_request(&self, alter_table: AlterTable) -> Result<AlterTableRequest> {
        let (catalog_name, schema_name, table_name) =
            table_idents_to_full_name(alter_table.table_name()).context(error::ParseSqlSnafu)?;

        let alter_kind = match alter_table.alter_operation() {
            AlterTableOperation::AddConstraint(table_constraint) => {
                return error::InvalidSqlSnafu {
                    msg: format!("unsupported table constraint {}", table_constraint),
                }
                .fail()
            }
            AlterTableOperation::AddColumn { column_def } => AlterKind::AddColumns {
                columns: vec![AddColumnRequest {
                    column_schema: column_def_to_schema(column_def)
                        .context(error::ParseSqlSnafu)?,
                    // FIXME(dennis): supports adding key column
                    is_key: false,
                }],
            },
        };
        Ok(AlterTableRequest {
            catalog_name,
            schema_name,
            table_name,
            alter_kind,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use datatypes::prelude::ConcreteDataType;
    use sql::dialect::GenericDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;
    use crate::tests::test_util::create_mock_sql_handler;

    fn parse_sql(sql: &str) -> AlterTable {
        let mut stmt = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, stmt.len());
        let stmt = stmt.remove(0);
        assert_matches!(stmt, Statement::Alter(_));
        match stmt {
            Statement::Alter(alter_table) => alter_table,
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_alter_to_request_with_adding_column() {
        let handler = create_mock_sql_handler().await;
        let alter_table = parse_sql("ALTER TABLE my_metric_1 ADD tagk_i STRING Null;");
        let req = handler.alter_to_request(alter_table).unwrap();
        assert_eq!(req.catalog_name, None);
        assert_eq!(req.schema_name, None);
        assert_eq!(req.table_name, "my_metric_1");

        let alter_kind = req.alter_kind;
        assert_matches!(alter_kind, AlterKind::AddColumns { .. });
        match alter_kind {
            AlterKind::AddColumns { columns } => {
                let new_column = &columns[0].column_schema;

                assert_eq!(new_column.name, "tagk_i");
                assert!(new_column.is_nullable);
                assert_eq!(new_column.data_type, ConcreteDataType::string_datatype());
            }
            _ => unreachable!(),
        }
    }
}
