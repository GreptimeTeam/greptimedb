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

use catalog::RenameTableRequest;
use common_query::Output;
use snafu::prelude::*;
use sql::statements::alter::{AlterTable, AlterTableOperation};
use sql::statements::column_def_to_schema;
use table::engine::{EngineContext, TableReference};
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest};

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn alter(&self, req: AlterTableRequest) -> Result<Output> {
        let ctx = EngineContext {};
        let table_name = req.table_name.clone();
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &table_name,
        };

        let full_table_name = table_ref.to_string();

        // fetches table via catalog
        let table = self.get_table(&table_ref).await?;
        // checks the table engine exist
        let table_engine = self.table_engine(table)?;
        ensure!(
            table_engine.table_exists(&ctx, &table_ref),
            error::TableNotFoundSnafu {
                table_name: &full_table_name,
            }
        );
        let is_rename = req.is_rename_table();

        let table = table_engine
            .alter_table(&ctx, req)
            .await
            .context(error::AlterTableSnafu {
                table_name: full_table_name,
            })?;
        if is_rename {
            let table_info = &table.table_info();
            let rename_table_req = RenameTableRequest {
                catalog: table_info.catalog_name.clone(),
                schema: table_info.schema_name.clone(),
                table_name,
                new_table_name: table_info.name.clone(),
                table_id: table_info.ident.table_id,
            };
            self.catalog_manager
                .rename_table(rename_table_req)
                .await
                .context(error::RenameTableSnafu)?;
        }
        // Tried in MySQL, it really prints "Affected Rows: 0".
        Ok(Output::AffectedRows(0))
    }

    pub(crate) fn alter_to_request(
        &self,
        alter_table: AlterTable,
        table_ref: TableReference,
    ) -> Result<AlterTableRequest> {
        let alter_kind = match &alter_table.alter_operation() {
            AlterTableOperation::AddConstraint(table_constraint) => {
                return error::InvalidSqlSnafu {
                    msg: format!("unsupported table constraint {table_constraint}"),
                }
                .fail()
            }
            AlterTableOperation::AddColumn { column_def } => AlterKind::AddColumns {
                columns: vec![AddColumnRequest {
                    column_schema: column_def_to_schema(column_def, false)
                        .context(error::ParseSqlSnafu)?,
                    // FIXME(dennis): supports adding key column
                    is_key: false,
                }],
            },
            AlterTableOperation::DropColumn { name } => AlterKind::DropColumns {
                names: vec![name.value.clone()],
            },
            AlterTableOperation::RenameTable { new_table_name } => AlterKind::RenameTable {
                new_table_name: new_table_name.clone(),
            },
        };
        Ok(AlterTableRequest {
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
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
        let req = handler
            .alter_to_request(
                alter_table,
                TableReference::full("greptime", "public", "my_metric_1"),
            )
            .unwrap();
        assert_eq!(req.catalog_name, "greptime");
        assert_eq!(req.schema_name, "public");
        assert_eq!(req.table_name, "my_metric_1");

        let alter_kind = req.alter_kind;
        assert_matches!(alter_kind, AlterKind::AddColumns { .. });
        match alter_kind {
            AlterKind::AddColumns { columns } => {
                let new_column = &columns[0].column_schema;

                assert_eq!(new_column.name, "tagk_i");
                assert!(new_column.is_nullable());
                assert_eq!(new_column.data_type, ConcreteDataType::string_datatype());
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_alter_to_request_with_renaming_table() {
        let handler = create_mock_sql_handler().await;
        let alter_table = parse_sql("ALTER TABLE test_table RENAME table_t;");
        let req = handler
            .alter_to_request(
                alter_table,
                TableReference::full("greptime", "public", "test_table"),
            )
            .unwrap();
        assert_eq!(req.catalog_name, "greptime");
        assert_eq!(req.schema_name, "public");
        assert_eq!(req.table_name, "test_table");

        let alter_kind = req.alter_kind;
        assert_matches!(alter_kind, AlterKind::RenameTable { .. });

        match alter_kind {
            AlterKind::RenameTable { new_table_name } => {
                assert_eq!(new_table_name, "table_t");
            }
            _ => unreachable!(),
        }
    }
}
