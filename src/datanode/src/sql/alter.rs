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

use common_procedure::{watcher, ProcedureWithId};
use common_query::Output;
use common_telemetry::logging::info;
use snafu::prelude::*;
use sql::statements::alter::{AlterTable, AlterTableOperation};
use sql::statements::column_def_to_schema;
use table::engine::TableReference;
use table::metadata::TableId;
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest};
use table_procedure::AlterTableProcedure;

use crate::error::{self, Result, UnexpectedSnafu};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn alter_table(&self, req: AlterTableRequest) -> Result<Output> {
        let table_name = req.table_name.clone();
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &table_name,
        };

        match self.get_table(&table_ref).await {
            Ok(table) => {
                let engine_procedure = self.engine_procedure(table)?;

                let procedure =
                    AlterTableProcedure::new(req, self.catalog_manager.clone(), engine_procedure);
                let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
                let procedure_id = procedure_with_id.id;

                info!("Alter table {} by procedure {}", table_name, procedure_id);

                let mut watcher = self
                    .procedure_manager
                    .submit(procedure_with_id)
                    .await
                    .context(error::SubmitProcedureSnafu { procedure_id })?;

                watcher::wait(&mut watcher)
                    .await
                    .context(error::WaitProcedureSnafu { procedure_id })?;
            }
            Err(err) => {
                // TODO(weny): Retrieves table by table_id
                if let AlterKind::RenameTable { new_table_name } = req.alter_kind {
                    let new_table_ref = TableReference {
                        catalog: &req.catalog_name,
                        schema: &req.schema_name,
                        table: &new_table_name,
                    };

                    let table = self.get_table(&new_table_ref).await?;

                    ensure!(
                        table.table_info().table_id() == req.table_id,
                        UnexpectedSnafu {
                            violated: format!(
                                "expected table id: {}, actual: {}",
                                req.table_id,
                                table.table_info().table_id()
                            )
                        }
                    )
                } else {
                    return Err(err);
                }
            }
        }

        // Tried in MySQL, it really prints "Affected Rows: 0".
        Ok(Output::AffectedRows(0))
    }

    pub(crate) fn alter_to_request(
        alter_table: AlterTable,
        table_ref: TableReference,
        table_id: TableId,
    ) -> Result<AlterTableRequest> {
        let alter_kind = match &alter_table.alter_operation() {
            AlterTableOperation::AddConstraint(table_constraint) => {
                return error::InvalidSqlSnafu {
                    msg: format!("unsupported table constraint {table_constraint}"),
                }
                .fail()
            }
            AlterTableOperation::AddColumn {
                column_def,
                location,
            } => AlterKind::AddColumns {
                columns: vec![AddColumnRequest {
                    column_schema: column_def_to_schema(column_def, false)
                        .context(error::ParseSqlSnafu)?,
                    // FIXME(dennis): supports adding key column
                    is_key: false,
                    location: location.clone(),
                }],
            },
            AlterTableOperation::DropColumn { name } => AlterKind::DropColumns {
                names: vec![name.value.clone()],
            },
            AlterTableOperation::RenameTable { new_table_name } => AlterKind::RenameTable {
                new_table_name: new_table_name.clone(),
            },
            AlterTableOperation::SetOptions { .. } => {
                todo!("dennis");
            }
        };
        Ok(AlterTableRequest {
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
            table_id,
            alter_kind,
            table_version: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use datatypes::prelude::ConcreteDataType;
    use query::parser::{QueryLanguageParser, QueryStatement};
    use query::query_engine::SqlStatementExecutor;
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;
    use crate::tests::test_util::MockInstance;

    fn parse_sql(sql: &str) -> AlterTable {
        let mut stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
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
        let alter_table = parse_sql("ALTER TABLE my_metric_1 ADD tagk_i STRING Null;");
        let req = SqlHandler::alter_to_request(
            alter_table,
            TableReference::full("greptime", "public", "my_metric_1"),
            1,
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
        let alter_table = parse_sql("ALTER TABLE test_table RENAME table_t;");
        let req = SqlHandler::alter_to_request(
            alter_table,
            TableReference::full("greptime", "public", "test_table"),
            1,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_alter_table_by_procedure() {
        let instance = MockInstance::new("alter_table_by_procedure").await;

        // Create table first.
        let sql = r#"create table test_alter(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let stmt = match QueryLanguageParser::parse_sql(sql).unwrap() {
            QueryStatement::Sql(sql) => sql,
            _ => unreachable!(),
        };
        let output = instance
            .inner()
            .execute_sql(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        // Alter table.
        let sql = r#"alter table test_alter add column memory double"#;
        let stmt = match QueryLanguageParser::parse_sql(sql).unwrap() {
            QueryStatement::Sql(sql) => sql,
            _ => unreachable!(),
        };
        let output = instance
            .inner()
            .execute_sql(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));
    }
}
