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
use common_telemetry::info;
use snafu::ResultExt;
use table::engine::TableReference;
use table::requests::DropTableRequest;
use table_procedure::DropTableProcedure;

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn drop_table(&self, req: DropTableRequest) -> Result<Output> {
        let table_name = req.table_name.clone();
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &table_name,
        };

        let table = self.get_table(&table_ref).await?;
        let engine_procedure = self.engine_procedure(table)?;

        let procedure =
            DropTableProcedure::new(req, self.catalog_manager.clone(), engine_procedure);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;

        info!("Drop table {} by procedure {}", table_name, procedure_id);

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu { procedure_id })?;

        watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu { procedure_id })?;

        Ok(Output::AffectedRows(1))
    }
}

#[cfg(test)]
mod tests {
    use query::parser::{QueryLanguageParser, QueryStatement};
    use query::query_engine::SqlStatementExecutor;
    use session::context::QueryContext;

    use super::*;
    use crate::tests::test_util::MockInstance;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_table_by_procedure() {
        let instance = MockInstance::new("drop_table_by_procedure").await;

        // Create table first.
        let sql = r#"create table test_drop(
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

        // Drop table.
        let sql = r#"drop table test_drop"#;
        let stmt = match QueryLanguageParser::parse_sql(sql).unwrap() {
            QueryStatement::Sql(sql) => sql,
            _ => unreachable!(),
        };
        let output = instance
            .inner()
            .execute_sql(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));
    }
}
